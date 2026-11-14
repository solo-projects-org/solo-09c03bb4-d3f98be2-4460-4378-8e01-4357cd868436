"""测站 × UTC 日期的逐小时汇总：一致快照计算、整日原子发布、快照新旧仲裁。

设计要点
--------
1. **一致快照**：刷新在 `REPEATABLE READ READ ONLY` 事务里取
   `pg_current_snapshot()`，并在同一事务快照下聚合 `readings`。计算期间
   新提交的观测不会混入本次结果；它们由下一次刷新按 `observed_at`
   进入其所属小时（迟到观测也按 observed_at 归桶，不看 ingested_at）。
2. **整日发布**：24 个小时行与修订行在同一事务内写入，查询按“现行
   修订号”取小时行。因此查询要么看到上一修订的完整 24 小时，要么看到
   新修订的完整 24 小时，不会看到半个日期的中间态。刷新失败或进程被
   杀时事务回滚，先前修订与查询结果保持不变。
3. **并发刷新**：`station_day_refresh` 的 UPSERT 带
   `WHERE hydro_snapshot_not_older(EXCLUDED.snapshot, 现行 snapshot)` 条件，
   即一次 CAS。两个实例并发刷新同一站日时，快照较旧的刷新更新不到行
   （返回空），不会覆盖较新结果、也不会抬升修订号。
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, time, timedelta, timezone

import psycopg
from psycopg_pool import AsyncConnectionPool

from .config import settings

HOURS = 24

_AGGREGATE_SQL = """
SELECT EXTRACT(HOUR FROM observed_at AT TIME ZONE 'UTC')::int AS hour,
       count(*)::bigint AS good_count,
       max(water_level) AS max_water_level
FROM readings
WHERE station_id = %(station_id)s
  AND observed_at >= %(start)s
  AND observed_at <  %(end)s
  AND quality = 'GOOD'
GROUP BY 1
ORDER BY 1
"""

# 修订行 CAS：只有新快照不旧于已发布快照时才更新；返回空表示落败，
# 本次结果不得发布。
_UPSERT_REVISION_SQL = """
INSERT INTO station_day_refresh
    (station_id, observed_date, revision, snapshot, refreshed_at)
VALUES (%(station_id)s, %(observed_date)s, 1, %(snapshot)s::pg_snapshot, now())
ON CONFLICT (station_id, observed_date) DO UPDATE
SET revision = station_day_refresh.revision + 1,
    snapshot = EXCLUDED.snapshot,
    refreshed_at = now()
WHERE hydro_snapshot_not_older(EXCLUDED.snapshot, station_day_refresh.snapshot)
RETURNING revision
"""

_UPSERT_HOUR_SQL = """
INSERT INTO station_day_hourly
    (station_id, observed_date, hour, good_count, max_water_level, revision)
VALUES (%(station_id)s, %(observed_date)s, %(hour)s, %(good_count)s,
        %(max_water_level)s, %(revision)s)
ON CONFLICT (station_id, observed_date, hour) DO UPDATE
SET good_count = EXCLUDED.good_count,
    max_water_level = EXCLUDED.max_water_level,
    revision = EXCLUDED.revision
"""

# 只取“现行修订号”对应的小时行：整日发布保证它们恰好是完整 24 行。
_SELECT_SUMMARY_SQL = """
SELECT r.revision,
       r.snapshot::text AS snapshot,
       r.refreshed_at,
       h.hour,
       h.good_count,
       h.max_water_level
FROM station_day_refresh AS r
LEFT JOIN station_day_hourly AS h
       ON h.station_id = r.station_id
      AND h.observed_date = r.observed_date
      AND h.revision = r.revision
WHERE r.station_id = %(station_id)s
  AND r.observed_date = %(observed_date)s
ORDER BY h.hour
"""


class StationNotFound(LookupError):
    pass


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _build_summary(row: dict | None, rows: list[dict], day: date,
                   station_id: int) -> dict:
    """把查询行整理成固定 24 小时的响应；从未刷新过时修订号为 0。"""
    by_hour: dict[int, dict] = {}
    for r in rows:
        if r["hour"] is None:
            continue
        by_hour[int(r["hour"])] = {
            "hour": int(r["hour"]),
            "good_count": int(r["good_count"] or 0),
            "max_water_level": (
                float(r["max_water_level"])
                if r["max_water_level"] is not None else None
            ),
        }
    revision = int(row["revision"]) if row else 0
    return {
        "station_id": station_id,
        "date": day,
        "timezone": "UTC",
        "revision": revision,
        "published": revision > 0,
        "refreshed_at": row["refreshed_at"] if row else None,
        "snapshot": row["snapshot"] if row else None,
        "hours": [
            by_hour.get(h, {"hour": h, "good_count": 0,
                            "max_water_level": None})
            for h in range(HOURS)
        ],
    }


async def _select_summary(conn: psycopg.AsyncConnection, station_id: int,
                          day: date) -> dict:
    cur = await conn.execute(
        _SELECT_SUMMARY_SQL,
        {"station_id": station_id, "observed_date": day},
    )
    rows = list(await cur.fetchall())
    row = rows[0] if rows else None
    return _build_summary(row, rows, day, station_id)


async def _station_exists(conn: psycopg.AsyncConnection, station_id: int) -> bool:
    cur = await conn.execute(
        "SELECT 1 FROM stations WHERE station_id = %s", (station_id,)
    )
    return await cur.fetchone() is not None


async def get_summary(pool: AsyncConnectionPool, station_id: int,
                      day: date) -> dict:
    """读取最近一次已发布的完整 24 小时；从未刷新过则返回修订号 0 的空日。"""
    async with pool.connection() as conn:
        if not await _station_exists(conn, station_id):
            raise StationNotFound(station_id)
        result = await _select_summary(conn, station_id, day)
        await conn.commit()
    return result


async def _compute_snapshot(pool: AsyncConnectionPool, station_id: int,
                            day: date) -> tuple[str, list[dict]]:
    """在只读 REPEATABLE READ 事务里取快照并聚合，返回快照文本与 24 小时。"""
    start, end = _day_bounds(day)
    async with pool.connection() as conn:
        # 同一事务里“取快照 + 读数据”：得到一致的已提交状态
        await conn.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        )
        try:
            cur = await conn.execute(
                "SELECT pg_current_snapshot()::text AS snapshot"
            )
            snapshot = (await cur.fetchone())["snapshot"]
            cur = await conn.execute(
                _AGGREGATE_SQL,
                {"station_id": station_id, "start": start, "end": end},
            )
            agg = list(await cur.fetchall())
        finally:
            await conn.rollback()

    by_hour = {int(r["hour"]): r for r in agg}
    hours = []
    for h in range(HOURS):
        r = by_hour.get(h)
        hours.append({
            "hour": h,
            "good_count": int(r["good_count"]) if r else 0,
            "max_water_level": (
                float(r["max_water_level"])
                if r and r["max_water_level"] is not None else None
            ),
        })
    return snapshot, hours


async def refresh_day(pool: AsyncConnectionPool, station_id: int, day: date,
                      *, publish_delay: float = 0.0) -> dict:
    """刷新一个站日并整日原子发布，返回最新已发布摘要（含 applied 标志）。

    `publish_delay` 仅用于测试/演示：在取得快照之后、发布之前等待，
    用于稳定复现“旧快照刷新晚到”的并发场景；生产环境保持 0。
    """
    async with pool.connection() as conn:
        exists = await _station_exists(conn, station_id)
        await conn.commit()
    if not exists:
        raise StationNotFound(station_id)

    snapshot, hours = await _compute_snapshot(pool, station_id, day)
    if publish_delay > 0:
        await asyncio.sleep(publish_delay)

    async with pool.connection() as conn:
        try:
            cur = await conn.execute(
                _UPSERT_REVISION_SQL,
                {"station_id": station_id, "observed_date": day,
                 "snapshot": snapshot},
            )
            rec = await cur.fetchone()
            if rec is None:
                # 现行修订来自更新的快照：本次落败，不覆盖、不抬修订号，
                # 回读现行已发布结果返回（查询侧看到的内容保持不变）
                result = await _select_summary(conn, station_id, day)
                await conn.commit()
                result["applied"] = False
                return result

            revision = int(rec["revision"])
            async with conn.cursor() as cur:
                await cur.executemany(
                    _UPSERT_HOUR_SQL,
                    [{"station_id": station_id, "observed_date": day,
                      "hour": h["hour"], "good_count": h["good_count"],
                      "max_water_level": h["max_water_level"],
                      "revision": revision} for h in hours],
                )
            if settings.station_day_fail_before_commit:
                # 演示/测试注入：整日发布事务整体回滚，旧修订保持不变
                raise RuntimeError(
                    "injected refresh failure before commit "
                    "(HYDRO_STATION_DAY_FAIL_BEFORE_COMMIT)"
                )
            # 同一事务内回读：响应内容与提交内容严格一致
            result = await _select_summary(conn, station_id, day)
            await conn.commit()
        except BaseException:
            await conn.rollback()
            raise

    result["applied"] = True
    return result
