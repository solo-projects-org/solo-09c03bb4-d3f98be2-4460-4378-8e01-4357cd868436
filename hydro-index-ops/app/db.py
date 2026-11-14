"""psycopg3 异步连接池与表 DDL（索引批次 + 站日逐小时汇总）。"""
from __future__ import annotations

import logging

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .config import settings

log = logging.getLogger("hydro.db")

pool: AsyncConnectionPool | None = None

# 批次状态机：
#   PENDING    已受理，等待构建（含 CLEANING 清理失败后的待重试）
#   BUILDING   CREATE INDEX CONCURRENTLY 会话进行中
#   CANCELLING 已请求取消，等待构建会话结束并清理残件
#   CLEANING   构建失败/取消后正在 DROP 残件
#   SUCCEEDED  构建成功且索引有效（终态）
#   FAILED     构建失败且残件已清净（终态）
#   CANCELLED  取消完成且残件已清净（终态）
DDL_BATCH_TABLE = """
CREATE TABLE IF NOT EXISTS index_build_batches (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state            TEXT NOT NULL,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    build_pid        INTEGER,
    phase            TEXT,
    error            TEXT,
    cleanup_attempts INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_batch_state CHECK (
        state IN ('PENDING', 'BUILDING', 'CANCELLING', 'CLEANING',
                  'SUCCEEDED', 'FAILED', 'CANCELLED')
    )
);

-- 同一时刻只允许一个非终态批次（从数据库层面兜底“只受理一批”）
CREATE UNIQUE INDEX IF NOT EXISTS uq_batch_one_active
    ON index_build_batches ((1))
    WHERE state IN ('PENDING', 'BUILDING', 'CANCELLING', 'CLEANING');
"""

# 快照新旧比较：a 至少和 b 一样新 ⇔ a 的可见范围覆盖 b 看到的所有已提交事务。
# 用于并发刷新同一站日时的发布仲裁：旧快照绝不能覆盖新结果。
DDL_SNAPSHOT_COMPARE_FUNCTION = """
CREATE OR REPLACE FUNCTION hydro_snapshot_not_older(a pg_snapshot, b pg_snapshot)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT pg_snapshot_xmax(a) >= pg_snapshot_xmax(b)
       AND NOT EXISTS (
           SELECT 1
           FROM pg_snapshot_xip(a) AS x
           WHERE pg_visible_in_snapshot(x, b)
       )
$$
"""

# 站日汇总的已发布修订：每个 (station_id, observed_date) 一行，记录修订号与快照。
DDL_STATION_DAY_REFRESH = """
CREATE TABLE IF NOT EXISTS station_day_refresh (
    station_id    BIGINT NOT NULL REFERENCES stations (station_id),
    observed_date DATE NOT NULL,
    revision      BIGINT NOT NULL CHECK (revision > 0),
    snapshot      pg_snapshot NOT NULL,
    refreshed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_station_day_refresh PRIMARY KEY (station_id, observed_date)
);

COMMENT ON TABLE station_day_refresh IS
    '站日汇总的已发布修订（整日原子发布，快照用于并发刷新仲裁）';
"""

# 逐小时汇总：hours 0..23（UTC 桶），外键指向修订行，禁止孤立小时行。
DDL_STATION_DAY_HOURLY = """
CREATE TABLE IF NOT EXISTS station_day_hourly (
    station_id       BIGINT NOT NULL,
    observed_date    DATE NOT NULL,
    hour             SMALLINT NOT NULL CHECK (hour BETWEEN 0 AND 23),
    good_count       BIGINT NOT NULL CHECK (good_count >= 0),
    max_water_level  NUMERIC(8, 3),
    revision         BIGINT NOT NULL,
    CONSTRAINT pk_station_day_hourly
        PRIMARY KEY (station_id, observed_date, hour),
    CONSTRAINT fk_station_day_hourly_refresh
        FOREIGN KEY (station_id, observed_date)
        REFERENCES station_day_refresh (station_id, observed_date)
        ON DELETE CASCADE
);

COMMENT ON TABLE station_day_hourly IS
    '站日逐小时汇总，按 UTC 小时桶；查询只取现行修订号对应的 24 行';
"""

DDL_STATEMENTS = (
    DDL_BATCH_TABLE,
    DDL_SNAPSHOT_COMPARE_FUNCTION,
    DDL_STATION_DAY_REFRESH,
    DDL_STATION_DAY_HOURLY,
)


async def init_pool() -> AsyncConnectionPool:
    global pool
    pool = AsyncConnectionPool(
        conninfo=settings.pg_dsn,
        min_size=2,
        max_size=10,
        kwargs={"row_factory": dict_row, "autocommit": False},
        check=AsyncConnectionPool.check_connection,
        open=False,
    )
    await pool.open()
    async with pool.connection() as conn:
        for statement in DDL_STATEMENTS:
            await conn.execute(statement)
        await conn.commit()
    log.info("connection pool ready, batch and station-day tables ensured")
    return pool


async def close_pool() -> None:
    if pool is not None:
        await pool.close()
        log.info("connection pool closed")
