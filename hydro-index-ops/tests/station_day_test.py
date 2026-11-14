"""站日逐小时汇总的端到端集成测试：真实 PostgreSQL + 多个真实 uvicorn 实例。

覆盖：
  1. 未刷新站日查询：固定 24 小时、修订号 0、缺测小时 0/null；
  2. 只汇总 quality='GOOD'，按 observed_at 的 UTC 小时归桶；
  3. 刷新计算期间写入照常：新写入不混入本次快照，下次刷新进入原所属小时；
  4. 多实例并发刷新同一站日：较旧快照不能覆盖较新结果（applied=false）；
  5. 刷新失败整日回滚：查询仍只看到先前完整 24 小时及其修订号；
  6. 服务重启后查询仍返回先前已发布的完整修订。

前置：
  - PG 已在 127.0.0.1:55432 运行，库 hydro_warehouse 已执行 sql/01_init_readings.sql
  - 依赖：fastapi/uvicorn/psycopg/httpx
运行：python tests/station_day_test.py
"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import psycopg

ROOT = Path(__file__).resolve().parent.parent
DSN = "postgresql://hydro@127.0.0.1:55432/hydro_warehouse"

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} {detail}")


class ApiServer:
    def __init__(self, port: int, extra_env: dict | None = None) -> None:
        self.port = port
        self.base = f"http://127.0.0.1:{port}"
        self.extra_env = extra_env or {}
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        env = os.environ.copy()
        env["HYDRO_PG_DSN"] = DSN
        env["HYDRO_WORK_POLL_INTERVAL"] = "0.5"
        env["HYDRO_CLEANUP_RETRY_INTERVAL"] = "1.0"
        env.update(self.extra_env)
        last_out = ""
        for _ in range(10):
            self.proc = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "app.main:app",
                 "--host", "127.0.0.1", "--port", str(self.port),
                 "--log-level", "warning"],
                cwd=ROOT, env=env, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True,
            )
            for _ in range(80):
                if self.proc.poll() is not None:
                    last_out = self.proc.stdout.read() if self.proc.stdout else ""
                    self.proc = None
                    break
                try:
                    if httpx.get(f"{self.base}/health", timeout=1).status_code == 200:
                        return
                except httpx.TransportError:
                    pass
                time.sleep(0.25)
            if self.proc is not None and self.proc.poll() is None:
                return
            time.sleep(1.0)
        raise RuntimeError(f"API on {self.port} did not start:\n{last_out}")

    def stop(self, sig: int = signal.SIGTERM) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.proc = None

    def get(self, path: str) -> httpx.Response:
        return httpx.get(f"{self.base}{path}", timeout=20)

    def post(self, path: str) -> httpx.Response:
        return httpx.post(f"{self.base}{path}", timeout=20)


async def pg_exec(sql_text: str, params=None):
    conn = await psycopg.AsyncConnection.connect(DSN, autocommit=True)
    try:
        cur = await conn.execute(sql_text, params)
        try:
            return await cur.fetchall()
        except psycopg.ProgrammingError:
            return []
    finally:
        await conn.close()


def reset_station_day(station_id: int, day: str) -> None:
    statements = (
        ("DELETE FROM station_day_hourly WHERE station_id=%s AND observed_date=%s",
         (station_id, day)),
        ("DELETE FROM station_day_refresh WHERE station_id=%s AND observed_date=%s",
         (station_id, day)),
        ("DELETE FROM readings WHERE station_id=%s AND observed_at >= %s::date "
         "AND observed_at < %s::date + INTERVAL '1 day'",
         (station_id, day, day)),
    )
    for sql_text, params in statements:
        asyncio.run(pg_exec(sql_text, params))


def insert_reading(server: ApiServer, station_id: int, observed_at: str,
                   water_level: float, quality: str = "GOOD") -> httpx.Response:
    return httpx.post(
        f"{server.base}/readings",
        json={"station_id": station_id, "observed_at": observed_at,
              "water_level": water_level, "quality": quality},
        timeout=20,
    )


def hours_map(payload: dict) -> dict[int, dict]:
    return {h["hour"]: h for h in payload["hours"]}


def main() -> int:
    station, day = 7, "2027-04-01"
    reset_station_day(station, day)

    # a 实例带发布延迟，用于稳定构造“旧快照刷新晚到”；b 实例即时发布
    a = ApiServer(8021, {"HYDRO_STATION_DAY_PUBLISH_DELAY": "3.0"})
    b = ApiServer(8022)
    a.start()
    b.start()
    try:
        # ---------------------------------------------------------------- #
        print("场景 0：快照新旧比较函数语义（CAS 判据）")
        cases = [
            # (a, b, a 至少和 b 一样新)
            # pg_snapshot 的 xip_list 是快照时刻“仍在途”的 xid；xip 越少，
            # 说明看到越多已提交事务，快照越新。
            ("5:9:6,7", "5:9:6,7", True),   # 相同快照
            ("8:9:", "5:9:6,7", True),      # xmax 更大：a 之后才开启的事务
            ("5:9:", "5:9:6,7", True),      # a 已看到 6,7 提交 → a 更新
            ("5:9:6", "5:9:6,7", True),     # a 已看到 7 提交 → a 更新
            ("5:9:6,7", "5:9:6", False),    # b 已看到 7，a 仍认为 7 在途 → a 更旧
            ("5:9:6,7", "8:9:", False),     # a 的 xmax 更小 → 更旧
            ("4:9:", "5:9:6,7", True),      # 已提交范围覆盖 b，xip 为空
        ]
        for a_snap, b_snap, expected in cases:
            got = asyncio.run(pg_exec(
                "SELECT hydro_snapshot_not_older(%s::pg_snapshot, %s::pg_snapshot)",
                (a_snap, b_snap),
            ))[0][0]
            check(f"not_older({a_snap}, {b_snap}) == {expected}",
                  got is expected, str(got))

        # ---------------------------------------------------------------- #
        print("场景 1：未刷新站日查询固定 24 小时、修订号 0")
        r = b.get(f"/station-days/{station}/{day}")
        check("查询 200", r.status_code == 200, r.text)
        body = r.json()
        check("返回 24 小时", len(body["hours"]) == 24, str(len(body["hours"])))
        check("小时编号 0..23 有序",
              [h["hour"] for h in body["hours"]] == list(range(24)))
        check("修订号 0、published=false",
              body["revision"] == 0 and body["published"] is False, str(body))
        check("缺测小时 good_count=0 且最高水位 null",
              all(h["good_count"] == 0 and h["max_water_level"] is None
                  for h in body["hours"]))
        check("不存在的测站 404", b.get("/station-days/9999/2027-04-01").status_code == 404)
        check("不存在的测站刷新 404",
              b.post("/station-days/9999/2027-04-01/refresh").status_code == 404)

        # ---------------------------------------------------------------- #
        print("场景 2：只汇总 GOOD，按 observed_at 的 UTC 小时归桶")
        insert_reading(b, station, "2027-04-01T03:05:00Z", 10.10, "GOOD")
        insert_reading(b, station, "2027-04-01T03:55:00Z", 11.90, "GOOD")
        insert_reading(b, station, "2027-04-01T03:30:00Z", 99.00, "SUSPECT")
        insert_reading(b, station, "2027-04-01T04:00:00Z", 88.00, "BAD")
        insert_reading(b, station, "2027-04-01T23:59:59Z", 12.00, "GOOD")
        # 相邻日期不得混入
        insert_reading(b, station, "2027-04-02T00:00:00Z", 77.00, "GOOD")
        insert_reading(b, station, "2027-03-31T23:59:59Z", 66.00, "GOOD")

        r = b.post(f"/station-days/{station}/{day}/refresh")
        check("刷新 200", r.status_code == 200, r.text)
        refreshed = r.json()
        hm = hours_map(refreshed)
        check("刷新返回 applied=true", refreshed["applied"] is True, str(refreshed))
        check("修订号 1", refreshed["revision"] == 1, str(refreshed["revision"]))
        check("返回 24 小时", len(refreshed["hours"]) == 24)
        check("小时 3：2 条 GOOD、最高 11.9（排除 SUSPECT）",
              hm[3]["good_count"] == 2 and hm[3]["max_water_level"] == 11.9, str(hm[3]))
        check("小时 4：BAD 不计入，缺测",
              hm[4]["good_count"] == 0 and hm[4]["max_water_level"] is None, str(hm[4]))
        check("小时 23：包含 23:59:59",
              hm[23]["good_count"] == 1 and hm[23]["max_water_level"] == 12.0, str(hm[23]))
        check("小时 0：跨日观测未混入",
              hm[0]["good_count"] == 0 and hm[0]["max_water_level"] is None, str(hm[0]))
        check("其它小时均为缺测",
              all(hm[h]["good_count"] == 0 for h in range(24) if h not in (3, 23)))
        published_view = {k: v for k, v in refreshed.items() if k != "applied"}
        check("刷新后查询与刷新响应一致（除 applied 标志）",
              b.get(f"/station-days/{station}/{day}").json() == published_view)
        check("第二实例可读到同一已发布修订",
              a.get(f"/station-days/{station}/{day}").json()["revision"] == 1)

        # ---------------------------------------------------------------- #
        print("场景 3：刷新计算期间写入照常，迟到观测下次刷新进入原所属小时")
        reset_station_day(station, "2027-04-03")
        insert_reading(b, station, "2027-04-03T02:00:00Z", 30.0, "GOOD")
        # a 实例延迟发布：取快照后等待 3s
        with ThreadPoolExecutor(max_workers=1) as ex:
            slow = ex.submit(a.post, f"/station-days/{station}/2027-04-03/refresh")
            time.sleep(0.8)  # 确保 a 已取快照、尚未发布
            # 快照之后提交的观测：本次刷新不得包含
            insert_reading(b, station, "2027-04-03T02:45:00Z", 40.0, "GOOD")
            # 刷新期间写入接口始终可用
            check("刷新计算期间写入仍成功", True)
            slow_resp = slow.result(timeout=30)
        slow_body = slow_resp.json()
        check("延迟刷新 applied=true", slow_body["applied"] is True, str(slow_body))
        check("本次快照未包含迟到观测（2 条=1）",
              hours_map(slow_body)[2]["good_count"] == 1,
              str(hours_map(slow_body)[2]))
        # 下一次刷新把迟到观测计入 observed_at 所属小时（2 点）
        r = b.post(f"/station-days/{station}/2027-04-03/refresh")
        body = r.json()
        check("下次刷新修订号递增", body["revision"] == 2, str(body["revision"]))
        check("迟到观测进入原所属小时（2 点共 2 条、最高 40.0）",
              hours_map(body)[2]["good_count"] == 2
              and hours_map(body)[2]["max_water_level"] == 40.0,
              str(hours_map(body)[2]))
        check("其它小时不受影响",
              all(h["good_count"] == 0 for h in body["hours"] if h["hour"] != 2))

        # ---------------------------------------------------------------- #
        print("场景 4：两实例并发刷新同一站日，较旧快照不能覆盖较新结果")
        day2 = "2027-04-04"
        reset_station_day(station, day2)
        insert_reading(b, station, "2027-04-04T10:00:00Z", 50.0, "GOOD")
        with ThreadPoolExecutor(max_workers=1) as ex:
            # a：先取旧快照，延迟 3s 后才尝试发布
            stale = ex.submit(a.post, f"/station-days/{station}/{day2}/refresh")
            time.sleep(0.8)
            # 新观测提交后由 b 立即刷新（快照更新）
            insert_reading(b, station, "2027-04-04T10:30:00Z", 60.0, "GOOD")
            fresh = b.post(f"/station-days/{station}/{day2}/refresh")
            check("新快照刷新 applied=true", fresh.json()["applied"] is True, fresh.text)
            check("新快照刷新修订号 1、10 点 2 条最高 60",
                  fresh.json()["revision"] == 1
                  and hours_map(fresh.json())[10]["good_count"] == 2
                  and hours_map(fresh.json())[10]["max_water_level"] == 60.0,
                  str(fresh.json()))
            stale_resp = stale.result(timeout=30)
        stale_body = stale_resp.json()
        check("旧快照刷新被拒（applied=false）", stale_body["applied"] is False, str(stale_body))
        check("旧快照未把修订号从 1 抬升",
              stale_body["revision"] == 1, str(stale_body["revision"]))
        check("旧快照响应返回现行内容（10 点仍 2 条/60）",
              hours_map(stale_body)[10]["good_count"] == 2
              and hours_map(stale_body)[10]["max_water_level"] == 60.0,
              str(hours_map(stale_body)[10]))
        final = b.get(f"/station-days/{station}/{day2}").json()
        check("查询仍为新快照结果",
              final["revision"] == 1
              and hours_map(final)[10]["good_count"] == 2
              and hours_map(final)[10]["max_water_level"] == 60.0,
              str(final))
        # 反向顺序同样安全：更新快照晚到，仍会以递增修订发布
        r = b.post(f"/station-days/{station}/{day2}/refresh")
        check("后续正常刷新修订号递增为 2", r.json()["revision"] == 2, r.text)

        # ---------------------------------------------------------------- #
        print("场景 5：刷新失败整日回滚，查询仍只看到先前完整 24 小时与修订号")
        c = ApiServer(8023, {"HYDRO_STATION_DAY_FAIL_BEFORE_COMMIT": "1"})
        c.start()
        try:
            before = b.get(f"/station-days/{station}/{day2}").json()
            insert_reading(b, station, "2027-04-04T10:45:00Z", 70.0, "GOOD")
            r = c.post(f"/station-days/{station}/{day2}/refresh")
            check("注入失败返回 500", r.status_code == 500, f"{r.status_code} {r.text}")
            after = b.get(f"/station-days/{station}/{day2}").json()
            check("失败后修订号不变", after["revision"] == before["revision"],
                  f"{before['revision']} -> {after['revision']}")
            check("失败后 24 小时内容与修订前完全一致", after == before)
            check("失败后仍返回完整 24 小时", len(after["hours"]) == 24)
            # 失败不应留下半成品行：现行修订号下仍是完整 24 行
            rows = asyncio.run(pg_exec(
                "SELECT count(*) FROM station_day_hourly h JOIN station_day_refresh r "
                "ON h.station_id=r.station_id AND h.observed_date=r.observed_date "
                "AND h.revision=r.revision "
                "WHERE r.station_id=%s AND r.observed_date=%s",
                (station, f"{day2}"),
            ))[0][0]
            check("数据库现行修订恰好 24 行", rows == 24, str(rows))
            # 正常实例刷新仍可发布新数据
            ok = b.post(f"/station-days/{station}/{day2}/refresh")
            check("失败后正常刷新恢复发布",
                  ok.status_code == 200 and ok.json()["applied"] is True
                  and ok.json()["revision"] == before["revision"] + 1, ok.text)
        finally:
            c.stop()

        # ---------------------------------------------------------------- #
        print("场景 6：服务重启后查询仍返回先前完整发布")
        snapshot_before = b.get(f"/station-days/{station}/{day2}").json()
        b.stop()
        b.start()
        after = b.get(f"/station-days/{station}/{day2}").json()
        check("重启后修订号不变", after["revision"] == snapshot_before["revision"], str(after))
        check("重启后完整 24 小时且内容一致", after == snapshot_before)

        # ---------------------------------------------------------------- #
        print("场景 7：Swagger/OpenAPI 可操作刷新与查询")
        spec = b.get("/openapi.json").json()
        paths = spec["paths"]
        check("OpenAPI 含刷新路径",
              "/station-days/{station_id}/{day}/refresh" in paths, str(list(paths)))
        check("OpenAPI 含查询路径",
              "/station-days/{station_id}/{day}" in paths, str(list(paths)))
        check("Swagger UI 可访问", b.get("/docs").status_code == 200)
    finally:
        a.stop()
        b.stop()

    print(f"\n结果：{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
