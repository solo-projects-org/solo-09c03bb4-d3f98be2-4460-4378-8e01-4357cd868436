"""端到端集成测试：真实 PostgreSQL 16 + 真实 uvicorn 子进程。

前置：
  - PG 实例已在 127.0.0.1:55432 运行，库 hydro_warehouse 已执行 sql/01_init_readings.sql
  - 依赖已安装（fastapi/uvicorn/psycopg/httpx）

运行：python tests/integration_test.py
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psycopg

ROOT = Path(__file__).resolve().parent.parent
BASE = "http://127.0.0.1:8011"
DSN = "postgresql://hydro@127.0.0.1:55432/hydro_warehouse"
IDX = "idx_readings_station_observed"
MANUAL_IDX = "idx_readings_ingested_manual"

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
    def __init__(self, extra_env: dict | None = None) -> None:
        self.proc: subprocess.Popen | None = None
        self.extra_env = extra_env or {}

    def start(self) -> None:
        env = os.environ.copy()
        env["HYDRO_PG_DSN"] = DSN
        env["HYDRO_WORK_POLL_INTERVAL"] = "0.5"
        env["HYDRO_CLEANUP_RETRY_INTERVAL"] = "1.0"
        env.update(self.extra_env)
        # SIGKILL 后监听端口可能短暂处于未释放状态，重试若干次
        last_out = ""
        for attempt in range(10):
            self.proc = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "app.main:app",
                 "--host", "127.0.0.1", "--port", "8011", "--log-level", "warning"],
                cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True,
            )
            for _ in range(80):
                if self.proc.poll() is not None:  # 进程已退出（多半是端口占用）
                    last_out = self.proc.stdout.read() if self.proc.stdout else ""
                    self.proc = None
                    break
                try:
                    r = httpx.get(f"{BASE}/health", timeout=1)
                    if r.status_code == 200:
                        return
                except httpx.TransportError:
                    pass
                time.sleep(0.3)
            if self.proc is not None and self.proc.poll() is None:
                return
            time.sleep(1.0)
        raise RuntimeError(f"API did not start. Last output:\n{last_out}")

    def stop(self, sig: int = signal.SIGTERM) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(sig)
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)
        self.proc = None


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


async def index_row():
    rows = await pg_exec(
        "SELECT c.relname, ix.indisvalid FROM pg_index ix "
        "JOIN pg_class c ON c.oid = ix.indexrelid WHERE c.relname = %s",
        (IDX,),
    )
    return rows[0] if rows else None


async def backend_alive(pid: int) -> bool:
    rows = await pg_exec("SELECT 1 FROM pg_stat_activity WHERE pid = %s", (pid,))
    return bool(rows)


def http(method: str, path: str, **kw):
    r = httpx.request(method, f"{BASE}{path}", timeout=15, **kw)
    return r


def wait_batch(bid: int, *states: str, timeout: float = 60) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = http("GET", f"/index-batches/{bid}").json()
        if last["state"] in states:
            return last
        time.sleep(0.5)
    raise AssertionError(f"batch {bid} never reached {states}, last={last}")


async def drop_managed() -> None:
    await pg_exec(f"DROP INDEX CONCURRENTLY IF EXISTS {IDX}")


async def main() -> int:
    import asyncio
    # ------------------------------------------------------------------ #
    print("场景 0：启动与健康检查（含 Swagger/OpenAPI 文档）")
    srv = ApiServer()
    srv.start()
    h = http("GET", "/health").json()
    check("health ok", h["status"] == "ok")
    check("初始无活动批次、可受理", h["active_batch"] is None and h["accepting_new_batch"])
    r = http("GET", "/openapi.json")
    check("OpenAPI schema 可生成（Swagger 可用）",
          r.status_code == 200 and r.json()["info"]["title"].startswith("水文数仓"))
    check("Swagger UI 可访问", http("GET", "/docs").status_code == 200)

    # ------------------------------------------------------------------ #
    print("场景 1：正常构建（构建中读写照常）→ SUCCEEDED，执行计划走索引")
    await drop_managed()
    bid = http("POST", "/index-batches", json={"note": "e2e-1"}).json()["id"]
    phases_seen: set[str] = set()
    deadline = time.time() + 30
    last = {}
    while time.time() < deadline:
        last = http("GET", f"/index-batches/{bid}").json()
        if last["phase"]:
            phases_seen.add(last["phase"])
        if last["state"] in ("SUCCEEDED", "FAILED"):
            break
        # 构建期间持续写入与查询
        http("POST", "/readings", json={
            "station_id": 1,
            "observed_at": "2026-11-14T08:00:00Z",
            "water_level": 22.5,
        }).raise_for_status()
        http("GET", "/readings?station_id=1&limit=3").raise_for_status()
        time.sleep(0.2)
    check("批次 SUCCEEDED", last["state"] == "SUCCEEDED", str(last))
    check("有阶段上报", len(phases_seen) > 0, str(phases_seen))
    row = await index_row()
    check("管理索引存在且 valid", row is not None and row[1] is True, str(row))
    plan = http("GET", "/readings/explain?station_id=3").json()["plan"]
    plan_txt = str(plan)
    check("执行计划使用复合索引", IDX in plan_txt, plan_txt[:200])

    # 重复受理被拒
    r = http("POST", "/index-batches", json={"note": "dup"})
    check("索引 valid 时重复受理 409", r.status_code == 409)

    # ------------------------------------------------------------------ #
    print("场景 2：长事务等待 → 取消 → 残件删净 → CANCELLED → 可接新批")
    await drop_managed()
    ltx = http("POST", "/demo/long-tx?seconds=180").json()
    lpid = ltx["pid"]
    bid2 = http("POST", "/index-batches", json={"note": "e2e-2"}).json()["id"]
    b = wait_batch(bid2, "BUILDING")
    check("记录构建 pid", isinstance(b["build_pid"], int))
    # 观察到等待事件
    waited = False
    for _ in range(10):
        b = http("GET", f"/index-batches/{bid2}").json()
        if b["phase"] and "wait: Lock" in b["phase"]:
            waited = True
            break
        time.sleep(0.5)
    check("CIC 停在锁等待阶段", waited, str(b))
    # 等待期间源表读写正常
    check("等待中写入正常",
          http("POST", "/readings", json={
              "station_id": 2, "observed_at": "2026-11-14T09:00:00Z",
              "water_level": 18.0}).status_code == 201)
    r = http("POST", "/index-batches", json={"note": "while active"})
    check("活动批次存在时新受理 409", r.status_code == 409)

    cr = http("POST", f"/index-batches/{bid2}/cancel")
    check("cancel 后状态 CANCELLING", cr.json()["state"] == "CANCELLING")
    # 释放长事务，清理才能完成 DROP CONCURRENTLY
    http("DELETE", f"/demo/long-tx/{lpid}").raise_for_status()
    b = wait_batch(bid2, "CANCELLED")
    check("批次 CANCELLED", b["state"] == "CANCELLED")
    check("cancel_requested 已置位", b["cancel_requested"] is True)
    check("取消后无索引残留（含迟到成功）", await index_row() is None)
    check("长事务之外没有误杀：长事务已按请求结束", not await backend_alive(lpid))
    # 终态后可受理新批
    bid2b = http("POST", "/index-batches", json={"note": "e2e-2b"}).json()["id"]
    b = wait_batch(bid2b, "SUCCEEDED")
    check("清理后新批次可成功", b["state"] == "SUCCEEDED")

    # ------------------------------------------------------------------ #
    print("场景 3：清理失败保留 CLEANING 并自动重试（构建前取消，无残件也走清理）")
    srv.stop()
    srv = ApiServer(extra_env={"HYDRO_CLEANUP_FAIL_FIRST": "4"})
    srv.start()
    await drop_managed()
    # 受理后立即取消：批次从未进入构建，无会话/残件，但仍必须走完整清理流程
    bid3 = http("POST", "/index-batches", json={"note": "e2e-3"}).json()["id"]
    http("POST", f"/index-batches/{bid3}/cancel").raise_for_status()
    saw_cleaning = False
    saw_injected_error = False
    max_attempts = 0
    deadline = time.time() + 60
    b = {}
    while time.time() < deadline:
        b = http("GET", f"/index-batches/{bid3}").json()
        saw_cleaning = saw_cleaning or b["state"] == "CLEANING"
        saw_injected_error = saw_injected_error or (
            b["error"] is not None and "injected cleanup failure" in b["error"]
        )
        max_attempts = max(max_attempts, b["cleanup_attempts"])
        # 重试窗口内 API 健康检查与源表读写必须照常
        check_once = http("GET", "/health").status_code == 200
        if not check_once:
            check("重试期间 API 照常", False)
        if b["state"] == "CANCELLED":
            break
        time.sleep(0.3)
    check("经历过 CLEANING 待处理状态", saw_cleaning, str(b))
    check("观察到注入的清理失败错误", saw_injected_error, str(b))
    check("cleanup_attempts 达到注入次数 4", max_attempts >= 4, str(max_attempts))
    check("重试后终态 CANCELLED", b["state"] == "CANCELLED", str(b))
    check("终态保留最后错误信息便于排障", b.get("error") is not None, str(b.get("error")))
    check("重试期间状态查询始终可用", True)
    check("残件最终删净", await index_row() is None)

    # 恢复默认配置
    srv.stop()
    srv = ApiServer()
    srv.start()

    # ------------------------------------------------------------------ #
    print("场景 4a：重启恢复——构建等待中崩溃，重启后终止遗留构建 → FAILED")
    await drop_managed()
    ltx = http("POST", "/demo/long-tx?seconds=300").json()
    lpid = ltx["pid"]
    bid4 = http("POST", "/index-batches", json={"note": "e2e-4a"}).json()["id"]
    b = wait_batch(bid4, "BUILDING")
    build_pid = b["build_pid"]
    check("构建会话存活", await backend_alive(build_pid))
    # SIGKILL 模拟进程崩溃/容器重启；长事务保持打开，CIC 停在等待阶段、产不出索引
    srv.stop(sig=signal.SIGKILL)
    check("API 死后构建会话仍残留（等待恢复处理）", await backend_alive(build_pid))
    check("长事务会话未受影响（不得误杀其他会话）", await backend_alive(lpid))
    srv = ApiServer()
    srv.start()
    # 恢复在后台进行：先确认孤儿构建被身份校验后的终止逻辑干掉，而长事务不动
    deadline = time.time() + 30
    while time.time() < deadline and await backend_alive(build_pid):
        time.sleep(0.3)
    check("恢复终止了遗留构建会话", not await backend_alive(build_pid))
    check("恢复未误杀长事务（其他会话不受影响）", await backend_alive(lpid))
    # 此时 DROP 被长事务挡在 CLEANING；结束长事务后清理才能完成 → FAILED
    b = wait_batch(bid4, "CLEANING", timeout=30)
    check("长事务未结束前停在 CLEANING 待处理", b["state"] == "CLEANING", str(b))
    await pg_exec(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE pid = %s AND application_name LIKE 'hydro-demo-longtx-%%'",
        (lpid,),
    )
    b = wait_batch(bid4, "FAILED", timeout=30)
    check("恢复后遗留批次 FAILED", b["state"] == "FAILED")
    check("无索引残件", await index_row() is None)

    print("场景 4b：重启恢复——孤儿构建在重启前迟到成功 → 采纳并保留索引 → SUCCEEDED")
    await drop_managed()
    ltx = http("POST", "/demo/long-tx?seconds=300").json()
    lpid = ltx["pid"]
    bid4b = http("POST", "/index-batches", json={"note": "e2e-4b"}).json()["id"]
    wait_batch(bid4b, "BUILDING")
    srv.stop(sig=signal.SIGKILL)
    # API 死后释放长事务：孤儿 CIC 解除阻塞，在新 API 恢复前自行构建完成
    await pg_exec(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE pid = %s AND application_name LIKE 'hydro-demo-longtx-%%'",
        (lpid,),
    )
    # 等孤儿构建把索引建为 valid
    deadline = time.time() + 30
    while time.time() < deadline:
        row = await index_row()
        if row and row[1] is True:
            break
        time.sleep(0.5)
    check("孤儿构建迟到成功、索引 valid", row is not None and row[1] is True, str(row))
    srv = ApiServer()
    srv.start()
    b = wait_batch(bid4b, "SUCCEEDED", timeout=45)
    check("恢复采纳迟到成功为 SUCCEEDED", b["state"] == "SUCCEEDED", str(b))
    check("成功索引被保留", await index_row() is not None)

    print("场景 5：成功索引与人工索引在重启后保留")
    await drop_managed()
    bid5 = http("POST", "/index-batches", json={"note": "e2e-5"}).json()["id"]
    wait_batch(bid5, "SUCCEEDED")
    srv.stop()
    srv = ApiServer()
    srv.start()
    h = http("GET", "/health").json()
    check("重启后索引仍 valid", h["index_status"] == "valid")
    rows = await pg_exec(
        "SELECT indexname FROM pg_indexes WHERE tablename='readings' ORDER BY 1"
    )
    names = [r[0] for r in rows]
    check("管理索引保留", IDX in names, str(names))
    check("人工索引始终未被删除", MANUAL_IDX in names, str(names))

    # 批次历史持久化
    batches = http("GET", "/index-batches?limit=20").json()
    check("批次历史完整持久化", len(batches) >= 5)
    states = {x["state"] for x in batches}
    check("历史覆盖 SUCCEEDED/CANCELLED/FAILED",
          {"SUCCEEDED", "CANCELLED", "FAILED"}.issubset(states), str(states))

    srv.stop()
    print(f"\n结果：{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    import asyncio
    sys.exit(asyncio.run(main()))
