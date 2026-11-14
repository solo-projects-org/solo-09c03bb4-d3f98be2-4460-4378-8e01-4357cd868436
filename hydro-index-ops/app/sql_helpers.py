"""对 PostgreSQL 的封装查询：索引状态、CIC 进度、会话终止、咨询锁。"""
from __future__ import annotations

import logging

import psycopg

from .config import settings

log = logging.getLogger("hydro.sql")


async def index_status(conn: psycopg.AsyncConnection) -> str | None:
    """返回本系统索引的状态。

    'valid'   有效索引（成功产物，必须保留）
    'invalid' 失败残件（CREATE INDEX CONCURRENTLY 失败后不可用，必须 DROP）
    None      不存在
    只按固定名称识别本系统索引，任何人工索引都不会被误判/误删。
    """
    row = await conn.execute(
        """
        SELECT ix.indisvalid
        FROM pg_index ix
        JOIN pg_class c ON c.oid = ix.indexrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relname = %s
        """,
        (settings.index_name,),
    )
    rec = await row.fetchone()
    if rec is None:
        return None
    return "valid" if rec["indisvalid"] else "invalid"


async def fetch_build_phase(conn: psycopg.AsyncConnection, pid: int | None) -> str | None:
    """读取 CIC 构建阶段；构建不在进度视图中时回退到会话等待事件描述。"""
    if pid is None:
        return None
    row = await conn.execute(
        """
        SELECT phase, wait_event_type, wait_event
        FROM pg_stat_progress_create_index p
        JOIN pg_stat_activity a USING (pid)
        WHERE pid = %s
        """,
        (pid,),
    )
    rec = await row.fetchone()
    if rec is not None:
        phase = rec["phase"]
        if rec["wait_event_type"]:
            return f"{phase} (wait: {rec['wait_event_type']}.{rec['wait_event'] or 'null'})"
        return phase
    # 尚未进入 create_index 进度上报阶段（如等待锁/旧快照的早期阶段）
    row = await conn.execute(
        """
        SELECT state, wait_event_type, wait_event, query
        FROM pg_stat_activity WHERE pid = %s
        """,
        (pid,),
    )
    rec = await row.fetchone()
    if rec is None:
        return None
    if rec["wait_event_type"]:
        return f"initializing (wait: {rec['wait_event_type']}.{rec['wait_event'] or 'null'})"
    return f"initializing ({rec['state']})"


async def session_is_alive(conn: psycopg.AsyncConnection, pid: int | None) -> bool:
    if pid is None:
        return False
    row = await conn.execute("SELECT count(*) AS n FROM pg_stat_activity WHERE pid = %s", (pid,))
    rec = await row.fetchone()
    return bool(rec and rec["n"] > 0)


async def terminate_our_backend(conn: psycopg.AsyncConnection, pid: int | None) -> bool:
    """只终止按 pid + application_name 双重确认的本系统构建会话，绝不误伤其他会话。"""
    if pid is None:
        return True
    row = await conn.execute(
        """
        SELECT pg_terminate_backend(pid) AS ok
        FROM pg_stat_activity
        WHERE pid = %s AND application_name LIKE %s
        """,
        (pid, settings.app_name_prefix + "-%"),
    )
    rec = await row.fetchone()
    ok = bool(rec and rec["ok"])
    log.warning("terminate build backend pid=%s result=%s", pid, ok)
    return ok


# ---------------------------------------------------------------------------
# 批次表 CRUD（状态全部持久化，重启可恢复）
# ---------------------------------------------------------------------------

async def insert_batch(conn: psycopg.AsyncConnection, note: str | None) -> int:
    row = await conn.execute(
        "INSERT INTO index_build_batches (state, phase, error) VALUES ('PENDING', %s, %s) "
        "RETURNING id",
        ("queued", note),
    )
    rec = await row.fetchone()
    return rec["id"]


async def get_active_batch(conn: psycopg.AsyncConnection) -> dict | None:
    row = await conn.execute(
        "SELECT * FROM index_build_batches "
        "WHERE state IN ('PENDING','BUILDING','CANCELLING','CLEANING') "
        "ORDER BY id LIMIT 1"
    )
    return await row.fetchone()


async def get_batch(conn: psycopg.AsyncConnection, batch_id: int) -> dict | None:
    row = await conn.execute("SELECT * FROM index_build_batches WHERE id = %s", (batch_id,))
    return await row.fetchone()


async def list_batches(conn: psycopg.AsyncConnection, limit: int = 50) -> list[dict]:
    row = await conn.execute(
        "SELECT * FROM index_build_batches ORDER BY id DESC LIMIT %s", (limit,)
    )
    return list(await row.fetchall())


async def update_state(
    conn: psycopg.AsyncConnection,
    batch_id: int,
    *,
    state: str | None = None,
    phase: str | None = None,
    error: str | None = None,
    build_pid: int | None = None,
    cancel_requested: bool | None = None,
    bump_cleanup: bool = False,
    clear_pid: bool = False,
) -> None:
    """按给定字段做增量更新；None 表示该字段不动。"""
    sets: list[str] = ["updated_at = now()"]
    params: list = []
    if state is not None:
        sets.append("state = %s")
        params.append(state)
    if phase is not None:
        sets.append("phase = %s")
        params.append(phase)
    if error is not None:
        sets.append("error = %s")
        params.append(error)
    if cancel_requested is not None:
        sets.append("cancel_requested = %s")
        params.append(cancel_requested)
    if build_pid is not None:
        sets.append("build_pid = %s")
        params.append(build_pid)
    if clear_pid:
        sets.append("build_pid = NULL")
    if bump_cleanup:
        sets.append("cleanup_attempts = cleanup_attempts + 1")
    params.append(batch_id)
    await conn.execute(
        f"UPDATE index_build_batches SET {', '.join(sets)} WHERE id = %s", params
    )
