"""FastAPI 入口：批次受理/查询/取消、水位明细读写、长事务演示。

Swagger UI:  http://localhost:8000/docs
ReDoc:       http://localhost:8000/redoc
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid

import psycopg
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, HTTPException, Query, status
from psycopg_pool import AsyncConnectionPool

from . import db
from . import sql_helpers as sql
from .config import settings
from .db import close_pool, init_pool
from .models import BatchCreate, BatchOut, LongTxOut, ReadingIn, ReadingOut
from .worker import TERMINAL_STATES, worker

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("hydro.api")

# 演示长事务的连接注册表 {token: (conn, task)}
_long_tx: dict[str, tuple[psycopg.AsyncConnection, asyncio.Task]] = {}
# 受理接口的进程内串行锁（数据库部分唯一索引兜底）
_submit_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool()
    await worker.start()
    # 只等 worker 进入运行态即开始服务；启动恢复在后台进行（可能因长事务阻塞
    # 耗时数十秒），恢复完成前活动批次仍在，受理接口会拒绝新批，语义不变。
    await worker.wait_started()
    log.info("startup complete (recovery continues in background if needed)")
    try:
        yield
    finally:
        for token, (conn, task) in list(_long_tx.items()):
            task.cancel()
            with contextlib.suppress(Exception):
                await conn.close()
        _long_tx.clear()
        await worker.stop()
        await close_pool()


app = FastAPI(
    title="水文数仓 · 水位明细索引运维 API",
    version="1.0.0",
    description=(
        "在 PostgreSQL `readings(station_id, observed_at)` 上以 "
        "**CREATE INDEX CONCURRENTLY** 构建普通复合索引的批次化运维服务。\n\n"
        "- 同一时刻只受理一个批次，可查询构建阶段与错误、可取消；\n"
        "- 取消或失败必须等构建会话结束、索引残件删净才进入终态并接收新批；\n"
        "- 清理失败保留待处理状态并自动重试，期间源表普通读写照常；\n"
        "- 批次状态持久化，重启后先停遗留构建、清残件再接新批；\n"
        "- 只管理固定名称索引与自有构建会话，不删人工索引、不断其他会话。"
    ),
    lifespan=lifespan,
)


def get_pool() -> AsyncConnectionPool:
    if db.pool is None:  # pragma: no cover
        raise HTTPException(503, "database pool not initialized")
    return db.pool


def _to_batch_out(row: dict) -> BatchOut:
    return BatchOut(**row)


# --------------------------------------------------------------------------- #
# 健康检查
# --------------------------------------------------------------------------- #
@app.get("/health", tags=["system"], summary="健康检查（含活动批次概要）")
async def health(p: AsyncConnectionPool = Depends(get_pool)) -> dict:
    async with p.connection() as conn:
        await conn.execute("SELECT 1")
        active = await sql.get_active_batch(conn)
        idx = await sql.index_status(conn)
        await conn.commit()
    return {
        "status": "ok",
        "managed_index": settings.index_name,
        "index_status": idx,
        "active_batch": _to_batch_out(active) if active else None,
        "accepting_new_batch": active is None,
    }


# --------------------------------------------------------------------------- #
# 索引构建批次
# --------------------------------------------------------------------------- #
@app.post(
    "/index-batches",
    response_model=BatchOut,
    status_code=status.HTTP_201_CREATED,
    tags=["index-batches"],
    summary="受理一个索引构建批次（同一时刻只受理一批）",
)
async def create_batch(
    body: BatchCreate, p: AsyncConnectionPool = Depends(get_pool)
) -> BatchOut:
    async with _submit_lock:
        async with p.connection() as conn:
            active = await sql.get_active_batch(conn)
            if active is not None:
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    detail=(
                        f"another batch is still in progress (id={active['id']}, "
                        f"state={active['state']}); new batches are accepted only "
                        "after the current build session ends and remnants are removed"
                    ),
                )
            idx = await sql.index_status(conn)
            if idx == "valid":
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    detail=(
                        f"managed index {settings.index_name} already exists and is valid; "
                        "nothing to build (drop it manually outside this service first if intended)"
                    ),
                )
            try:
                batch_id = await sql.insert_batch(conn, body.note)
                await conn.commit()
            except psycopg.errors.UniqueViolation:
                await conn.rollback()
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    detail="one active batch at a time (unique guard)",
                )
            row = await sql.get_batch(conn, batch_id)
            await conn.commit()
    log.info("batch %s accepted", batch_id)
    return _to_batch_out(row)


@app.get(
    "/index-batches",
    response_model=list[BatchOut],
    tags=["index-batches"],
    summary="批次列表（最新在前）",
)
async def list_batches(
    limit: int = Query(50, ge=1, le=200),
    p: AsyncConnectionPool = Depends(get_pool),
) -> list[BatchOut]:
    async with p.connection() as conn:
        rows = await sql.list_batches(conn, limit)
        await conn.commit()
    return [_to_batch_out(r) for r in rows]


@app.get(
    "/index-batches/{batch_id}",
    response_model=BatchOut,
    tags=["index-batches"],
    summary="查询批次状态/构建阶段/错误",
)
async def get_batch(
    batch_id: int, p: AsyncConnectionPool = Depends(get_pool)
) -> BatchOut:
    async with p.connection() as conn:
        row = await sql.get_batch(conn, batch_id)
        if row is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "batch not found")
        # 构建中实时刷新阶段（进度视图），状态机自身也在周期刷新，这里做即时读
        if row["state"] == "BUILDING" and row["build_pid"]:
            live = await sql.fetch_build_phase(conn, row["build_pid"])
            if live and live != row["phase"]:
                await sql.update_state(conn, batch_id, phase=live)
                await conn.commit()
                row = await sql.get_batch(conn, batch_id)
        await conn.commit()
    return _to_batch_out(row)


@app.post(
    "/index-batches/{batch_id}/cancel",
    response_model=BatchOut,
    tags=["index-batches"],
    summary="请求取消批次（须等会话结束、残件删净才进入 CANCELLED）",
)
async def cancel_batch(
    batch_id: int, p: AsyncConnectionPool = Depends(get_pool)
) -> BatchOut:
    async with p.connection() as conn:
        row = await sql.get_batch(conn, batch_id)
        if row is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "batch not found")
        if row["state"] in TERMINAL_STATES:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                detail=f"batch already in terminal state {row['state']}",
            )
        await sql.update_state(
            conn, batch_id, cancel_requested=True,
            state="CANCELLING" if row["state"] == "BUILDING" else row["state"],
            phase=("cancel requested; waiting build session to end"
                   if row["state"] == "BUILDING" else "cancel requested before build start"),
        )
        # 立即给构建会话发取消信号（worker 会兜底重发/terminate）；
        # 带 application_name 校验，防止 pid 复用误伤其他会话
        if row["build_pid"]:
            await conn.execute(
                "SELECT pg_cancel_backend(pid) FROM pg_stat_activity "
                "WHERE pid = %s AND application_name LIKE %s",
                (row["build_pid"], f"{settings.app_name_prefix}-%"),
            )
        await conn.commit()
        row = await sql.get_batch(conn, batch_id)
        await conn.commit()
    log.warning("batch %s: cancel requested", batch_id)
    return _to_batch_out(row)


# --------------------------------------------------------------------------- #
# 水位明细：构建期间普通读写照常
# --------------------------------------------------------------------------- #
@app.post(
    "/readings",
    response_model=ReadingOut,
    status_code=status.HTTP_201_CREATED,
    tags=["readings"],
    summary="写入一条水位观测（索引构建/等待/清理期间始终可用）",
)
async def insert_reading(
    body: ReadingIn, p: AsyncConnectionPool = Depends(get_pool)
) -> ReadingOut:
    async with p.connection() as conn:
        try:
            cur = await conn.execute(
                """
                INSERT INTO readings
                    (station_id, observed_at, water_level, discharge, quality)
                VALUES (%(station_id)s, %(observed_at)s, %(water_level)s,
                        %(discharge)s, %(quality)s)
                RETURNING id, station_id, observed_at, water_level, discharge,
                          quality, ingested_at
                """,
                body.model_dump(),
            )
            row = await cur.fetchone()
            await conn.commit()
        except psycopg.Error as exc:
            await conn.rollback()
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc))
    return ReadingOut(**row)


@app.get(
    "/readings",
    response_model=list[ReadingOut],
    tags=["readings"],
    summary="按站号+时间查询水位明细（索引建成后走复合索引）",
)
async def query_readings(
    station_id: int = Query(..., description="站号"),
    start: str | None = Query(None, description="起始观测时间 ISO8601"),
    end: str | None = Query(None, description="截止观测时间 ISO8601"),
    limit: int = Query(100, ge=1, le=1000),
    p: AsyncConnectionPool = Depends(get_pool),
) -> list[ReadingOut]:
    clauses = ["station_id = %(station_id)s"]
    params: dict = {"station_id": station_id, "limit": limit}
    if start:
        clauses.append("observed_at >= %(start)s")
        params["start"] = start
    if end:
        clauses.append("observed_at <= %(end)s")
        params["end"] = end
    sql_text = (
        "SELECT id, station_id, observed_at, water_level, discharge, quality, ingested_at "
        "FROM readings WHERE " + " AND ".join(clauses) +
        " ORDER BY observed_at DESC LIMIT %(limit)s"
    )
    async with p.connection() as conn:
        try:
            cur = await conn.execute(sql_text, params)
            rows = await cur.fetchall()
            await conn.commit()
        except psycopg.Error as exc:
            await conn.rollback()
            raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc))
    return [ReadingOut(**r) for r in rows]


@app.get(
    "/readings/explain",
    tags=["readings"],
    summary="查看按站号+时间查询的执行计划（验证索引是否被使用）",
)
async def explain_readings(
    station_id: int = Query(...),
    p: AsyncConnectionPool = Depends(get_pool),
) -> dict:
    async with p.connection() as conn:
        cur = await conn.execute(
            "EXPLAIN (FORMAT JSON) SELECT * FROM readings "
            "WHERE station_id = %s ORDER BY observed_at DESC LIMIT 100",
            (station_id,),
        )
        plan = await cur.fetchall()
        await conn.commit()
    return {"plan": plan}


# --------------------------------------------------------------------------- #
# 长事务演示：让 CIC 停在“等待旧快照/写事务”阶段
# --------------------------------------------------------------------------- #
@app.post(
    "/demo/long-tx",
    response_model=LongTxOut,
    tags=["demo"],
    summary="开启一个持有 readings 快照的长事务（演示 CIC 构建等待）",
)
async def open_long_tx(
    seconds: int = Query(60, ge=5, le=3600, description="事务保持秒数"),
    p: AsyncConnectionPool = Depends(get_pool),
) -> LongTxOut:
    conn = await psycopg.AsyncConnection.connect(
        settings.pg_dsn, autocommit=False,
        application_name=f"hydro-demo-longtx-{uuid.uuid4().hex[:8]}",
    )
    await conn.execute("BEGIN")
    await conn.execute("SELECT count(*) FROM readings")  # 固定快照
    pid = conn.pgconn.backend_pid
    token = uuid.uuid4().hex

    async def hold() -> None:
        try:
            await conn.execute(f"SELECT pg_sleep({int(seconds)})")
            await conn.commit()
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await conn.rollback()
            raise
        except psycopg.Error as exc:
            log.info("long tx %s ended: %s", pid, exc)
        finally:
            with contextlib.suppress(Exception):
                await conn.close()
            _long_tx.pop(token, None)

    task = asyncio.create_task(hold())
    _long_tx[token] = (conn, task)
    return LongTxOut(
        pid=pid, seconds=seconds, state="open",
        message=(
            f"long transaction pid={pid} holds a snapshot for {seconds}s. "
            "Now POST /index-batches to observe the build waiting "
            "(waiting for writers before build / old snapshots)."
        ),
    )


@app.delete(
    "/demo/long-tx/{pid}",
    tags=["demo"],
    summary="提前结束演示长事务（仅终止 hydro-demo-longtx-* 会话）",
)
async def close_long_tx(
    pid: int, p: AsyncConnectionPool = Depends(get_pool)
) -> dict:
    async with p.connection() as conn:
        cur = await conn.execute(
            "SELECT pg_terminate_backend(pid) AS ok FROM pg_stat_activity "
            "WHERE pid = %s AND application_name LIKE 'hydro-demo-longtx-%%'",
            (pid,),
        )
        rec = await cur.fetchone()
        await conn.commit()
    if not rec or not rec["ok"]:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            "demo long transaction not found for this pid")
    return {"pid": pid, "terminated": True}
