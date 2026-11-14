"""psycopg3 异步连接池与批次表 DDL。"""
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
        await conn.execute(DDL_BATCH_TABLE)
        await conn.commit()
    log.info("connection pool ready, batch table ensured")
    return pool


async def close_pool() -> None:
    if pool is not None:
        await pool.close()
        log.info("connection pool closed")
