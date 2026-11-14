"""索引构建批次状态机（单 worker 驱动，咨询锁保证多副本只有一个生效）。

关键保证：
- CREATE INDEX CONCURRENTLY 跑在独立 autocommit 会话，不阻塞 readings 普通读写；
- 取消/失败后，必须等构建会话结束、残件 DROP 干净才进入终态，之后才接新批；
- 取消后若构建迟到成功，索引仍被删除，批次记 CANCELLED，绝不报成功；
- 清理失败保留 CLEANING（待处理）状态并定时自动重试，期间 API 与源表读写照常；
- 只按固定索引名和 application_name 操作本系统对象，不碰人工索引、不断其他会话。
"""
from __future__ import annotations

import asyncio
import contextlib
import logging

import psycopg

from . import db
from . import sql_helpers as sql
from .config import settings

log = logging.getLogger("hydro.worker")

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}
ACTIVE_STATES = ("PENDING", "BUILDING", "CANCELLING", "CLEANING")


class BuildWorker:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._started = asyncio.Event()
        self._holds_lock = False
        self._lock_conn: psycopg.AsyncConnection | None = None
        # 本实例正在驱动的构建协程 {batch_id: task}；据此区分“本地构建”与“孤儿构建”
        self._local_builds: dict[int, asyncio.Task] = {}
        # 启动恢复进行中标志：恢复期间主循环不调度，避免并发操作同一批次
        self._recovery_done = asyncio.Event()
        self._recovery_task: asyncio.Task | None = None

    # ------------------------------------------------------------------ 生命周期
    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="index-build-worker")

    async def stop(self) -> None:
        self._stop.set()
        # 显式取消本实例的构建协程与恢复协程，避免事件循环关闭时留下孤儿任务；
        # 中途取消 CIC 可能留下 INVALID 残件，由下次启动恢复统一清理（状态已持久化）
        for t in list(self._local_builds.values()):
            t.cancel()
        if self._recovery_task is not None:
            self._recovery_task.cancel()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        # 正常路径会在 _run finally 释放；这里兜底关闭锁连接
        if self._lock_conn is not None:
            with contextlib.suppress(Exception):
                await self._lock_conn.close()
            self._lock_conn = None

    async def wait_started(self) -> None:
        """等待 worker 拿到咨询锁并进入运行态（不等同于恢复完成）。"""
        await self._started.wait()

    async def wait_recovery_done(self) -> None:
        """等待启动恢复彻底完成（主要用于测试与优雅关停）。"""
        await self._recovery_done.wait()

    # ------------------------------------------------------------------ 主循环
    async def _run(self) -> None:
        assert db.pool is not None
        # 等数据库就绪（compose 已做健康检查，这里再兜底重试）
        while not self._stop.is_set():
            try:
                self._lock_conn = await psycopg.AsyncConnection.connect(
                    settings.pg_dsn, autocommit=True,
                    application_name="hydro-cic-worker-lock",
                )
                row = await self._lock_conn.execute(
                    "SELECT pg_try_advisory_lock(%s)", (982451653,)
                )
                rec = await row.fetchone()
                self._holds_lock = bool(rec and rec[0])
                break
            except Exception as exc:  # pragma: no cover - 启动期数据库未就绪
                log.warning("waiting for database: %s", exc)
                if self._lock_conn is not None:
                    with contextlib.suppress(Exception):
                        await self._lock_conn.close()
                    self._lock_conn = None
                await asyncio.sleep(1.0)

        if not self._holds_lock:
            log.warning("advisory lock held by another instance; worker idles (API only)")
            if self._lock_conn is not None:
                await self._lock_conn.close()
                self._lock_conn = None
            # 非持锁实例不负责恢复，也不调度任何状态机动作，仅保留 API 服务
            self._recovery_done.set()
            self._started.set()
            # 等停止信号，避免进入下面的持锁调度循环
            await self._stop.wait()
            return

        try:
            # 恢复在后台执行：终止被长事务阻塞的孤儿构建可能需要数十秒，
            # 不能阻塞 API 启动（健康检查/状态查询需尽快可用）。恢复期间旧批次
            # 仍处活动态，受理接口因“只受理一批”拒绝新请求，语义不受影响。
            recovery_task = asyncio.create_task(
                self._guarded_recovery(), name="startup-recovery"
            )
            self._recovery_task = recovery_task
            self._started.set()

            while not self._stop.is_set():
                try:
                    await self._dispatch_once()
                except Exception as exc:
                    log.exception("worker iteration failed: %s", exc)
                await asyncio.sleep(settings.poll_interval)
            # 关停时若恢复仍在跑，等待其结束以免遗留半完成状态
            if not recovery_task.done():
                with contextlib.suppress(asyncio.CancelledError):
                    await recovery_task
        finally:
            # 咨询锁必须在“获取它的同一条会话”上释放
            if self._lock_conn is not None:
                with contextlib.suppress(Exception):
                    await self._lock_conn.execute(
                        "SELECT pg_advisory_unlock(%s)", (982451653,)
                    )
                    await self._lock_conn.close()
                self._lock_conn = None
            self._holds_lock = False

    async def _dispatch_once(self) -> None:
        assert db.pool is not None
        # 恢复未完成前不调度，避免与恢复流程并发处理同一活动批次
        if not self._recovery_done.is_set():
            return
        async with db.pool.connection() as conn:
            batch = await sql.get_active_batch(conn)
            if batch is None:
                return
            state = batch["state"]
            cancel = batch["cancel_requested"]

        if state == "PENDING":
            if cancel:
                # 受理后、构建开始前就被取消：无会话要停，直接清净收尾
                await self._cleanup_and_finish(batch["id"], cancelled=True)
            else:
                task = asyncio.create_task(self._build(batch["id"]))
                self._local_builds[batch["id"]] = task

                def _settled(t: asyncio.Task, b: int = batch["id"]) -> None:
                    self._local_builds.pop(b, None)
                    if not t.cancelled() and t.exception() is not None:
                        log.exception("build task for batch %s crashed: %s",
                                      b, t.exception())

                task.add_done_callback(_settled)
        elif state == "BUILDING":
            # 咨询锁保证全局只有本 worker 驱动状态机，因此这里的 BUILDING
            # 若不是本实例协程在跑，必然是原属进程已死留下的孤儿构建
            local = self._local_builds.get(batch["id"])
            if local is not None and not local.done():
                return  # 本实例 _build 协程正在驱动，无需插手
            await self._reap_orphan_building(batch)
        elif state in ("CANCELLING", "CLEANING"):
            await self._cleanup_once(batch)
        else:  # pragma: no cover
            log.warning("unexpected active state: %s", state)

    async def _guarded_recovery(self) -> None:
        """包装启动恢复：任何异常都不能杀死 worker，记录后下一轮调度兜底。"""
        try:
            await self._startup_recovery()
            log.info("startup recovery finished, accepting batch work")
        except Exception as exc:
            log.exception("startup recovery failed, will rely on dispatch: %s", exc)
        finally:
            self._recovery_done.set()

    # ------------------------------------------------------------ 启动恢复
    async def _startup_recovery(self) -> None:
        """重启后：停遗留构建 → 清残件 → 再接新批；成功索引保留。"""
        assert db.pool is not None
        async with db.pool.connection() as conn:
            batch = await sql.get_active_batch(conn)
            if batch is None:
                log.info("recovery: no active batch")
                return
            batch_id = batch["id"]
            pid = batch["build_pid"]
            log.warning("recovery: batch %s found in state %s (pid=%s)",
                        batch_id, batch["state"], pid)

        # 1) 停掉遗留构建会话（双重身份校验，绝不终止其他会话）
        await self._terminate_and_wait(pid)

        async with db.pool.connection() as conn:
            await sql.update_state(conn, batch_id, clear_pid=True,
                                   phase="recovery: cleaning remnants")
            await conn.commit()
            status = await sql.index_status(conn)
            cancel = batch["cancel_requested"]
            await conn.commit()

        # 2) 取消批次：迟到完成的索引也必须删除，直到残件清净
        if cancel:
            await self._cleanup_and_finish(batch_id, cancelled=True)
            return

        # 3) 非取消批次：有效索引视为迟到的成功产物，保留并记成功
        if status == "valid":
            async with db.pool.connection() as conn:
                await sql.update_state(
                    conn, batch_id, state="SUCCEEDED",
                    phase="done (recovered valid index, preserved)", error=None,
                )
                await conn.commit()
            log.info("recovery: batch %s adopted valid index as SUCCEEDED", batch_id)
            return

        if status == "invalid":
            # 构建崩溃留下 INVALID 残件：清净后记失败，允许后续重新受理
            await self._cleanup_and_finish(batch_id, cancelled=False)
            return

        # status is None：从未真正建索引。PENDING 保留待 worker 正常构建，
        # 其余中间态说明构建中途蒸发，记失败。
        if batch["state"] != "PENDING":
            async with db.pool.connection() as conn:
                await sql.update_state(
                    conn, batch_id, state="FAILED",
                    phase="recovery: build session vanished, no index remnant",
                )
                await conn.commit()

    # ------------------------------------------------------------ 构建
    async def _build(self, batch_id: int) -> None:
        assert db.pool is not None
        log.info("batch %s: opening CREATE INDEX CONCURRENTLY session", batch_id)
        app_name = f"{settings.app_name_prefix}-{batch_id}"
        try:
            build_conn = await psycopg.AsyncConnection.connect(
                settings.pg_dsn, autocommit=True, application_name=app_name
            )
        except Exception as exc:
            await self._handle_build_error(batch_id, None, exc)
            return
        pid = build_conn.pgconn.backend_pid
        done = asyncio.Event()

        async with db.pool.connection() as conn:
            await sql.update_state(
                conn, batch_id, state="BUILDING", build_pid=pid,
                phase="initializing", error=None,
            )
            await conn.commit()

        async def poll_phase() -> None:
            while not done.is_set():
                await asyncio.sleep(settings.poll_interval)
                try:
                    async with db.pool.connection() as c:
                        live = await sql.fetch_build_phase(c, pid)
                        if live:
                            await sql.update_state(c, batch_id, phase=live)
                            await c.commit()
                except Exception as exc:  # 轮询失败不影响构建本身
                    log.debug("phase poll failed: %s", exc)

        poller = asyncio.create_task(poll_phase())
        ddl = (
            f"CREATE INDEX CONCURRENTLY {settings.index_name} "
            f"ON {settings.table_name} (station_id, observed_at)"
        )
        try:
            await build_conn.execute(ddl)
        except psycopg.Error as exc:
            done.set()
            poller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await poller
            await build_conn.close()
            await self._handle_build_error(batch_id, pid, exc)
            return
        except Exception as exc:  # 非 psycopg 异常也按构建失败收尾，避免任务裸崩
            done.set()
            poller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await poller
            with contextlib.suppress(Exception):
                await build_conn.close()
            await self._handle_build_error(batch_id, pid, exc)
            return

        done.set()
        poller.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await poller
        await build_conn.close()

        # 构建语句已返回。先看取消标志：取消后的迟到完成绝不允许留索引/报成功。
        async with db.pool.connection() as conn:
            batch = await sql.get_batch(conn, batch_id)
            cancelled = bool(batch and batch["cancel_requested"])
            status = await sql.index_status(conn)
            await conn.commit()

        if cancelled:
            log.warning("batch %s: build finished after cancel request; dropping index",
                        batch_id)
            await self._cleanup_and_finish(batch_id, cancelled=True)
            return

        if status == "valid":
            async with db.pool.connection() as conn:
                await sql.update_state(
                    conn, batch_id, state="SUCCEEDED", clear_pid=True,
                    phase="done: index valid and ready", error=None,
                )
                await conn.commit()
            log.info("batch %s: SUCCEEDED, index %s online", batch_id, settings.index_name)
        else:
            # 返回成功却查不到有效索引（异常情况）：按失败走清理流程
            await self._handle_build_error(
                batch_id, pid,
                RuntimeError(f"build returned but index status={status}"),
            )

    async def _handle_build_error(self, batch_id: int, pid: int | None,
                                  exc: BaseException) -> None:
        assert db.pool is not None
        msg = f"{type(exc).__name__}: {exc}".strip()
        log.error("batch %s: build failed: %s", batch_id, msg)

        # 确认构建会话已经结束（取消信号下可能还在收尾）
        await self._terminate_and_wait(pid)

        async with db.pool.connection() as conn:
            await sql.update_state(
                conn, batch_id, clear_pid=True, error=msg,
                phase="build ended; cleaning index remnant",
            )
            await conn.commit()

        cancelled = False
        async with db.pool.connection() as conn:
            batch = await sql.get_batch(conn, batch_id)
            cancelled = bool(batch and batch["cancel_requested"])
        await self._cleanup_and_finish(batch_id, cancelled=cancelled)

    async def _reap_orphan_building(self, batch: dict) -> None:
        """BUILDING 但本 worker 没有在跑构建：原属进程已死，按失败终止并收尾。

        咨询锁保证同一时刻只有一个 worker 持锁驱动状态机，所以持锁 worker
        看到的非本地 BUILDING 一定是孤儿，不存在“另一实例正在正常构建”的情况。
        """
        assert db.pool is not None
        pid = batch["build_pid"]
        async with db.pool.connection() as conn:
            alive = await sql.session_is_alive(conn, pid)
            await conn.commit()
        if alive:
            log.warning("batch %s: orphan build pid=%s reaped, terminating",
                        batch["id"], pid)
        await self._handle_build_error(
            batch["id"], pid,
            RuntimeError("build session orphaned (owning worker restarted)"),
        )

    # ------------------------------------------------------------ 取消支撑
    async def _terminate_and_wait(self, pid: int | None) -> None:
        """先 pg_cancel_backend 优雅取消，超时再 terminate，直到会话消失。"""
        if pid is None:
            return
        assert db.pool is not None
        for attempt in range(30):  # 最多等待约 30s
            async with db.pool.connection() as conn:
                alive = await sql.session_is_alive(conn, pid)
                if not alive:
                    return
                if attempt == 0:
                    await conn.execute(
                        "SELECT pg_cancel_backend(pid) FROM pg_stat_activity "
                        "WHERE pid = %s AND application_name LIKE %s",
                        (pid, f"{settings.app_name_prefix}-%"),
                    )
                elif attempt == 10:
                    await sql.terminate_our_backend(conn, pid)
                await conn.commit()
            await asyncio.sleep(1.0)
        # 兜底强制终止（仍带身份校验）
        async with db.pool.connection() as conn:
            await sql.terminate_our_backend(conn, pid)
            await conn.commit()

    # ------------------------------------------------------------ 清理
    async def _cleanup_and_finish(self, batch_id: int, *, cancelled: bool) -> None:
        """循环清理直到残件删净；清理失败保持 CLEANING 自动重试，期间服务照常。"""
        assert db.pool is not None
        terminal = "CANCELLED" if cancelled else "FAILED"
        while not self._stop.is_set():
            async with db.pool.connection() as conn:
                batch = await sql.get_batch(conn, batch_id)
                if batch is None or batch["state"] in TERMINAL_STATES:
                    return
                # 兜底：构建会话仍在就绝不能 DROP（CIC 与 DROP 同对象会报错），
                # 先等本批构建会话彻底结束
                if batch["build_pid"]:
                    pid = batch["build_pid"]
                    alive = await sql.session_is_alive(conn, pid)
                else:
                    pid, alive = None, False
                await conn.commit()
            if alive:
                log.info("batch %s: waiting build session pid=%s to end before cleanup",
                         batch_id, pid)
                await self._terminate_and_wait(pid)
                await asyncio.sleep(settings.poll_interval)
                continue

            async with db.pool.connection() as conn:
                await sql.update_state(
                    conn, batch_id, state="CLEANING", clear_pid=True,
                    phase="dropping index remnant (DROP INDEX CONCURRENTLY)",
                )
                await conn.commit()
            try:
                await self._drop_remnant(batch_id)
            except Exception as exc:
                msg = f"{type(exc).__name__}: {exc}".strip()
                log.warning("batch %s: cleanup failed, will retry: %s", batch_id, msg)
                async with db.pool.connection() as conn:
                    await sql.update_state(conn, batch_id, error=msg, bump_cleanup=True)
                    await conn.commit()
                await asyncio.sleep(settings.cleanup_retry_interval)
                continue

            async with db.pool.connection() as conn:
                status = await sql.index_status(conn)
                await conn.commit()
            if status is not None:
                # DROP 未报错但索引仍在（极端并发）：留 CLEANING 重试
                log.warning("batch %s: index still present after drop, retrying", batch_id)
                await asyncio.sleep(settings.cleanup_retry_interval)
                continue

            async with db.pool.connection() as conn:
                # 保留最后一条错误信息便于排障；仅成功路径才清空 error
                await sql.update_state(
                    conn, batch_id, state=terminal, clear_pid=True,
                    phase=f"done: remnant removed, batch {terminal.lower()}",
                )
                await conn.commit()
            log.info("batch %s: cleanup complete -> %s; new batches accepted",
                     batch_id, terminal)
            return

    async def _cleanup_once(self, batch: dict) -> None:
        """主循环对 CANCELLING/CLEANING 的调度入口（复用同一重试循环）。"""
        await self._cleanup_and_finish(
            batch["id"], cancelled=batch["cancel_requested"]
        )

    async def _drop_remnant(self, batch_id: int) -> None:
        assert db.pool is not None
        # 演示注入：前 N 次清理故意失败，验证待处理状态与自动重试
        async with db.pool.connection() as conn:
            current = await sql.get_batch(conn, batch_id)
            attempts = current["cleanup_attempts"] if current else 0
        if attempts < settings.cleanup_fail_first:
            raise RuntimeError(
                f"injected cleanup failure ({attempts + 1}/"
                f"{settings.cleanup_fail_first}) via HYDRO_CLEANUP_FAIL_FIRST"
            )

        # DROP INDEX CONCURRENTLY 同样不能在事务块内，使用独立 autocommit 会话，
        # 不阻塞 readings 的普通读写；IF EXISTS 兼容构建极早期失败、无残件的情况。
        conn = await psycopg.AsyncConnection.connect(
            settings.pg_dsn, autocommit=True,
            application_name=f"{settings.app_name_prefix}-cleanup-{batch_id}",
        )
        try:
            await conn.execute(
                f"DROP INDEX CONCURRENTLY IF EXISTS {settings.index_name}"
            )
        finally:
            await conn.close()


worker = BuildWorker()
