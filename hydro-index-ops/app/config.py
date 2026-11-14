"""全局配置（全部来自环境变量，含默认值）。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    pg_dsn: str = os.getenv(
        "HYDRO_PG_DSN",
        "postgresql://hydro:hydro_pw@localhost:5432/hydro_warehouse",
    )
    # 轮询待处理批次/构建进度的间隔
    poll_interval: float = _get_float("HYDRO_WORK_POLL_INTERVAL", 1.0)
    # 清理失败后的重试间隔
    cleanup_retry_interval: float = _get_float("HYDRO_CLEANUP_RETRY_INTERVAL", 3.0)
    # 演示开关：前 N 次 DROP INDEX 清理故意失败（生产必须为 0）
    cleanup_fail_first: int = _get_int("HYDRO_CLEANUP_FAIL_FIRST", 0)
    # 测试/演示开关：站日刷新在取快照后、发布前等待的秒数（生产必须为 0）。
    # 用于稳定复现“旧快照刷新晚到”的多实例并发场景。
    station_day_publish_delay: float = _get_float(
        "HYDRO_STATION_DAY_PUBLISH_DELAY", 0.0
    )
    # 测试/演示开关：为 true 时在整日发布事务内、提交前抛错（生产必须为 false），
    # 用于验证刷新失败后查询仍只看到先前完整的 24 小时与修订号。
    station_day_fail_before_commit: bool = (
        os.getenv("HYDRO_STATION_DAY_FAIL_BEFORE_COMMIT", "").lower()
        in ("1", "true", "yes", "on")
    )
    log_level: str = os.getenv("HYDRO_LOG_LEVEL", "INFO")

    # 本系统管理的索引与构建会话标识——清理/恢复只认这些，绝不触碰人工索引
    index_name: str = "idx_readings_station_observed"
    table_name: str = "readings"
    app_name_prefix: str = "hydro-cic-builder"


settings = Settings()
