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
    log_level: str = os.getenv("HYDRO_LOG_LEVEL", "INFO")

    # 本系统管理的索引与构建会话标识——清理/恢复只认这些，绝不触碰人工索引
    index_name: str = "idx_readings_station_observed"
    table_name: str = "readings"
    app_name_prefix: str = "hydro-cic-builder"


settings = Settings()
