"""Pydantic 出入参模型。"""
from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, Field


class BatchCreate(BaseModel):
    note: str | None = Field(default=None, max_length=500, description="备注（可选）")


class BatchOut(BaseModel):
    id: int
    state: str
    cancel_requested: bool
    build_pid: int | None
    phase: str | None
    error: str | None
    cleanup_attempts: int
    created_at: dt.datetime
    updated_at: dt.datetime


class ReadingIn(BaseModel):
    station_id: int = Field(gt=0, description="站号")
    observed_at: dt.datetime = Field(description="观测时间")
    water_level: float = Field(description="水位（米）")
    discharge: float | None = Field(default=None, description="流量（可选）")
    quality: str = Field(default="GOOD", description="GOOD/SUSPECT/BAD")


class ReadingOut(ReadingIn):
    id: int
    ingested_at: dt.datetime


class LongTxOut(BaseModel):
    pid: int
    seconds: int
    state: str
    message: str


class HourlyCell(BaseModel):
    hour: int = Field(ge=0, le=23, description="UTC 小时（0~23）")
    good_count: int = Field(
        ge=0,
        description="该小时 quality='GOOD' 的有效观测数；缺测小时为 0",
    )
    max_water_level: float | None = Field(
        default=None,
        description="该小时 GOOD 观测的最高水位；缺测小时为 null",
    )


class StationDaySummary(BaseModel):
    station_id: int
    date: dt.date
    timezone: str = Field(description="小时桶所用时区，固定为 UTC")
    revision: int = Field(
        description="已发布修订号；0 表示该站日从未成功刷新（published=false）"
    )
    published: bool = Field(description="是否存在已完成的整日发布")
    refreshed_at: dt.datetime | None = Field(
        default=None, description="该修订的发布时间"
    )
    snapshot: str | None = Field(
        default=None, description="该修订计算所基于的 PostgreSQL 快照"
    )
    hours: list[HourlyCell] = Field(
        description="固定 24 项（0~23，按顺序），缺测小时 good_count=0、max null"
    )


class StationDayRefreshOut(StationDaySummary):
    applied: bool = Field(
        description=(
            "本次刷新是否发布成功；false 表示并发中有更新的快照已先发布，"
            "本次旧快照结果被拒绝，返回的是现行已发布汇总"
        )
    )
