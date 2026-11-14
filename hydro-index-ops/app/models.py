"""Pydantic 出入参模型。"""
from __future__ import annotations

from datetime import datetime

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
    created_at: datetime
    updated_at: datetime


class ReadingIn(BaseModel):
    station_id: int = Field(gt=0, description="站号")
    observed_at: datetime = Field(description="观测时间")
    water_level: float = Field(description="水位（米）")
    discharge: float | None = Field(default=None, description="流量（可选）")
    quality: str = Field(default="GOOD", description="GOOD/SUSPECT/BAD")


class ReadingOut(ReadingIn):
    id: int
    ingested_at: datetime


class LongTxOut(BaseModel):
    pid: int
    seconds: int
    state: str
    message: str
