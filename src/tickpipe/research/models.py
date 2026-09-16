"""SQLAlchemy models and pydantic records for the experiment registry."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict
from sqlalchemy import JSON, BigInteger, Boolean, DateTime, String, Uuid, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Generic JSON on every backend; real JSONB on PostgreSQL.
JSON_TYPE = JSON().with_variant(postgresql.JSONB(), "postgresql")


class Base(DeclarativeBase):
    pass


class ExperimentRun(Base):
    """One experiment run in the registry."""

    __tablename__ = "experiments"

    run_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    git_commit: Mapped[str] = mapped_column(String(64), nullable=False)
    git_dirty: Mapped[bool] = mapped_column(Boolean, nullable=False)
    dataset_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    feature_set: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON_TYPE, default=list, nullable=False
    )
    params: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict, nullable=False)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON_TYPE, default=dict, nullable=False)
    code_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    seed: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="registered", nullable=False)


class ExperimentRunRecord(BaseModel):
    """Registry-facing representation of one run."""

    model_config = ConfigDict(from_attributes=True)

    run_id: uuid.UUID
    created_at: datetime
    git_commit: str
    git_dirty: bool
    dataset_fingerprint: str | None
    feature_set: list[dict[str, Any]]
    params: dict[str, Any]
    metrics: dict[str, Any]
    code_hash: str
    seed: int
    status: str


__all__ = ["Base", "ExperimentRun", "ExperimentRunRecord", "JSON_TYPE"]
