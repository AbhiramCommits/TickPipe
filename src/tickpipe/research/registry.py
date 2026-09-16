"""PostgreSQL-backed experiment registry (SQLAlchemy; schema via Alembic).

Runs are refused from dirty working trees unless ``allow_dirty`` is passed;
every run records the git commit, code hash, dataset fingerprint, feature set,
params, metrics, and the RNG seed so it can be replayed bit-for-bit.
"""

from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from tickpipe.research.models import Base, ExperimentRun, ExperimentRunRecord

logger = structlog.get_logger(__name__)

DEFAULT_STATUS_REGISTERED = "registered"
DEFAULT_STATUS_COMPLETE = "complete"


class DirtyWorkingTreeError(Exception):
    """Raised when registering a run from a dirty working tree without allow_dirty."""


@dataclass(frozen=True)
class RunComparison:
    """Diff between two runs: params and metrics."""

    run_id_a: uuid.UUID
    run_id_b: uuid.UUID
    params_only_in_a: dict[str, Any]
    params_only_in_b: dict[str, Any]
    params_differences: dict[str, tuple[Any, Any]]
    metrics_a: dict[str, Any]
    metrics_b: dict[str, Any]
    metric_differences: dict[str, tuple[Any, Any]]


def git_head_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()


def git_is_dirty() -> bool:
    result = subprocess.run(
        ["git", "status", "--porcelain"], capture_output=True, text=True, check=True
    )
    return bool(result.stdout.strip())


class ExperimentRegistry:
    """Experiment registry over a SQLAlchemy engine (PostgreSQL or SQLite in tests)."""

    def __init__(self, url: str) -> None:
        self.engine = create_engine(url)
        self.session_factory = sessionmaker(self.engine, expire_on_commit=False)

    def create_schema(self) -> None:
        Base.metadata.create_all(self.engine)

    def register_run(
        self,
        *,
        dataset_fingerprint: str | None = None,
        feature_set: list[dict[str, Any]] | None = None,
        params: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
        code_hash: str,
        seed: int,
        git_commit: str | None = None,
        git_dirty: bool | None = None,
        allow_dirty: bool = False,
        status: str = DEFAULT_STATUS_REGISTERED,
    ) -> uuid.UUID:
        """Insert a run and return its UUID.

        ``git_dirty``/``git_commit`` default to the actual repository state;
        a dirty tree is refused unless ``allow_dirty`` is set.
        """
        if git_commit is None:
            git_commit = git_head_commit()
        if git_dirty is None:
            git_dirty = git_is_dirty()
        if git_dirty and not allow_dirty:
            raise DirtyWorkingTreeError(
                "refusing to register a run from a dirty working tree; pass allow_dirty=True"
            )
        run = ExperimentRun(
            git_commit=git_commit,
            git_dirty=bool(git_dirty),
            dataset_fingerprint=dataset_fingerprint,
            feature_set=feature_set or [],
            params=params or {},
            metrics=metrics or {},
            code_hash=code_hash,
            seed=seed,
            status=status,
        )
        with self.session_factory() as session:
            session.add(run)
            session.commit()
        logger.info(
            "registry_run_registered",
            run_id=str(run.run_id),
            status=status,
            git_dirty=bool(git_dirty),
        )
        return run.run_id

    def record_metrics(
        self,
        run_id: uuid.UUID,
        metrics: dict[str, Any],
        *,
        status: str = DEFAULT_STATUS_COMPLETE,
    ) -> None:
        with self.session_factory() as session:
            run = session.get(ExperimentRun, run_id)
            if run is None:
                raise LookupError(f"no run with id {run_id}")
            run.metrics = metrics
            run.status = status
            session.commit()

    def get_run(self, run_id: uuid.UUID) -> ExperimentRunRecord | None:
        with self.session_factory() as session:
            run = session.get(ExperimentRun, run_id)
            if run is None:
                return None
            return ExperimentRunRecord.model_validate(run)

    def list_runs(
        self,
        *,
        status: str | None = None,
        dataset_fingerprint: str | None = None,
        limit: int = 100,
    ) -> list[ExperimentRunRecord]:
        statement = select(ExperimentRun)
        if status is not None:
            statement = statement.where(ExperimentRun.status == status)
        if dataset_fingerprint is not None:
            statement = statement.where(
                ExperimentRun.dataset_fingerprint == dataset_fingerprint
            )
        statement = statement.order_by(ExperimentRun.created_at.desc()).limit(limit)
        with self.session_factory() as session:
            rows = session.scalars(statement).all()
        return [ExperimentRunRecord.model_validate(row) for row in rows]

    def compare(self, run_id_a: uuid.UUID, run_id_b: uuid.UUID) -> RunComparison:
        """Diff the params and metrics of two runs."""
        run_a = self.get_run(run_id_a)
        run_b = self.get_run(run_id_b)
        if run_a is None:
            raise LookupError(f"no run with id {run_id_a}")
        if run_b is None:
            raise LookupError(f"no run with id {run_id_b}")
        keys_a = set(run_a.params)
        keys_b = set(run_b.params)
        metric_keys = set(run_a.metrics) | set(run_b.metrics)
        return RunComparison(
            run_id_a=run_id_a,
            run_id_b=run_id_b,
            params_only_in_a={key: run_a.params[key] for key in keys_a - keys_b},
            params_only_in_b={key: run_b.params[key] for key in keys_b - keys_a},
            params_differences={
                key: (run_a.params[key], run_b.params[key])
                for key in keys_a & keys_b
                if run_a.params[key] != run_b.params[key]
            },
            metrics_a=dict(run_a.metrics),
            metrics_b=dict(run_b.metrics),
            metric_differences={
                key: (run_a.metrics[key], run_b.metrics[key])
                for key in metric_keys
                if key in run_a.metrics
                and key in run_b.metrics
                and run_a.metrics[key] != run_b.metrics[key]
            },
        )


__all__ = [
    "DirtyWorkingTreeError",
    "ExperimentRegistry",
    "RunComparison",
    "git_head_commit",
    "git_is_dirty",
]
