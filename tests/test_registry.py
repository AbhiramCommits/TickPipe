"""Tests for the experiment registry (SQLAlchemy models, gate, compare, Alembic)."""

from __future__ import annotations

import uuid

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect

from tickpipe.research.registry import (
    DirtyWorkingTreeError,
    ExperimentRegistry,
)

CODE_HASH = "a" * 64
FINGERPRINT = "b" * 64


def make_registry(database_url: str) -> ExperimentRegistry:
    registry = ExperimentRegistry(database_url)
    registry.create_schema()
    return registry


def test_register_get_list_and_record_metrics(database_url: str) -> None:
    registry = make_registry(database_url)
    run_id = registry.register_run(
        dataset_fingerprint=FINGERPRINT,
        feature_set=[{"name": "mid_price"}],
        params={"train": {"model": "ridge", "seed": 1}},
        code_hash=CODE_HASH,
        seed=1,
        git_commit="abc123",
        git_dirty=False,
    )
    assert isinstance(run_id, uuid.UUID)
    record = registry.get_run(run_id)
    assert record is not None
    assert record.status == "registered"
    assert record.seed == 1
    assert record.dataset_fingerprint == FINGERPRINT

    registry.record_metrics(run_id, {"sharpe": 1.5}, status="complete")
    completed = registry.get_run(run_id)
    assert completed is not None
    assert completed.status == "complete"
    assert completed.metrics == {"sharpe": 1.5}

    listed = registry.list_runs(status="complete", dataset_fingerprint=FINGERPRINT)
    assert len(listed) == 1
    assert listed[0].run_id == run_id
    # the status filter must never leak runs of other statuses
    registered = registry.list_runs(status="registered")
    assert all(run.status == "registered" for run in registered)
    assert run_id not in {run.run_id for run in registered}


def test_registry_refuses_dirty_working_tree(database_url: str) -> None:
    registry = make_registry(database_url)
    with pytest.raises(DirtyWorkingTreeError):
        registry.register_run(
            code_hash=CODE_HASH,
            seed=1,
            git_commit="abc",
            git_dirty=True,
            allow_dirty=False,
        )
    run_id = registry.register_run(
        code_hash=CODE_HASH,
        seed=2,
        git_commit="abc",
        git_dirty=True,
        allow_dirty=True,
    )
    record = registry.get_run(run_id)
    assert record is not None and record.git_dirty is True


def test_compare_diffs_params_and_metrics(database_url: str) -> None:
    registry = make_registry(database_url)
    run_a = registry.register_run(
        params={"train": {"model": "ridge", "seed": 1}, "symbols": ["BTC-USD"]},
        metrics={"sharpe": 1.0, "total_return": 0.1},
        code_hash=CODE_HASH,
        seed=1,
        git_commit="abc",
        git_dirty=False,
    )
    run_b = registry.register_run(
        params={"train": {"model": "gbm", "seed": 2}, "symbols": ["BTC-USD"]},
        metrics={"sharpe": 1.2, "total_return": 0.1, "max_drawdown": 0.3},
        code_hash=CODE_HASH,
        seed=2,
        git_commit="abc",
        git_dirty=False,
    )
    comparison = registry.compare(run_a, run_b)
    assert comparison.params_differences == {
        "train": ({"model": "ridge", "seed": 1}, {"model": "gbm", "seed": 2})
    }
    assert comparison.metric_differences == {"sharpe": (1.0, 1.2)}
    assert comparison.params_only_in_a == {}
    assert comparison.params_only_in_b == {}


def test_get_run_missing_returns_none(database_url: str) -> None:
    registry = make_registry(database_url)
    assert registry.get_run(uuid.uuid4()) is None


def test_alembic_migration_creates_experiments_table(tmp_path) -> None:
    database_path = tmp_path / "migrated.db"
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", f"sqlite:///{database_path}")
    command.upgrade(config, "head")
    engine = create_engine(f"sqlite:///{database_path}")
    columns = {column["name"] for column in inspect(engine).get_columns("experiments")}
    assert {
        "run_id",
        "created_at",
        "git_commit",
        "git_dirty",
        "dataset_fingerprint",
        "feature_set",
        "params",
        "metrics",
        "code_hash",
        "seed",
        "status",
    } <= columns
