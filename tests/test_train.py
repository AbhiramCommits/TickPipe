"""Tests for walk-forward training: strict time ordering and determinism."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

from tickpipe.core.types import Trade
from tickpipe.research.config import FeatureConfig, TrainConfig
from tickpipe.research.dataset import DatasetSpec, FeatureDataset
from tickpipe.research.train import (
    bar_backtest_metrics,
    train_evaluate,
    walk_forward_splits,
)
from tickpipe.store.pit import PointInTimeView
from tickpipe.store.reader import TickStore
from tickpipe.store.writer import PartitionedWriter

BASE_NS = 1_700_000_000_000_000_000
SCALE = 10**9


def build_matrix(tmp_path: Path, count: int = 120) -> object:
    rng = np.random.default_rng(11)
    writer = PartitionedWriter(tmp_path, "trades", max_open_s=None)
    price = 100.0
    for index in range(count):
        ts_ns = BASE_NS + index * 5 * SCALE
        price *= 1 + rng.normal(0, 2e-3)
        writer.write(
            Trade(
                symbol="BTC-USD",
                exchange_ts_ns=ts_ns,
                ingest_ts_ns=ts_ns + 1,
                price=Decimal(f"{price:.6f}"),
                size=Decimal("1"),
                trade_id=f"t-{index}",
                sequence=index + 1,
            )
        )
    writer.flush()
    end_ns = BASE_NS + (count - 1) * 5 * SCALE
    view = PointInTimeView(TickStore(tmp_path, "trades"), as_of_ns=end_ns)
    spec = DatasetSpec(
        dataset="trades",
        symbols=("BTC-USD",),
        bar_interval_ns=30 * SCALE,
        features=(
            FeatureConfig(name="mid_price"),
            FeatureConfig(name="realized_vol", window_s=120),
            FeatureConfig(name="trade_count", window_s=60),
        ),
        start_ns=BASE_NS,
        end_ns=end_ns,
    )
    return FeatureDataset(view, spec, data_dir=tmp_path).compute()[0]


def test_walk_forward_splits_are_strictly_time_ordered() -> None:
    folds = list(walk_forward_splits(100, 4))
    assert len(folds) == 4
    for train_index, valid_index in folds:
        assert len(train_index) > 0 and len(valid_index) > 0
        assert np.intersect1d(train_index, valid_index).size == 0
        assert train_index.max() < valid_index.min()
    # the final fold validates the very last row, never any earlier one
    assert folds[-1][1].max() == 99


def test_walk_forward_never_shuffles() -> None:
    for train_index, valid_index in walk_forward_splits(30, 3):
        assert list(train_index) == list(range(len(train_index)))
        assert list(valid_index) == list(
            range(len(train_index), len(train_index) + len(valid_index))
        )


def test_ridge_training_is_deterministic(tmp_path: Path) -> None:
    table = build_matrix(tmp_path)
    config = TrainConfig(model="ridge", horizon_bars=1, n_splits=3, seed=42)
    first = train_evaluate(table, config).combined()
    second = train_evaluate(table, config).combined()
    assert first == second
    assert first["val_mse"] >= 0


def test_gradient_boosting_is_deterministic_with_fixed_seed(tmp_path: Path) -> None:
    table = build_matrix(tmp_path, count=60)
    config = TrainConfig(
        model="gradient_boosting",
        horizon_bars=1,
        n_splits=2,
        seed=123,
        gbm_n_estimators=25,
        gbm_max_depth=2,
    )
    first = train_evaluate(table, config).combined()
    second = train_evaluate(table, config).combined()
    assert first == second


def test_different_seed_changes_gradient_boosting(tmp_path: Path) -> None:
    table = build_matrix(tmp_path, count=60)
    base = dict(horizon_bars=1, n_splits=2, gbm_n_estimators=25, gbm_max_depth=2)
    first = train_evaluate(table, TrainConfig(model="gradient_boosting", seed=1, **base))
    second = train_evaluate(table, TrainConfig(model="gradient_boosting", seed=2, **base))
    assert first.combined() != second.combined()


def test_perfect_signals_earn_positive_return() -> None:
    returns = np.array([0.01, -0.005, 0.02, -0.01, 0.005])
    bar_end = np.array([0, 1, 2, 3, 4], dtype=np.int64)
    metrics = bar_backtest_metrics(np.sign(returns), returns, bar_end)
    assert metrics["backtest_total_return"] > 0
    assert metrics["backtest_fill_count"] > 0
    assert metrics["backtest_sharpe"] > 0


def test_train_requires_mid_price(tmp_path: Path) -> None:
    import pyarrow as pa

    table = pa.table(
        {"symbol": ["A"], "bar_end_ns": [1], "other": [0.5]}
    )
    with pytest.raises(ValueError, match="mid_price"):
        train_evaluate(table, TrainConfig(model="ridge"))
