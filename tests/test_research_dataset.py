"""Tests for feature-matrix materialization and the dataset fingerprint."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

from tickpipe.core.types import Trade
from tickpipe.research.config import FeatureConfig
from tickpipe.research.dataset import DatasetSpec, FeatureDataset, load_dataset_spec
from tickpipe.store.pit import PointInTimeView
from tickpipe.store.reader import TickStore
from tickpipe.store.writer import PartitionedWriter

BASE_NS = 1_700_000_000_000_000_000
SCALE = 10**9


def write_trades(data_dir: Path, count: int = 60, seed: int = 7) -> tuple[int, int]:
    rng = np.random.default_rng(seed)
    writer = PartitionedWriter(data_dir, "trades", max_open_s=None)
    price = 100.0
    for index in range(count):
        ts_ns = BASE_NS + index * 5 * SCALE
        price *= 1 + rng.normal(0, 1e-3)
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
    return BASE_NS, BASE_NS + (count - 1) * 5 * SCALE


def make_dataset(
    data_dir: Path, start_ns: int, end_ns: int
) -> FeatureDataset:
    view = PointInTimeView(TickStore(data_dir, "trades"), as_of_ns=end_ns)
    spec = DatasetSpec(
        dataset="trades",
        symbols=("BTC-USD",),
        bar_interval_ns=30 * SCALE,
        features=(
            FeatureConfig(name="mid_price"),
            FeatureConfig(name="trade_count", window_s=60),
        ),
        start_ns=start_ns,
        end_ns=end_ns,
    )
    return FeatureDataset(view, spec, data_dir=data_dir)


def test_dataset_compute_and_fingerprint_stability(tmp_path: Path) -> None:
    start_ns, end_ns = write_trades(tmp_path)
    dataset = make_dataset(tmp_path, start_ns, end_ns)
    table, fingerprint = dataset.compute()
    again, fingerprint_again = dataset.compute()
    assert fingerprint == fingerprint_again
    assert len(fingerprint) == 64
    assert table.column_names[:2] == ["symbol", "bar_end_ns"]
    assert "mid_price" in table.column_names
    assert "trade_count_60s" in table.column_names
    assert table.schema.field("mid_price").type == table.schema.field("trade_count_60s").type


def test_trade_count_feature_values_are_exact(tmp_path: Path) -> None:
    start_ns, end_ns = write_trades(tmp_path, count=24)  # 5s spacing
    dataset = make_dataset(tmp_path, start_ns, end_ns)
    table, _ = dataset.compute()
    counts = table.column("trade_count_60s").to_pylist()
    tick_times = [start_ns + index * 5 * SCALE for index in range(24)]
    bars = list(range(start_ns, end_ns + 1, 30 * SCALE))
    expected = [
        sum(1 for ts_ns in tick_times if bar_end - 60 * SCALE <= ts_ns <= bar_end)
        for bar_end in bars
    ]
    assert counts == expected
    assert counts[-1] == 13  # fully covered 60s window at 5s spacing


def test_fingerprint_changes_when_data_changes(tmp_path: Path) -> None:
    start_ns, end_ns = write_trades(tmp_path, count=20)
    dataset = make_dataset(tmp_path, start_ns, end_ns)
    _, before = dataset.compute()
    write_trades(tmp_path, count=40)
    _, after = dataset.compute()
    assert before != after


def test_materialize_writes_parquet_and_spec_json(tmp_path: Path) -> None:
    start_ns, end_ns = write_trades(tmp_path)
    dataset = make_dataset(tmp_path, start_ns, end_ns)
    _, fingerprint = dataset.materialize()
    parquet_path = tmp_path / "features" / f"{fingerprint}.parquet"
    spec_path = tmp_path / "features" / f"{fingerprint}.json"
    assert parquet_path.exists() and spec_path.exists()
    assert pq.read_table(parquet_path).num_rows > 0
    restored = load_dataset_spec(tmp_path, fingerprint)
    assert restored == dataset.spec


def test_load_dataset_spec_missing_fingerprint_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_dataset_spec(tmp_path, "f" * 64)
