"""Deterministic sample market data for demos, quickstart, and benchmarks."""

from __future__ import annotations

import shutil
from decimal import Decimal
from pathlib import Path

import numpy as np

from tickpipe.core.types import Trade
from tickpipe.store.writer import PartitionedWriter, date_str_to_ns

DEFAULT_SYMBOL = "BTC-USD"
DEFAULT_COUNT = 500
DEFAULT_SEED = 7
TICK_SPACING_NS = 5 * 10**9


def generate_sample_trades(
    data_dir: str | Path,
    *,
    dataset: str = "trades",
    symbol: str = DEFAULT_SYMBOL,
    count: int = DEFAULT_COUNT,
    seed: int = DEFAULT_SEED,
    force: bool = False,
) -> int:
    """Write ``count`` deterministic trades for ``symbol`` into the store.

    Returns the number of trades written (0 when data already exists and
    ``force`` is false). Prices follow a seeded geometric random walk.
    """
    data_dir = Path(data_dir)
    dataset_path = data_dir / dataset
    if dataset_path.exists() and not force:
        return 0
    if force:
        shutil.rmtree(dataset_path, ignore_errors=True)
    rng = np.random.default_rng(seed)
    base_ts_ns = date_str_to_ns("2024-01-01")
    writer = PartitionedWriter(data_dir, dataset, max_open_s=None)
    price = 100.0
    for index in range(count):
        ts_ns = base_ts_ns + index * TICK_SPACING_NS
        price *= 1 + rng.normal(0, 1e-3)
        writer.write(
            Trade(
                symbol=symbol,
                exchange_ts_ns=ts_ns,
                ingest_ts_ns=ts_ns + 1,
                price=Decimal(f"{price:.6f}"),
                size=Decimal("1"),
                trade_id=f"t-{index}",
                sequence=index + 1,
            )
        )
    writer.flush()
    return count


__all__ = ["generate_sample_trades"]
