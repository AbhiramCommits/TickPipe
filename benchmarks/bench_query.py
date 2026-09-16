"""Measure store query latency over a partitioned dataset.

Writes 64 partitions (2 symbols x 32 dates, 5k trades each), then times
single-symbol single-day scans (pruned) and full scans over repeated runs,
reporting p50/p99 wall-clock latency.
"""

from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path

from benchmarks._util import report
from tickpipe.core.types import Trade
from tickpipe.store.reader import TickStore
from tickpipe.store.writer import PartitionedWriter, date_str_to_ns

SYMBOLS = ("AAA-BBB", "CCC-DDD")
DATES = 32
ROWS_PER_PARTITION = 5_000
RUNS = 30
BASE_NS = date_str_to_ns("2024-01-01")
DAY_NS = 86_400 * 10**9


def write_partitions(data_dir: Path) -> int:
    writer = PartitionedWriter(data_dir, "trades", max_rows_per_file=50_000, max_open_s=None)
    for symbol in SYMBOLS:
        for day in range(DATES):
            for row in range(ROWS_PER_PARTITION):
                writer.write(
                    Trade(
                        symbol=symbol,
                        exchange_ts_ns=BASE_NS + day * DAY_NS + row * 10**9,
                        ingest_ts_ns=BASE_NS + day * DAY_NS + row * 10**9 + 1,
                        price=Decimal("42000.5"),
                        size=Decimal("0.1"),
                        trade_id=f"{symbol}-{day}-{row}",
                        sequence=row + 1,
                    )
                )
    manifests = writer.flush()
    return len(manifests)


def measure(
    store: TickStore,
    symbols: list[str] | None,
    start_ns: int | None,
    end_ns: int | None,
) -> list[float]:
    latencies = []
    for _ in range(RUNS):
        begin = time.perf_counter()
        store.scan(symbols=symbols, start_ns=start_ns, end_ns=end_ns)
        latencies.append((time.perf_counter() - begin) * 1_000)
    return latencies


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    index = int(q / 100 * (len(ordered) - 1))
    return ordered[index]


def main() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        partition_count = write_partitions(data_dir)
        store = TickStore(data_dir, "trades")
        measure(store, None, 0, None)  # warmup
        pruned = measure(store, ["AAA-BBB"], BASE_NS, BASE_NS + DAY_NS)
        print(f"partitions: {partition_count}, files: {store.count_files_scanned(None, 0, None)}")
        report(
            "query_pruned_p50_ms_over_64_partitions",
            percentile(pruned, 50),
            "ms",
            f"pruned query p50 over {partition_count} partitions",
        )
        full = measure(store, None, 0, None)
        report(
            "query_full_p50_ms_over_64_partitions",
            percentile(full, 50),
            "ms",
            f"full-scan query p50 over {partition_count} partitions",
        )


if __name__ == "__main__":
    main()
