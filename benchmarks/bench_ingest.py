"""Measure end-to-end ingest pipeline throughput (messages/sec).

Replays a synthetic NDJSON fixture through the real pipeline
(source -> sequencing -> bounded queue -> batching writer) with a fast
in-memory writer, measuring the full message path including JSON parsing
and pydantic validation.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import time
from collections.abc import Sequence
from pathlib import Path

from benchmarks._util import report
from tickpipe.core.types import Tick
from tickpipe.ingest.pipeline import BackpressurePolicy, IngestPipeline
from tickpipe.ingest.sources import ReplayFileSource

MESSAGE_COUNT = 200_000
BASE_TS_NS = 1_700_000_000_000_000_000


class CountingWriter:
    """Fast in-memory writer that only counts messages."""

    def __init__(self) -> None:
        self.written = 0

    async def open(self) -> None:
        return None

    async def write(self, batch: Sequence[Tick]) -> None:
        self.written += len(batch)

    async def close(self) -> None:
        return None


def make_fixture(path: Path) -> None:
    lines = []
    for index in range(MESSAGE_COUNT):
        lines.append(
            json.dumps(
                {
                    "type": "trade",
                    "symbol": "BTC-USD",
                    "exchange_ts_ns": BASE_TS_NS + index,
                    "ingest_ts_ns": BASE_TS_NS + index + 1,
                    "price": "42000.5",
                    "size": "0.1",
                    "trade_id": f"t-{index}",
                    "sequence": index + 1,
                }
            )
        )
    path.write_text("\n".join(lines) + "\n")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        fixture = Path(tmp) / "ticks.ndjson"
        make_fixture(fixture)
        writer = CountingWriter()
        pipeline = IngestPipeline(
            ReplayFileSource(fixture),
            writer,
            queue_maxsize=4096,
            policy=BackpressurePolicy.BLOCK,
            batch_size=1000,
            reorder_window_ns=0,
            metrics_log_interval_s=3600.0,
        )
        start = time.perf_counter()
        metadata = asyncio.run(pipeline.run())
        elapsed = time.perf_counter() - start
        assert metadata.messages_written == MESSAGE_COUNT
    report(
        "ingest_msgs_per_sec",
        MESSAGE_COUNT / elapsed,
        "msgs/sec",
        "ingest throughput",
    )


if __name__ == "__main__":
    main()
