"""Measure C++17 replay engine throughput (ticks/sec).

Replays a 5M-tick contiguous buffer through the pybind11 engine with the GIL
released; the measured rate is the full ingest-plus-dispatch cost per tick.
"""

from __future__ import annotations

import time

import numpy as np

from benchmarks._util import report
from tickpipe.backtest import _replay
from tickpipe.backtest.engine import TICK_RECORD_DTYPE

TICK_COUNT = 5_000_000


def main() -> None:
    array = np.zeros(TICK_COUNT, dtype=TICK_RECORD_DTYPE)
    array["kind"] = 1
    array["exchange_ts_ns"] = np.arange(TICK_COUNT, dtype=np.int64)
    array["sequence"] = np.arange(TICK_COUNT, dtype=np.int64)
    array["price_ticks"] = 100 * 10**9
    array["size_ticks"] = 10**9
    array["trade_id"] = np.arange(TICK_COUNT, dtype=np.int64) + 1
    engine = _replay.ReplayEngine(speed=0.0, tick_scale=10**9)
    engine.set_ticks(array)
    start = time.perf_counter()
    engine.run()
    elapsed = time.perf_counter() - start
    report("replay_ticks_per_sec", TICK_COUNT / elapsed, "ticks/sec", "replay throughput")


if __name__ == "__main__":
    main()
