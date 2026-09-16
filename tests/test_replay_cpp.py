"""Tests for the C++17 replay core (event ordering, matching, throughput)."""

from __future__ import annotations

import random
import time

import numpy as np
import pytest

from tickpipe.backtest import _replay
from tickpipe.backtest.engine import TICK_RECORD_DTYPE

SCALE = 10**9


def make_tick_array(
    exchange_ts_ns: list[int],
    price_ticks: list[int],
    size_ticks: list[int],
    *,
    sequences: list[int] | None = None,
) -> np.ndarray:
    array = np.zeros(len(exchange_ts_ns), dtype=TICK_RECORD_DTYPE)
    array["kind"] = 1
    array["exchange_ts_ns"] = exchange_ts_ns
    array["ingest_ts_ns"] = exchange_ts_ns
    array["sequence"] = sequences if sequences is not None else exchange_ts_ns
    array["price_ticks"] = price_ticks
    array["size_ticks"] = size_ticks
    array["trade_id"] = np.arange(len(exchange_ts_ns), dtype="i8") + 1
    return array


def test_tick_record_layout_matches_cpp() -> None:
    assert TICK_RECORD_DTYPE.itemsize == _replay.TICK_RECORD_ITEMSIZE == 64


def test_event_queue_ordering_is_fully_deterministic_across_runs() -> None:
    rng = random.Random(42)
    events = [(rng.randrange(1_000), rng.randrange(100), rng.randrange(3)) for _ in range(10_000)]
    reference: list[tuple[int, int, int]] | None = None
    for _ in range(100):
        queue = _replay.EventQueue()
        for ts_ns, sequence, rank in events:
            queue.push(ts_ns, sequence, rank)
        order = []
        while not queue.empty():
            order.append(queue.pop())
        if reference is None:
            reference = order
        else:
            assert order == reference


def test_event_queue_documented_tie_break_rule() -> None:
    queue = _replay.EventQueue()
    queue.push(5, 1, 2)
    queue.push(4, 9, 9)
    queue.push(5, 1, 0)
    queue.push(5, 0, 9)
    queue.push(5, 1, 0)  # identical key triple: insertion order breaks the tie
    popped = [queue.pop() for _ in range(5)]
    assert popped == [
        (4, 9, 9),
        (5, 0, 9),
        (5, 1, 0),
        (5, 1, 0),
        (5, 1, 2),
    ]


def test_slippage_matches_hand_computed_fill_prices() -> None:
    engine = _replay.ReplayEngine(
        speed=0.0,
        tick_scale=SCALE,
        order_entry_latency_ns=0,
        slippage_fixed_ticks=10,
        slippage_per_share_ticks=2,
    )
    array = make_tick_array(
        exchange_ts_ns=[1_000, 2_000, 3_000],
        price_ticks=[100 * SCALE, 200 * SCALE, 300 * SCALE],
        size_ticks=[500_000_000, 1_000_000_000, 1_000_000_000],
    )
    engine.set_ticks(array)
    engine.submit_order(0, 1, 2_000_000_000)  # buy 2.0 shares
    engine.submit_order(0, 2, 500_000_000)  # sell 0.5 shares
    engine.run()
    fills = engine.pop_fills()

    # buy 2.0: 0.5 @ tick1 (+11), 1.0 @ tick2 (+12), 0.5 @ tick3 (+11)
    # sell 0.5: takes the tick3 remainder (-11)
    assert [
        (fill.side, fill.size_ticks, fill.tick_price_ticks, fill.fill_price_ticks) for fill in fills
    ] == [
        (1, 500_000_000, 100 * SCALE, 100 * SCALE + 11),
        (1, 1_000_000_000, 200 * SCALE, 200 * SCALE + 12),
        (1, 500_000_000, 300 * SCALE, 300 * SCALE + 11),
        (2, 500_000_000, 300 * SCALE, 300 * SCALE - 11),
    ]


def test_partial_fills_are_capped_by_tick_size() -> None:
    engine = _replay.ReplayEngine(speed=0.0, tick_scale=SCALE)
    array = make_tick_array(
        exchange_ts_ns=[1_000, 2_000],
        price_ticks=[100 * SCALE, 100 * SCALE],
        size_ticks=[600_000_000, 600_000_000],  # 0.6 and 0.6
    )
    engine.set_ticks(array)
    engine.submit_order(0, 1, 1_000_000_000)  # buy 1.0 across two ticks
    engine.run()
    fills = engine.pop_fills()
    assert [fill.size_ticks for fill in fills] == [600_000_000, 400_000_000]
    assert sum(fill.size_ticks for fill in fills) == 1_000_000_000


def test_latency_gate_blocks_ticks_before_submit_plus_latency() -> None:
    engine = _replay.ReplayEngine(speed=0.0, tick_scale=SCALE, order_entry_latency_ns=1_500)
    array = make_tick_array(
        exchange_ts_ns=[1_000, 2_000, 3_000],
        price_ticks=[100 * SCALE] * 3,
        size_ticks=[SCALE] * 3,
    )
    engine.set_ticks(array)
    engine.submit_order(0, 1, 2 * SCALE)  # submitted at engine time 0 (before run)
    engine.run()
    fills = engine.pop_fills()
    assert [fill.trade_ts_ns for fill in fills] == [2_000, 3_000]


def test_engine_rejects_invalid_inputs() -> None:
    engine = _replay.ReplayEngine(speed=0.0, tick_scale=SCALE)
    with pytest.raises(ValueError):
        engine.set_ticks(np.zeros(3, dtype=np.int64))  # wrong itemsize
    with pytest.raises(ValueError):
        engine.submit_order(0, 1, 0)
    with pytest.raises(ValueError):
        engine.submit_order(0, 3, SCALE)


def test_benchmark_replay_exceeds_1m_ticks_per_second() -> None:
    count = 1_000_000
    array = make_tick_array(
        exchange_ts_ns=list(range(count)),
        price_ticks=[100 * SCALE] * count,
        size_ticks=[SCALE] * count,
        sequences=list(range(count)),
    )
    engine = _replay.ReplayEngine(speed=0.0, tick_scale=SCALE)
    engine.set_ticks(array)
    start = time.perf_counter()
    engine.run()
    elapsed = time.perf_counter() - start
    rate = count / elapsed
    print(f"replay throughput: {rate:,.0f} ticks/s")
    assert rate > 1_000_000
