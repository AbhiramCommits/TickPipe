"""Tests for sequence tracking and the event-time reorder buffer."""

from __future__ import annotations

from pathlib import Path

import pytest

from tickpipe.core.types import Tick, Trade
from tickpipe.ingest.sequencing import (
    GapEvent,
    OutOfOrderEvent,
    Sequencer,
    SequenceTracker,
)

FIXTURE = Path(__file__).parent / "fixtures" / "replay_hand_built.ndjson"
DEFAULT_WINDOW_NS = 50_000_000


def make_trade(trade_id: str, sequence: int, exchange_ts_ns: int, symbol: str = "BTC-USD") -> Trade:
    return Trade(
        symbol=symbol,
        exchange_ts_ns=exchange_ts_ns,
        ingest_ts_ns=exchange_ts_ns + 100,
        price="42000",
        size="1",
        trade_id=trade_id,
        sequence=sequence,
    )


def replay_fixture() -> list[Tick]:
    from tickpipe.ingest.sources import parse_ndjson_line

    return [parse_ndjson_line(line) for line in FIXTURE.read_text().splitlines()]


def test_gap_duplicate_and_out_of_order_counts_are_exact() -> None:
    sequencer = Sequencer()
    events = []
    for msg in replay_fixture():
        events.extend(sequencer.process(msg).events)
    gaps = [e for e in events if isinstance(e, GapEvent)]
    out_of_orders = [e for e in events if isinstance(e, OutOfOrderEvent)]
    assert len(gaps) == 1
    assert len(out_of_orders) == 2
    assert gaps[0].symbol == "BTC-USD"
    assert gaps[0].expected_sequence == 3
    assert gaps[0].received_sequence == 5
    assert gaps[0].gap_size == 2
    assert [e.received_sequence for e in out_of_orders] == [4, 3]


def test_duplicate_produces_no_event() -> None:
    tracker = SequenceTracker("BTC-USD", DEFAULT_WINDOW_NS)
    tracker.process(make_trade("a", 1, 1_000))
    tracker.process(make_trade("b", 2, 2_000))
    result = tracker.process(make_trade("dup", 2, 3_000))
    assert result.events == []


def test_sequence_zero_bypasses_tracking() -> None:
    tracker = SequenceTracker("BTC-USD", DEFAULT_WINDOW_NS)
    tracker.process(make_trade("a", 5, 1_000))
    result = tracker.process(make_trade("b", 0, 2_000))
    assert result.events == []
    assert tracker.high_water_sequence == 5


def test_tracking_is_per_symbol() -> None:
    sequencer = Sequencer()
    sequencer.process(make_trade("a", 1, 1_000, symbol="A"))
    sequencer.process(make_trade("b", 1, 1_000, symbol="B"))
    result = sequencer.process(make_trade("c", 9, 2_000, symbol="A"))
    assert len(result.events) == 1
    assert isinstance(result.events[0], GapEvent)
    result_b = sequencer.process(make_trade("d", 2, 2_000, symbol="B"))
    assert result_b.events == []


def test_reorder_buffer_emits_in_exchange_timestamp_order() -> None:
    sequencer = Sequencer(DEFAULT_WINDOW_NS)
    for msg in replay_fixture():
        result = sequencer.process(msg)
        assert result.ready == []  # 6 ms spread < 50 ms window: nothing expires
    flushed = sequencer.flush()
    assert [m.trade_id for m in flushed] == ["t-1", "t-2", "t-3", "t-4", "t-5", "t-5-dup", "t-6"]


def test_reorder_window_flushes_expired_messages_progressively() -> None:
    sequencer = Sequencer(DEFAULT_WINDOW_NS)
    first = sequencer.process(make_trade("a", 1, 0))
    second = sequencer.process(make_trade("b", 2, 1_000_000))
    assert first.ready == [] and second.ready == []
    third = sequencer.process(make_trade("c", 3, 60_000_000))
    assert [m.trade_id for m in third.ready] == ["a", "b"]
    assert [m.trade_id for m in sequencer.flush()] == ["c"]


def test_out_of_order_arrival_counts_as_reordered() -> None:
    tracker = SequenceTracker("BTC-USD", DEFAULT_WINDOW_NS)
    tracker.process(make_trade("a", 1, 100))
    result = tracker.process(make_trade("b", 2, 50))
    assert result.reordered is True
    assert tracker.reordered_count == 1
    assert [m.trade_id for m in tracker.flush()] == ["b", "a"]


def test_reorder_window_must_be_non_negative() -> None:
    with pytest.raises(ValueError):
        SequenceTracker("BTC-USD", -1)
