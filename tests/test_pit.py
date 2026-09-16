"""Point-in-time view tests, including the hypothesis lookahead property."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tickpipe.core.types import BookDelta, Tick, Trade
from tickpipe.store.pit import LookaheadError, PointInTimeView
from tickpipe.store.reader import TickStore
from tickpipe.store.writer import PartitionedWriter, date_str_to_ns

BASE_NS = date_str_to_ns("2021-01-01")
SPAN_NS = 10 * 86_400 * 10**9

trade_strategy = st.builds(
    Trade,
    symbol=st.sampled_from(["AAA-BBB", "CCC-DDD"]),
    exchange_ts_ns=st.integers(min_value=BASE_NS, max_value=BASE_NS + SPAN_NS),
    ingest_ts_ns=st.integers(min_value=BASE_NS, max_value=BASE_NS + SPAN_NS),
    price=st.decimals(min_value="0.00000001", max_value="1000000", places=6),
    size=st.decimals(min_value="0", max_value="1000", places=6),
    trade_id=st.text(
        min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N"))
    ),
    sequence=st.integers(min_value=1, max_value=2**40),
)

book_delta_strategy = st.builds(
    BookDelta,
    symbol=st.sampled_from(["AAA-BBB", "CCC-DDD"]),
    exchange_ts_ns=st.integers(min_value=BASE_NS, max_value=BASE_NS + SPAN_NS),
    ingest_ts_ns=st.integers(min_value=BASE_NS, max_value=BASE_NS + SPAN_NS),
    side=st.sampled_from(["bid", "ask"]),
    price=st.decimals(min_value="0.00000001", max_value="1000000", places=6),
    size=st.decimals(min_value="0", max_value="1000", places=6),
    sequence=st.integers(min_value=0, max_value=2**40),
)

ticks_strategy = st.lists(st.one_of(trade_strategy, book_delta_strategy), min_size=1, max_size=40)

as_of_strategy = st.integers(min_value=BASE_NS, max_value=BASE_NS + SPAN_NS)


def _reset(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


@given(ticks=ticks_strategy, as_of=as_of_strategy)
@settings(
    deadline=None,
    max_examples=60,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_pit_view_never_returns_rows_past_as_of(
    tmp_path: Path, ticks: list[Tick], as_of: int
) -> None:
    _reset(tmp_path)
    writer = PartitionedWriter(tmp_path, "trades", max_open_s=None)
    writer.write_batch(ticks)
    writer.flush()

    view = PointInTimeView(TickStore(tmp_path, "trades"), as_of_ns=as_of)
    table = view.scan(symbols=None, start_ns=0, end_ns=None)
    timestamps = table.column("exchange_ts_ns").to_pylist()

    assert all(ts <= as_of for ts in timestamps)
    expected = sorted(t.exchange_ts_ns for t in ticks if t.exchange_ts_ns <= as_of)
    assert sorted(timestamps) == expected


@given(ticks=ticks_strategy, as_of=as_of_strategy)
@settings(
    deadline=None,
    max_examples=40,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_pit_view_narrowed_by_caller_as_of_still_clamped(
    tmp_path: Path, ticks: list[Tick], as_of: int
) -> None:
    _reset(tmp_path)
    writer = PartitionedWriter(tmp_path, "trades", max_open_s=None)
    writer.write_batch(ticks)
    writer.flush()

    view = PointInTimeView(TickStore(tmp_path, "trades"), as_of_ns=as_of)
    narrowed = max(0, as_of - SPAN_NS // 2)
    table = view.scan(symbols=None, start_ns=0, end_ns=None, as_of_ns=narrowed)
    timestamps = table.column("exchange_ts_ns").to_pylist()
    assert all(ts <= narrowed for ts in timestamps)


def _write_single_tick(tmp_path: Path, tick: Tick) -> PointInTimeView:
    writer = PartitionedWriter(tmp_path, "trades", max_open_s=None)
    writer.write(tick)
    writer.flush()
    return PointInTimeView(TickStore(tmp_path, "trades"), as_of_ns=tick.exchange_ts_ns)


def _tick(ts_ns: int) -> Trade:
    return Trade(
        symbol="BTC-USD",
        exchange_ts_ns=ts_ns,
        ingest_ts_ns=ts_ns + 1,
        price="100",
        size="1",
        trade_id=str(ts_ns),
        sequence=1,
    )


def test_pit_raises_lookahead_when_passing_later_as_of(tmp_path: Path) -> None:
    view = _write_single_tick(tmp_path, _tick(1_000))
    with pytest.raises(LookaheadError):
        view.scan(symbols=None, start_ns=0, end_ns=None, as_of_ns=2_000)


def test_pit_allows_narrowing_as_of(tmp_path: Path) -> None:
    view = _write_single_tick(tmp_path, _tick(1_000))
    table = view.scan(symbols=None, start_ns=0, end_ns=None, as_of_ns=500)
    assert table.num_rows == 0
    same = view.scan(symbols=None, start_ns=0, end_ns=None, as_of_ns=1_000)
    assert same.num_rows == 1


def test_pit_clamps_end_ns_beyond_as_of_inclusively(tmp_path: Path) -> None:
    view = _write_single_tick(tmp_path, _tick(1_000))
    table = view.scan(symbols=None, start_ns=0, end_ns=None)
    assert table.num_rows == 1  # ts == as_of is visible
    beyond = view.scan(symbols=None, start_ns=0, end_ns=10_000)
    assert beyond.num_rows == 1


def test_pit_hides_rows_written_with_newer_exchange_ts(tmp_path: Path) -> None:
    writer = PartitionedWriter(tmp_path, "trades", max_open_s=None)
    writer.write_batch([_tick(1_000), _tick(5_000)])
    writer.flush()
    view = PointInTimeView(TickStore(tmp_path, "trades"), as_of_ns=1_000)
    table = view.scan(symbols=None, start_ns=0, end_ns=None)
    assert table.column("exchange_ts_ns").to_pylist() == [1_000]


def test_pit_rejects_negative_as_of(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        PointInTimeView(TickStore(tmp_path, "trades"), as_of_ns=-1)
