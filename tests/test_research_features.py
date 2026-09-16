"""Tests for the feature registry and point-in-time feature functions."""

from __future__ import annotations

import math
from decimal import Decimal
from pathlib import Path

import pytest

from tickpipe.backtest.portfolio import YEAR_NS
from tickpipe.core.types import BookDelta, Tick, Trade
from tickpipe.research.features import (
    FEATURE_REGISTRY,
    FeatureContext,
    resolve_feature,
)
from tickpipe.store.pit import LookaheadError, PointInTimeView
from tickpipe.store.reader import TickStore
from tickpipe.store.writer import PartitionedWriter

SCALE = 10**9
BASE_NS = 1_700_000_000_000_000_000


def make_trade(price: Decimal, ts_ns: int, size: Decimal = Decimal("1")) -> Trade:
    return Trade(
        symbol="BTC-USD",
        exchange_ts_ns=ts_ns,
        ingest_ts_ns=ts_ns + 1,
        price=price,
        size=size,
        trade_id=f"t-{ts_ns}",
        sequence=ts_ns,
    )


def make_delta(side: str, price: Decimal, size: Decimal, ts_ns: int) -> BookDelta:
    return BookDelta(
        symbol="BTC-USD",
        exchange_ts_ns=ts_ns,
        ingest_ts_ns=ts_ns + 1,
        side=side,
        price=price,
        size=size,
        sequence=ts_ns,
    )


def make_view(tmp_path: Path, ticks: list[Tick]) -> PointInTimeView:
    writer = PartitionedWriter(tmp_path, "trades", max_open_s=None)
    writer.write_batch(ticks)
    writer.flush()
    return PointInTimeView(
        TickStore(tmp_path, "trades"), as_of_ns=max(t.exchange_ts_ns for t in ticks)
    )


def test_mid_price_from_trades(tmp_path: Path) -> None:
    view = make_view(
        tmp_path,
        [make_trade(Decimal("100"), BASE_NS), make_trade(Decimal("101"), BASE_NS + 10)],
    )
    context = FeatureContext(view, "BTC-USD", BASE_NS + 10, lookback_ns=60 * SCALE)
    assert FEATURE_REGISTRY["mid_price"].compute(context) == 101.0


def test_mid_price_prefers_book_over_trades(tmp_path: Path) -> None:
    view = make_view(
        tmp_path,
        [
            make_trade(Decimal("105"), BASE_NS),
            make_delta("bid", Decimal("99"), Decimal("1"), BASE_NS + 10),
            make_delta("ask", Decimal("101"), Decimal("1"), BASE_NS + 10),
        ],
    )
    context = FeatureContext(view, "BTC-USD", BASE_NS + 10, lookback_ns=60 * SCALE)
    assert FEATURE_REGISTRY["mid_price"].compute(context) == 100.0


def test_spread_bps_hand_computed(tmp_path: Path) -> None:
    view = make_view(
        tmp_path,
        [
            make_delta("bid", Decimal("99"), Decimal("1"), BASE_NS),
            make_delta("ask", Decimal("101"), Decimal("1"), BASE_NS),
        ],
    )
    context = FeatureContext(view, "BTC-USD", BASE_NS, lookback_ns=60 * SCALE)
    assert FEATURE_REGISTRY["spread_bps"].compute(context) == pytest.approx(200.0)


def test_order_flow_imbalance_hand_computed(tmp_path: Path) -> None:
    view = make_view(
        tmp_path,
        [
            make_trade(Decimal("100"), BASE_NS, size=Decimal("1")),
            make_trade(Decimal("101"), BASE_NS + 1, size=Decimal("2")),
            make_trade(Decimal("102"), BASE_NS + 2, size=Decimal("3")),
        ],
    )
    context = FeatureContext(view, "BTC-USD", BASE_NS + 2, lookback_ns=60 * SCALE)
    # signs: +1, +1 -> (2 + 3) / 6
    assert FEATURE_REGISTRY["order_flow_imbalance"].compute(context) == pytest.approx(5 / 6)


def test_realized_vol_hand_computed(tmp_path: Path) -> None:
    prices = [Decimal("100"), Decimal("101"), Decimal("100.5")]
    view = make_view(tmp_path, [make_trade(p, BASE_NS + i) for i, p in enumerate(prices)])
    lookback_ns = 60 * SCALE
    context = FeatureContext(view, "BTC-USD", BASE_NS + 2, lookback_ns=lookback_ns)
    r1 = math.log(1.01)
    r2 = math.log(100.5 / 101)
    mean = (r1 + r2) / 2
    std = math.sqrt(((r1 - mean) ** 2 + (r2 - mean) ** 2) / 1)
    expected = std * math.sqrt(YEAR_NS / lookback_ns)
    assert FEATURE_REGISTRY["realized_vol"].compute(context) == pytest.approx(expected, rel=1e-12)


def test_trade_count_and_windowed_resolution(tmp_path: Path) -> None:
    view = make_view(
        tmp_path, [make_trade(Decimal("100"), BASE_NS + i) for i in range(5)]
    )
    context = FeatureContext(view, "BTC-USD", BASE_NS + 4, lookback_ns=60 * SCALE)
    assert FEATURE_REGISTRY["trade_count"].compute(context) == 5.0
    windowed = resolve_feature("trade_count", {"window_s": 120})
    assert windowed.name == "trade_count_120s"
    assert windowed.lookback_ns == 120 * SCALE
    assert windowed.version == FEATURE_REGISTRY["trade_count"].version


def test_context_rejects_windows_past_the_bar(tmp_path: Path) -> None:
    view = make_view(tmp_path, [make_trade(Decimal("100"), BASE_NS)])
    context = FeatureContext(view, "BTC-USD", BASE_NS, lookback_ns=60 * SCALE)
    with pytest.raises(LookaheadError):
        context.scan(start_ns=BASE_NS + 1)


def test_context_clamps_scan_to_bar_as_of(tmp_path: Path) -> None:
    view = make_view(
        tmp_path,
        [make_trade(Decimal("100"), BASE_NS), make_trade(Decimal("101"), BASE_NS + 100)],
    )
    context = FeatureContext(view, "BTC-USD", BASE_NS + 10, lookback_ns=60 * SCALE)
    table = context.scan()
    timestamps = table.column("exchange_ts_ns").to_pylist()
    assert timestamps == [BASE_NS]  # the later trade is hidden


def test_unknown_feature_raises() -> None:
    with pytest.raises(KeyError):
        resolve_feature("does_not_exist")


def test_duplicate_registration_raises() -> None:
    from tickpipe.research.features import register_feature

    with pytest.raises(ValueError):
        register_feature("mid_price", version=2, lookback_ns=0)(lambda ctx: 0.0)
