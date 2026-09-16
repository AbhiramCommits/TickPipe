"""Tests for core domain types and the native extension."""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

import tickpipe._ext as _ext
from tickpipe.core import BookDelta, Trade

TS = 1_700_000_000_000_000_000


def test_native_extension_builds_and_imports() -> None:
    assert _ext.__version__ == "0.1.0"
    assert _ext.add_ns(TS, 42) == TS + 42


def test_trade_accepts_valid_inputs() -> None:
    trade = Trade(
        symbol="BTC-USD",
        exchange_ts_ns=TS,
        ingest_ts_ns=TS + 1,
        price=Decimal("42000.5"),
        size=Decimal("0.01"),
        trade_id="t-1",
        sequence=1,
    )
    assert trade.price == Decimal("42000.5")
    assert trade.exchange_ts_ns == TS


def test_trade_is_frozen() -> None:
    trade = Trade(
        symbol="BTC-USD",
        exchange_ts_ns=TS,
        ingest_ts_ns=TS + 1,
        price=Decimal("42000"),
        size=Decimal("1"),
        trade_id="t-1",
        sequence=1,
    )
    with pytest.raises(ValidationError):
        trade.sequence = 2


def test_trade_rejects_negative_timestamps() -> None:
    with pytest.raises(ValidationError):
        Trade(
            symbol="BTC-USD",
            exchange_ts_ns=-1,
            ingest_ts_ns=TS,
            price=Decimal("42000"),
            size=Decimal("1"),
            trade_id="t-1",
            sequence=1,
        )


def test_book_delta_zero_size_removes_level() -> None:
    delta = BookDelta(
        symbol="BTC-USD",
        exchange_ts_ns=TS,
        ingest_ts_ns=TS + 1,
        side="bid",
        price=Decimal("42000"),
        size=Decimal("0"),
        sequence=1,
    )
    assert delta.size == 0


def test_book_delta_rejects_unknown_side() -> None:
    with pytest.raises(ValidationError):
        BookDelta(
            symbol="BTC-USD",
            exchange_ts_ns=TS,
            ingest_ts_ns=TS + 1,
            side="mid",
            price=Decimal("42000"),
            size=Decimal("1"),
            sequence=1,
        )


@given(
    symbol=st.text(min_size=1, max_size=32),
    price=st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal("1000000")),
    size=st.decimals(min_value=Decimal("0"), max_value=Decimal("1000")),
    sequence=st.integers(min_value=0, max_value=2**63 - 1),
)
def test_trade_accepts_arbitrary_valid_inputs(
    symbol: str, price: Decimal, size: Decimal, sequence: int
) -> None:
    trade = Trade(
        symbol=symbol,
        exchange_ts_ns=TS,
        ingest_ts_ns=TS + 1,
        price=price,
        size=size,
        trade_id=f"t-{sequence}",
        sequence=sequence,
    )
    assert trade.price == price
    assert trade.sequence == sequence
