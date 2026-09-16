"""Tests for market-data sources (no network access)."""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path

import pytest

from tickpipe.core.types import BookDelta, Trade
from tickpipe.ingest.sources import (
    CoinbaseWebSocketSource,
    ReplayFileSource,
    iso_8601_to_utc_ns,
    parse_coinbase_message,
)

FIXTURE = Path(__file__).parent / "fixtures" / "replay_hand_built.ndjson"

MATCH = (
    '{"type":"match","trade_id":12345,"sequence":10250,'
    '"maker_order_id":"m-1","taker_order_id":"k-1","side":"buy",'
    '"time":"2021-01-01T12:00:00.123456Z","product_id":"BTC-USD",'
    '"size":"0.0100","price":"42000.50"}'
)

L2UPDATE = (
    '{"type":"l2update","product_id":"BTC-USD","time":"2021-01-01T12:00:01.000000Z",'
    '"changes":[["buy","42000.00","0.5"],["sell","42100.00","1.25"]]}'
)

SNAPSHOT = (
    '{"type":"snapshot","product_id":"BTC-USD",'
    '"bids":[["42000.00","0.5"]],"asks":[["42100.00","1.25"]]}'
)


def test_iso_8601_to_utc_ns_is_exact() -> None:
    assert iso_8601_to_utc_ns("2021-01-01T12:00:00.123456Z") == 1_609_502_400_123_456_000


def test_replay_source_reads_hand_built_fixture() -> None:
    async def read_all() -> list[Trade]:
        return [msg async for msg in ReplayFileSource(FIXTURE).stream()]

    ticks = asyncio.run(read_all())
    assert len(ticks) == 7
    assert all(isinstance(t, Trade) for t in ticks)
    assert ticks[0].trade_id == "t-1"
    assert ticks[0].price == Decimal("42000.5")
    assert [t.sequence for t in ticks] == [1, 2, 5, 4, 3, 5, 6]
    assert all(t.ingest_ts_ns > t.exchange_ts_ns for t in ticks)


def test_replay_source_parses_book_deltas(tmp_path: Path) -> None:
    fixture = tmp_path / "book.ndjson"
    fixture.write_text(
        '{"type": "book_delta", "symbol": "BTC-USD", "exchange_ts_ns": 1, '
        '"ingest_ts_ns": 2, "side": "bid", "price": "42000", "size": "0", "sequence": 1}\n'
    )

    async def read_all() -> list[BookDelta]:
        return [msg async for msg in ReplayFileSource(fixture).stream()]

    ticks = asyncio.run(read_all())
    assert ticks[0].side == "bid"
    assert ticks[0].size == Decimal("0")


def test_replay_source_raises_on_invalid_line(tmp_path: Path) -> None:
    fixture = tmp_path / "bad.ndjson"
    fixture.write_text('{"type": "unknown", "symbol": "BTC-USD"}\n')

    async def read_all() -> None:
        async for _ in ReplayFileSource(fixture).stream():
            pass

    with pytest.raises(ValueError, match="line 1"):
        asyncio.run(read_all())


def test_parse_coinbase_match() -> None:
    ticks = parse_coinbase_message(MATCH, ingest_ts_ns=1_609_502_400_123_456_100)
    assert len(ticks) == 1
    trade = ticks[0]
    assert isinstance(trade, Trade)
    assert trade.symbol == "BTC-USD"
    assert trade.exchange_ts_ns == 1_609_502_400_123_456_000
    assert trade.ingest_ts_ns == 1_609_502_400_123_456_100
    assert trade.price == Decimal("42000.50")
    assert trade.size == Decimal("0.0100")
    assert trade.trade_id == "12345"
    assert trade.sequence == 10250


def test_parse_coinbase_l2update() -> None:
    ticks = parse_coinbase_message(L2UPDATE, ingest_ts_ns=1_609_502_401_000_000_100)
    assert len(ticks) == 2
    bid, ask = ticks
    assert isinstance(bid, BookDelta) and isinstance(ask, BookDelta)
    assert bid.side == "bid" and ask.side == "ask"
    assert bid.price == Decimal("42000.00") and ask.price == Decimal("42100.00")
    assert bid.size == Decimal("0.5") and ask.size == Decimal("1.25")
    assert bid.sequence == 0 and ask.sequence == 0  # Coinbase l2 has no venue sequence
    assert bid.exchange_ts_ns == 1_609_502_401_000_000_000


def test_parse_coinbase_snapshot() -> None:
    ticks = parse_coinbase_message(SNAPSHOT, ingest_ts_ns=42)
    assert len(ticks) == 2
    assert all(t.sequence == 0 for t in ticks)
    assert all(t.exchange_ts_ns == 42 for t in ticks)  # snapshot carries no venue time
    assert [t.side for t in ticks] == ["bid", "ask"]


def test_parse_coinbase_unrelated_message_yields_nothing() -> None:
    assert parse_coinbase_message('{"type":"heartbeat"}', ingest_ts_ns=0) == []
    assert parse_coinbase_message('{"type":"subscriptions"}', ingest_ts_ns=0) == []


def test_coinbase_source_subscribe_payload() -> None:
    source = CoinbaseWebSocketSource(["BTC-USD", "ETH-USD"])
    payload = json.loads(source.subscribe_payload)
    assert payload["type"] == "subscribe"
    assert payload["product_ids"] == ["BTC-USD", "ETH-USD"]
    assert payload["channels"] == ["matches", "level2_batch"]


def test_coinbase_source_requires_symbols() -> None:
    with pytest.raises(ValueError):
        CoinbaseWebSocketSource([])
