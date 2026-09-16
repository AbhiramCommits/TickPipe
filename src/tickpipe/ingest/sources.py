"""Market-data sources.

Every source implements :class:`MarketDataSource` and yields
:class:`~tickpipe.core.types.Trade` and :class:`~tickpipe.core.types.BookDelta`
ticks. Sources are responsible for stamping ``ingest_ts_ns``.

Coinbase sequence conventions: ``matches`` messages carry a venue sequence;
level-2 messages do not, so their ``sequence`` is set to 0 (reserved for "no
venue sequence" and bypassed by the sequencer). Level-2 snapshots carry no
venue timestamp either, so their ``exchange_ts_ns`` is set to the ingest time
as a best effort.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Protocol

import structlog
from websockets.asyncio.client import connect

from tickpipe.core.types import BookDelta, Side, Tick, Trade

logger = structlog.get_logger(__name__)

DEFAULT_COINBASE_WS_URL: Final[str] = "wss://ws-feed.exchange.coinbase.com"
DEFAULT_COINBASE_CHANNELS: Final[tuple[str, ...]] = ("matches", "level2_batch")

_COINBASE_SIDE: Final[dict[str, Side]] = {"buy": "bid", "sell": "ask"}
_SNAPSHOT_LEVELS: Final[tuple[tuple[Side, str], ...]] = (("bid", "bids"), ("ask", "asks"))

_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)


def iso_8601_to_utc_ns(value: str) -> int:
    """Convert an ISO 8601 timestamp (e.g. ``2021-01-01T12:00:00.123456Z``) to int64 ns UTC."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    delta = dt.astimezone(UTC) - _EPOCH_UTC
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


class MarketDataSource(Protocol):
    """A source of market-data ticks consumed as an async iterator."""

    def stream(self) -> AsyncIterator[Tick]: ...


class CoinbaseWebSocketSource:
    """Streams trades and level-2 book deltas from the Coinbase Exchange public feed.

    Connects without authentication to ``wss://ws-feed.exchange.coinbase.com``,
    subscribes to ``matches`` and ``level2_batch`` for the configured symbols,
    and reconnects with exponential backoff on failure.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        url: str = DEFAULT_COINBASE_WS_URL,
        channels: Sequence[str] = DEFAULT_COINBASE_CHANNELS,
        backoff_base_s: float = 1.0,
        backoff_max_s: float = 30.0,
    ) -> None:
        if not symbols:
            raise ValueError("at least one symbol is required")
        self.symbols = tuple(symbols)
        self.url = url
        self.channels = tuple(channels)
        self.backoff_base_s = backoff_base_s
        self.backoff_max_s = backoff_max_s

    @property
    def subscribe_payload(self) -> str:
        return json.dumps(
            {
                "type": "subscribe",
                "product_ids": list(self.symbols),
                "channels": list(self.channels),
            }
        )

    async def stream(self) -> AsyncIterator[Tick]:
        backoff_s = self.backoff_base_s
        while True:
            try:
                async with connect(self.url) as ws:
                    await ws.send(self.subscribe_payload)
                    logger.info(
                        "coinbase_subscribed",
                        url=self.url,
                        symbols=list(self.symbols),
                        channels=list(self.channels),
                    )
                    backoff_s = self.backoff_base_s
                    async for raw in ws:
                        payload = raw if isinstance(raw, str) else raw.decode("utf-8")
                        for tick in parse_coinbase_message(payload, time.time_ns()):
                            yield tick
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("coinbase_stream_error", error=str(exc), retry_in_s=backoff_s)
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, self.backoff_max_s)


class ReplayFileSource:
    """Replays ticks from a newline-delimited JSON fixture.

    Each line is a JSON object with a ``"type"`` field of ``"trade"`` or
    ``"book_delta"`` plus the fields of the corresponding pydantic model.
    Never touches the network; used for deterministic tests and offline work.
    """

    def __init__(self, path: str | Path, *, emit_delay_s: float = 0.0) -> None:
        self.path = Path(path)
        self.emit_delay_s = emit_delay_s

    async def stream(self) -> AsyncIterator[Tick]:
        with self.path.open() as handle:
            for line_number, line in enumerate(handle, start=1):
                if self.emit_delay_s > 0:
                    await asyncio.sleep(self.emit_delay_s)
                try:
                    yield parse_ndjson_line(line)
                except Exception as exc:
                    raise ValueError(
                        f"invalid replay fixture line {line_number} in {self.path}: {exc}"
                    ) from exc


def parse_ndjson_line(line: str) -> Tick:
    obj = json.loads(line)
    kind = obj.pop("type", None)
    if kind == "trade":
        return Trade.model_validate(obj)
    if kind == "book_delta":
        return BookDelta.model_validate(obj)
    raise ValueError(f"unknown fixture message type {kind!r}")


def parse_coinbase_message(payload: str, ingest_ts_ns: int) -> list[Tick]:
    """Parse one Coinbase Exchange WebSocket message into zero or more ticks."""
    obj = json.loads(payload)
    msg_type = obj.get("type")
    if msg_type == "match":
        return [_trade_from_match(obj, ingest_ts_ns)]
    if msg_type == "l2update":
        return list(_book_deltas_from_l2update(obj, ingest_ts_ns))
    if msg_type == "snapshot":
        return list(_book_deltas_from_snapshot(obj, ingest_ts_ns))
    if msg_type == "error":
        logger.warning("coinbase_feed_error", message=obj.get("message"), reason=obj.get("reason"))
    return []


def _trade_from_match(obj: Any, ingest_ts_ns: int) -> Trade:
    return Trade(
        symbol=str(obj["product_id"]),
        exchange_ts_ns=iso_8601_to_utc_ns(str(obj["time"])),
        ingest_ts_ns=ingest_ts_ns,
        price=Decimal(str(obj["price"])),
        size=Decimal(str(obj["size"])),
        trade_id=str(obj["trade_id"]),
        sequence=int(obj["sequence"]),
    )


def _book_deltas_from_l2update(obj: Any, ingest_ts_ns: int) -> list[BookDelta]:
    exchange_ts_ns = iso_8601_to_utc_ns(str(obj["time"]))
    deltas: list[BookDelta] = []
    for change in obj["changes"]:
        side_raw, price_raw, size_raw = change
        deltas.append(
            BookDelta(
                symbol=str(obj["product_id"]),
                exchange_ts_ns=exchange_ts_ns,
                ingest_ts_ns=ingest_ts_ns,
                side=_COINBASE_SIDE[str(side_raw)],
                price=Decimal(str(price_raw)),
                size=Decimal(str(size_raw)),
                sequence=0,
            )
        )
    return deltas


def _book_deltas_from_snapshot(obj: Any, ingest_ts_ns: int) -> list[BookDelta]:
    deltas: list[BookDelta] = []
    for side, key in _SNAPSHOT_LEVELS:
        for price_raw, size_raw in obj[key]:
            deltas.append(
                BookDelta(
                    symbol=str(obj["product_id"]),
                    exchange_ts_ns=ingest_ts_ns,
                    ingest_ts_ns=ingest_ts_ns,
                    side=side,
                    price=Decimal(str(price_raw)),
                    size=Decimal(str(size_raw)),
                    sequence=0,
                )
            )
    return deltas


__all__ = [
    "CoinbaseWebSocketSource",
    "DEFAULT_COINBASE_CHANNELS",
    "DEFAULT_COINBASE_WS_URL",
    "MarketDataSource",
    "ReplayFileSource",
    "iso_8601_to_utc_ns",
    "parse_coinbase_message",
    "parse_ndjson_line",
]
