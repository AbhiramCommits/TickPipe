"""Core market-data domain types.

Conventions
-----------
Timestamps
    All timestamps are signed 64-bit integers counting nanoseconds since the
    Unix epoch (1970-01-01T00:00:00Z). ``exchange_ts_ns`` is the timestamp
    assigned by the venue; ``ingest_ts_ns`` is the time the tick entered this
    system, stamped by the ingestor as early in the pipeline as possible.

Prices
    Prices are carried as ``decimal.Decimal`` at the API boundary so user code
    never loses precision to binary floating point. Internally — on the write
    path to Parquet/Arrow, in the C++ extension, and in any tick arithmetic —
    values must be converted to integer ticks: ``ticks = price * 10**price_scale``
    for an instrument with ``price_scale`` decimal places. The ``price_scale``
    must travel with the data so ticks can be converted back losslessly.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Side = Literal["bid", "ask"]


class Trade(BaseModel):
    """A single executed trade."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    exchange_ts_ns: int = Field(ge=0, description="Venue timestamp, int64 ns UTC")
    ingest_ts_ns: int = Field(ge=0, description="Local ingest timestamp, int64 ns UTC")
    price: Decimal = Field(gt=0, description="Trade price, decimal at the boundary")
    size: Decimal = Field(ge=0, description="Trade quantity in base units")
    trade_id: str
    sequence: int = Field(ge=0, description="Monotonic venue sequence number")


class BookDelta(BaseModel):
    """An incremental order-book update for a single price level."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    exchange_ts_ns: int = Field(ge=0, description="Venue timestamp, int64 ns UTC")
    ingest_ts_ns: int = Field(ge=0, description="Local ingest timestamp, int64 ns UTC")
    side: Side
    price: Decimal = Field(gt=0, description="Level price, decimal at the boundary")
    size: Decimal = Field(ge=0, description="Level size; zero removes the level")
    sequence: int = Field(ge=0, description="Monotonic venue sequence number")


__all__ = ["BookDelta", "Side", "Trade"]
