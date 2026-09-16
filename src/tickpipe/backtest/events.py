"""Shared event types for the backtest layer."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

OrderSide = Literal["buy", "sell"]


class OrderHandle(BaseModel):
    """Handle returned by :meth:`Strategy.submit_order`."""

    model_config = ConfigDict(frozen=True)

    order_id: int
    symbol: str
    side: OrderSide
    size_ticks: int
    submitted_ts_ns: int


class FillEvent(BaseModel):
    """One simulated fill, in integer ticks."""

    model_config = ConfigDict(frozen=True)

    order_id: int
    symbol: str
    side: OrderSide
    size_ticks: int
    tick_price_ticks: int
    fill_price_ticks: int
    slippage_ticks: int
    trade_ts_ns: int


__all__ = ["FillEvent", "OrderHandle", "OrderSide"]
