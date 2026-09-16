"""Reference strategies for the backtest engine."""

from __future__ import annotations

from collections import deque
from decimal import Decimal

from tickpipe.backtest.events import FillEvent
from tickpipe.backtest.strategy import Strategy
from tickpipe.store.pit import PointInTimeView
from tickpipe.store.writer import decimal_to_ticks


class RollingZScoreMeanReversion(Strategy):
    """Reference mean-reversion strategy on a rolling z-score of trade prices.

    Long-only: when the rolling z-score of the last ``window`` trade prices
    falls to or below ``-entry_z`` while flat, buy ``order_size``; when the
    z-score rises to or above ``exit_z`` while long, sell the position.
    """

    def __init__(
        self,
        view: PointInTimeView,
        *,
        symbol: str,
        window: int = 20,
        entry_z: float = 1.0,
        exit_z: float = 0.0,
        order_size: Decimal = Decimal("1"),
    ) -> None:
        super().__init__(view)
        if window < 2:
            raise ValueError("window must be at least 2")
        if entry_z < 0 or exit_z < 0:
            raise ValueError("z-score thresholds must be non-negative")
        self.symbol = symbol
        self.window = window
        self.entry_z = entry_z
        self.exit_z = exit_z
        self.order_size_ticks = decimal_to_ticks(order_size)
        self._prices: deque[int] = deque(maxlen=window)
        self._position_ticks = 0

    def on_trade(
        self,
        symbol: str,
        exchange_ts_ns: int,
        price_ticks: int,
        size_ticks: int,
        trade_id: str,
    ) -> None:
        if symbol != self.symbol:
            return
        self._prices.append(price_ticks)
        if len(self._prices) < self.window:
            return
        mean = sum(self._prices) / len(self._prices)
        variance = sum((price - mean) ** 2 for price in self._prices) / len(self._prices)
        std = variance**0.5
        if std == 0:
            return
        z_score = (price_ticks - mean) / std
        if self._position_ticks == 0 and z_score <= -self.entry_z:
            self.submit_order(self.symbol, "buy", self.order_size_ticks)
        elif self._position_ticks > 0 and z_score >= self.exit_z:
            self.submit_order(self.symbol, "sell", self._position_ticks)

    def on_book_delta(
        self,
        symbol: str,
        exchange_ts_ns: int,
        side: str,
        price_ticks: int,
        size_ticks: int,
    ) -> None:
        return None

    def on_fill(self, fill: FillEvent) -> None:
        if fill.symbol != self.symbol:
            return
        if fill.side == "buy":
            self._position_ticks += fill.size_ticks
        else:
            self._position_ticks -= fill.size_ticks


__all__ = ["RollingZScoreMeanReversion"]
