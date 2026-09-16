"""Position and PnL accounting in integer ticks.

All quantities are integer ticks (see ``tickpipe.store.writer.TICK_SCALE``).
Notional values are computed as ``size_ticks * price_ticks // TICK_SCALE``.

Metrics definitions:
* ``total_return`` — final equity / initial equity - 1.
* ``sharpe`` — ``mean(returns) / std(returns, ddof=1) * sqrt(events_per_year)``
  where ``events_per_year`` scales by the elapsed span between the first and
  last mark (zero when the standard deviation or span is zero).
* ``max_drawdown`` — largest peak-to-trough decline of the equity curve, as a
  positive fraction.
* ``turnover`` — total traded notional / initial capital.
* ``fill_count`` — number of fills.
* ``avg_slippage_bps`` — mean over fills of
  ``slippage_ticks / tick_price_ticks * 10_000``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

import numpy as np

from tickpipe.backtest.events import FillEvent
from tickpipe.store.writer import TICK_SCALE

YEAR_NS: Final[float] = 365.0 * 86_400 * 1e9


class Portfolio:
    """Multi-symbol position/PnL accounting in integer ticks."""

    def __init__(self, initial_capital_ticks: int) -> None:
        if initial_capital_ticks <= 0:
            raise ValueError("initial_capital_ticks must be positive")
        self.initial_capital_ticks = initial_capital_ticks
        self.cash_ticks = initial_capital_ticks
        self.positions_ticks: dict[str, int] = {}
        self.turnover_ticks = 0
        self._last_prices_ticks: dict[str, int] = {}
        self._fills: list[FillEvent] = []
        self._equity_marks: list[tuple[int, int]] = []

    @property
    def fills(self) -> Sequence[FillEvent]:
        return self._fills

    def apply_fill(self, fill: FillEvent) -> None:
        """Apply one fill to cash and position; call before marking the same ts."""
        notional = fill.size_ticks * fill.fill_price_ticks // TICK_SCALE
        if fill.side == "buy":
            self.cash_ticks -= notional
            self.positions_ticks[fill.symbol] = (
                self.positions_ticks.get(fill.symbol, 0) + fill.size_ticks
            )
        else:
            self.cash_ticks += notional
            self.positions_ticks[fill.symbol] = (
                self.positions_ticks.get(fill.symbol, 0) - fill.size_ticks
            )
        self.turnover_ticks += notional
        self._fills.append(fill)

    def observe_price(self, symbol: str, price_ticks: int) -> None:
        """Record the latest observed trade price for a symbol."""
        self._last_prices_ticks[symbol] = price_ticks

    def mark(self, ts_ns: int) -> None:
        """Mark equity to the latest observed prices."""
        equity = self.cash_ticks
        for symbol, position in self.positions_ticks.items():
            price = self._last_prices_ticks.get(symbol)
            if price is not None:
                equity += position * price // TICK_SCALE
        self._equity_marks.append((ts_ns, equity))

    def metrics(self) -> dict[str, float | int]:
        """Compute the metrics dict; zeros when there is nothing to compute."""
        fill_count = len(self._fills)
        avg_slippage_bps = (
            sum(fill.slippage_ticks / fill.tick_price_ticks for fill in self._fills)
            * 10_000
            / fill_count
            if fill_count
            else 0.0
        )
        turnover = self.turnover_ticks / self.initial_capital_ticks
        if len(self._equity_marks) < 2:
            return {
                "total_return": 0.0,
                "sharpe": 0.0,
                "max_drawdown": 0.0,
                "turnover": turnover,
                "fill_count": fill_count,
                "avg_slippage_bps": avg_slippage_bps,
            }
        equities = np.array([e for _, e in self._equity_marks], dtype=np.float64)
        returns = np.diff(equities) / np.where(equities[:-1] != 0, equities[:-1], 1.0)
        total_return = float(equities[-1] / equities[0] - 1.0)
        span_years = (self._equity_marks[-1][0] - self._equity_marks[0][0]) / YEAR_NS
        events_per_year = len(returns) / span_years if span_years > 0 else 0.0
        std = float(returns.std(ddof=1))
        sharpe = float(returns.mean() / std * np.sqrt(events_per_year)) if std > 0 else 0.0
        peak = np.maximum.accumulate(equities)
        drawdown = (peak - equities) / np.where(peak != 0, peak, 1.0)
        max_drawdown = float(drawdown.max())
        return {
            "total_return": total_return,
            "sharpe": sharpe,
            "max_drawdown": max_drawdown,
            "turnover": turnover,
            "fill_count": fill_count,
            "avg_slippage_bps": avg_slippage_bps,
        }


__all__ = ["Portfolio", "YEAR_NS"]
