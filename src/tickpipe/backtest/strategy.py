"""Strategy interface for the backtest engine.

Strategies are constructed with — and only with — a
:class:`~tickpipe.store.pit.PointInTimeView`; they never receive a raw
``TickStore``. During a backtest the engine streams ticks into the strategy
via ``on_trade`` / ``on_book_delta`` (in event-time order) and reports fills
via ``on_fill``. The concrete :meth:`submit_order` handle is wired by the
backtest at attach time.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable

from tickpipe.backtest.events import FillEvent, OrderHandle, OrderSide
from tickpipe.store.pit import PointInTimeView

OrderSubmitter = Callable[[str, OrderSide, int], OrderHandle]


class Strategy(ABC):
    """Base class for backtest strategies."""

    def __init__(self, view: PointInTimeView) -> None:
        self._view = view
        self._order_submitter: OrderSubmitter | None = None

    @property
    def view(self) -> PointInTimeView:
        """The point-in-time view this strategy sees; never a raw TickStore."""
        return self._view

    @abstractmethod
    def on_trade(
        self,
        symbol: str,
        exchange_ts_ns: int,
        price_ticks: int,
        size_ticks: int,
        trade_id: str,
    ) -> None:
        """A trade tick, delivered in replay order."""

    @abstractmethod
    def on_book_delta(
        self,
        symbol: str,
        exchange_ts_ns: int,
        side: str,
        price_ticks: int,
        size_ticks: int,
    ) -> None:
        """An order-book delta, delivered in replay order."""

    def on_fill(self, fill: FillEvent) -> None:
        """A simulated fill for one of this strategy's orders (optional hook)."""
        return None

    def submit_order(self, symbol: str, side: OrderSide, size_ticks: int) -> OrderHandle:
        """Submit a market order through the backtest engine.

        Only available while the strategy is attached to a
        :class:`~tickpipe.backtest.engine.Backtest`.
        """
        if self._order_submitter is None:
            raise RuntimeError("strategy is not attached to a Backtest")
        return self._order_submitter(symbol, side, size_ticks)

    def _attach_order_submitter(self, submitter: OrderSubmitter) -> None:
        self._order_submitter = submitter


__all__ = ["OrderSubmitter", "Strategy"]
