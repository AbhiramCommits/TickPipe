"""Backtesting engine.

The C++17 replay core lives in ``tickpipe.backtest._replay``; the Python
layer feeds it data from a :class:`~tickpipe.store.pit.PointInTimeView`,
wires strategies, and computes portfolio metrics.
"""

from tickpipe.backtest.engine import TICK_RECORD_DTYPE, Backtest, BacktestConfig, BacktestResult
from tickpipe.backtest.events import FillEvent, OrderHandle, OrderSide
from tickpipe.backtest.portfolio import Portfolio
from tickpipe.backtest.strategies import RollingZScoreMeanReversion
from tickpipe.backtest.strategy import Strategy

__all__ = [
    "Backtest",
    "BacktestConfig",
    "BacktestResult",
    "FillEvent",
    "OrderHandle",
    "OrderSide",
    "Portfolio",
    "RollingZScoreMeanReversion",
    "Strategy",
    "TICK_RECORD_DTYPE",
]
