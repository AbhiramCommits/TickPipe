"""Backtest driver: feeds the C++ replay engine and collects fills.

The backtest scans its :class:`~tickpipe.store.pit.PointInTimeView` for the
configured window, converts the Arrow table into one contiguous numpy
structured buffer (matching the C++ ``TickRecord`` layout exactly), hands it
to the C++ engine zero-copy, and replays. The C++ engine dispatches trade and
book callbacks to the strategy in event-time order and reports fills; fills
are also collected here and, combined with observed trade prices, fed through
:class:`~tickpipe.backtest.portfolio.Portfolio` for PnL metrics.

Strategies are constructed with — and only with — the ``PointInTimeView``;
this constructor enforces that when given a strategy class.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pydantic import BaseModel, ConfigDict

from tickpipe.backtest import _replay
from tickpipe.backtest.events import FillEvent, OrderHandle, OrderSide
from tickpipe.backtest.portfolio import Portfolio
from tickpipe.backtest.strategy import Strategy
from tickpipe.store.pit import PointInTimeView
from tickpipe.store.writer import TICK_SCALE

TICK_RECORD_DTYPE: Final[np.dtype] = np.dtype(
    [
        ("kind", "i1"),
        ("side", "i1"),
        ("exchange_ts_ns", "<i8"),
        ("ingest_ts_ns", "<i8"),
        ("sequence", "<i8"),
        ("price_ticks", "<i8"),
        ("size_ticks", "<i8"),
        ("symbol_id", "<i4"),
        ("trade_id", "<i8"),
    ],
    align=True,
)


class BacktestConfig(BaseModel):
    """Configuration for one backtest run."""

    model_config = ConfigDict(frozen=True)

    start_ns: int = 0
    end_ns: int | None = None
    symbols: tuple[str, ...] | None = None
    speed: float = 0.0
    order_entry_latency_ns: int = 0
    slippage_fixed_ticks: int = 0
    slippage_per_share_ticks: int = 0
    initial_capital_ticks: int = 10**18


@dataclass(frozen=True)
class BacktestResult:
    """Outcome of a backtest run: portfolio metrics plus all fills."""

    metrics: dict[str, float | int]
    fills: tuple[FillEvent, ...]


class Backtest:
    """Replays tick data through a strategy and a simulated execution model."""

    def __init__(
        self,
        view: PointInTimeView,
        strategy: Strategy | type[Strategy],
        config: BacktestConfig | None = None,
    ) -> None:
        self.view = view
        self.config = config or BacktestConfig()
        if isinstance(strategy, type):
            strategy = strategy(view)
        elif not isinstance(strategy, Strategy):
            raise TypeError("strategy must be a Strategy instance or class")
        self.strategy = strategy
        self._engine: _replay.ReplayEngine | None = None

    def run(self) -> BacktestResult:
        """Run the backtest once and return metrics plus fills."""
        table = self._load_table()
        tick_array, symbol_names, trade_id_names = _table_to_tick_array(table)
        if TICK_RECORD_DTYPE.itemsize != _replay.TICK_RECORD_ITEMSIZE:
            raise RuntimeError("numpy TICK_RECORD_DTYPE and C++ TickRecord layouts are out of sync")

        engine = _replay.ReplayEngine(
            speed=self.config.speed,
            tick_scale=TICK_SCALE,
            order_entry_latency_ns=self.config.order_entry_latency_ns,
            slippage_fixed_ticks=self.config.slippage_fixed_ticks,
            slippage_per_share_ticks=self.config.slippage_per_share_ticks,
        )
        engine.set_symbols(symbol_names)
        engine.set_trade_ids(trade_id_names)
        engine.set_trade_callback(self._on_trade)
        engine.set_book_delta_callback(self._on_book_delta)
        engine.set_fill_callback(self._on_fill)
        self._engine = engine
        self._symbol_ids = {name: index for index, name in enumerate(symbol_names)}
        self.strategy._attach_order_submitter(self._submit_order)
        try:
            engine.set_ticks(tick_array)
            engine.run()
        finally:
            self.strategy._attach_order_submitter(_detached_submitter)

        fills = tuple(
            FillEvent(
                order_id=int(fill.order_id),
                symbol=symbol_names[fill.symbol_id],
                side="buy" if fill.side == 1 else "sell",
                size_ticks=int(fill.size_ticks),
                tick_price_ticks=int(fill.tick_price_ticks),
                fill_price_ticks=int(fill.fill_price_ticks),
                slippage_ticks=int(fill.slippage_ticks),
                trade_ts_ns=int(fill.trade_ts_ns),
            )
            for fill in engine.pop_fills()
        )
        metrics = self._compute_metrics(fills, tick_array, symbol_names)
        return BacktestResult(metrics=metrics, fills=fills)

    def _load_table(self) -> pa.Table:
        end_ns = self.config.end_ns if self.config.end_ns is not None else self.view.as_of_ns
        table = self.view.scan(
            symbols=list(self.config.symbols) if self.config.symbols else None,
            start_ns=self.config.start_ns,
            end_ns=end_ns,
        )
        return table.sort_by(
            [
                ("exchange_ts_ns", "ascending"),
                ("sequence", "ascending"),
                ("kind", "ascending"),
            ]
        )

    def _on_trade(
        self,
        symbol: str,
        exchange_ts_ns: int,
        price_ticks: int,
        size_ticks: int,
        trade_id: str,
    ) -> None:
        self.strategy.on_trade(symbol, exchange_ts_ns, price_ticks, size_ticks, trade_id)

    def _on_book_delta(
        self, symbol: str, exchange_ts_ns: int, side: str, price_ticks: int, size_ticks: int
    ) -> None:
        self.strategy.on_book_delta(symbol, exchange_ts_ns, side, price_ticks, size_ticks)

    def _on_fill(
        self,
        order_id: int,
        symbol: str,
        side: int,
        size_ticks: int,
        tick_price_ticks: int,
        fill_price_ticks: int,
        slippage_ticks: int,
        trade_ts_ns: int,
    ) -> None:
        self.strategy.on_fill(
            FillEvent(
                order_id=int(order_id),
                symbol=symbol,
                side="buy" if side == 1 else "sell",
                size_ticks=int(size_ticks),
                tick_price_ticks=int(tick_price_ticks),
                fill_price_ticks=int(fill_price_ticks),
                slippage_ticks=int(slippage_ticks),
                trade_ts_ns=int(trade_ts_ns),
            )
        )

    def _submit_order(self, symbol: str, side: OrderSide, size_ticks: int) -> OrderHandle:
        engine = self._engine
        if engine is None:
            raise RuntimeError("no backtest in progress")
        order_id = engine.submit_order(
            self._symbol_ids[symbol], 1 if side == "buy" else 2, size_ticks
        )
        return OrderHandle(
            order_id=int(order_id),
            symbol=symbol,
            side=side,
            size_ticks=size_ticks,
            submitted_ts_ns=engine.current_ts_ns(),
        )

    def _compute_metrics(
        self,
        fills: tuple[FillEvent, ...],
        tick_array: np.ndarray,
        symbol_names: list[str],
    ) -> dict[str, float | int]:
        portfolio = Portfolio(self.config.initial_capital_ticks)
        fills_sorted = sorted(fills, key=lambda fill: fill.trade_ts_ns)
        fill_index = 0
        trade_rows = tick_array[tick_array["kind"] == 1]
        for row in trade_rows:
            ts_ns = int(row["exchange_ts_ns"])
            while fill_index < len(fills_sorted) and fills_sorted[fill_index].trade_ts_ns <= ts_ns:
                portfolio.apply_fill(fills_sorted[fill_index])
                fill_index += 1
            portfolio.observe_price(symbol_names[int(row["symbol_id"])], int(row["price_ticks"]))
            portfolio.mark(ts_ns)
        while fill_index < len(fills_sorted):
            portfolio.apply_fill(fills_sorted[fill_index])
            fill_index += 1
        return portfolio.metrics()


def _detached_submitter(symbol: str, side: OrderSide, size_ticks: int) -> OrderHandle:
    raise RuntimeError("strategy is not attached to a Backtest")


def _table_to_tick_array(
    table: pa.Table,
) -> tuple[np.ndarray, list[str], list[str]]:
    """Convert a canonical scan table to the C++ tick record buffer.

    Symbol and trade-id strings are mapped to dense integer codes; the
    returned numpy array matches the C++ ``TickRecord`` layout exactly so the
    engine can consume it zero-copy.
    """
    count = table.num_rows
    records = np.zeros(count, dtype=TICK_RECORD_DTYPE)
    if count == 0:
        return records, [], []
    for name in (
        "exchange_ts_ns",
        "ingest_ts_ns",
        "sequence",
        "price_ticks",
        "size_ticks",
    ):
        records[name] = table[name].combine_chunks().to_numpy(zero_copy_only=False)

    kind_array = pc.dictionary_encode(table["kind"]).combine_chunks()
    kind_values = kind_array.dictionary.to_pylist()
    records["kind"] = (kind_array.indices.to_numpy() == kind_values.index("trade")).astype("i1")

    side_column = table["side"].combine_chunks()
    is_bid = pc.fill_null(pc.equal(side_column, "bid"), False)
    is_ask = pc.fill_null(pc.equal(side_column, "ask"), False)
    side_codes = np.where(
        is_bid.to_numpy(zero_copy_only=False),
        1,
        np.where(is_ask.to_numpy(zero_copy_only=False), 2, 0),
    )
    records["side"] = side_codes.astype("i1")

    symbol_array = pc.dictionary_encode(table["symbol"]).combine_chunks()
    records["symbol_id"] = symbol_array.indices.to_numpy().astype("i4")
    symbol_names = symbol_array.dictionary.to_pylist()

    trade_ids = table["trade_id"].combine_chunks().to_pylist()
    trade_id_codes: dict[str, int] = {}
    trade_id_names: list[str] = []
    codes = np.zeros(count, dtype="i8")
    for index, value in enumerate(trade_ids):
        if value is None:
            codes[index] = 0
            continue
        code = trade_id_codes.get(value)
        if code is None:
            code = len(trade_id_names) + 1
            trade_id_codes[value] = code
            trade_id_names.append(value)
        codes[index] = code
    records["trade_id"] = codes
    return records, symbol_names, trade_id_names


__all__ = [
    "Backtest",
    "BacktestConfig",
    "BacktestResult",
    "TICK_RECORD_DTYPE",
]
