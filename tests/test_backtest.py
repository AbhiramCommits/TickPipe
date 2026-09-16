"""Tests for the Python backtest layer (strategy wiring, latency, portfolio)."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from tickpipe.backtest import Backtest, BacktestConfig, Portfolio, RollingZScoreMeanReversion
from tickpipe.backtest.events import FillEvent
from tickpipe.backtest.portfolio import YEAR_NS
from tickpipe.backtest.strategy import Strategy
from tickpipe.core.types import Trade
from tickpipe.store.pit import PointInTimeView
from tickpipe.store.reader import TickStore
from tickpipe.store.writer import PartitionedWriter, decimal_to_ticks

SCALE = 10**9


def make_view(tmp_path: Path, prices: list[Decimal], base_ts_ns: int = 1_000) -> PointInTimeView:
    ticks = [
        Trade(
            symbol="BTC-USD",
            exchange_ts_ns=base_ts_ns + index * 1_000_000,
            ingest_ts_ns=base_ts_ns + index * 1_000_000 + 1,
            price=str(price),
            size=Decimal("1"),
            trade_id=f"t-{index}",
            sequence=index + 1,
        )
        for index, price in enumerate(prices)
    ]
    writer = PartitionedWriter(tmp_path, "trades", max_open_s=None)
    writer.write_batch(ticks)
    writer.flush()
    return PointInTimeView(
        TickStore(tmp_path, "trades"),
        as_of_ns=base_ts_ns + len(prices) * 1_000_000,
    )


class SubmitOnFirstTrade(Strategy):
    """Submits one buy order when it sees the first trade."""

    def __init__(self, view: PointInTimeView) -> None:
        super().__init__(view)
        self.submitted = False

    def on_trade(
        self, symbol: str, exchange_ts_ns: int, price_ticks: int, size_ticks: int, trade_id: str
    ) -> None:
        if not self.submitted:
            self.submitted = True
            self.submit_order(symbol, "buy", SCALE)

    def on_book_delta(
        self, symbol: str, exchange_ts_ns: int, side: str, price_ticks: int, size_ticks: int
    ) -> None:
        return None


def test_latency_modeling_prevents_same_tick_fills(tmp_path: Path) -> None:
    view = make_view(tmp_path, [Decimal("100")] * 3, base_ts_ns=1_000)
    with_latency = Backtest(
        view,
        SubmitOnFirstTrade(view),
        BacktestConfig(order_entry_latency_ns=500, initial_capital_ticks=10**18),
    )
    result = with_latency.run()
    assert len(result.fills) == 1
    # submitted at ts 1_000 with 500ns latency: ineligible for the submit tick
    # itself, first eligible tick is the one at 1_001_000.
    assert result.fills[0].trade_ts_ns == 1_001_000


def test_zero_latency_allows_same_tick_fills(tmp_path: Path) -> None:
    view = make_view(tmp_path, [Decimal("100")] * 3, base_ts_ns=1_000)
    backtest = Backtest(
        view,
        SubmitOnFirstTrade(view),
        BacktestConfig(order_entry_latency_ns=0, initial_capital_ticks=10**18),
    )
    result = backtest.run()
    assert len(result.fills) == 1
    assert result.fills[0].trade_ts_ns == 1_000  # fills on the same tick


def test_reference_mean_reversion_strategy_round_trips(tmp_path: Path) -> None:
    prices = [Decimal("100")] * 5 + [Decimal("96")] * 5 + [Decimal("100")] * 5
    view = make_view(tmp_path, prices)
    strategy = RollingZScoreMeanReversion(
        view,
        symbol="BTC-USD",
        window=5,
        entry_z=0.5,
        exit_z=0.0,
        order_size=Decimal("1"),
    )
    result = Backtest(view, strategy, BacktestConfig(initial_capital_ticks=10**18)).run()
    assert [fill.side for fill in result.fills] == ["buy", "sell"]
    assert result.metrics["fill_count"] == 2
    for key in ("total_return", "sharpe", "max_drawdown", "turnover", "avg_slippage_bps"):
        assert key in result.metrics


def test_backtest_enforces_pit_view_in_strategy_constructor(tmp_path: Path) -> None:
    view = make_view(tmp_path, [Decimal("100")] * 3)
    backtest = Backtest(view, SubmitOnFirstTrade, BacktestConfig())
    assert backtest.strategy.view is view
    with pytest.raises(TypeError):
        Backtest(view, "not a strategy", BacktestConfig())


def test_strategy_submit_outside_backtest_raises(tmp_path: Path) -> None:
    view = make_view(tmp_path, [Decimal("100")] * 3)
    strategy = SubmitOnFirstTrade(view)
    with pytest.raises(RuntimeError):
        strategy.submit_order("BTC-USD", "buy", SCALE)


def test_portfolio_metrics_hand_computed() -> None:
    portfolio = Portfolio(initial_capital_ticks=1_000_000_000_000)  # 1000.0 quote
    portfolio.observe_price("BTC-USD", 100 * SCALE)
    portfolio.mark(0)
    portfolio.apply_fill(
        FillEvent(
            order_id=1,
            symbol="BTC-USD",
            side="buy",
            size_ticks=SCALE,
            tick_price_ticks=100 * SCALE,
            fill_price_ticks=100 * SCALE,
            slippage_ticks=0,
            trade_ts_ns=10,
        )
    )
    portfolio.observe_price("BTC-USD", 110 * SCALE)
    portfolio.mark(YEAR_NS)
    portfolio.apply_fill(
        FillEvent(
            order_id=2,
            symbol="BTC-USD",
            side="sell",
            size_ticks=SCALE,
            tick_price_ticks=110 * SCALE,
            fill_price_ticks=110 * SCALE,
            slippage_ticks=0,
            trade_ts_ns=YEAR_NS + 10,
        )
    )
    portfolio.observe_price("BTC-USD", 110 * SCALE)
    portfolio.mark(2 * YEAR_NS)

    metrics = portfolio.metrics()
    assert metrics["total_return"] == pytest.approx(0.01)
    assert metrics["sharpe"] == pytest.approx(2**-0.5)
    assert metrics["max_drawdown"] == 0.0
    assert metrics["turnover"] == pytest.approx(0.21)
    assert metrics["fill_count"] == 2
    assert metrics["avg_slippage_bps"] == 0.0


def test_portfolio_avg_slippage_bps_hand_computed() -> None:
    portfolio = Portfolio(initial_capital_ticks=10**18)
    portfolio.apply_fill(
        FillEvent(
            order_id=1,
            symbol="BTC-USD",
            side="buy",
            size_ticks=SCALE,
            tick_price_ticks=100 * SCALE,
            fill_price_ticks=100 * SCALE + 10_000_000,
            slippage_ticks=10_000_000,
            trade_ts_ns=10,
        )
    )
    metrics = portfolio.metrics()
    # 1e7 ticks on a 1e11-tick price is exactly 1.0 bps
    assert metrics["avg_slippage_bps"] == pytest.approx(1.0)
    assert metrics["fill_count"] == 1


def test_portfolio_empty_metrics_are_zero() -> None:
    metrics = Portfolio(initial_capital_ticks=10**18).metrics()
    assert metrics == {
        "total_return": 0.0,
        "sharpe": 0.0,
        "max_drawdown": 0.0,
        "turnover": 0.0,
        "fill_count": 0,
        "avg_slippage_bps": 0.0,
    }


def test_decimal_order_sizes_convert_to_ticks() -> None:
    assert decimal_to_ticks(Decimal("0.5")) == 500_000_000
    assert decimal_to_ticks(Decimal("1.234567890")) == 1_234_567_890
