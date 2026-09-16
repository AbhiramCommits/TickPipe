"""Feature specification registry and point-in-time feature functions.

Every feature is a named, versioned pure function over a
:class:`FeatureContext` — a ``PointInTimeView`` clamped to a single bar's
``as_of_ns``. A feature can never read past its bar: the context is the only
data access path, it always passes the bar's ``as_of_ns`` into the view's
lookahead clamp, and it rejects any window that starts after the bar ends.

Each feature declares its lookback window in nanoseconds; the dataset
pipeline scans exactly ``[as_of_ns - lookback_ns, as_of_ns]`` for every
feature call and asserts (via :class:`~tickpipe.store.pit.LookaheadError`)
that nothing reads past the bar.

Features return deterministic ``float`` values; missing data yields NaN.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd
import pyarrow as pa

from tickpipe.backtest.portfolio import YEAR_NS
from tickpipe.store.pit import LookaheadError, PointInTimeView
from tickpipe.store.writer import TICK_SCALE

NAN: Final[float] = float("nan")
DEFAULT_BOOK_LOOKBACK_NS: Final[int] = 60 * 10**9


class FeatureContext:
    """Clamped data access for one feature computation at one bar."""

    def __init__(
        self,
        view: PointInTimeView,
        symbol: str,
        as_of_ns: int,
        lookback_ns: int,
    ) -> None:
        self._view = view
        self.symbol = symbol
        self.as_of_ns = as_of_ns
        self.lookback_ns = lookback_ns

    def scan(self, start_ns: int | None = None) -> pa.Table:
        """Scan ticks in ``[start_ns, as_of_ns]``; default start is the lookback.

        Raises :class:`LookaheadError` if ``start_ns`` lies past the bar's
        ``as_of_ns``. The returned rows are always clamped to
        ``exchange_ts_ns <= as_of_ns`` by the underlying point-in-time view.
        """
        if start_ns is None:
            start_ns = self.as_of_ns - self.lookback_ns
        if start_ns > self.as_of_ns:
            raise LookaheadError(
                f"feature window starts at {start_ns}, past the bar's as_of {self.as_of_ns}"
            )
        return self._view.scan(
            symbols=[self.symbol], start_ns=start_ns, end_ns=None, as_of_ns=self.as_of_ns
        )


@dataclass(frozen=True)
class FeatureSpec:
    """A named, versioned feature with a declared lookback window."""

    name: str
    version: int
    lookback_ns: int
    compute: Callable[[FeatureContext], float]


FEATURE_REGISTRY: dict[str, FeatureSpec] = {}


def register_feature(
    name: str, version: int, lookback_ns: int
) -> Callable[[Callable[[FeatureContext], float]], Callable[[FeatureContext], float]]:
    """Register a feature under ``name``; rejects duplicate registrations."""

    def decorator(
        function: Callable[[FeatureContext], float],
    ) -> Callable[[FeatureContext], float]:
        if name in FEATURE_REGISTRY:
            raise ValueError(f"feature {name!r} is already registered")
        FEATURE_REGISTRY[name] = FeatureSpec(name, version, lookback_ns, function)
        return function

    return decorator


def resolve_feature(name: str, params: dict[str, int] | None = None) -> FeatureSpec:
    """Resolve a feature config to a concrete spec.

    ``{"window_s": N}`` derives a windowed variant named
    ``f"{name}_{N}s"`` with ``lookback_ns = N * 10**9``, sharing the base
    feature's implementation and version.
    """
    base = FEATURE_REGISTRY.get(name)
    if base is None:
        raise KeyError(f"unknown feature {name!r}")
    if not params:
        return base
    window_s = params.get("window_s")
    if window_s is None:
        raise ValueError(f"feature {name!r} does not accept params {params}")
    window_ns = int(window_s) * 10**9
    resolved_name = f"{name}_{int(window_s)}s"
    if resolved_name not in FEATURE_REGISTRY:
        FEATURE_REGISTRY[resolved_name] = FeatureSpec(
            resolved_name, base.version, window_ns, base.compute
        )
    return FEATURE_REGISTRY[resolved_name]


def _trades(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["kind"] == "trade"].sort_values("exchange_ts_ns")


def _book_levels(book: pd.DataFrame) -> tuple[int | None, int | None]:
    levels: dict[tuple[str, int], int] = {}
    ordered = book.sort_values("exchange_ts_ns")
    sides = ordered["side"].to_numpy()
    prices = ordered["price_ticks"].to_numpy()
    sizes = ordered["size_ticks"].to_numpy()
    for side, price_ticks, size_ticks in zip(sides, prices, sizes, strict=True):
        key = (str(side), int(price_ticks))
        if int(size_ticks) == 0:
            levels.pop(key, None)
        else:
            levels[key] = int(size_ticks)
    bids = [price for (side, price), size in levels.items() if side == "bid" and size > 0]
    asks = [price for (side, price), size in levels.items() if side == "ask" and size > 0]
    if not bids or not asks:
        return None, None
    return max(bids), min(asks)


@register_feature("mid_price", version=1, lookback_ns=DEFAULT_BOOK_LOOKBACK_NS)
def mid_price(ctx: FeatureContext) -> float:
    """Best bid/ask midpoint in ticks (book first, last trade as fallback)."""
    table = ctx.scan()
    if table.num_rows == 0:
        return NAN
    df = table.to_pandas()
    book = df[df["kind"] == "book_delta"]
    if not book.empty:
        bid, ask = _book_levels(book)
        if bid is not None and ask is not None:
            return (bid + ask) / (2 * TICK_SCALE)
    trades = _trades(df)
    if trades.empty:
        return NAN
    return float(trades.iloc[-1]["price_ticks"]) / TICK_SCALE


@register_feature("spread_bps", version=1, lookback_ns=DEFAULT_BOOK_LOOKBACK_NS)
def spread_bps(ctx: FeatureContext) -> float:
    """Best bid/ask spread in basis points of the midpoint."""
    table = ctx.scan()
    if table.num_rows == 0:
        return NAN
    df = table.to_pandas()
    book = df[df["kind"] == "book_delta"]
    if book.empty:
        return NAN
    bid, ask = _book_levels(book)
    if bid is None or ask is None:
        return NAN
    mid = (bid + ask) / 2.0
    return (ask - bid) / mid * 10_000.0


@register_feature("order_flow_imbalance", version=1, lookback_ns=DEFAULT_BOOK_LOOKBACK_NS)
def order_flow_imbalance(ctx: FeatureContext) -> float:
    """Tick-rule signed volume over total volume in the window, in [-1, 1]."""
    table = ctx.scan()
    if table.num_rows == 0:
        return NAN
    trades = _trades(table.to_pandas())
    if trades.empty:
        return NAN
    prices = trades["price_ticks"].to_numpy(dtype=np.float64)
    sizes = trades["size_ticks"].to_numpy(dtype=np.float64)
    total = sizes.sum()
    if total == 0:
        return NAN
    signs = np.sign(np.diff(prices))
    return float((signs * sizes[1:]).sum() / total)


@register_feature("realized_vol", version=1, lookback_ns=DEFAULT_BOOK_LOOKBACK_NS)
def realized_vol(ctx: FeatureContext) -> float:
    """Annualized standard deviation of tick-level log returns over the window."""
    table = ctx.scan()
    if table.num_rows == 0:
        return NAN
    trades = _trades(table.to_pandas())
    if len(trades) < 2:
        return NAN
    prices = trades["price_ticks"].to_numpy(dtype=np.float64) / TICK_SCALE
    returns = np.diff(np.log(prices))
    std = float(returns.std(ddof=1))
    return std * math.sqrt(YEAR_NS / max(ctx.lookback_ns, 1))


@register_feature("trade_count", version=1, lookback_ns=DEFAULT_BOOK_LOOKBACK_NS)
def trade_count(ctx: FeatureContext) -> float:
    """Number of trades in the window."""
    table = ctx.scan()
    if table.num_rows == 0:
        return 0.0
    return float((table.to_pandas()["kind"] == "trade").sum())


__all__ = [
    "FEATURE_REGISTRY",
    "FeatureContext",
    "FeatureSpec",
    "mid_price",
    "order_flow_imbalance",
    "realized_vol",
    "register_feature",
    "resolve_feature",
    "spread_bps",
    "trade_count",
]
