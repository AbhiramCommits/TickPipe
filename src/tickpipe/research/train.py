"""Walk-forward model training for feature matrices.

Splits are strictly time-ordered expanding windows — never shuffled, never
``KFold``. Targets are forward returns over ``horizon_bars`` computed from
the matrix's ``mid_price`` column. All randomness is seeded from
``TrainConfig.seed`` so replays are bit-for-bit identical.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Final

import numpy as np
import pyarrow as pa
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error, r2_score

from tickpipe.backtest.portfolio import YEAR_NS
from tickpipe.research.config import TrainConfig

METRIC_COLUMNS: Final[tuple[str, ...]] = ("symbol", "bar_end_ns")

BACKTEST_METRIC_KEYS: Final[tuple[str, ...]] = (
    "total_return",
    "sharpe",
    "max_drawdown",
    "turnover",
    "fill_count",
    "avg_slippage_bps",
)


@dataclass(frozen=True)
class TrainResult:
    """Aggregated training, validation, and backtest metrics."""

    train_metrics: dict[str, float]
    validation_metrics: dict[str, float]
    backtest_metrics: dict[str, float]

    def combined(self) -> dict[str, float]:
        return {**self.train_metrics, **self.validation_metrics, **self.backtest_metrics}


def walk_forward_splits(
    n_rows: int, n_splits: int
) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    """Expanding-window, strictly time-ordered train/validation splits.

    Row order is assumed sorted by time. For each fold the training set is
    ``[0, cut)`` and the validation set ``[cut, next_cut)`` — no shuffling,
    no future leakage.
    """
    if n_splits < 2:
        raise ValueError("n_splits must be at least 2")
    cuts = np.linspace(0, n_rows, n_splits + 2, dtype=np.int64)
    for fold in range(1, n_splits + 1):
        train_end = int(cuts[fold])
        valid_end = int(cuts[fold + 1])
        if valid_end <= train_end:
            raise ValueError(f"n_rows={n_rows} is too small for {n_splits} splits")
        yield (
            np.arange(0, train_end, dtype=np.int64),
            np.arange(train_end, valid_end, dtype=np.int64),
        )


def _make_model(config: TrainConfig) -> Any:
    if config.model == "ridge":
        return Ridge(alpha=1.0)
    return GradientBoostingRegressor(
        random_state=config.seed,
        n_estimators=config.gbm_n_estimators,
        max_depth=config.gbm_max_depth,
        learning_rate=0.05,
    )


def bar_backtest_metrics(
    signals: np.ndarray, returns: np.ndarray, bar_end_ns: np.ndarray
) -> dict[str, float]:
    """Vectorized bar-level backtest: position = sign(signal)."""
    if len(signals) == 0:
        return {f"backtest_{key}": 0.0 for key in BACKTEST_METRIC_KEYS}
    positions = np.sign(signals)
    strategy_returns = positions * returns
    equity = np.cumprod(1.0 + strategy_returns)
    span_years = (bar_end_ns[-1] - bar_end_ns[0]) / YEAR_NS
    events_per_year = len(strategy_returns) / span_years if span_years > 0 else 0.0
    std = float(strategy_returns.std(ddof=1))
    sharpe = (
        float(strategy_returns.mean() / std * np.sqrt(events_per_year)) if std > 0 else 0.0
    )
    peak = np.maximum.accumulate(equity)
    max_drawdown = float(((peak - equity) / peak).max())
    changes = np.diff(positions, prepend=positions[:1])
    return {
        "backtest_total_return": float(equity[-1] - 1.0),
        "backtest_sharpe": sharpe,
        "backtest_max_drawdown": max_drawdown,
        "backtest_turnover": float(np.abs(changes).sum() / len(changes)),
        "backtest_fill_count": int(np.count_nonzero(changes)),
        "backtest_avg_slippage_bps": 0.0,
    }


def train_evaluate(table: pa.Table, config: TrainConfig) -> TrainResult:
    """Fit the configured model on expanding walk-forward folds and evaluate."""
    df = table.to_pandas().sort_values("bar_end_ns").reset_index(drop=True)
    feature_columns = [c for c in table.column_names if c not in METRIC_COLUMNS]
    if "mid_price" not in feature_columns:
        raise ValueError("feature matrix must include the mid_price feature")
    df["target"] = (
        df.groupby("symbol")["mid_price"].shift(-config.horizon_bars) / df["mid_price"] - 1.0
    )
    valid = df.dropna(subset=feature_columns + ["target"]).reset_index(drop=True)
    if len(valid) < config.n_splits + 2:
        raise ValueError(
            f"not enough valid rows ({len(valid)}) for {config.n_splits} walk-forward splits"
        )
    features = valid[feature_columns].to_numpy(dtype=np.float64)
    targets = valid["target"].to_numpy(dtype=np.float64)
    bar_end = valid["bar_end_ns"].to_numpy(dtype=np.int64)

    train_mse: list[float] = []
    train_r2: list[float] = []
    val_mse: list[float] = []
    val_r2: list[float] = []
    val_predictions: list[np.ndarray] = []
    val_actuals: list[np.ndarray] = []
    val_bar_end: list[np.ndarray] = []
    for train_index, valid_index in walk_forward_splits(len(valid), config.n_splits):
        model = _make_model(config)
        model.fit(features[train_index], targets[train_index])
        train_prediction = model.predict(features[train_index])
        valid_prediction = model.predict(features[valid_index])
        train_mse.append(mean_squared_error(targets[train_index], train_prediction))
        train_r2.append(r2_score(targets[train_index], train_prediction))
        val_mse.append(mean_squared_error(targets[valid_index], valid_prediction))
        val_r2.append(r2_score(targets[valid_index], valid_prediction))
        val_predictions.append(valid_prediction)
        val_actuals.append(targets[valid_index])
        val_bar_end.append(bar_end[valid_index])

    return TrainResult(
        train_metrics={
            "train_mse": float(np.mean(train_mse)),
            "train_r2": float(np.mean(train_r2)),
        },
        validation_metrics={
            "val_mse": float(np.mean(val_mse)),
            "val_r2": float(np.mean(val_r2)),
        },
        backtest_metrics=bar_backtest_metrics(
            np.concatenate(val_predictions),
            np.concatenate(val_actuals),
            np.concatenate(val_bar_end),
        ),
    )


__all__ = ["TrainResult", "bar_backtest_metrics", "train_evaluate", "walk_forward_splits"]
