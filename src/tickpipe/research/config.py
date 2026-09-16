"""Configuration models for research experiments."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

ModelName = Literal["ridge", "gradient_boosting"]


class FeatureConfig(BaseModel):
    """One feature entry in an experiment config."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    window_s: int | None = Field(default=None, gt=0)


class TrainConfig(BaseModel):
    """Walk-forward training configuration."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    model: ModelName = "ridge"
    horizon_bars: int = Field(default=1, ge=1)
    n_splits: int = Field(default=4, ge=2)
    seed: int = 42
    gbm_n_estimators: int = Field(default=100, ge=1)
    gbm_max_depth: int = Field(default=3, ge=1)


class ExperimentConfig(BaseModel):
    """Top-level experiment configuration (YAML)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    dataset: str = "trades"
    symbols: tuple[str, ...] = ()
    bar_interval_s: int = Field(default=60, gt=0)
    features: tuple[FeatureConfig, ...]
    train: TrainConfig = TrainConfig()


def load_experiment_config(path: str | Path) -> ExperimentConfig:
    """Load and validate an experiment config from YAML."""
    with Path(path).open() as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise ValueError(f"experiment config {path} must contain a YAML mapping")
    return ExperimentConfig.model_validate(raw)


__all__ = [
    "ExperimentConfig",
    "FeatureConfig",
    "ModelName",
    "TrainConfig",
    "load_experiment_config",
]
