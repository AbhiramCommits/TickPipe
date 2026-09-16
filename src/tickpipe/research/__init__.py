"""Research utilities: feature pipeline, experiment registry, and training."""

from tickpipe.research.config import ExperimentConfig, FeatureConfig, TrainConfig
from tickpipe.research.dataset import DatasetSpec, FeatureDataset
from tickpipe.research.features import FeatureContext, FeatureSpec, resolve_feature
from tickpipe.research.registry import (
    DirtyWorkingTreeError,
    ExperimentRegistry,
    RunComparison,
)
from tickpipe.research.train import TrainResult, train_evaluate, walk_forward_splits

__all__ = [
    "DatasetSpec",
    "DirtyWorkingTreeError",
    "ExperimentConfig",
    "ExperimentRegistry",
    "FeatureConfig",
    "FeatureContext",
    "FeatureDataset",
    "FeatureSpec",
    "RunComparison",
    "TrainConfig",
    "TrainResult",
    "resolve_feature",
    "train_evaluate",
    "walk_forward_splits",
]
