"""End-to-end experiment orchestration: build → train → backtest → register."""

from __future__ import annotations

import hashlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog

from tickpipe.research.config import TrainConfig, load_experiment_config
from tickpipe.research.dataset import DatasetSpec, FeatureDataset, load_dataset_spec
from tickpipe.research.models import ExperimentRunRecord
from tickpipe.research.registry import ExperimentRegistry
from tickpipe.research.train import train_evaluate
from tickpipe.store.pit import PointInTimeView
from tickpipe.store.reader import TickStore
from tickpipe.store.writer import PartitionManifest

logger = structlog.get_logger(__name__)

ENV_REGISTRY_URL: str = "TICKPIPE_EXPERIMENT_REGISTRY_URL"


def default_database_url(data_dir: Path) -> str:
    from_environment = os.environ.get(ENV_REGISTRY_URL)
    if from_environment:
        return from_environment
    data_dir.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{data_dir / 'experiments.db'}"


def dataset_bounds(data_dir: Path, dataset: str) -> tuple[int, int]:
    """(min, max) exchange_ts_ns across all partition manifests of a dataset."""
    min_ts: int | None = None
    max_ts: int | None = None
    for manifest_path in (data_dir / dataset).rglob("_manifest.json"):
        manifest = PartitionManifest.model_validate_json(manifest_path.read_text())
        if min_ts is None or manifest.min_exchange_ts_ns < min_ts:
            min_ts = manifest.min_exchange_ts_ns
        if max_ts is None or manifest.max_exchange_ts_ns > max_ts:
            max_ts = manifest.max_exchange_ts_ns
    if max_ts is None or min_ts is None:
        raise ValueError(f"no partition manifests found under {data_dir / dataset}")
    return min_ts, max_ts


def discover_symbols(data_dir: Path, dataset: str) -> list[str]:
    prefix = "symbol="
    return sorted(path.name[len(prefix) :] for path in (data_dir / dataset).glob("symbol=*"))


def compute_code_hash() -> str:
    """SHA-256 over the research, backtest, and store package sources."""
    digest = hashlib.sha256()
    packages_root = Path(__file__).parent.parent
    for package in sorted(
        [packages_root / "research", packages_root / "backtest", packages_root / "store"]
    ):
        for path in sorted(package.rglob("*.py")):
            digest.update(path.name.encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
    return digest.hexdigest()


def build_view(data_dir: Path, dataset: str) -> PointInTimeView:
    _start, end_ns = dataset_bounds(data_dir, dataset)
    return PointInTimeView(TickStore(data_dir, dataset), as_of_ns=end_ns)


def run_experiment(
    config_path: str | Path,
    *,
    data_dir: str | Path,
    database_url: str | None = None,
    allow_dirty: bool = False,
) -> ExperimentRunRecord:
    """Run the full path: dataset build -> train -> backtest -> register."""
    config = load_experiment_config(config_path)
    data_dir = Path(data_dir)
    start_ns, end_ns = dataset_bounds(data_dir, config.dataset)
    view = PointInTimeView(TickStore(data_dir, config.dataset), as_of_ns=end_ns)
    symbols = tuple(config.symbols) or tuple(discover_symbols(data_dir, config.dataset))
    spec = DatasetSpec(
        dataset=config.dataset,
        symbols=symbols,
        bar_interval_ns=int(config.bar_interval_s * 10**9),
        features=config.features,
        start_ns=start_ns,
        end_ns=end_ns,
    )
    dataset = FeatureDataset(view, spec, data_dir=data_dir)
    table, fingerprint = dataset.materialize()
    train_result = train_evaluate(table, config.train)
    metrics = train_result.combined()

    registry = ExperimentRegistry(database_url or default_database_url(data_dir))
    registry.create_schema()
    run_id = registry.register_run(
        dataset_fingerprint=fingerprint,
        feature_set=[feature.model_dump() for feature in config.features],
        params={
            "dataset": config.dataset,
            "symbols": list(symbols),
            "bar_interval_s": config.bar_interval_s,
            "train": config.train.model_dump(),
        },
        metrics={},
        code_hash=compute_code_hash(),
        seed=config.train.seed,
        allow_dirty=allow_dirty,
    )
    registry.record_metrics(run_id, metrics, status="complete")
    record = registry.get_run(run_id)
    if record is None:
        raise RuntimeError(f"registered run {run_id} vanished from the registry")
    logger.info("experiment_complete", run_id=str(record.run_id), **metrics)
    return record


@dataclass(frozen=True)
class ReplayResult:
    """Outcome of replaying a run from its registry record."""

    run_id: uuid.UUID
    recorded_fingerprint: str
    recreated_fingerprint: str
    fingerprint_ok: bool
    metric_mismatches: dict[str, tuple[Any, Any]]
    metrics: dict[str, float]


def replay_run(
    run_id: str | uuid.UUID,
    *,
    data_dir: str | Path,
    database_url: str | None = None,
) -> ReplayResult:
    """Rebuild the dataset and retrain from a registry record alone.

    The recreated dataset fingerprint must equal the recorded one, and every
    metric must match the recorded metrics exactly (``==``).
    """
    data_dir = Path(data_dir)
    registry = ExperimentRegistry(database_url or default_database_url(data_dir))
    record = registry.get_run(uuid.UUID(str(run_id)))
    if record is None:
        raise LookupError(f"no run with id {run_id} in the registry")
    if record.dataset_fingerprint is None:
        raise ValueError(f"run {run_id} has no dataset fingerprint to replay")
    spec = load_dataset_spec(data_dir, record.dataset_fingerprint)
    _start, end_ns = dataset_bounds(data_dir, spec.dataset)
    view = PointInTimeView(TickStore(data_dir, spec.dataset), as_of_ns=end_ns)
    dataset = FeatureDataset(view, spec, data_dir=data_dir)
    table, recreated_fingerprint = dataset.compute()

    train_config = TrainConfig(**record.params["train"])
    metrics = train_evaluate(table, train_config).combined()
    mismatches = {
        key: (record.metrics[key], metrics[key])
        for key in metrics
        if record.metrics.get(key) != metrics[key]
    }
    return ReplayResult(
        run_id=uuid.UUID(str(run_id)),
        recorded_fingerprint=record.dataset_fingerprint,
        recreated_fingerprint=recreated_fingerprint,
        fingerprint_ok=recreated_fingerprint == record.dataset_fingerprint,
        metric_mismatches=mismatches,
        metrics=metrics,
    )


__all__ = [
    "ENV_REGISTRY_URL",
    "ReplayResult",
    "build_view",
    "compute_code_hash",
    "dataset_bounds",
    "default_database_url",
    "discover_symbols",
    "replay_run",
    "run_experiment",
]
