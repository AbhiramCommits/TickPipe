"""Feature-matrix materialization with a reproducibility fingerprint.

``dataset_fingerprint`` = SHA-256 over a canonical JSON document containing:
sorted input partition manifest content hashes, the resolved feature names,
versions and params, the bar interval, the symbol list, and the [start, end)
window — everything that determines the matrix content. Identical inputs
produce an identical fingerprint, so a replayed experiment can prove it
rebuilt the same data.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from tickpipe.research.config import FeatureConfig
from tickpipe.research.features import FeatureContext, FeatureSpec, resolve_feature
from tickpipe.store.pit import PointInTimeView
from tickpipe.store.writer import PartitionManifest

logger = structlog.get_logger(__name__)

FEATURES_DIR_NAME: Final[str] = "features"
FINGERPRINT_SCHEMA_VERSION: Final[int] = 1


@dataclass(frozen=True)
class DatasetSpec:
    """Everything needed to rebuild a feature matrix."""

    dataset: str
    symbols: tuple[str, ...]
    bar_interval_ns: int
    features: tuple[FeatureConfig, ...]
    start_ns: int
    end_ns: int

    def resolved_features(self) -> tuple[FeatureSpec, ...]:
        resolved = []
        for config in self.features:
            params = {"window_s": config.window_s} if config.window_s is not None else None
            resolved.append(resolve_feature(config.name, params))
        return tuple(resolved)

    def canonical_document(self, manifest_hashes: list[str]) -> dict[str, object]:
        return {
            "schema_version": FINGERPRINT_SCHEMA_VERSION,
            "dataset": self.dataset,
            "symbols": list(self.symbols),
            "bar_interval_ns": self.bar_interval_ns,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "manifest_hashes": sorted(manifest_hashes),
            "features": sorted(
                (
                    {
                        "name": feature.name,
                        "version": feature.version,
                        "lookback_ns": feature.lookback_ns,
                    }
                    for feature in self.resolved_features()
                ),
                key=lambda entry: str(entry["name"]),
            ),
        }


class FeatureDataset:
    """Computes feature matrices from a point-in-time view."""

    def __init__(
        self,
        view: PointInTimeView,
        spec: DatasetSpec,
        *,
        data_dir: str | Path,
    ) -> None:
        self.view = view
        self.spec = spec
        self.data_dir = Path(data_dir)

    def manifest_hashes(self) -> list[str]:
        dataset_path = self.data_dir / self.spec.dataset
        hashes = []
        for manifest_path in sorted(dataset_path.rglob("_manifest.json")):
            manifest = PartitionManifest.model_validate_json(manifest_path.read_text())
            hashes.append(manifest.content_sha256)
        return sorted(hashes)

    def fingerprint(self) -> str:
        document = self.spec.canonical_document(self.manifest_hashes())
        payload = json.dumps(document, sort_keys=True).encode()
        return hashlib.sha256(payload).hexdigest()

    def compute(self) -> tuple[pa.Table, str]:
        """Compute the feature matrix and its fingerprint."""
        resolved = self.spec.resolved_features()
        bars = list(range(self.spec.start_ns, self.spec.end_ns + 1, self.spec.bar_interval_ns))
        columns: dict[str, list[Any]] = {
            "symbol": [],
            "bar_end_ns": [],
            **{feature.name: [] for feature in resolved},
        }
        for symbol in self.spec.symbols:
            for bar_end_ns in bars:
                columns["symbol"].append(symbol)
                columns["bar_end_ns"].append(bar_end_ns)
                for feature in resolved:
                    context = FeatureContext(self.view, symbol, bar_end_ns, feature.lookback_ns)
                    columns[feature.name].append(feature.compute(context))
        table = pa.table(
            {
                "symbol": pa.array(columns["symbol"], type=pa.string()),
                "bar_end_ns": pa.array(columns["bar_end_ns"], type=pa.int64()),
                **{
                    feature.name: pa.array(columns[feature.name], type=pa.float64())
                    for feature in resolved
                },
            }
        )
        return table, self.fingerprint()

    def materialize(self) -> tuple[pa.Table, str]:
        """Compute and write ``{data_dir}/features/{fingerprint}.parquet`` + spec JSON."""
        table, fingerprint = self.compute()
        features_dir = self.data_dir / FEATURES_DIR_NAME
        features_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = features_dir / f"{fingerprint}.parquet"
        pq.write_table(table, parquet_path, compression="zstd")
        spec_document = {
            "fingerprint": fingerprint,
            "dataset": self.spec.dataset,
            "symbols": list(self.spec.symbols),
            "bar_interval_ns": self.spec.bar_interval_ns,
            "start_ns": self.spec.start_ns,
            "end_ns": self.spec.end_ns,
            "features": [feature.model_dump() for feature in self.spec.features],
        }
        (features_dir / f"{fingerprint}.json").write_text(
            json.dumps(spec_document, sort_keys=True, indent=2)
        )
        logger.info(
            "dataset_materialized",
            fingerprint=fingerprint,
            rows=table.num_rows,
            path=str(parquet_path),
        )
        return table, fingerprint


def load_dataset_spec(data_dir: str | Path, fingerprint: str) -> DatasetSpec:
    """Rebuild a DatasetSpec from a stored fingerprint (used by replay)."""
    spec_path = Path(data_dir) / FEATURES_DIR_NAME / f"{fingerprint}.json"
    if not spec_path.exists():
        raise FileNotFoundError(f"no stored dataset spec for fingerprint {fingerprint}")
    document = json.loads(spec_path.read_text())
    features = tuple(
        FeatureConfig(name=entry["name"], window_s=entry.get("window_s"))
        for entry in document["features"]
    )
    return DatasetSpec(
        dataset=str(document["dataset"]),
        symbols=tuple(document["symbols"]),
        bar_interval_ns=int(document["bar_interval_ns"]),
        features=features,
        start_ns=int(document["start_ns"]),
        end_ns=int(document["end_ns"]),
    )


__all__ = ["DatasetSpec", "FeatureDataset", "load_dataset_spec"]
