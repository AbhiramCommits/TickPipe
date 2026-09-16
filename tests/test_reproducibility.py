"""The reproducibility gate: replaying a run must be bit-for-bit identical.

Runs the full experiment path (dataset build -> train -> backtest -> register)
through the CLI, captures the run_id, then replays from the registry record
alone via ``tickpipe replay-run``. The recreated dataset fingerprint must be
identical and every metric must match exactly (``==``, never
``pytest.approx``). Any nondeterminism fails this test loudly.

In CI this runs against PostgreSQL (TICKPIPE_TEST_DATABASE_URL).
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from pathlib import Path

import numpy as np
from typer.testing import CliRunner

from tickpipe.cli.main import app
from tickpipe.core.types import Trade
from tickpipe.research.features import FEATURE_REGISTRY, FeatureSpec, mid_price
from tickpipe.research.registry import ExperimentRegistry
from tickpipe.research.runner import replay_run
from tickpipe.store.writer import PartitionedWriter

BASE_NS = 1_700_000_000_000_000_000
SCALE = 10**9

runner = CliRunner()

CONFIG_YAML = """\
dataset: trades
bar_interval_s: 60
features:
  - name: mid_price
  - name: realized_vol
    window_s: 120
  - name: trade_count
    window_s: 60
  - name: order_flow_imbalance
    window_s: 120
train:
  model: gradient_boosting
  horizon_bars: 1
  n_splits: 3
  seed: 42
  gbm_n_estimators: 30
  gbm_max_depth: 2
"""


def write_synthetic_trades(data_dir: Path, count: int = 300) -> None:
    rng = np.random.default_rng(7)
    writer = PartitionedWriter(data_dir, "trades", max_open_s=None)
    price = 100.0
    for index in range(count):
        ts_ns = BASE_NS + index * 5 * SCALE
        price *= 1 + rng.normal(0, 1e-3)
        writer.write(
            Trade(
                symbol="BTC-USD",
                exchange_ts_ns=ts_ns,
                ingest_ts_ns=ts_ns + 1,
                price=Decimal(f"{price:.6f}"),
                size=Decimal("1"),
                trade_id=f"t-{index}",
                sequence=index + 1,
            )
        )
    writer.flush()


def parse_run_id(output: str) -> str:
    for line in output.splitlines():
        if line.startswith("run_id="):
            return line[len("run_id=") :]
    raise AssertionError(f"no run_id in CLI output:\n{output}")


def test_experiment_replay_is_bit_for_bit_identical(tmp_path: Path, database_url: str) -> None:
    data_dir = tmp_path / "data"
    write_synthetic_trades(data_dir)
    config_path = tmp_path / "experiment.yaml"
    config_path.write_text(CONFIG_YAML)

    first = runner.invoke(
        app,
        [
            "run-experiment",
            "--config",
            str(config_path),
            "--data-dir",
            str(data_dir),
            "--database-url",
            database_url,
            "--allow-dirty",
        ],
    )
    assert first.exit_code == 0, first.output
    run_id = parse_run_id(first.output)

    second = runner.invoke(
        app,
        [
            "replay-run",
            run_id,
            "--data-dir",
            str(data_dir),
            "--database-url",
            database_url,
        ],
    )
    assert second.exit_code == 0, second.output
    assert "fingerprint_identical=True" in second.output
    assert "metric_mismatches=0" in second.output

    # The hard gate: exact equality, never pytest.approx.
    registry = ExperimentRegistry(database_url)
    record = registry.get_run(uuid.UUID(run_id))
    assert record is not None
    replayed = replay_run(run_id, data_dir=data_dir, database_url=database_url)
    assert replayed.fingerprint_ok
    assert replayed.recorded_fingerprint == record.dataset_fingerprint
    assert replayed.metric_mismatches == {}
    assert replayed.metrics == record.metrics  # bit-for-bit
    assert record.seed == 42


def test_replay_fails_loudly_on_nondeterminism(
    tmp_path: Path, database_url: str, monkeypatch
) -> None:
    data_dir = tmp_path / "data"
    write_synthetic_trades(data_dir)
    config_path = tmp_path / "experiment.yaml"
    config_path.write_text(CONFIG_YAML)

    first = runner.invoke(
        app,
        [
            "run-experiment",
            "--config",
            str(config_path),
            "--data-dir",
            str(data_dir),
            "--database-url",
            database_url,
            "--allow-dirty",
        ],
    )
    assert first.exit_code == 0, first.output
    run_id = parse_run_id(first.output)

    # Corrupt determinism: a feature that returns noise on every call.
    rng = np.random.default_rng()
    noisy = FeatureSpec(
        name="mid_price",
        version=1,
        lookback_ns=60 * SCALE,
        compute=lambda ctx: mid_price(ctx) * (1 + 1e-9 * rng.random()),
    )
    monkeypatch.setitem(FEATURE_REGISTRY, "mid_price", noisy)

    second = runner.invoke(
        app,
        [
            "replay-run",
            run_id,
            "--data-dir",
            str(data_dir),
            "--database-url",
            database_url,
        ],
    )
    assert second.exit_code == 1  # fails loudly
    assert "metric mismatches" in second.output
