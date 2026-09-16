"""Tests for the bundled deterministic sample dataset generator."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from tickpipe.cli.main import app
from tickpipe.sample_data import generate_sample_trades
from tickpipe.store.reader import TickStore

runner = CliRunner()


def test_sample_data_command_writes_deterministic_dataset(tmp_path: Path) -> None:
    first = runner.invoke(
        app, ["sample-data", "--data-dir", str(tmp_path), "--count", "50"]
    )
    assert first.exit_code == 0
    second = runner.invoke(
        app, ["sample-data", "--data-dir", str(tmp_path), "--count", "50"]
    )
    assert second.exit_code == 0
    assert "0 trades" in second.output  # idempotent without --force
    table = TickStore(tmp_path, "trades").scan(symbols=None, start_ns=0, end_ns=None)
    assert table.num_rows == 50


def test_sample_data_force_regenerates(tmp_path: Path) -> None:
    generate_sample_trades(tmp_path, count=10)
    generate_sample_trades(tmp_path, count=20, force=True)
    table = TickStore(tmp_path, "trades").scan(symbols=None, start_ns=0, end_ns=None)
    assert table.num_rows == 20
