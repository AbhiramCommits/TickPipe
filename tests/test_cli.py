"""Smoke tests for the tickpipe CLI."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from tickpipe.cli.main import app

runner = CliRunner()

SUBCOMMANDS = [
    "ingest",
    "backfill",
    "query",
    "backtest",
    "run-experiment",
    "replay-run",
]


@pytest.mark.parametrize("subcommand", SUBCOMMANDS)
def test_subcommand_shows_help_and_exits_zero(subcommand: str) -> None:
    result = runner.invoke(app, [subcommand, "--help"])
    assert result.exit_code == 0
    assert "--help" in result.output
    assert subcommand in result.output
