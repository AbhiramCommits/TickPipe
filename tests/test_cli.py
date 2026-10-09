"""Smoke tests for the tickpipe CLI."""

from __future__ import annotations

import re

import pytest
from typer.testing import CliRunner

from tickpipe.cli.main import app

runner = CliRunner()

_ANSI = re.compile(r"\x1b\[[0-9;]*m")

SUBCOMMANDS = [
    "ingest",
    "sample-data",
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
    # Rich colours help output when it detects CI (e.g. GITHUB_ACTIONS), which
    # splits "--help" with ANSI escapes; compare against the plain text.
    output = _ANSI.sub("", result.output)
    assert "--help" in output
    assert subcommand in output
