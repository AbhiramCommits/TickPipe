"""tickpipe command-line interface."""

from __future__ import annotations

import asyncio
import io
import json
import sys
from pathlib import Path
from typing import TextIO, cast

import structlog
import typer

from tickpipe.research.registry import DirtyWorkingTreeError
from tickpipe.research.runner import (
    replay_run as replay_experiment_run,
)
from tickpipe.research.runner import (
    run_experiment as run_research_experiment,
)
from tickpipe.store.backfill import backfill_trades

app = typer.Typer(
    name="tickpipe",
    help="Market-data ingestion and backtest research harness.",
    no_args_is_help=True,
)


class _LiveStderr(io.TextIOBase):
    """File-like that always writes to the *current* sys.stderr."""

    def write(self, message: str) -> int:
        return sys.stderr.write(message)

    def flush(self) -> None:
        sys.stderr.flush()


@app.callback()
def main() -> None:
    """Route structured logs to stderr so stdout stays machine-parseable."""
    structlog.configure(
        logger_factory=structlog.PrintLoggerFactory(file=cast(TextIO, _LiveStderr()))
    )


@app.command()
def ingest() -> None:
    """Ingest market data from live exchange feeds. (stub)"""
    typer.echo("ingest: not implemented")


@app.command()
def backfill(
    symbol: str = typer.Option(..., "--symbol", help="Product symbol, e.g. BTC-USD"),
    start_date: str = typer.Option(
        ..., "--start-date", help="First day to backfill, YYYY-MM-DD (inclusive)"
    ),
    end_date: str = typer.Option(
        ..., "--end-date", help="Last day to backfill, YYYY-MM-DD (inclusive)"
    ),
    data_dir: Path = typer.Option(Path("data"), "--data-dir", help="Root data directory"),
    dataset: str = typer.Option("trades", "--dataset", help="Dataset name"),
    base_url: str = typer.Option(
        "https://api.exchange.coinbase.com", "--base-url", help="Public REST base URL"
    ),
) -> None:
    """Backfill historical trades from a public REST endpoint."""
    summary = asyncio.run(
        backfill_trades(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            data_dir=data_dir,
            dataset=dataset,
            base_url=base_url,
        )
    )
    typer.echo(
        f"backfilled {summary.rows} rows: "
        f"{summary.written_partitions} partitions written, "
        f"{summary.skipped_partitions} skipped"
    )


@app.command()
def query() -> None:
    """Query the market-data store. (stub)"""
    typer.echo("query: not implemented")


@app.command()
def backtest() -> None:
    """Run a backtest against stored market data. (stub)"""
    typer.echo("backtest: not implemented")


@app.command()
def run_experiment(
    config: Path = typer.Option(..., "--config", exists=True, help="Experiment YAML config"),
    data_dir: Path = typer.Option(Path("data"), "--data-dir", help="Root data directory"),
    database_url: str | None = typer.Option(
        None, "--database-url", help="Registry database URL (default: env or local sqlite)"
    ),
    allow_dirty: bool = typer.Option(
        False, "--allow-dirty", help="Register even when the git working tree is dirty"
    ),
) -> None:
    """Run the full experiment path: dataset build, train, backtest, register."""
    try:
        record = run_research_experiment(
            config,
            data_dir=data_dir,
            database_url=database_url,
            allow_dirty=allow_dirty,
        )
    except DirtyWorkingTreeError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"run_id={record.run_id}")
    typer.echo(json.dumps(record.metrics, sort_keys=True, indent=2))


@app.command()
def replay_run(
    run_id: str = typer.Argument(..., help="Registry run id (UUID)"),
    data_dir: Path = typer.Option(Path("data"), "--data-dir", help="Root data directory"),
    database_url: str | None = typer.Option(
        None, "--database-url", help="Registry database URL (default: env or local sqlite)"
    ),
) -> None:
    """Replay a registered run and verify bit-for-bit reproducibility."""
    result = replay_experiment_run(
        run_id, data_dir=data_dir, database_url=database_url
    )
    typer.echo(f"run_id={result.run_id}")
    typer.echo(f"recorded_fingerprint={result.recorded_fingerprint}")
    typer.echo(f"recreated_fingerprint={result.recreated_fingerprint}")
    typer.echo(f"fingerprint_identical={result.fingerprint_ok}")
    if result.metric_mismatches:
        typer.echo("metric mismatches:")
        for key, (recorded, replayed) in result.metric_mismatches.items():
            typer.echo(f"  {key}: recorded={recorded} replayed={replayed}")
    else:
        typer.echo("metric_mismatches=0")
    if not result.fingerprint_ok or result.metric_mismatches:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
