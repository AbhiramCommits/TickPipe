"""tickpipe command-line interface."""

from __future__ import annotations

import typer

app = typer.Typer(
    name="tickpipe",
    help="Market-data ingestion and backtest research harness.",
    no_args_is_help=True,
)


@app.command()
def ingest() -> None:
    """Ingest market data from live exchange feeds. (stub)"""
    typer.echo("ingest: not implemented")


@app.command()
def backfill() -> None:
    """Backfill historical market data from an exchange archive. (stub)"""
    typer.echo("backfill: not implemented")


@app.command()
def query() -> None:
    """Query the market-data store. (stub)"""
    typer.echo("query: not implemented")


@app.command()
def backtest() -> None:
    """Run a backtest against stored market data. (stub)"""
    typer.echo("backtest: not implemented")


@app.command()
def run_experiment() -> None:
    """Run a registered experiment. (stub)"""
    typer.echo("run-experiment: not implemented")


if __name__ == "__main__":
    app()
