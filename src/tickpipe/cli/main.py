"""tickpipe command-line interface."""

from __future__ import annotations

import asyncio
from pathlib import Path

import typer

from tickpipe.store.backfill import backfill_trades

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
def run_experiment() -> None:
    """Run a registered experiment. (stub)"""
    typer.echo("run-experiment: not implemented")


if __name__ == "__main__":
    app()
