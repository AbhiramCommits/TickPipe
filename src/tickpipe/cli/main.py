"""tickpipe command-line interface."""

from __future__ import annotations

import asyncio
import io
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, TextIO, cast

import structlog
import typer

from tickpipe.core.types import Tick
from tickpipe.research.registry import DirtyWorkingTreeError
from tickpipe.research.runner import (
    replay_run as replay_experiment_run,
)
from tickpipe.research.runner import (
    run_experiment as run_research_experiment,
)
from tickpipe.store.backfill import backfill_trades
from tickpipe.store.writer import PartitionedWriter

if TYPE_CHECKING:
    from tickpipe.ingest.metrics import MetricsServer
    from tickpipe.ingest.pipeline import IngestPipeline


class _AsyncPartitionedWriter:
    """Async BatchWriter adapter over the synchronous PartitionedWriter."""

    def __init__(self, writer: PartitionedWriter) -> None:
        self._writer = writer

    async def open(self) -> None:
        return None

    async def write(self, batch: Sequence[Tick]) -> None:
        self._writer.write_batch(batch)

    async def close(self) -> None:
        self._writer.flush()


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


def _configure_json_logging() -> None:
    """Structured JSON logs on stderr, with run_id correlation from the pipeline."""
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.PrintLoggerFactory(file=cast(TextIO, _LiveStderr())),
    )


@app.callback()
def main() -> None:
    """Route structured logs to stderr so stdout stays machine-parseable."""
    factory = structlog.PrintLoggerFactory(file=cast(TextIO, _LiveStderr()))
    structlog.configure(logger_factory=factory)


@app.command()
def ingest(
    symbol: list[str] = typer.Option(..., "--symbol", help="Product symbol; repeatable"),
    data_dir: Path = typer.Option(Path("data"), "--data-dir", help="Root data directory"),
    dataset: str = typer.Option("trades", "--dataset", help="Dataset name"),
    metrics_port: int = typer.Option(9090, "--metrics-port", help="Metrics/health port"),
    policy: str = typer.Option("block", "--policy", help="Queue-full policy: block | drop-oldest"),
    queue_maxsize: int = typer.Option(1024, "--queue-maxsize", help="Bounded queue size"),
    batch_size: int = typer.Option(512, "--batch-size", help="Writer batch size"),
) -> None:
    """Ingest live market data into the Parquet store (SIGINT/SIGTERM stop cleanly)."""
    from tickpipe.ingest.metrics import IngestMetrics, MetricsServer
    from tickpipe.ingest.pipeline import BackpressurePolicy, IngestPipeline
    from tickpipe.ingest.sources import CoinbaseWebSocketSource

    _configure_json_logging()
    try:
        backpressure = BackpressurePolicy(policy)
    except ValueError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    source = CoinbaseWebSocketSource(symbol)
    writer = _AsyncPartitionedWriter(PartitionedWriter(data_dir, dataset))
    metrics = IngestMetrics()
    pipeline = IngestPipeline(
        source,
        writer,
        policy=backpressure,
        queue_maxsize=queue_maxsize,
        batch_size=batch_size,
        metrics=metrics,
    )
    server = MetricsServer(metrics, host="0.0.0.0", port=metrics_port)
    asyncio.run(_serve_and_ingest(pipeline, server))


async def _serve_and_ingest(pipeline: IngestPipeline, server: MetricsServer) -> None:
    await server.start()
    try:
        metadata = await pipeline.run()
        typer.echo(
            json.dumps(
                {
                    "run_id": str(metadata.run_id),
                    "messages_received": metadata.messages_received,
                    "messages_written": metadata.messages_written,
                    "messages_dropped": metadata.messages_dropped,
                },
                sort_keys=True,
            )
        )
    finally:
        await server.stop()


@app.command()
def sample_data(
    data_dir: Path = typer.Option(Path("data"), "--data-dir", help="Root data directory"),
    count: int = typer.Option(500, "--count", help="Number of trades to generate"),
    seed: int = typer.Option(7, "--seed", help="RNG seed (deterministic)"),
    force: bool = typer.Option(False, "--force", help="Regenerate even if data already exists"),
) -> None:
    """Generate the bundled deterministic sample dataset."""
    from tickpipe.sample_data import generate_sample_trades

    written = generate_sample_trades(data_dir, count=count, seed=seed, force=force)
    typer.echo(f"sample data ready: {written} trades")


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
    result = replay_experiment_run(run_id, data_dir=data_dir, database_url=database_url)
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
