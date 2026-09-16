"""Resumable backfill of historical trades from a public REST endpoint.

Pulls trades day by day, normalizes them to
:class:`~tickpipe.core.types.Trade` ticks, and writes each day through
:class:`PartitionedWriter`. A day whose ``_manifest.json`` content hash
already matches the freshly pulled data is skipped, making backfill
resumable: re-running after a partial failure only writes missing or changed
partitions.

Coinbase Exchange REST trades expose ``trade_id`` but no venue sequence;
``trade_id`` is monotonic and is used as the tick sequence.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import httpx
import structlog

from tickpipe.core.types import Trade
from tickpipe.ingest.sources import iso_8601_to_utc_ns
from tickpipe.store.writer import (
    PartitionedWriter,
    date_str_to_ns,
    partition_content_hash,
    read_manifest,
)

logger = structlog.get_logger(__name__)

DEFAULT_BASE_URL: str = "https://api.exchange.coinbase.com"
DEFAULT_PAGE_LIMIT: int = 100


@dataclass(frozen=True)
class BackfillSummary:
    """Outcome of a backfill run."""

    symbol: str
    days_requested: int
    rows: int
    written_partitions: int
    skipped_partitions: int


async def backfill_trades(
    symbol: str,
    start_date: str,
    end_date: str,
    data_dir: str | Path,
    dataset: str = "trades",
    base_url: str = DEFAULT_BASE_URL,
    *,
    client: httpx.AsyncClient | None = None,
    page_limit: int = DEFAULT_PAGE_LIMIT,
) -> BackfillSummary:
    """Backfill ``symbol`` over the inclusive ``[start_date, end_date]`` range."""
    start_day = date.fromisoformat(start_date)
    end_day = date.fromisoformat(end_date)
    if end_day < start_day:
        raise ValueError("end_date must not be before start_date")
    days = _day_range(start_day, end_day)
    if client is None:
        async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as http:
            return await _backfill_days(
                http, symbol, days, data_dir, dataset, page_limit=page_limit
            )
    return await _backfill_days(client, symbol, days, data_dir, dataset, page_limit=page_limit)


async def _backfill_days(
    client: httpx.AsyncClient,
    symbol: str,
    days: list[date],
    data_dir: str | Path,
    dataset: str,
    *,
    page_limit: int,
) -> BackfillSummary:
    writer = PartitionedWriter(data_dir, dataset)
    rows_total = 0
    written = 0
    skipped = 0
    for day in days:
        day_start_ns = date_str_to_ns(day.isoformat())
        day_end_ns = day_start_ns + 86_400 * 10**9
        ticks = await fetch_day_trades(
            client, symbol, day_start_ns, day_end_ns, page_limit=page_limit
        )
        if not ticks:
            continue
        content = partition_content_hash(ticks)
        partition_dir = Path(data_dir) / dataset / f"symbol={symbol}" / f"date={day.isoformat()}"
        existing = read_manifest(partition_dir)
        if existing is not None and existing.content_sha256 == content:
            logger.info(
                "backfill_partition_skipped",
                symbol=symbol,
                date=day.isoformat(),
                row_count=existing.row_count,
            )
            skipped += 1
            rows_total += existing.row_count
            continue
        writer.write_batch(ticks)
        writer.flush()
        logger.info(
            "backfill_partition_written",
            symbol=symbol,
            date=day.isoformat(),
            rows=len(ticks),
        )
        written += 1
        rows_total += len(ticks)
    return BackfillSummary(
        symbol=symbol,
        days_requested=len(days),
        rows=rows_total,
        written_partitions=written,
        skipped_partitions=skipped,
    )


def _day_range(start_day: date, end_day: date) -> list[date]:
    return [start_day + timedelta(days=i) for i in range((end_day - start_day).days + 1)]


async def fetch_day_trades(
    client: httpx.AsyncClient,
    symbol: str,
    day_start_ns: int,
    day_end_ns: int,
    *,
    page_limit: int = DEFAULT_PAGE_LIMIT,
) -> list[Trade]:
    """Fetch and normalize all trades for ``[day_start_ns, day_end_ns)``."""
    ticks: list[Trade] = []
    seen: set[str] = set()
    after: str | None = None
    while True:
        page = await _fetch_page(
            client, symbol, day_start_ns, day_end_ns, after=after, limit=page_limit
        )
        if not page:
            break
        ingest_ts_ns = time.time_ns()
        for row in page:
            trade_id = str(row["trade_id"])
            if trade_id in seen:
                continue
            seen.add(trade_id)
            ticks.append(normalize_trade_row(row, symbol, ingest_ts_ns))
        if len(page) < page_limit:
            break
        after = str(page[-1]["trade_id"])
    return ticks


async def _fetch_page(
    client: httpx.AsyncClient,
    symbol: str,
    start_ns: int,
    end_ns: int,
    *,
    after: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    params: dict[str, str] = {
        "start": _ns_to_iso(start_ns),
        "end": _ns_to_iso(end_ns),
        "limit": str(limit),
    }
    if after is not None:
        params["after"] = after
    response = await client.get(f"/products/{symbol}/trades", params=params)
    response.raise_for_status()
    return cast(list[dict[str, Any]], response.json())


def normalize_trade_row(row: dict[str, Any], symbol: str, ingest_ts_ns: int) -> Trade:
    """Normalize one Coinbase-style REST trade row into a Trade tick."""
    trade_id = int(row["trade_id"])
    return Trade(
        symbol=symbol,
        exchange_ts_ns=iso_8601_to_utc_ns(str(row["time"])),
        ingest_ts_ns=ingest_ts_ns,
        price=Decimal(str(row["price"])),
        size=Decimal(str(row["size"])),
        trade_id=str(trade_id),
        sequence=trade_id,
    )


def _ns_to_iso(ts_ns: int) -> str:
    seconds = ts_ns // 10**9
    dt = datetime.fromtimestamp(seconds, tz=UTC)
    return dt.isoformat().replace("+00:00", "Z")


__all__ = [
    "BackfillSummary",
    "backfill_trades",
    "fetch_day_trades",
    "normalize_trade_row",
]
