"""Tests for resumable REST backfill (offline, via httpx MockTransport)."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from tickpipe.store.backfill import backfill_trades
from tickpipe.store.reader import TickStore
from tickpipe.store.writer import decimal_to_ticks, read_manifest

ROWS_BY_DATE = {
    "2021-01-01": [
        {
            "time": "2021-01-01T00:00:00.100000Z",
            "trade_id": 10,
            "price": "100.5",
            "size": "0.1",
            "side": "buy",
        },
        {
            "time": "2021-01-01T00:00:01.000000Z",
            "trade_id": 11,
            "price": "101.0",
            "size": "0.2",
            "side": "sell",
        },
    ],
    "2021-01-02": [
        {
            "time": "2021-01-02T00:00:00.000000Z",
            "trade_id": 20,
            "price": "102.5",
            "size": "1.0",
            "side": "buy",
        },
        {
            "time": "2021-01-02T12:00:00.000000Z",
            "trade_id": 21,
            "price": "103.0",
            "size": "0.5",
            "side": "sell",
        },
        {
            "time": "2021-01-02T23:59:59.999999Z",
            "trade_id": 22,
            "price": "104.0",
            "size": "0.25",
            "side": "buy",
        },
    ],
}


def make_handler(rows_by_date: dict[str, list[dict]], request_counter: dict[str, int]):
    def handler(request: httpx.Request) -> httpx.Response:
        request_counter["requests"] += 1
        assert request.url.path == "/products/BTC-USD/trades"
        day = request.url.params["start"][:10]
        rows = [r for r in rows_by_date.get(day, [])]
        after = request.url.params.get("after")
        if after is not None:
            rows = [r for r in rows if r["trade_id"] > int(after)]
        limit = int(request.url.params["limit"])
        return httpx.Response(200, json=rows[:limit])

    return handler


async def run_backfill(tmp_path: Path, page_limit: int = 100) -> tuple:
    counter = {"requests": 0}
    transport = httpx.MockTransport(make_handler(ROWS_BY_DATE, counter))
    async with httpx.AsyncClient(transport=transport, base_url="https://example.invalid") as client:
        first = await backfill_trades(
            "BTC-USD", "2021-01-01", "2021-01-02", tmp_path, client=client, page_limit=page_limit
        )
        second = await backfill_trades(
            "BTC-USD", "2021-01-01", "2021-01-02", tmp_path, client=client, page_limit=page_limit
        )
    return first, second, counter


def test_backfill_writes_then_skips_on_identical_hash(tmp_path: Path) -> None:
    first, second, _ = asyncio.run(run_backfill(tmp_path))
    assert first.rows == 5
    assert first.written_partitions == 2
    assert first.skipped_partitions == 0
    assert second.rows == 5
    assert second.written_partitions == 0
    assert second.skipped_partitions == 2

    manifests = sorted(tmp_path.rglob("_manifest.json"))
    assert len(manifests) == 2
    first_manifest = read_manifest(manifests[0].parent)
    assert first_manifest is not None
    assert first_manifest.symbol == "BTC-USD"
    assert first_manifest.row_count in (2, 3)

    store = TickStore(tmp_path, "trades")
    table = store.scan(symbols=None, start_ns=0, end_ns=None)
    assert table.num_rows == 5
    prices = sorted(table.column("price_ticks").to_pylist())
    expected_prices = sorted(
        decimal_to_ticks(Decimal(row["price"])) for rows in ROWS_BY_DATE.values() for row in rows
    )
    assert prices == expected_prices


def test_backfill_paginates_with_after_cursor(tmp_path: Path) -> None:
    first, _, counter = asyncio.run(run_backfill(tmp_path, page_limit=1))
    assert first.rows == 5
    assert first.written_partitions == 2
    assert counter["requests"] > 5  # one page per row plus termination pages


def test_backfill_empty_range_writes_nothing(tmp_path: Path) -> None:
    async def scenario() -> None:
        counter = {"requests": 0}
        transport = httpx.MockTransport(make_handler({}, counter))
        async with httpx.AsyncClient(
            transport=transport, base_url="https://example.invalid"
        ) as client:
            summary = await backfill_trades(
                "BTC-USD", "2021-01-03", "2021-01-03", tmp_path, client=client
            )
        assert summary.rows == 0
        assert summary.written_partitions == 0

    asyncio.run(scenario())
    assert not list(tmp_path.rglob("*.parquet"))


def test_backfill_rejects_reversed_dates(tmp_path: Path) -> None:
    async def scenario() -> None:
        transport = httpx.MockTransport(make_handler(ROWS_BY_DATE, {"requests": 0}))
        async with httpx.AsyncClient(
            transport=transport, base_url="https://example.invalid"
        ) as client:
            with pytest.raises(ValueError):
                await backfill_trades(
                    "BTC-USD", "2021-01-03", "2021-01-01", tmp_path, client=client
                )

    asyncio.run(scenario())
