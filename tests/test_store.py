"""Tests for the partitioned Parquet store: writing, reading, pruning."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tickpipe.core.types import BookDelta, Tick, Trade
from tickpipe.store.reader import TickStore
from tickpipe.store.writer import (
    SCAN_COLUMNS,
    PartitionedWriter,
    date_str_to_ns,
    decimal_to_ticks,
    read_manifest,
    ticks_to_table,
)

BASE_NS = date_str_to_ns("2021-01-01")
HOUR_NS = 3_600 * 10**9


def make_trade(
    trade_id: str,
    exchange_ts_ns: int,
    symbol: str = "BTC-USD",
    sequence: int | None = None,
) -> Trade:
    return Trade(
        symbol=symbol,
        exchange_ts_ns=exchange_ts_ns,
        ingest_ts_ns=exchange_ts_ns + 100,
        price=Decimal("42000.5000001"),
        size=Decimal("0.123456789"),
        trade_id=trade_id,
        sequence=int(trade_id) if sequence is None else sequence,
    )


def make_delta(exchange_ts_ns: int, side: str = "bid") -> BookDelta:
    return BookDelta(
        symbol="BTC-USD",
        exchange_ts_ns=exchange_ts_ns,
        ingest_ts_ns=exchange_ts_ns + 100,
        side=side,
        price=Decimal("42001.25"),
        size=Decimal("1.5"),
        sequence=7,
    )


def mixed_ticks() -> list[Tick]:
    return [
        make_trade("1", BASE_NS),
        make_trade("2", BASE_NS + 1),
        make_delta(BASE_NS + 2, side="bid"),
        make_trade("3", BASE_NS + HOUR_NS),
        make_trade("4", BASE_NS + HOUR_NS + 1),
        make_trade("5", BASE_NS + 24 * HOUR_NS, symbol="ETH-USD"),
        make_delta(BASE_NS + 24 * HOUR_NS + 1, side="ask"),
    ]


def canonicalize(table: pa.Table) -> pa.Table:
    columns = []
    for name in SCAN_COLUMNS:
        column = table[name]
        if pa.types.is_dictionary(column.type):
            column = column.cast(pa.string())
        columns.append(column)
    return pa.table(dict(zip(SCAN_COLUMNS, columns, strict=True)))


def test_round_trip_is_byte_identical_in_values_and_dtypes(tmp_path: Path) -> None:
    ticks = mixed_ticks()
    writer = PartitionedWriter(tmp_path, "trades", max_rows_per_file=3, max_open_s=None)
    writer.write_batch(ticks)
    manifests = writer.flush()

    assert len(manifests) == 3  # (BTC-USD x2 dates) + (ETH-USD x1 date)
    assert not list(tmp_path.rglob("*.tmp"))

    part_files = sorted(tmp_path.rglob("part-*.parquet"))
    assert len(part_files) == 4  # 7 rows at 3 rows per file

    written_schema = pq.read_schema(part_files[0])
    assert written_schema.field("exchange_ts_ns").type == pa.int64()
    assert written_schema.field("ingest_ts_ns").type == pa.int64()
    assert written_schema.field("sequence").type == pa.int64()
    assert written_schema.field("price_ticks").type == pa.int64()
    assert written_schema.field("size_ticks").type == pa.int64()
    assert pa.types.is_dictionary(written_schema.field("symbol").type)
    assert pa.types.is_dictionary(written_schema.field("kind").type)

    for manifest in manifests:
        assert manifest.row_count > 0
        assert manifest.min_exchange_ts_ns <= manifest.max_exchange_ts_ns
        assert manifest.parts
        assert len(manifest.content_sha256) == 64

    store = TickStore(tmp_path, "trades")
    got = store.scan(symbols=None, start_ns=0, end_ns=None)
    expected = ticks_to_table(ticks)

    sort_keys = [("exchange_ts_ns", "ascending"), ("sequence", "ascending"), ("kind", "ascending")]
    assert canonicalize(got).sort_by(sort_keys).equals(canonicalize(expected).sort_by(sort_keys))

    first = read_manifest(Path(part_files[0]).parent)
    assert first is not None
    assert first.dataset == "trades"
    assert first.row_count == 5
    assert len(first.parts) == 2


def test_partition_pruning_reads_strictly_fewer_files_than_full_scan(tmp_path: Path) -> None:
    writer = PartitionedWriter(tmp_path, "trades", max_rows_per_file=10, max_open_s=None)
    ticks: list[Tick] = []
    for symbol in ("AAA-BBB", "CCC-DDD"):
        for day_offset in range(3):
            for row in range(25):
                ticks.append(
                    make_trade(
                        f"{symbol}-{day_offset}-{row}",
                        BASE_NS + day_offset * 24 * HOUR_NS + row,
                        symbol=symbol,
                        sequence=row,
                    )
                )
    writer.write_batch(ticks)
    writer.flush()

    store = TickStore(tmp_path, "trades")
    full_files = store.count_files_scanned(symbols=None, start_ns=0, end_ns=None)
    assert full_files == 18  # 2 symbols x 3 dates x 3 parts

    one_day_ns = date_str_to_ns("2021-01-01")
    pruned_files = store.count_files_scanned(
        symbols=["AAA-BBB"], start_ns=one_day_ns, end_ns=one_day_ns + 24 * HOUR_NS
    )
    assert pruned_files == 3
    assert pruned_files < full_files

    table = store.scan(
        symbols=["AAA-BBB"], start_ns=one_day_ns, end_ns=one_day_ns + 24 * HOUR_NS
    )
    assert table.num_rows == 25
    symbols = set(table.column("symbol").to_pylist())
    assert symbols == {"AAA-BBB"}
    assert min(table.column("exchange_ts_ns").to_pylist()) >= one_day_ns
    assert max(table.column("exchange_ts_ns").to_pylist()) < one_day_ns + 24 * HOUR_NS


def test_scan_of_empty_range_returns_empty_table(tmp_path: Path) -> None:
    writer = PartitionedWriter(tmp_path, "trades", max_open_s=None)
    writer.write(make_trade("1", BASE_NS))
    writer.flush()
    store = TickStore(tmp_path, "trades")
    table = store.scan(symbols=None, start_ns=BASE_NS + 2, end_ns=BASE_NS + 3)
    assert table.num_rows == 0
    assert store.count_files_scanned(symbols=None, start_ns=BASE_NS + 2, end_ns=BASE_NS + 3) == 0


def test_scan_missing_dataset_returns_empty(tmp_path: Path) -> None:
    store = TickStore(tmp_path, "trades")
    table = store.scan(symbols=None, start_ns=0, end_ns=None)
    assert table.num_rows == 0


def test_tick_scale_conversions_round_trip() -> None:
    from tickpipe.store.writer import TICK_SCALE, decimal_to_ticks, ticks_to_decimal

    assert decimal_to_ticks(Decimal("42000.5000001")) == 42_000_500_000_100
    assert ticks_to_decimal(42_000_500_000_100) == Decimal("42000.5000001")
    assert TICK_SCALE == 10**9


def test_writer_rejects_non_positive_file_sizes(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        PartitionedWriter(tmp_path, "trades", max_rows_per_file=0)


def test_prices_round_trip_through_ticks_on_disk(tmp_path: Path) -> None:
    trade = make_trade("42", BASE_NS)
    writer = PartitionedWriter(tmp_path, "trades", max_open_s=None)
    writer.write(trade)
    writer.flush()
    table = TickStore(tmp_path, "trades").scan(symbols=None, start_ns=0, end_ns=None)
    assert table.column("price_ticks").to_pylist() == [decimal_to_ticks(trade.price)]
    assert table.column("size_ticks").to_pylist() == [decimal_to_ticks(trade.size)]
