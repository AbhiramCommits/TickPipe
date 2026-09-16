"""Partitioned, atomic Parquet writing for tick data.

Layout: ``{base}/{dataset}/symbol={SYM}/date={YYYY-MM-DD}/part-{n}.parquet``
plus a ``_manifest.json`` per ``(symbol, date)`` partition.

On-disk conventions
-------------------
Prices and sizes are converted to int64 ticks with a fixed scale of
``TICK_SCALE = 10**9`` (see ``tickpipe.core.types`` for the Decimal-at-the-
boundary / int-ticks-internally convention). Values with at most nine
fractional digits round-trip exactly.

Writes are atomic: files are written to a ``.tmp`` path and renamed into
place. The content hash in each manifest is computed over the canonical
per-row representation in arrival order, so it is independent of file
splits and stable across identical backfill pulls; backfill uses it to skip
already-written partitions.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Final

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from pydantic import BaseModel, ConfigDict

from tickpipe.core.types import BookDelta, Tick, Trade

logger = structlog.get_logger(__name__)

TICK_SCALE: Final[int] = 10**9
DATE_FORMAT: Final[str] = "%Y-%m-%d"
MANIFEST_NAME: Final[str] = "_manifest.json"
TMP_SUFFIX: Final[str] = ".tmp"

DEFAULT_MAX_ROWS_PER_FILE: Final[int] = 500_000
DEFAULT_MAX_OPEN_S: Final[float] = 60.0
DEFAULT_COMPRESSION: Final[str] = "zstd"
DEFAULT_COMPRESSION_LEVEL: Final[int] = 3

ROW_GROUP_TARGET_BYTES: Final[int] = 128 * 1024 * 1024
MIN_ROWS_PER_ROW_GROUP: Final[int] = 1_024
MAX_ROWS_PER_ROW_GROUP: Final[int] = 1_000_000

_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)
_NANOS_PER_DAY: Final[int] = 86_400 * 10**9
MAX_INT64: Final[int] = 2**63 - 1

TICK_ARROW_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("kind", pa.dictionary(pa.int32(), pa.string()), nullable=False),
        pa.field("symbol", pa.dictionary(pa.int32(), pa.string()), nullable=False),
        pa.field("exchange_ts_ns", pa.int64(), nullable=False),
        pa.field("ingest_ts_ns", pa.int64(), nullable=False),
        pa.field("sequence", pa.int64(), nullable=False),
        pa.field("price_ticks", pa.int64(), nullable=False),
        pa.field("size_ticks", pa.int64(), nullable=False),
        pa.field("side", pa.string(), nullable=True),
        pa.field("trade_id", pa.string(), nullable=True),
    ]
)

SCAN_COLUMNS: Final[tuple[str, ...]] = (
    "kind",
    "symbol",
    "exchange_ts_ns",
    "ingest_ts_ns",
    "sequence",
    "price_ticks",
    "size_ticks",
    "side",
    "trade_id",
)


def decimal_to_ticks(value: Decimal, *, scale: int = TICK_SCALE) -> int:
    """Convert a Decimal to int64 ticks at the given scale, rounding half-up."""
    return int((value * scale).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def ticks_to_decimal(ticks: int, *, scale: int = TICK_SCALE) -> Decimal:
    return Decimal(ticks) / Decimal(scale)


def ns_to_date_str(exchange_ts_ns: int) -> str:
    seconds = exchange_ts_ns // 10**9
    return datetime.fromtimestamp(seconds, tz=UTC).strftime(DATE_FORMAT)


def date_str_to_ns(date_str: str) -> int:
    day = datetime.strptime(date_str, DATE_FORMAT).replace(tzinfo=UTC)
    delta = day - _EPOCH_UTC
    return (delta.days * 86_400 + delta.seconds) * 10**9


def ticks_to_table(ticks: Sequence[Tick]) -> pa.Table:
    """Convert ticks to a table with the canonical on-disk Arrow schema."""
    kinds = ["trade" if isinstance(tick, Trade) else "book_delta" for tick in ticks]
    symbols = [tick.symbol for tick in ticks]
    exchange = [tick.exchange_ts_ns for tick in ticks]
    ingest = [tick.ingest_ts_ns for tick in ticks]
    sequences = [tick.sequence for tick in ticks]
    prices = [decimal_to_ticks(tick.price) for tick in ticks]
    sizes = [decimal_to_ticks(tick.size) for tick in ticks]
    sides = [tick.side if isinstance(tick, BookDelta) else None for tick in ticks]
    trade_ids = [tick.trade_id if isinstance(tick, Trade) else None for tick in ticks]
    return pa.table(
        {
            "kind": pa.array(kinds, type=pa.dictionary(pa.int32(), pa.string())),
            "symbol": pa.array(symbols, type=pa.dictionary(pa.int32(), pa.string())),
            "exchange_ts_ns": pa.array(exchange, type=pa.int64()),
            "ingest_ts_ns": pa.array(ingest, type=pa.int64()),
            "sequence": pa.array(sequences, type=pa.int64()),
            "price_ticks": pa.array(prices, type=pa.int64()),
            "size_ticks": pa.array(sizes, type=pa.int64()),
            "side": pa.array(sides, type=pa.string()),
            "trade_id": pa.array(trade_ids, type=pa.string()),
        },
        schema=TICK_ARROW_SCHEMA,
    )


def tick_canonical_bytes(tick: Tick) -> bytes:
    """Canonical bytes for one tick, used for partition content hashing.

    ``ingest_ts_ns`` is deliberately excluded: it is a local stamp that
    legitimately varies between pulls, while the hash is meant to capture the
    venue content of a partition so backfill can skip identical re-pulls.
    """
    kind = "trade" if isinstance(tick, Trade) else "book_delta"
    side = tick.side if isinstance(tick, BookDelta) else ""
    trade_id = tick.trade_id if isinstance(tick, Trade) else ""
    return (
        f"{kind}|{tick.symbol}|{tick.exchange_ts_ns}|"
        f"{tick.sequence}|{decimal_to_ticks(tick.price)}|{decimal_to_ticks(tick.size)}|"
        f"{side}|{trade_id}".encode()
    )


def partition_content_hash(ticks: Iterable[Tick]) -> str:
    """SHA-256 over the canonical per-row bytes, in arrival order."""
    digest = hashlib.sha256()
    for tick in ticks:
        digest.update(tick_canonical_bytes(tick))
        digest.update(b"\n")
    return digest.hexdigest()


class PartitionManifest(BaseModel):
    """Per-partition metadata: counts, time bounds, and a content hash."""

    model_config = ConfigDict(frozen=True)

    schema_version: int = 1
    dataset: str
    symbol: str
    date: str
    row_count: int
    min_exchange_ts_ns: int
    max_exchange_ts_ns: int
    content_sha256: str
    parts: tuple[str, ...]


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + TMP_SUFFIX)
    tmp.write_bytes(data)
    os.replace(tmp, path)


def write_manifest(partition_dir: Path, manifest: PartitionManifest) -> None:
    data = json.dumps(manifest.model_dump(), indent=2, sort_keys=True).encode()
    _atomic_write_bytes(partition_dir / MANIFEST_NAME, data)


def read_manifest(partition_dir: Path) -> PartitionManifest | None:
    path = partition_dir / MANIFEST_NAME
    if not path.exists():
        return None
    return PartitionManifest.model_validate_json(path.read_text())


def _row_group_size_for_schema(schema: pa.Schema) -> int:
    estimated_bytes_per_row = 0
    for column in schema:
        if pa.types.is_integer(column.type):
            estimated_bytes_per_row += 8
        else:
            estimated_bytes_per_row += 32
    estimated_bytes_per_row += 16
    rows = ROW_GROUP_TARGET_BYTES // estimated_bytes_per_row
    return max(MIN_ROWS_PER_ROW_GROUP, min(MAX_ROWS_PER_ROW_GROUP, rows))


@dataclass
class _PartitionBuffer:
    ticks: list[Tick]
    started_monotonic: float
    row_count: int = 0
    min_exchange_ts_ns: int = MAX_INT64
    max_exchange_ts_ns: int = 0
    digest: hashlib._Hash = field(default_factory=hashlib.sha256)
    parts: list[str] = field(default_factory=list)


class PartitionedWriter:
    """Buffers ticks per ``(symbol, date)`` and writes atomic Parquet part files.

    Files roll when a partition buffer reaches ``max_rows_per_file`` rows or
    has been open longer than ``max_open_s`` seconds. Manifests are written
    on :meth:`flush`.
    """

    def __init__(
        self,
        base_path: str | Path,
        dataset: str = "trades",
        *,
        max_rows_per_file: int = DEFAULT_MAX_ROWS_PER_FILE,
        max_open_s: float | None = DEFAULT_MAX_OPEN_S,
        compression: str = DEFAULT_COMPRESSION,
        compression_level: int | None = DEFAULT_COMPRESSION_LEVEL,
    ) -> None:
        if max_rows_per_file <= 0:
            raise ValueError("max_rows_per_file must be positive")
        self.base_path = Path(base_path)
        self.dataset = dataset
        self.max_rows_per_file = max_rows_per_file
        self.max_open_s = max_open_s
        self.compression = compression
        self.compression_level = compression_level
        self._buffers: dict[tuple[str, str], _PartitionBuffer] = {}

    @property
    def dataset_path(self) -> Path:
        return self.base_path / self.dataset

    def write(self, tick: Tick) -> None:
        key = (tick.symbol, ns_to_date_str(tick.exchange_ts_ns))
        buffer = self._buffers.get(key)
        if buffer is None:
            buffer = _PartitionBuffer(ticks=[], started_monotonic=time.monotonic())
            self._buffers[key] = buffer
        buffer.ticks.append(tick)
        buffer.digest.update(tick_canonical_bytes(tick))
        buffer.digest.update(b"\n")
        buffer.min_exchange_ts_ns = min(buffer.min_exchange_ts_ns, tick.exchange_ts_ns)
        buffer.max_exchange_ts_ns = max(buffer.max_exchange_ts_ns, tick.exchange_ts_ns)
        if self._should_roll(buffer):
            self._roll(key, buffer)

    def write_batch(self, ticks: Iterable[Tick]) -> None:
        for tick in ticks:
            self.write(tick)

    def flush(self) -> list[PartitionManifest]:
        """Write remaining buffers and manifests; returns the manifests written."""
        manifests: list[PartitionManifest] = []
        for key, buffer in list(self._buffers.items()):
            self._roll(key, buffer)
            if buffer.row_count == 0:
                continue
            manifest = PartitionManifest(
                dataset=self.dataset,
                symbol=key[0],
                date=key[1],
                row_count=buffer.row_count,
                min_exchange_ts_ns=buffer.min_exchange_ts_ns,
                max_exchange_ts_ns=buffer.max_exchange_ts_ns,
                content_sha256=buffer.digest.hexdigest(),
                parts=tuple(buffer.parts),
            )
            partition_dir = self._partition_dir(*key)
            write_manifest(partition_dir, manifest)
            manifests.append(manifest)
            logger.info(
                "store_partition_flushed",
                dataset=self.dataset,
                symbol=key[0],
                date=key[1],
                row_count=buffer.row_count,
                parts=list(buffer.parts),
            )
        self._buffers.clear()
        return manifests

    def _should_roll(self, buffer: _PartitionBuffer) -> bool:
        if len(buffer.ticks) >= self.max_rows_per_file:
            return True
        if self.max_open_s is not None:
            return time.monotonic() - buffer.started_monotonic >= self.max_open_s
        return False

    def _roll(self, key: tuple[str, str], buffer: _PartitionBuffer) -> None:
        if not buffer.ticks:
            return
        partition_dir = self._partition_dir(*key)
        partition_dir.mkdir(parents=True, exist_ok=True)
        part_index = len(list(partition_dir.glob("part-*.parquet")))
        table = ticks_to_table(buffer.ticks)
        tmp_path = partition_dir / f"part-{part_index}.parquet{TMP_SUFFIX}"
        final_path = partition_dir / f"part-{part_index}.parquet"
        pq.write_table(
            table,
            tmp_path,
            compression=self.compression,
            compression_level=self.compression_level,
            row_group_size=_row_group_size_for_schema(table.schema),
        )
        os.replace(tmp_path, final_path)
        buffer.parts.append(final_path.name)
        buffer.row_count += len(buffer.ticks)
        buffer.ticks.clear()
        buffer.started_monotonic = time.monotonic()

    def _partition_dir(self, symbol: str, date_str: str) -> Path:
        return self.dataset_path / f"symbol={symbol}" / f"date={date_str}"


__all__ = [
    "DATE_FORMAT",
    "MANIFEST_NAME",
    "MAX_INT64",
    "PartitionManifest",
    "PartitionedWriter",
    "SCAN_COLUMNS",
    "TICK_ARROW_SCHEMA",
    "TICK_SCALE",
    "date_str_to_ns",
    "decimal_to_ticks",
    "ns_to_date_str",
    "partition_content_hash",
    "read_manifest",
    "ticks_to_decimal",
    "ticks_to_table",
    "write_manifest",
]
