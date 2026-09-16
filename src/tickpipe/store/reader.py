"""DuckDB-backed scanning of partitioned tick Parquet data.

``TickStore`` resolves queries to the exact set of ``(symbol, date)``
partition directories that can overlap the requested symbol/time window and
passes only those files to DuckDB's ``read_parquet``, so partitions are
pruned before any Parquet metadata is touched. The remaining row-level filter
(``exchange_ts_ns`` range) is pushed into the Parquet scan where DuckDB prunes
row groups via zone maps.

Lookahead-bias policy: feature and backtest code must query through a
:class:`~tickpipe.store.pit.PointInTimeView`, never a raw ``TickStore``, and
must filter on ``exchange_ts_ns`` (venue time), never ``ingest_ts_ns``.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Final

import duckdb
import pyarrow as pa
import structlog

from tickpipe.store.writer import (
    MAX_INT64,
    SCAN_COLUMNS,
    TICK_ARROW_SCHEMA,
    date_str_to_ns,
)

logger = structlog.get_logger(__name__)

_NANOS_PER_DAY: Final[int] = 86_400 * 10**9


class TickStore:
    """Reads tick Parquet partitions through DuckDB."""

    def __init__(
        self,
        base_path: str | Path,
        dataset: str = "trades",
        *,
        connection: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self.base_path = Path(base_path)
        self.dataset = dataset
        self._conn = connection if connection is not None else duckdb.connect()

    @property
    def dataset_path(self) -> Path:
        return self.base_path / self.dataset

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def scan(
        self,
        symbols: Sequence[str] | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
    ) -> pa.Table:
        """Scan ticks with ``start_ns <= exchange_ts_ns < end_ns``.

        Rows are returned in canonical column order; ordering between rows is
        unspecified.
        """
        files = self._partition_files(symbols, start_ns, end_ns)
        if not files:
            return TICK_ARROW_SCHEMA.empty_table()
        sql = self._scan_sql(files, symbols, start_ns, end_ns)
        return self._conn.execute(sql).to_arrow_table()

    def count_files_scanned(
        self,
        symbols: Sequence[str] | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
    ) -> int:
        """Number of distinct Parquet files a scan would open."""
        files = self._partition_files(symbols, start_ns, end_ns)
        if not files:
            return 0
        where = self._where_clause(symbols, start_ns, end_ns)
        sql = (
            "SELECT count(DISTINCT filename) AS file_count "
            f"FROM read_parquet([{_quote_files(files)}], union_by_name=true, filename=true) "
            f"WHERE {where}"
        )
        row = self._conn.execute(sql).fetchone()
        return int(row[0]) if row is not None else 0

    def _scan_sql(
        self,
        files: list[str],
        symbols: Sequence[str] | None,
        start_ns: int | None,
        end_ns: int | None,
    ) -> str:
        columns = ", ".join(SCAN_COLUMNS)
        where = self._where_clause(symbols, start_ns, end_ns)
        return (
            f"SELECT {columns} "
            f"FROM read_parquet([{_quote_files(files)}], union_by_name=true) "
            f"WHERE {where}"
        )

    def _where_clause(
        self, symbols: Sequence[str] | None, start_ns: int | None, end_ns: int | None
    ) -> str:
        conditions = [
            f"exchange_ts_ns >= {start_ns if start_ns is not None else 0}",
            f"exchange_ts_ns < {end_ns if end_ns is not None else MAX_INT64}",
        ]
        if symbols:
            quoted = ", ".join(_quote_string(symbol) for symbol in symbols)
            conditions.append(f"symbol IN ({quoted})")
        return " AND ".join(conditions)

    def _partition_files(
        self,
        symbols: Sequence[str] | None,
        start_ns: int | None,
        end_ns: int | None,
    ) -> list[str]:
        if not self.dataset_path.exists():
            return []
        if symbols is None:
            symbol_dirs = sorted(self.dataset_path.glob("symbol=*"))
        else:
            symbol_dirs = [self.dataset_path / f"symbol={symbol}" for symbol in symbols]
        start = start_ns if start_ns is not None else 0
        end = end_ns if end_ns is not None else MAX_INT64
        files: list[str] = []
        for symbol_dir in symbol_dirs:
            if not symbol_dir.is_dir():
                continue
            for date_dir in sorted(symbol_dir.glob("date=*")):
                date_str = date_dir.name[len("date=") :]
                try:
                    day_start = date_str_to_ns(date_str)
                except ValueError:
                    logger.warning("store_unparseable_partition_dir", path=str(date_dir))
                    continue
                day_end = day_start + _NANOS_PER_DAY
                if day_start >= end or day_end <= start:
                    continue
                files.extend(sorted(str(p) for p in date_dir.glob("part-*.parquet")))
        return files


def _quote_files(files: list[str]) -> str:
    return ", ".join(_quote_string(f) for f in files)


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


__all__ = ["TickStore"]
