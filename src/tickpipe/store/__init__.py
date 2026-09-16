"""Market-data storage and query layer.

Writers produce Hive-partitioned Parquet under ``{base}/{dataset}/``;
readers query through DuckDB. All data access for features and backtests
must go through :class:`PointInTimeView` for lookahead-bias protection.
"""

from tickpipe.store.pit import LookaheadError, PointInTimeView
from tickpipe.store.reader import TickStore
from tickpipe.store.writer import (
    TICK_ARROW_SCHEMA,
    TICK_SCALE,
    PartitionedWriter,
    PartitionManifest,
    decimal_to_ticks,
    partition_content_hash,
    read_manifest,
    ticks_to_decimal,
    ticks_to_table,
)

__all__ = [
    "LookaheadError",
    "PartitionManifest",
    "PartitionedWriter",
    "PointInTimeView",
    "TICK_ARROW_SCHEMA",
    "TICK_SCALE",
    "TickStore",
    "decimal_to_ticks",
    "partition_content_hash",
    "read_manifest",
    "ticks_to_decimal",
    "ticks_to_table",
]
