"""The ingest pipeline: ``source -> bounded queue -> batching writer``.

Backpressure policy
-------------------
When the bounded queue is full the reader applies an explicit, configurable
policy:

``BLOCK``
    Await queue space, which slows the producer down to consumer speed.
``DROP_OLDEST``
    Evict the oldest queued tick, increment ``messages_dropped``, and enqueue
    the new tick. Every eviction is logged as a warning.

The policy is never applied silently: it is recorded in the
:class:`RunMetadata` returned by :meth:`IngestPipeline.run` and announced in
the ``ingest.run_start`` structured log event.

Shutdown
--------
On ``SIGINT`` the pipeline stops reading the source, flushes the sequencing
reorder buffer, drains the queue through the writer, and closes the writer,
so buffered messages are never lost. A ``SIGINT`` handler is only installed
when the running event loop is on the main thread and supports signal
handlers.
"""

from __future__ import annotations

import asyncio
import signal
import time
from collections.abc import Sequence
from enum import StrEnum
from typing import Final, Protocol

import structlog
from pydantic import BaseModel

from tickpipe.core.types import Tick
from tickpipe.ingest.metrics import (
    COUNTER_MESSAGES_DROPPED,
    COUNTER_MESSAGES_RECEIVED,
    COUNTER_MESSAGES_REORDERED,
    COUNTER_MESSAGES_WRITTEN,
    COUNTER_SEQUENCING_GAPS,
    COUNTER_SEQUENCING_OUT_OF_ORDER,
    IngestMetrics,
    PeriodicMetricsLogger,
)
from tickpipe.ingest.sequencing import GapEvent, Sequencer, SequencingEvent
from tickpipe.ingest.sources import MarketDataSource

logger = structlog.get_logger(__name__)

DEFAULT_QUEUE_MAXSIZE: Final[int] = 1024
DEFAULT_BATCH_SIZE: Final[int] = 512
DEFAULT_BATCH_TIMEOUT_S: Final[float] = 1.0
DEFAULT_REORDER_WINDOW_NS: Final[int] = 50_000_000


class BackpressurePolicy(StrEnum):
    """Queue-full behavior for the reader."""

    BLOCK = "block"
    DROP_OLDEST = "drop_oldest"


class BatchWriter(Protocol):
    """Sink for batches of ticks."""

    async def open(self) -> None: ...
    async def write(self, batch: Sequence[Tick]) -> None: ...
    async def close(self) -> None: ...


class NullBatchWriter:
    """A no-op writer that counts ticks; used for dry runs."""

    def __init__(self) -> None:
        self.written = 0

    async def open(self) -> None:
        return None

    async def write(self, batch: Sequence[Tick]) -> None:
        self.written += len(batch)

    async def close(self) -> None:
        return None


class RunMetadata(BaseModel):
    """Record of one pipeline run, including the applied backpressure policy."""

    policy: BackpressurePolicy
    queue_maxsize: int
    batch_size: int
    batch_timeout_s: float
    reorder_window_ns: int
    symbols: tuple[str, ...]
    started_at_ns: int
    finished_at_ns: int
    messages_received: int
    messages_written: int
    messages_dropped: int
    sequencing_gaps: int
    sequencing_out_of_order: int
    messages_reordered: int
    latency_p50_ns: float | None
    latency_p99_ns: float | None


class IngestPipeline:
    """Wires a source, a bounded queue, sequencing, and a batching writer."""

    def __init__(
        self,
        source: MarketDataSource,
        writer: BatchWriter,
        *,
        queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE,
        policy: BackpressurePolicy = BackpressurePolicy.BLOCK,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_timeout_s: float = DEFAULT_BATCH_TIMEOUT_S,
        reorder_window_ns: int = DEFAULT_REORDER_WINDOW_NS,
        metrics: IngestMetrics | None = None,
        metrics_log_interval_s: float = 1.0,
    ) -> None:
        if queue_maxsize <= 0:
            raise ValueError("queue_maxsize must be positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self._source = source
        self._writer = writer
        self.queue_maxsize = queue_maxsize
        self.policy = policy
        self.batch_size = batch_size
        self.batch_timeout_s = batch_timeout_s
        self.reorder_window_ns = reorder_window_ns
        self.metrics = metrics or IngestMetrics()
        self._metrics_log_interval_s = metrics_log_interval_s
        self._queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=queue_maxsize)
        self._sequencer = Sequencer(reorder_window_ns)
        self._symbols: set[str] = set()
        self._stop = asyncio.Event()
        self._started_at_ns = 0
        self._logger = logger.bind(policy=policy.value, queue_maxsize=queue_maxsize)
        self.reader_finished = asyncio.Event()

    @property
    def dropped_messages(self) -> int:
        return self.metrics.counter(COUNTER_MESSAGES_DROPPED)

    async def run(self) -> RunMetadata:
        """Run until the source is exhausted or SIGINT is received; returns metadata."""
        self._started_at_ns = time.time_ns()
        self._stop = asyncio.Event()
        self.reader_finished.clear()
        self._symbols = set()
        self._sequencer = Sequencer(self.reorder_window_ns)
        self._log_run_start()
        loop = asyncio.get_running_loop()
        signal_added = False
        try:
            loop.add_signal_handler(signal.SIGINT, self._stop.set)
            signal_added = True
        except (NotImplementedError, RuntimeError):
            pass

        reader_task = asyncio.create_task(self._read_loop(), name="tickpipe-ingest-reader")
        metrics_log_task = asyncio.create_task(
            PeriodicMetricsLogger(
                self.metrics, interval_s=self._metrics_log_interval_s
            ).run(),
            name="tickpipe-ingest-metrics-log",
        )
        try:
            await self._writer.open()
            writer_task = asyncio.create_task(
                self._write_loop(), name="tickpipe-ingest-writer"
            )
            stop_task = asyncio.create_task(self._stop.wait(), name="tickpipe-ingest-stop")
            try:
                done, _pending = await asyncio.wait(
                    {reader_task, writer_task, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done:
                    reader_task.cancel()
                    await asyncio.gather(reader_task, return_exceptions=True)
                await writer_task
                if reader_task in done and not reader_task.cancelled():
                    reader_task.result()
            finally:
                stop_task.cancel()
                await asyncio.gather(stop_task, return_exceptions=True)
        finally:
            if not reader_task.done():
                reader_task.cancel()
                await asyncio.gather(reader_task, return_exceptions=True)
            metrics_log_task.cancel()
            await asyncio.gather(metrics_log_task, return_exceptions=True)
            if signal_added:
                loop.remove_signal_handler(signal.SIGINT)
            try:
                await self._writer.close()
            except Exception as exc:
                logger.exception("writer_close_failed", error=str(exc))

        metadata = self._build_metadata()
        self._log_run_finish(metadata)
        return metadata

    async def _read_loop(self) -> None:
        pending: Tick | None = None
        try:
            async for msg in self._source.stream():
                self._on_received(msg)
                result = self._sequencer.process(msg)
                for event in result.events:
                    self._record_sequence_event(event)
                if result.reordered:
                    self.metrics.incr(COUNTER_MESSAGES_REORDERED)
                for ready in result.ready:
                    pending = ready
                    await self._put(ready)
                    pending = None
            pending = None
            for ready in self._sequencer.flush():
                pending = ready
                await self._put_blocking(ready)
                pending = None
        except asyncio.CancelledError:
            if pending is not None:
                await self._put_blocking(pending)
            for ready in self._sequencer.flush():
                await self._put_blocking(ready)
            raise
        finally:
            self.reader_finished.set()

    async def _put(self, msg: Tick) -> None:
        if self.policy is BackpressurePolicy.BLOCK:
            await self._queue.put(msg)
            return
        while True:
            try:
                self._queue.put_nowait(msg)
                return
            except asyncio.QueueFull:
                self._queue.get_nowait()
                self.metrics.incr(COUNTER_MESSAGES_DROPPED)
                self._logger.warning(
                    "queue_full_dropped_oldest",
                    symbol=msg.symbol,
                    sequence=msg.sequence,
                    exchange_ts_ns=msg.exchange_ts_ns,
                )

    async def _put_blocking(self, msg: Tick) -> None:
        await self._queue.put(msg)

    def _on_received(self, msg: Tick) -> None:
        self.metrics.incr(COUNTER_MESSAGES_RECEIVED)
        self.metrics.observe_latency_ns(msg.ingest_ts_ns - msg.exchange_ts_ns)
        self._symbols.add(msg.symbol)

    def _record_sequence_event(self, event: SequencingEvent) -> None:
        if isinstance(event, GapEvent):
            self.metrics.incr(COUNTER_SEQUENCING_GAPS)
            event_name = "sequence_gap"
        else:
            self.metrics.incr(COUNTER_SEQUENCING_OUT_OF_ORDER)
            event_name = "sequence_out_of_order"
        self._logger.warning(event_name, **event.model_dump())

    async def _write_loop(self) -> None:
        batch: list[Tick] = []
        deadline: float | None = None
        try:
            while True:
                if self._queue.empty() and self.reader_finished.is_set():
                    break
                if deadline is not None:
                    timeout = deadline - asyncio.get_running_loop().time()
                else:
                    timeout = self.batch_timeout_s
                try:
                    msg = await asyncio.wait_for(self._queue.get(), timeout=max(timeout, 0.0))
                except TimeoutError:
                    if batch:
                        await self._write_batch(batch)
                        batch = []
                        deadline = None
                    continue
                self._queue.task_done()
                batch.append(msg)
                if deadline is None:
                    deadline = asyncio.get_running_loop().time() + self.batch_timeout_s
                if len(batch) >= self.batch_size:
                    await self._write_batch(batch)
                    batch = []
                    deadline = None
                elif self._queue.empty() and self.reader_finished.is_set():
                    await self._write_batch(batch)
                    batch = []
        finally:
            if batch:
                await self._write_batch(batch)

    async def _write_batch(self, batch: list[Tick]) -> None:
        await self._writer.write(batch)
        self.metrics.incr(COUNTER_MESSAGES_WRITTEN, len(batch))

    def _build_metadata(self) -> RunMetadata:
        counters = self.metrics.counters_snapshot()
        snapshot = self.metrics.snapshot()
        latency_p50 = snapshot["latency_p50_ns"]
        latency_p99 = snapshot["latency_p99_ns"]
        return RunMetadata(
            policy=self.policy,
            queue_maxsize=self.queue_maxsize,
            batch_size=self.batch_size,
            batch_timeout_s=self.batch_timeout_s,
            reorder_window_ns=self.reorder_window_ns,
            symbols=tuple(sorted(self._symbols)),
            started_at_ns=self._started_at_ns,
            finished_at_ns=time.time_ns(),
            messages_received=counters[COUNTER_MESSAGES_RECEIVED],
            messages_written=counters[COUNTER_MESSAGES_WRITTEN],
            messages_dropped=counters[COUNTER_MESSAGES_DROPPED],
            sequencing_gaps=counters[COUNTER_SEQUENCING_GAPS],
            sequencing_out_of_order=counters[COUNTER_SEQUENCING_OUT_OF_ORDER],
            messages_reordered=counters[COUNTER_MESSAGES_REORDERED],
            latency_p50_ns=float(latency_p50) if latency_p50 is not None else None,
            latency_p99_ns=float(latency_p99) if latency_p99 is not None else None,
        )

    def _log_run_start(self) -> None:
        self._logger.info(
            "ingest.run_start",
            policy=self.policy.value,
            queue_maxsize=self.queue_maxsize,
            batch_size=self.batch_size,
            batch_timeout_s=self.batch_timeout_s,
            reorder_window_ns=self.reorder_window_ns,
        )

    def _log_run_finish(self, metadata: RunMetadata) -> None:
        self._logger.info("ingest.run_finish", **metadata.model_dump())


__all__ = [
    "BackpressurePolicy",
    "BatchWriter",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_BATCH_TIMEOUT_S",
    "DEFAULT_QUEUE_MAXSIZE",
    "DEFAULT_REORDER_WINDOW_NS",
    "IngestPipeline",
    "NullBatchWriter",
    "RunMetadata",
]
