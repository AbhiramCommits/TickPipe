"""Tests for the ingest pipeline: backpressure, shutdown, and run metadata."""

from __future__ import annotations

import asyncio
import os
import signal
import uuid
from collections.abc import AsyncIterator, Callable, Sequence
from pathlib import Path

from tickpipe.core.types import Tick
from tickpipe.ingest.pipeline import BackpressurePolicy, IngestPipeline, RunMetadata
from tickpipe.ingest.sources import ReplayFileSource

FIXTURE = Path(__file__).parent / "fixtures" / "replay_hand_built.ndjson"

EXPECTED_ARRIVAL_ORDER = ["t-1", "t-2", "t-5", "t-4", "t-3", "t-5-dup", "t-6"]
EXPECTED_TS_ORDER = ["t-1", "t-2", "t-3", "t-4", "t-5", "t-5-dup", "t-6"]


class CollectingWriter:
    """Fast writer that records batches in memory."""

    def __init__(self) -> None:
        self.batches: list[list[Tick]] = []
        self.closed = False

    async def open(self) -> None:
        return None

    async def write(self, batch: Sequence[Tick]) -> None:
        self.batches.append(list(batch))

    async def close(self) -> None:
        self.closed = True


class GatedWriter:
    """Slow consumer: blocks in ``open`` until the gate is released."""

    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.batches: list[list[Tick]] = []
        self.closed = False

    async def open(self) -> None:
        await self.gate.wait()

    async def write(self, batch: Sequence[Tick]) -> None:
        self.batches.append(list(batch))

    async def close(self) -> None:
        self.closed = True


class HangingReplaySource:
    """Replays the fixture then blocks forever, to simulate a live feed."""

    def __init__(self, path: Path) -> None:
        self.path = path

    async def stream(self) -> AsyncIterator[Tick]:
        async for msg in ReplayFileSource(self.path).stream():
            yield msg
        await asyncio.Event().wait()


async def wait_for_condition(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(0.01)


def written_trade_ids(writer: CollectingWriter | GatedWriter) -> list[str]:
    return [m.trade_id for batch in writer.batches for m in batch]


def test_drop_oldest_drops_exactly_the_expected_number_under_a_slow_consumer() -> None:
    async def scenario() -> tuple[RunMetadata, GatedWriter, IngestPipeline]:
        writer = GatedWriter()
        pipeline = IngestPipeline(
            ReplayFileSource(FIXTURE),
            writer,
            queue_maxsize=2,
            policy=BackpressurePolicy.DROP_OLDEST,
            batch_size=1,
            reorder_window_ns=0,
            metrics_log_interval_s=60.0,
        )
        run_task = asyncio.create_task(pipeline.run())
        await wait_for_condition(lambda: pipeline.reader_finished.is_set())
        writer.gate.set()
        metadata = await asyncio.wait_for(run_task, timeout=5)
        return metadata, writer, pipeline

    metadata, writer, pipeline = asyncio.run(scenario())
    assert metadata.policy is BackpressurePolicy.DROP_OLDEST
    assert metadata.messages_received == 7
    assert metadata.messages_written == 2
    assert metadata.messages_dropped == 5
    assert metadata.messages_received == metadata.messages_written + metadata.messages_dropped
    assert pipeline.dropped_messages == 5
    assert written_trade_ids(writer) == ["t-5-dup", "t-6"]
    assert writer.closed


def test_block_policy_never_drops_and_metadata_records_policy() -> None:
    async def scenario() -> tuple[RunMetadata, CollectingWriter]:
        writer = CollectingWriter()
        pipeline = IngestPipeline(
            ReplayFileSource(FIXTURE),
            writer,
            queue_maxsize=2,
            policy=BackpressurePolicy.BLOCK,
            batch_size=1,
            reorder_window_ns=0,
            metrics_log_interval_s=60.0,
        )
        metadata = await pipeline.run()
        return metadata, writer

    metadata, writer = asyncio.run(scenario())
    assert metadata.policy is BackpressurePolicy.BLOCK
    assert metadata.messages_received == 7
    assert metadata.messages_written == 7
    assert metadata.messages_dropped == 0
    assert metadata.sequencing_gaps == 1
    assert metadata.sequencing_out_of_order == 2
    assert metadata.messages_reordered == 2
    assert written_trade_ids(writer) == EXPECTED_ARRIVAL_ORDER
    assert metadata.symbols == ("BTC-USD",)
    assert writer.closed


def test_sigint_shuts_down_cleanly_without_losing_buffered_messages() -> None:
    async def scenario() -> tuple[RunMetadata, CollectingWriter]:
        writer = CollectingWriter()
        pipeline = IngestPipeline(
            HangingReplaySource(FIXTURE),
            writer,
            queue_maxsize=16,
            policy=BackpressurePolicy.BLOCK,
            batch_size=100,
            batch_timeout_s=0.05,
            metrics_log_interval_s=60.0,
        )
        run_task = asyncio.create_task(pipeline.run())
        await wait_for_condition(lambda: pipeline.metrics.counter("messages_received") == 7)
        os.kill(os.getpid(), signal.SIGINT)
        metadata = await asyncio.wait_for(run_task, timeout=5)
        return metadata, writer

    metadata, writer = asyncio.run(scenario())
    assert metadata.messages_received == 7
    assert metadata.messages_written == 7
    assert metadata.messages_dropped == 0
    assert written_trade_ids(writer) == EXPECTED_TS_ORDER
    assert writer.closed


def test_sigterm_shuts_down_cleanly_without_losing_buffered_messages() -> None:
    async def scenario() -> tuple[RunMetadata, CollectingWriter]:
        writer = CollectingWriter()
        pipeline = IngestPipeline(
            HangingReplaySource(FIXTURE),
            writer,
            queue_maxsize=16,
            policy=BackpressurePolicy.BLOCK,
            batch_size=100,
            batch_timeout_s=0.05,
            metrics_log_interval_s=60.0,
        )
        run_task = asyncio.create_task(pipeline.run())
        await wait_for_condition(lambda: pipeline.metrics.counter("messages_received") == 7)
        os.kill(os.getpid(), signal.SIGTERM)
        metadata = await asyncio.wait_for(run_task, timeout=5)
        return metadata, writer

    metadata, writer = asyncio.run(scenario())
    assert metadata.messages_received == 7
    assert metadata.messages_written == 7
    assert metadata.messages_dropped == 0
    assert written_trade_ids(writer) == EXPECTED_TS_ORDER
    assert writer.closed


def test_run_metadata_carries_a_run_id() -> None:
    async def scenario() -> RunMetadata:
        writer = CollectingWriter()
        pipeline = IngestPipeline(
            ReplayFileSource(FIXTURE),
            writer,
            reorder_window_ns=0,
            metrics_log_interval_s=60.0,
        )
        return await pipeline.run()

    metadata = asyncio.run(scenario())
    assert isinstance(metadata.run_id, uuid.UUID)
