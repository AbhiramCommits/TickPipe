"""Ingest counters, a latency histogram, and their Prometheus exposure.

Latency is measured as ``ingest_ts_ns - exchange_ts_ns`` per tick. The
histogram keeps fixed nanosecond buckets for Prometheus export and a bounded
reservoir of the most recent samples for p50/p99 reporting. Snapshots are
logged once per second through structlog by :class:`PeriodicMetricsLogger`.
"""

from __future__ import annotations

import asyncio
import json
from collections import deque
from typing import Final

import numpy as np
import structlog
from numpy.typing import NDArray

logger = structlog.get_logger(__name__)

COUNTER_MESSAGES_RECEIVED: Final = "messages_received"
COUNTER_MESSAGES_WRITTEN: Final = "messages_written"
COUNTER_MESSAGES_DROPPED: Final = "messages_dropped"
COUNTER_SEQUENCING_GAPS: Final = "sequencing_gaps"
COUNTER_SEQUENCING_OUT_OF_ORDER: Final = "sequencing_out_of_order"
COUNTER_MESSAGES_REORDERED: Final = "messages_reordered"

LATENCY_NAME: Final = "tickpipe_ingest_latency_ns"

RESERVOIR_SIZE: Final = 10_000

LATENCY_BUCKETS_NS: Final[tuple[int, ...]] = (
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
)

_COUNTER_SPECS: Final[tuple[tuple[str, str, str], ...]] = (
    (
        COUNTER_MESSAGES_RECEIVED,
        "tickpipe_ingest_messages_received_total",
        "Ticks received from the source.",
    ),
    (
        COUNTER_MESSAGES_WRITTEN,
        "tickpipe_ingest_messages_written_total",
        "Ticks written downstream.",
    ),
    (
        COUNTER_MESSAGES_DROPPED,
        "tickpipe_ingest_messages_dropped_total",
        "Ticks dropped by the DROP_OLDEST backpressure policy.",
    ),
    (
        COUNTER_SEQUENCING_GAPS,
        "tickpipe_ingest_sequencing_gaps_total",
        "Sequence gaps detected.",
    ),
    (
        COUNTER_SEQUENCING_OUT_OF_ORDER,
        "tickpipe_ingest_sequencing_out_of_order_total",
        "Out-of-order sequence numbers detected.",
    ),
    (
        COUNTER_MESSAGES_REORDERED,
        "tickpipe_ingest_messages_reordered_total",
        "Ticks reordered by the event-time reorder buffer.",
    ),
)


class IngestMetrics:
    """Thread-unsafe counters and a latency histogram; confined to one event loop."""

    def __init__(self, reservoir_size: int = RESERVOIR_SIZE) -> None:
        self._counters = {key: 0 for key, _, _ in _COUNTER_SPECS}
        self._reservoir: deque[int] = deque(maxlen=reservoir_size)
        self._bucket_counts = [0] * len(LATENCY_BUCKETS_NS)
        self._latency_sum = 0
        self._latency_count = 0

    def incr(self, counter: str, amount: int = 1) -> None:
        self._counters[counter] += amount

    def counter(self, counter: str) -> int:
        return self._counters[counter]

    def observe_latency_ns(self, latency_ns: int) -> None:
        self._reservoir.append(latency_ns)
        self._latency_sum += latency_ns
        self._latency_count += 1
        for index, upper_bound in enumerate(LATENCY_BUCKETS_NS):
            if latency_ns <= upper_bound:
                self._bucket_counts[index] += 1
                break

    def percentile_ns(self, percentile: float) -> float | None:
        if not self._reservoir:
            return None
        return float(np.percentile(_reservoir_array(self._reservoir), percentile))

    @property
    def p50_ns(self) -> float | None:
        return self.percentile_ns(50)

    @property
    def p99_ns(self) -> float | None:
        return self.percentile_ns(99)

    def counters_snapshot(self) -> dict[str, int]:
        return dict(self._counters)

    def snapshot(self) -> dict[str, int | float | None]:
        return {
            **self.counters_snapshot(),
            "latency_p50_ns": self.p50_ns,
            "latency_p99_ns": self.p99_ns,
            "latency_count": self._latency_count,
        }

    def render(self) -> str:
        """Render all metrics in Prometheus text exposition format."""
        lines: list[str] = []
        for counter, name, help_text in _COUNTER_SPECS:
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {self._counters[counter]}")
        lines.append(
            "# HELP tickpipe_ingest_latency_ns "
            "Latency between venue and ingest timestamps in nanoseconds."
        )
        lines.append("# TYPE tickpipe_ingest_latency_ns histogram")
        cumulative = 0
        for index, upper_bound in enumerate(LATENCY_BUCKETS_NS):
            cumulative += self._bucket_counts[index]
            lines.append(
                f'tickpipe_ingest_latency_ns_bucket{{le="{upper_bound:.0e}"}} {cumulative}'
            )
        lines.append(f'tickpipe_ingest_latency_ns_bucket{{le="+Inf"}} {self._latency_count}')
        lines.append(f"tickpipe_ingest_latency_ns_sum {self._latency_sum}")
        lines.append(f"tickpipe_ingest_latency_ns_count {self._latency_count}")
        return "\n".join(lines) + "\n"


def _reservoir_array(reservoir: deque[int]) -> NDArray[np.int64]:
    return np.asarray(reservoir, dtype=np.int64)


class PeriodicMetricsLogger:
    """Logs a metrics snapshot through structlog once per interval."""

    def __init__(
        self,
        metrics: IngestMetrics,
        *,
        interval_s: float = 1.0,
        logger_: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self.metrics = metrics
        self.interval_s = interval_s
        self._logger = logger_ or logger

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval_s)
            self.log_snapshot()

    def log_snapshot(self) -> None:
        self._logger.info("metrics_snapshot", **self.metrics.snapshot())


class MetricsServer:
    """Minimal HTTP server exposing metrics at ``/metrics`` and health at ``/healthz``."""

    def __init__(
        self,
        metrics: IngestMetrics,
        *,
        host: str = "127.0.0.1",
        port: int = 9090,
    ) -> None:
        self.metrics = metrics
        self.host = host
        self.port = port
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    @property
    def bound_port(self) -> int:
        if self._server is None or self._server.sockets is None:
            raise RuntimeError("server is not running")
        return int(self._server.sockets[0].getsockname()[1])

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                return
            parts = request_line.decode("ascii", errors="replace").split()
            if len(parts) < 2:
                return
            while await reader.readline() not in (b"", b"\r\n"):
                pass
            if parts[1] == "/metrics":
                body = self.metrics.render().encode()
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: text/plain; version=0.0.4\r\n"
                    b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                    b"\r\n" + body
                )
            elif parts[1] == "/healthz":
                payload = {
                    "status": "ok",
                    "messages_received": self.metrics.counter(COUNTER_MESSAGES_RECEIVED),
                    "messages_dropped": self.metrics.counter(COUNTER_MESSAGES_DROPPED),
                }
                body = json.dumps(payload, sort_keys=True).encode()
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                    b"\r\n" + body
                )
            else:
                response = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"
            writer.write(response)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


__all__ = [
    "COUNTER_MESSAGES_DROPPED",
    "COUNTER_MESSAGES_RECEIVED",
    "COUNTER_MESSAGES_REORDERED",
    "COUNTER_MESSAGES_WRITTEN",
    "COUNTER_SEQUENCING_GAPS",
    "COUNTER_SEQUENCING_OUT_OF_ORDER",
    "IngestMetrics",
    "LATENCY_BUCKETS_NS",
    "MetricsServer",
    "PeriodicMetricsLogger",
]
