"""Tests for ingest metrics: counters, latency percentiles, Prometheus, logging."""

from __future__ import annotations

import asyncio

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

from tickpipe.ingest.metrics import (
    COUNTER_MESSAGES_DROPPED,
    IngestMetrics,
    MetricsServer,
    PeriodicMetricsLogger,
)


def test_counters_and_latency_percentiles() -> None:
    metrics = IngestMetrics()
    assert metrics.p50_ns is None
    for latency in range(1, 11):
        metrics.observe_latency_ns(latency)
    assert metrics.p50_ns == pytest.approx(5.5)
    assert metrics.p99_ns == pytest.approx(9.91)
    metrics.incr(COUNTER_MESSAGES_DROPPED)
    metrics.incr(COUNTER_MESSAGES_DROPPED, 3)
    assert metrics.counter(COUNTER_MESSAGES_DROPPED) == 4


def test_render_prometheus_text_format() -> None:
    metrics = IngestMetrics()
    metrics.incr("messages_received", 7)
    metrics.observe_latency_ns(5_000)
    metrics.observe_latency_ns(5_000_000_000)
    rendered = metrics.render()
    assert "# HELP tickpipe_ingest_messages_received_total" in rendered
    assert "# TYPE tickpipe_ingest_messages_received_total counter" in rendered
    assert "tickpipe_ingest_messages_received_total 7" in rendered
    assert "# TYPE tickpipe_ingest_latency_ns histogram" in rendered
    assert 'tickpipe_ingest_latency_ns_bucket{le="1e+03"} 0' in rendered
    assert 'tickpipe_ingest_latency_ns_bucket{le="1e+06"} 1' in rendered
    assert 'tickpipe_ingest_latency_ns_bucket{le="+Inf"} 2' in rendered
    assert "tickpipe_ingest_latency_ns_count 2" in rendered
    assert "tickpipe_ingest_latency_ns_sum 5000005000" in rendered


def test_metrics_server_serves_prometheus_endpoint() -> None:
    async def scenario() -> None:
        metrics = IngestMetrics()
        metrics.incr("messages_received", 3)
        server = MetricsServer(metrics, host="127.0.0.1", port=0)
        await server.start()
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"http://127.0.0.1:{server.bound_port}/metrics")
                assert response.status_code == 200
                assert "text/plain" in response.headers["content-type"]
                assert "tickpipe_ingest_messages_received_total 3" in response.text
                missing = await client.get(f"http://127.0.0.1:{server.bound_port}/other")
                assert missing.status_code == 404
        finally:
            await server.stop()

    asyncio.run(scenario())


def test_periodic_logger_emits_snapshots() -> None:
    async def scenario() -> None:
        metrics = IngestMetrics()
        metrics.observe_latency_ns(100)
        logger = PeriodicMetricsLogger(metrics, interval_s=0.01)
        task = asyncio.create_task(logger.run())
        try:
            with capture_logs() as captured:
                await asyncio.sleep(0.06)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        snapshots = [event for event in captured if event["event"] == "metrics_snapshot"]
        assert len(snapshots) >= 2
        assert snapshots[0]["messages_received"] == 0
        assert snapshots[0]["latency_p50_ns"] == 100

    asyncio.run(scenario())


def test_periodic_logger_uses_bound_logger() -> None:
    async def scenario() -> None:
        metrics = IngestMetrics()
        logger = PeriodicMetricsLogger(
            metrics, interval_s=0.01, logger_=structlog.get_logger("test.metrics")
        )
        task = asyncio.create_task(logger.run())
        try:
            with capture_logs() as captured:
                await asyncio.sleep(0.03)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert any(event["event"] == "metrics_snapshot" for event in captured)

    asyncio.run(scenario())
