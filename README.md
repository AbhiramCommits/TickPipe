# tickpipe

![coverage](https://img.shields.io/badge/coverage-%E2%89%A585%25-brightgreen)
![python](https://img.shields.io/badge/python-3.11-blue)
![license](https://img.shields.io/badge/license-MIT-green)

tickpipe is a market-data ingestion and backtest research harness for quantitative trading research. It streams trades and book deltas from live feeds into a partitioned, columnar Parquet store, serves data through point-in-time views that make lookahead bias structurally impossible, replays ticks through a C++17 event-driven backtest engine with a latency-aware execution model, and registers every experiment run in a database with a SHA-256 dataset fingerprint so that any run can be replayed and verified bit-for-bit. The reproducibility of every reported number is enforced by a CI gate, not by convention.

```
                        live exchange feeds (websocket)
                                   │
                                   ▼
                 ┌─────────────────────────────────┐
                 │  ingest pipeline (asyncio)       │
                 │  sequencing · reorder buffer     │
                 │  bounded queue · backpressure    │
                 └───────────────┬─────────────────┘
                                 ▼
                 ┌─────────────────────────────────┐
                 │  Parquet store (pyarrow/duckdb) │
                 │  Hive partitions · manifests    │
                 └───────────────┬─────────────────┘
                                 ▼
                 ┌─────────────────────────────────┐
                 │  PointInTimeView (as_of clamp)  │
                 │  features · datasets (fingerprint)│
                 └───────────────┬─────────────────┘
                                 ▼
                 ┌─────────────────────────────────┐
                 │  C++17 replay engine (pybind11) │
                 │  deterministic event queue      │
                 │  latency-aware matching         │
                 └───────────────┬─────────────────┘
                                 ▼
                 ┌─────────────────────────────────┐
                 │  portfolio metrics (int ticks)  │
                 └───────────────┬─────────────────┘
                                 ▼
                 ┌─────────────────────────────────┐
                 │  experiment registry (Postgres) │
                 │  run_id · fingerprint · replay  │
                 └─────────────────────────────────┘
```

## Quickstart (5 minutes)

The only prerequisites are Docker and `docker compose`. One command builds the
container, starts Postgres, generates the bundled deterministic sample
dataset, runs a real experiment (feature build → walk-forward training →
backtest → registry), and replays it to prove reproducibility:

```sh
docker compose up --abort-on-container-exit
docker compose down
```

Expected output ends with the experiment metrics and then the replay check:

```text
run_id=<uuid>
{... train_mse, val_mse, backtest_total_return, backtest_sharpe, ...}
--- replaying <uuid> for reproducibility ---
fingerprint_identical=True
metric_mismatches=0
```

The sample data lands in `./data/` (Hive-partitioned Parquet plus manifests),
and the run is registered in the Postgres experiment registry.

To run the same flow natively:

```sh
make install
tickpipe sample-data --data-dir data --count 500 --force
tickpipe run-experiment --config experiments/baseline.yaml --data-dir data --allow-dirty
tickpipe replay-run <run_id> --data-dir data
```

## Design decisions

**Why nanosecond int64 timestamps.** Venue feeds stamp events in nanoseconds;
anything coarser (float seconds, microseconds, datetimes) either loses
precision or loses order — two events in the same microsecond are still two
events, and backtests are only reproducible when their event order is a total
order. int64 nanoseconds is the exact wire format, sorts naturally, compares
in one instruction in the C++ engine, and is immune to float rounding across
the year-2000-and-later epoch range.

**Why point-in-time views are constructor-enforced instead of a convention.**
A convention ("please remember to filter by as-of") fails silently and
produces numbers that look better than reality — the worst possible failure
for a research harness. `PointInTimeView` makes the clamp unconditional:
every query is clamped to `exchange_ts_ns <= as_of_ns`, the as-of cannot be
widened after construction, and `Strategy`, `Backtest`, and `FeatureContext`
constructors accept only a `PointInTimeView`, never a raw `TickStore`.
Compiling-time enforcement where possible, a runtime `LookaheadError` where
not.

**The event-ordering tie-break rule.** The replay engine orders events by
`(timestamp_ns, sequence, rank, insertion_counter)` in ascending order of
each component. The first three keys are semantic: venue time, venue
sequence, and event-type rank (book delta < order activation < trade, so an
order that becomes executable at `t` can trade against a tick at `t`). The
monotonic insertion counter removes the last possible ambiguity between
events with identical triples, making the replay order unique and
reproducible across runs — the C++-level test replays 10k duplicate-timestamp
events 100 times and requires identical order.

**The backpressure policy trade-off.** A bounded queue between the feed and
the writer means the slowest component decides the cadence, never memory.
`BLOCK` waits for space — zero data loss, but a slow writer slows the feed
reader, and a stalled writer stalls everything upstream. `DROP_OLDEST`
evicts the oldest queued message to keep latency bounded — at the cost of
silent data loss, which is why it is never silent here: every eviction
increments a counter, emits a warning log, and the policy is recorded in the
run metadata. Choose `BLOCK` for correctness-critical research data,
`DROP_OLDEST` for latency-sensitive operational feeds.

## Measured performance

Measured with `make bench` on **Mac15,12 (Apple M3), Python 3.11.14** (see
`benchmarks/` for the reproducible scripts):

| Benchmark | Result | Notes |
|---|---|---|
| Ingest throughput | **19,102 msgs/sec** | Full pipeline: JSON parse → pydantic validation → sequencing → bounded queue → batched writer |
| Replay throughput | **10,151,979 ticks/sec** | C++17 engine, 5M-tick contiguous buffer, zero-copy ingestion |
| Pruned query latency (p50) | **0.87 ms** | Single symbol, single day, over a 64-partition dataset |
| Full-scan query latency (p50) | **39.11 ms** | All 64 partitions, 320k rows |

Coverage on `src/` is enforced at ≥85% in CI and locally (`make test`).

## Layout

```
cpp/src/           C++17 replay core (event queue, matching, pybind11)
src/tickpipe/
  ingest/          live ingestion pipeline (sequencing, backpressure, metrics)
  store/           partitioned Parquet store + PointInTimeView + backfill
  backtest/        strategy API, portfolio accounting, replay engine bindings
  research/        features, datasets, training, experiment registry
  cli/             tickpipe command line interface
benchmarks/        reproducible performance scripts (make bench)
docs/              design write-ups
experiments/       experiment configs (baseline.yaml)
migrations/        Alembic migrations for the experiment registry
```

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). In short: `make install`, `make test`
(coverage ≥85%), `make lint` (ruff + mypy strict), `make bench`, and
pre-commit hooks run ruff and mypy on every commit.

## License

[MIT](LICENSE)
