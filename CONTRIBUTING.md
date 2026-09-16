# Contributing

tickpipe is a market-data ingestion and backtest research harness. Welcome.

## Setup

```sh
python3.11 -m venv .venv
make install      # installs the package (builds the C++ extension) + dev deps
```

The project requires Python 3.11 and a C++17 compiler (clang or g++). The
pybind11 extension is built automatically by pip.

## Development loop

```sh
make test     # pytest with coverage (fails under 85% on src/)
make lint     # ruff check + mypy --strict on src/
make fmt      # ruff format
make bench    # reproducible benchmark suite
```

## Pre-commit

Install the hooks once (with the venv activated, so the mypy hook uses the
project environment):

```sh
source .venv/bin/activate
pip install pre-commit
pre-commit install
```

The hooks run ruff (lint + format) and mypy on every commit.

## Conventions

- All timestamps are int64 nanoseconds UTC; prices are `Decimal` at API
  boundaries and int64 ticks internally (`TICK_SCALE = 10**9`).
- Any code that reads market data — features, strategies, backtests — must
  take a `PointInTimeView`, never a raw `TickStore`. Filter on
  `exchange_ts_ns` only, never `ingest_ts_ns`.
- Every experiment is registered in the experiment registry; a run from a
  dirty git working tree is refused unless `--allow-dirty` is passed.
- CI runs the full suite against PostgreSQL, including the reproducibility
  gate (`tests/test_reproducibility.py`), which replays a registered run and
  requires bit-for-bit identical metrics.
- Write tests for new behavior; keep `make lint` and `make test` green.
