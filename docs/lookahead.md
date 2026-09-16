# Lookahead bias in tickpipe

## What it is

Lookahead bias is any leakage of information into a decision point that was
not available at that point in time. In backtesting the classic form is the
"impossible trade": the model sees a price (or a label, or a book state)
before it could have known it, so the backtest's results are better than any
live deployment could ever reproduce. It is the most dangerous failure mode
of a research harness because it inflates metrics *silently* — the numbers
look excellent, and nothing errors.

## Where it enters this system

Three places in tickpipe's pipeline are structurally exposed to lookahead,
and each has a dedicated countermeasure.

### 1. Storage reads without a time bound

The store is append-only and partitioned by venue time, so a naive reader
scanning `exchange_ts_ns` up to "now" at research time sees data that a live
system at time `t` could not have seen.

**Blocked by:** every read goes through `PointInTimeView(store, as_of_ns)`.
The view *unconditionally* clamps every query to `exchange_ts_ns <= as_of_ns`;
there is no API to widen the clamp after construction, and passing an
`as_of_ns` later than the view's own raises `LookaheadError`. Feature and
backtest code can only be constructed with a `PointInTimeView` (enforced in
constructor signatures), never with a raw `TickStore`. All row selection
filters on `exchange_ts_ns` (venue time); `ingest_ts_ns` is never a filter
key, because local arrival time is not a tradable timeline.

### 2. Model/feature construction on "all the data"

The classical mistake is computing a feature, normalizer, or model parameter
over the full dataset and then evaluating on a subset of it. A rolling
z-score centered on the future, a scaler fitted on the test window — both
silently embed future information into every prediction.

**Blocked by:** features are pure functions of a `FeatureContext`, which is
a point-in-time view clamped to the *current bar's* `as_of_ns`; a feature
window that starts past its bar raises `LookaheadError`, and the dataset
fingerprint pins the exact feature set, versions, and lookbacks so the
replay can prove the same numbers come back. Training uses strictly
time-ordered expanding walk-forward splits — never shuffled, never
`KFold` — so a fold's model only ever sees rows strictly before its
validation window.

### 3. Execution and sequencing inside the replay

Even with correct data windows, a backtest can trade at prices that were not
executable: an order "submitted" at tick `t` filling against tick `t` itself,
or against a book state that had not been published yet.

**Blocked by:** the C++ replay engine models order-entry latency — an order
submitted at `t` is eligible only for trades at `t + latency_ns` or later —
and fills are capped by the tick's traded size. Events are processed in a
fully deterministic order keyed by `(exchange_ts_ns, sequence, rank,
insertion_counter)`, so a replay re-executes identically, and the
reproducibility gate in CI re-runs a registered experiment from its registry
record alone and requires every metric to match bit-for-bit.

## The invariant

If it went through tickpipe, then at every decision point the system only
used data with `exchange_ts_ns <=` the decision's time, and CI proves it by
replay.
