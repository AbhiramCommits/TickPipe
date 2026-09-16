#!/bin/sh
# One-shot quickstart: generate the bundled sample dataset, run a real
# experiment, register it, and replay it to prove reproducibility.
set -e

tickpipe sample-data --data-dir /app/data --count 500 --force

OUTPUT=$(tickpipe run-experiment \
    --config /app/experiments/baseline.yaml \
    --data-dir /app/data \
    --allow-dirty)
echo "$OUTPUT"

RUN_ID=$(printf '%s\n' "$OUTPUT" | sed -n 's/^run_id=//p' | head -1)
echo "--- replaying $RUN_ID for reproducibility ---"
tickpipe replay-run "$RUN_ID" --data-dir /app/data

echo "quickstart complete"
