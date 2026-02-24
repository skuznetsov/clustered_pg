#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [tmp_root_abs_dir]" >&2
  exit 2
fi

TMP_ROOT="${1:-/private/tmp}"
if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp_root_abs_dir not found: $TMP_ROOT" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

set +e
OUTPUT="$(
  make -s -C "$ROOT_DIR" --no-print-directory unnest-ab-gate \
    UNNEST_GATE_REF_RUNS=1 \
    UNNEST_GATE_NEW_RUNS=1 \
    UNNEST_GATE_BATCH_SIZE=100 \
    UNNEST_GATE_BATCHES=5 \
    UNNEST_GATE_SELECT_ITERS=1 \
    UNNEST_GATE_PROBE_SIZE=8 \
    UNNEST_GATE_BASE_PORT=65390 \
    UNNEST_GATE_OUT_ROOT= \
    UNNEST_GATE_MIN_FRACTION=0.90 \
    UNNEST_GATE_STAT_MODE=median \
    2>&1
)"
STATUS=$?
set -e

if [ "$STATUS" -eq 0 ]; then
  echo "expected make unnest-ab-gate with empty UNNEST_GATE_OUT_ROOT to fail fast on out_root validation" >&2
  printf '%s\n' "$OUTPUT" >&2
  exit 1
fi

if ! printf '%s\n' "$OUTPUT" | grep -Fq "out_root_abs_dir must be absolute:"; then
  echo "expected explicit out_root validation failure when UNNEST_GATE_OUT_ROOT is empty (arg arity must remain intact)" >&2
  printf '%s\n' "$OUTPUT" >&2
  exit 1
fi

if printf '%s\n' "$OUTPUT" | grep -Fq "new_runs/batch_size/batches/select_iters/probe_size must be positive integers"; then
  echo "unexpected positional-arg shift detected: numeric validation fired instead of out_root validation" >&2
  printf '%s\n' "$OUTPUT" >&2
  exit 1
fi

echo "selftest_unnest_ab_gate_make_arg_contract status=ok"
