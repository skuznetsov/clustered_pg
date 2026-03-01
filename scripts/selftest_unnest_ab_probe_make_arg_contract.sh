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

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_probe_make_arg_contract_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

set +e
OUTPUT="$(
  make -s -C "$ROOT_DIR" --no-print-directory unnest-ab-probe \
    UNNEST_AB_RUNS=1 \
    UNNEST_AB_BATCH_SIZE=100 \
    UNNEST_AB_BATCHES=5 \
    UNNEST_AB_SELECT_ITERS=1 \
    UNNEST_AB_PROBE_SIZE=8 \
    UNNEST_AB_PORT=65490 \
    UNNEST_AB_OUT= \
    2>&1
)"
STATUS=$?
set -e

if [ "$STATUS" -ne 0 ]; then
  echo "expected make unnest-ab-probe to succeed with empty UNNEST_AB_OUT (quoted placeholder contract), got status=$STATUS" >&2
  printf '%s\n' "$OUTPUT" >&2
  exit 1
fi

if ! printf '%s\n' "$OUTPUT" | grep -Fq "unnest_ab_probe: runs=1 batch_size=100 batches=5 select_iters=1 probe_size=8"; then
  echo "expected probe status line in make arg contract selftest output" >&2
  printf '%s\n' "$OUTPUT" >&2
  exit 1
fi

echo "selftest_unnest_ab_probe_make_arg_contract status=ok"
