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
PROBE_SCRIPT="$SCRIPT_DIR/run_unnest_ab_probe_mixed_shapes.sh"
if [ ! -x "$PROBE_SCRIPT" ]; then
  echo "probe script not executable: $PROBE_SCRIPT" >&2
  exit 2
fi

set +e
OUT_BAD_WARMUP="$(
  UNNEST_AB_WARMUP_SELECTS=bad \
    "$PROBE_SCRIPT" 1 1 1 1 1 1025 "" 1 1 1 0 1 off "1" 2>&1
)"
STATUS_BAD_WARMUP=$?
set -e

if [ "$STATUS_BAD_WARMUP" -eq 0 ]; then
  echo "expected non-zero exit for invalid UNNEST_AB_WARMUP_SELECTS" >&2
  printf '%s\n' "$OUT_BAD_WARMUP" >&2
  exit 1
fi

if ! printf '%s\n' "$OUT_BAD_WARMUP" | grep -Fq "UNNEST_AB_WARMUP_SELECTS must be a non-negative integer"; then
  echo "expected warmup validation error in mixed-shapes probe script output" >&2
  printf '%s\n' "$OUT_BAD_WARMUP" >&2
  exit 1
fi

set +e
OUT_WARMUP_ZERO="$(
  UNNEST_AB_WARMUP_SELECTS=0 \
    "$PROBE_SCRIPT" 1 1 1 1 1 1024 "" 1 1 1 0 1 off "1" 2>&1
)"
STATUS_WARMUP_ZERO=$?
set -e

if [ "$STATUS_WARMUP_ZERO" -eq 0 ]; then
  echo "expected non-zero exit for invalid port in warmup=0 path" >&2
  printf '%s\n' "$OUT_WARMUP_ZERO" >&2
  exit 1
fi

if ! printf '%s\n' "$OUT_WARMUP_ZERO" | grep -Fq "port must be an integer in range 1025..65534"; then
  echo "expected port validation error for warmup=0 path" >&2
  printf '%s\n' "$OUT_WARMUP_ZERO" >&2
  exit 1
fi

if printf '%s\n' "$OUT_WARMUP_ZERO" | grep -Fq "UNNEST_AB_WARMUP_SELECTS must be a non-negative integer"; then
  echo "did not expect warmup validation error for warmup=0 path" >&2
  printf '%s\n' "$OUT_WARMUP_ZERO" >&2
  exit 1
fi

echo "selftest_run_unnest_ab_probe_mixed_shapes_warmup_validation status=ok"
