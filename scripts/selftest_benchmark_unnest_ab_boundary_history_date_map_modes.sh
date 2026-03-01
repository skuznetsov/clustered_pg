#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [tmp_root_abs_dir]" >&2
  exit 2
fi

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
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
BENCH_SCRIPT="$SCRIPT_DIR/benchmark_unnest_ab_boundary_history_date_map_modes.sh"
KV_CHECK_SCRIPT="$SCRIPT_DIR/check_unnest_ab_kv_output_fields.sh"

if [ ! -x "$BENCH_SCRIPT" ]; then
  echo "benchmark script not executable: $BENCH_SCRIPT" >&2
  exit 2
fi
if [ ! -x "$KV_CHECK_SCRIPT" ]; then
  echo "kv-output check script not executable: $KV_CHECK_SCRIPT" >&2
  exit 2
fi

set +e
OUT_BAD_MODE="$(
  bash "$BENCH_SCRIPT" "$TMP_ROOT" 100 1 map-only 1 invalid 2>&1
)"
STATUS_BAD_MODE=$?
set -e
if [ "$STATUS_BAD_MODE" -eq 0 ]; then
  echo "expected non-zero exit for invalid timer mode" >&2
  printf '%s\n' "$OUT_BAD_MODE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_BAD_MODE" | grep -Fq "timer_mode must be auto, ms, or seconds: invalid"; then
  echo "expected explicit timer_mode validation error for invalid timer mode" >&2
  printf '%s\n' "$OUT_BAD_MODE" >&2
  exit 1
fi

OUT_FORCED_FALLBACK="$(
  UNNEST_AB_BENCH_DISABLE_MS_BACKEND=1 bash "$BENCH_SCRIPT" "$TMP_ROOT" 100 1 map-only 1 ms
)"
if ! printf '%s\n' "$OUT_FORCED_FALLBACK" | grep -Fq "unnest_ab_boundary_history_date_map_mode_bench|status=ok|mode=map-only|"; then
  echo "expected benchmark success status for forced ms-backend fallback case" >&2
  printf '%s\n' "$OUT_FORCED_FALLBACK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_FORCED_FALLBACK" | grep -Fq "|timer_mode=ms|timer_effective=seconds|timer_backend=forced_seconds_fallback|ms_backend_disabled=1|"; then
  echo "expected deterministic forced fallback markers for timer_mode=ms case" >&2
  printf '%s\n' "$OUT_FORCED_FALLBACK" >&2
  exit 1
fi
bash "$KV_CHECK_SCRIPT" --require-schema-version 1 --schema-policy exact "$OUT_FORCED_FALLBACK" "unnest_ab_boundary_history_date_map_mode_bench" \
  status mode map_entries iterations samples timer_mode timer_effective timer_backend ms_backend_disabled csv_seconds index_seconds csv_millis index_millis schema_version speedup_x >/dev/null
bash "$KV_CHECK_SCRIPT" --require-schema-version 1 --schema-policy min "$OUT_FORCED_FALLBACK" "unnest_ab_boundary_history_date_map_mode_bench" \
  status mode schema_version >/dev/null

set +e
OUT_SCHEMA_EXACT_MISMATCH="$(
  bash "$KV_CHECK_SCRIPT" --require-schema-version 2 --schema-policy exact "$OUT_FORCED_FALLBACK" "unnest_ab_boundary_history_date_map_mode_bench" status 2>&1
)"
STATUS_SCHEMA_EXACT_MISMATCH=$?
set -e
if [ "$STATUS_SCHEMA_EXACT_MISMATCH" -eq 0 ]; then
  echo "expected schema check failure for exact-policy mismatch (required=2, actual=1)" >&2
  printf '%s\n' "$OUT_SCHEMA_EXACT_MISMATCH" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_SCHEMA_EXACT_MISMATCH" | grep -Fq "schema_version mismatch: policy=exact"; then
  echo "expected explicit exact-policy schema mismatch message" >&2
  printf '%s\n' "$OUT_SCHEMA_EXACT_MISMATCH" >&2
  exit 1
fi

set +e
OUT_SCHEMA_MIN_MISMATCH="$(
  bash "$KV_CHECK_SCRIPT" --require-schema-version 2 --schema-policy min "$OUT_FORCED_FALLBACK" "unnest_ab_boundary_history_date_map_mode_bench" status 2>&1
)"
STATUS_SCHEMA_MIN_MISMATCH=$?
set -e
if [ "$STATUS_SCHEMA_MIN_MISMATCH" -eq 0 ]; then
  echo "expected schema check failure for min-policy mismatch (required_min=2, actual=1)" >&2
  printf '%s\n' "$OUT_SCHEMA_MIN_MISMATCH" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_SCHEMA_MIN_MISMATCH" | grep -Fq "schema_version below minimum: policy=min"; then
  echo "expected explicit min-policy schema mismatch message" >&2
  printf '%s\n' "$OUT_SCHEMA_MIN_MISMATCH" >&2
  exit 1
fi

OUT_MAKE_FALLBACK="$(
  make -s -C "$ROOT_DIR" --no-print-directory unnest-ab-profile-boundary-history-date-map-benchmark \
    UNNEST_AB_SELFTEST_TMP_ROOT="$TMP_ROOT" \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_BENCH_MAP_ENTRIES=100 \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_BENCH_ITERATIONS=1 \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_BENCH_MODE=map-only \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_BENCH_SAMPLES=1 \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_BENCH_TIMER_MODE=ms \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_BENCH_DISABLE_MS_BACKEND=1
)"
if ! printf '%s\n' "$OUT_MAKE_FALLBACK" | grep -Fq "unnest_ab_boundary_history_date_map_mode_bench|status=ok|mode=map-only|"; then
  echo "expected make benchmark target to succeed in forced fallback mode" >&2
  printf '%s\n' "$OUT_MAKE_FALLBACK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MAKE_FALLBACK" | grep -Fq "|timer_mode=ms|timer_effective=seconds|timer_backend=forced_seconds_fallback|ms_backend_disabled=1|"; then
  echo "expected make benchmark output to preserve forced fallback markers" >&2
  printf '%s\n' "$OUT_MAKE_FALLBACK" >&2
  exit 1
fi
bash "$KV_CHECK_SCRIPT" --require-schema-version 1 --schema-policy exact "$OUT_MAKE_FALLBACK" "unnest_ab_boundary_history_date_map_mode_bench" \
  status mode map_entries iterations samples timer_mode timer_effective timer_backend ms_backend_disabled csv_seconds index_seconds csv_millis index_millis schema_version speedup_x >/dev/null

echo "selftest_benchmark_unnest_ab_boundary_history_date_map_modes status=ok"
