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
SUMMARY_SCRIPT="$SCRIPT_DIR/summarize_planner_probe_log.sh"
if [ ! -x "$SUMMARY_SCRIPT" ]; then
  echo "summary script not executable: $SUMMARY_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_planner_summary_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

expect_fail_contains() {
  local expected="$1"
  shift
  local out="$WORKDIR/expect_fail.out"
  if "$@" >"$out" 2>&1; then
    echo "expected failure but command succeeded: $*" >&2
    cat "$out" >&2
    exit 1
  fi
  if ! grep -Fq "$expected" "$out"; then
    echo "expected failure output to contain: $expected" >&2
    cat "$out" >&2
    exit 1
  fi
}

expect_ok_contains() {
  local expected="$1"
  shift
  local out="$WORKDIR/expect_ok.out"
  "$@" >"$out" 2>&1
  if ! grep -Fq "$expected" "$out"; then
    echo "expected success output to contain: $expected" >&2
    cat "$out" >&2
    exit 1
  fi
}

LOG_OK="$WORKDIR/planner_ok.log"
LOG_BAD="$WORKDIR/planner_bad.log"
JSON_OUT="$WORKDIR/summary.json"
CSV_OUT="$WORKDIR/summary.csv"

cat >"$LOG_OK" <<EOF
planner_probe_compare|rows=200|forced_point_off_total=1000010.01|forced_point_on_total=8.02|off_over_on=124689.527431
planner_probe_compare|rows=1000|forced_point_off_total=1000034.01|forced_point_on_total=8.02|off_over_on=124692.519950
planner_probe_summary|forced_index_hits=4|forced_index_cases=4
planner_probe_status=ok
EOF

cat >"$LOG_BAD" <<EOF
planner_probe_summary|forced_index_hits=0|forced_index_cases=0
planner_probe_status=ok
EOF

expect_ok_contains "\"status\": \"ok\"" \
  "$SUMMARY_SCRIPT" "$LOG_OK" "json"

"$SUMMARY_SCRIPT" "$LOG_OK" "json" "$JSON_OUT" >/dev/null
if ! grep -Fq "\"worst_case\"" "$JSON_OUT"; then
  echo "expected json output file to contain worst_case block" >&2
  cat "$JSON_OUT" >&2
  exit 1
fi

"$SUMMARY_SCRIPT" "$LOG_OK" "csv" "$CSV_OUT" >/dev/null
if ! grep -Fq "ratio_median" "$CSV_OUT"; then
  echo "expected csv output header with ratio_median" >&2
  cat "$CSV_OUT" >&2
  exit 1
fi

expect_fail_contains "no planner_probe_compare lines found" \
  "$SUMMARY_SCRIPT" "$LOG_BAD" "json"

echo "selftest_summarize_planner_probe_log status=ok"
