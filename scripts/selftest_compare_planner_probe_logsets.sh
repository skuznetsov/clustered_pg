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
COMPARE_SCRIPT="$SCRIPT_DIR/compare_planner_probe_logsets.sh"
if [ ! -x "$COMPARE_SCRIPT" ]; then
  echo "compare script not executable: $COMPARE_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_planner_setcmp_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

write_log() {
  local dir="$1"
  local idx="$2"
  local ratio="$3"
  cat >"$dir/pg_sorted_heap_planner_probe_${idx}.log" <<EOF
planner_probe_compare|rows=200|forced_point_off_total=1000000.00|forced_point_on_total=8.00|off_over_on=$ratio
EOF
}

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

REF="$WORKDIR/ref"
NEW_OK="$WORKDIR/new_ok"
NEW_BAD="$WORKDIR/new_bad"
REF_SMALL="$WORKDIR/ref_small"
NEW_SMALL="$WORKDIR/new_small"
REF_P05="$WORKDIR/ref_p05"
NEW_P05="$WORKDIR/new_p05"
mkdir -p "$REF" "$NEW_OK" "$NEW_BAD" "$REF_SMALL" "$NEW_SMALL" "$REF_P05" "$NEW_P05"

write_log "$REF" 1 120000.0
write_log "$REF" 2 121000.0
write_log "$REF" 3 119000.0
write_log "$NEW_OK" 1 118000.0
write_log "$NEW_OK" 2 117500.0
write_log "$NEW_OK" 3 118200.0
write_log "$NEW_BAD" 1 90000.0
write_log "$NEW_BAD" 2 89000.0
write_log "$NEW_BAD" 3 90500.0
write_log "$REF_SMALL" 1 120000.0
write_log "$REF_SMALL" 2 121000.0
write_log "$NEW_SMALL" 1 118000.0
write_log "$NEW_SMALL" 2 117500.0
write_log "$REF_P05" 1 120000.0
write_log "$REF_P05" 2 121000.0
write_log "$REF_P05" 3 119000.0
write_log "$REF_P05" 4 120500.0
write_log "$REF_P05" 5 120800.0
write_log "$NEW_P05" 1 120500.0
write_log "$NEW_P05" 2 121500.0
write_log "$NEW_P05" 3 120200.0
write_log "$NEW_P05" 4 70000.0
write_log "$NEW_P05" 5 120100.0

expect_ok_contains "planner_probe_set_compare status=ok" \
  "$COMPARE_SCRIPT" "$REF" "$NEW_OK" "0.90" "median"
expect_ok_contains "metric_polarity=higher_is_better" \
  "$COMPARE_SCRIPT" "$REF" "$NEW_OK" "0.90" "median"

expect_fail_contains "planner_probe_set_compare status=regression" \
  "$COMPARE_SCRIPT" "$REF" "$NEW_BAD" "0.90" "median"

expect_ok_contains "worst_ratio_p95" \
  "$COMPARE_SCRIPT" "$REF_P05" "$NEW_P05" "0.90" "p95" "5"

expect_fail_contains "planner_probe_set_compare status=regression" \
  "$COMPARE_SCRIPT" "$REF_P05" "$NEW_P05" "0.90" "p05" "5"

expect_fail_contains "need >= 3 logs" \
  "$COMPARE_SCRIPT" "$REF_SMALL" "$NEW_SMALL" "0.90" "p95"

echo "selftest_compare_planner_probe_logsets status=ok"
