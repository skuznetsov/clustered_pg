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
CHECK_SCRIPT="$SCRIPT_DIR/check_planner_probe_cost_ratio.sh"
if [ ! -x "$CHECK_SCRIPT" ]; then
  echo "checker script not executable: $CHECK_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_planner_ratio_selftest.XXXXXX")"
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

GOOD_LOG="$WORKDIR/good.log"
BAD_RATIO_LOG="$WORKDIR/bad_ratio.log"
BAD_FORMAT_LOG="$WORKDIR/bad_format.log"

cat >"$GOOD_LOG" <<EOF
planner_probe_compare|rows=200|forced_point_off_total=1000010.01|forced_point_on_total=8.02|off_over_on=124689.527431
planner_probe_compare|rows=1000|forced_point_off_total=1000034.01|forced_point_on_total=8.02|off_over_on=124692.519950
EOF

cat >"$BAD_RATIO_LOG" <<EOF
planner_probe_compare|rows=200|forced_point_off_total=900.00|forced_point_on_total=12.00|off_over_on=75.000000
EOF

cat >"$BAD_FORMAT_LOG" <<EOF
planner_probe_compare|rows=200|forced_point_off_total=900.00|forced_point_on_total=12.00
EOF

expect_ok_contains "planner_probe_ratio_check status=ok" \
  "$CHECK_SCRIPT" "$GOOD_LOG" "100.0"

expect_fail_contains "planner_probe_ratio_check status=regression" \
  "$CHECK_SCRIPT" "$BAD_RATIO_LOG" "100.0"

expect_fail_contains "malformed planner_probe_compare line" \
  "$CHECK_SCRIPT" "$BAD_FORMAT_LOG" "100.0"

echo "selftest_check_planner_probe_cost_ratio status=ok"
