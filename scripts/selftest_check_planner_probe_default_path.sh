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
CHECK_SCRIPT="$SCRIPT_DIR/check_planner_probe_default_path.sh"
if [ ! -x "$CHECK_SCRIPT" ]; then
  echo "checker script not executable: $CHECK_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_planner_defaultpath_selftest.XXXXXX")"
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
BAD_PLAN_LOG="$WORKDIR/bad_plan.log"
NO_SAMPLE_LOG="$WORKDIR/no_sample.log"
BAD_FORMAT_LOG="$WORKDIR/bad_format.log"

cat >"$GOOD_LOG" <<EOF
planner_probe|rows=1000|fastpath=on|query=point_default|plan=Index Scan using pg_sorted_heap_planner_probe_idx on pg_sorted_heap_planner_probe|startup_cost=0.00|total_cost=8.02|plan_rows=1|plan_width=8
planner_probe|rows=10000|fastpath=on|query=point_default|plan=Index Scan using pg_sorted_heap_planner_probe_idx on pg_sorted_heap_planner_probe|startup_cost=0.00|total_cost=8.02|plan_rows=1|plan_width=8
EOF

cat >"$BAD_PLAN_LOG" <<EOF
planner_probe|rows=20000|fastpath=on|query=point_default|plan=Seq Scan on pg_sorted_heap_planner_probe|startup_cost=0.00|total_cost=300.00|plan_rows=1|plan_width=8
EOF

cat >"$NO_SAMPLE_LOG" <<EOF
planner_probe|rows=500|fastpath=on|query=point_default|plan=Index Scan using pg_sorted_heap_planner_probe_idx on pg_sorted_heap_planner_probe|startup_cost=0.00|total_cost=8.02|plan_rows=1|plan_width=8
EOF

cat >"$BAD_FORMAT_LOG" <<EOF
planner_probe|rows=10000|fastpath=on|query=point_default|startup_cost=0.00|total_cost=8.02|plan_rows=1|plan_width=8
EOF

expect_ok_contains "planner_probe_default_path_check status=ok" \
  "$CHECK_SCRIPT" "$GOOD_LOG" "10000"

expect_fail_contains "planner_probe_default_path_check status=regression" \
  "$CHECK_SCRIPT" "$BAD_PLAN_LOG" "10000"

expect_fail_contains "no eligible point_default fastpath=on samples" \
  "$CHECK_SCRIPT" "$NO_SAMPLE_LOG" "10000"

expect_fail_contains "malformed planner_probe line" \
  "$CHECK_SCRIPT" "$BAD_FORMAT_LOG" "10000"

echo "selftest_check_planner_probe_default_path status=ok"
