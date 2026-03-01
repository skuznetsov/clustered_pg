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
COMPARE_SCRIPT="$SCRIPT_DIR/compare_perf_probe_logsets.sh"
if [ ! -x "$COMPARE_SCRIPT" ]; then
  echo "compare script not executable: $COMPARE_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_perf_set_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

write_log() {
  local dir="$1"
  local idx="$2"
  local baseline="$3"
  local churn="$4"
  cat >"$dir/pg_sorted_heap_perf_probe_${idx}.log" <<EOF
baseline_fastpath | $baseline
churn_fastpath | $churn
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

REF2="$WORKDIR/ref2"
NEW2="$WORKDIR/new2"
REF3="$WORKDIR/ref3"
NEW3="$WORKDIR/new3"
REF_BAD="$WORKDIR/ref_bad"
NEW_BAD="$WORKDIR/new_bad"

mkdir -p "$REF2" "$NEW2" "$REF3" "$NEW3" "$REF_BAD" "$NEW_BAD"

write_log "$REF2" 1 10.0 20.0
write_log "$REF2" 2 10.2 20.4
write_log "$NEW2" 1 10.1 20.1
write_log "$NEW2" 2 10.3 20.5

expect_fail_contains "need >= 3 logs" \
  "$COMPARE_SCRIPT" "$REF2" "$NEW2" "1.50" "p95"

write_log "$REF3" 1 10.0 20.0
write_log "$REF3" 2 10.2 20.4
write_log "$REF3" 3 10.1 20.2
write_log "$NEW3" 1 10.1 20.1
write_log "$NEW3" 2 10.3 20.5
write_log "$NEW3" 3 10.2 20.3

expect_ok_contains "perf_set_compare status=ok" \
  "$COMPARE_SCRIPT" "$REF3" "$NEW3" "1.50" "p95"
expect_ok_contains "metric_polarity=lower_is_better" \
  "$COMPARE_SCRIPT" "$REF3" "$NEW3" "1.50" "p95"

write_log "$REF_BAD" 1 0.0 20.0
write_log "$REF_BAD" 2 10.2 20.4
write_log "$REF_BAD" 3 10.1 20.2
write_log "$NEW_BAD" 1 10.1 20.1
write_log "$NEW_BAD" 2 10.3 20.5
write_log "$NEW_BAD" 3 10.2 20.3

expect_fail_contains "non-positive value for 'baseline_fastpath'" \
  "$COMPARE_SCRIPT" "$REF_BAD" "$NEW_BAD" "1.50" "median"

echo "selftest_compare_perf_probe_logsets status=ok"
