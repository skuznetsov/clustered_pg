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
COMPARE_SCRIPT="$SCRIPT_DIR/compare_unnest_ab_logsets.sh"
if [ ! -x "$COMPARE_SCRIPT" ]; then
  echo "compare script not executable: $COMPARE_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_unnest_setcmp_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

write_log() {
  local dir="$1"
  local idx="$2"
  local insert_ratio="$3"
  local join_ratio="$4"
  local any_ratio="$5"
  cat >"$dir/pg_sorted_heap_unnest_ab_${idx}.log" <<EOF
ratio_kv|insert=$insert_ratio|join_unnest=$join_ratio|any_array=$any_ratio
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
REF_INVALID="$WORKDIR/ref_invalid"
NEW_INVALID="$WORKDIR/new_invalid"
REF_P05="$WORKDIR/ref_p05"
NEW_P05="$WORKDIR/new_p05"
mkdir -p "$REF" "$NEW_OK" "$NEW_BAD" "$REF_SMALL" "$NEW_SMALL" "$REF_INVALID" "$NEW_INVALID" "$REF_P05" "$NEW_P05"

write_log "$REF" 1 1.00 1.00 1.00
write_log "$REF" 2 1.02 1.03 1.01
write_log "$REF" 3 0.99 1.01 1.00
write_log "$NEW_OK" 1 0.96 0.95 0.98
write_log "$NEW_OK" 2 0.98 0.97 0.99
write_log "$NEW_OK" 3 0.97 0.96 0.97
write_log "$NEW_BAD" 1 0.96 0.75 0.98
write_log "$NEW_BAD" 2 0.98 0.76 0.99
write_log "$NEW_BAD" 3 0.97 0.74 0.97
write_log "$REF_SMALL" 1 1.00 1.00 1.00
write_log "$REF_SMALL" 2 1.02 1.03 1.01
write_log "$NEW_SMALL" 1 0.96 0.95 0.98
write_log "$NEW_SMALL" 2 0.98 0.97 0.99
write_log "$REF_INVALID" 1 0.0 1.00 1.00
write_log "$REF_INVALID" 2 1.02 1.03 1.01
write_log "$REF_INVALID" 3 0.99 1.01 1.00
write_log "$NEW_INVALID" 1 0.96 0.95 0.98
write_log "$NEW_INVALID" 2 0.98 0.97 0.99
write_log "$NEW_INVALID" 3 0.97 0.96 0.97

write_log "$REF_P05" 1 1.00 1.00 1.00
write_log "$REF_P05" 2 1.01 1.01 1.01
write_log "$REF_P05" 3 0.99 0.99 0.99
write_log "$REF_P05" 4 1.00 1.00 1.00
write_log "$REF_P05" 5 1.02 1.02 1.02
write_log "$NEW_P05" 1 1.02 1.02 1.02
write_log "$NEW_P05" 2 1.01 1.01 1.01
write_log "$NEW_P05" 3 1.00 1.00 1.00
write_log "$NEW_P05" 4 0.60 1.00 1.00
write_log "$NEW_P05" 5 1.01 1.01 1.01

expect_ok_contains "unnest_ab_set_compare status=ok" \
  "$COMPARE_SCRIPT" "$REF" "$NEW_OK" "0.90" "median"
expect_ok_contains "metric_polarity=higher_is_better" \
  "$COMPARE_SCRIPT" "$REF" "$NEW_OK" "0.90" "median"

expect_ok_contains "insert_p05" \
  "$COMPARE_SCRIPT" "$REF" "$NEW_OK" "0.90" "p05" "3"

expect_fail_contains "unnest_ab_set_compare status=regression" \
  "$COMPARE_SCRIPT" "$REF" "$NEW_BAD" "0.90" "median"

expect_ok_contains "insert_p95" \
  "$COMPARE_SCRIPT" "$REF_P05" "$NEW_P05" "0.90" "p95" "5"

expect_fail_contains "unnest_ab_set_compare status=regression" \
  "$COMPARE_SCRIPT" "$REF_P05" "$NEW_P05" "0.90" "p05" "5"

expect_fail_contains "need >= 3 logs" \
  "$COMPARE_SCRIPT" "$REF_SMALL" "$NEW_SMALL" "0.90" "p95"

expect_fail_contains "non-positive value for metric 'insert'" \
  "$COMPARE_SCRIPT" "$REF_INVALID" "$NEW_INVALID" "0.90" "median"

echo "selftest_compare_unnest_ab_logsets status=ok"
