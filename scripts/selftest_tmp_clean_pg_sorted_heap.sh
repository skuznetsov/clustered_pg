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
CLEAN_SCRIPT="$SCRIPT_DIR/tmp_clean_pg_sorted_heap.sh"
if [ ! -x "$CLEAN_SCRIPT" ]; then
  echo "cleanup script not executable: $CLEAN_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_tmp_clean_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

M1="$WORKDIR/pg_sorted_heap_stress_dummy"
M2="$WORKDIR/pg_sorted_heap_planner_probe_dummy.log"
M3="$WORKDIR/pg_sorted_heap_perf_ref_set_dummy"
M4="$WORKDIR/pg_sorted_heap_planner_new_set_dummy"
M5="$WORKDIR/pg_sorted_heap_lightweight_selftests_dummy.jsonl"
M6="$WORKDIR/workflow_runner_guard.dummy"
M7="$WORKDIR/runtime_workflow_path_parity.dummy"
LIVE="$WORKDIR/pg_sorted_heap_regress_live"
RECENT="$WORKDIR/pg_sorted_heap_perf_probe_recent"
KEEP="$WORKDIR/unrelated_file"
mkdir -p "$M1"
touch "$M2"
mkdir -p "$M3"
mkdir -p "$M4"
touch "$M5"
mkdir -p "$M6"
mkdir -p "$M7"
mkdir -p "$LIVE"
mkdir -p "$RECENT"
printf "%s\n" "$$" > "$LIVE/postmaster.pid"
touch -t 200001010000 "$M1"
touch -t 200001010000 "$M2"
touch -t 200001010000 "$M3"
touch -t 200001010000 "$M4"
touch -t 200001010000 "$M5"
touch -t 200001010000 "$M6"
touch -t 200001010000 "$M7"
touch "$KEEP"

out="$("$CLEAN_SCRIPT" "$WORKDIR" 3600)"
printf "%s\n" "$out"

if [ -e "$M1" ] || [ -e "$M2" ] || [ -e "$M3" ] || [ -e "$M4" ] || [ -e "$M5" ] || [ -e "$M6" ] || [ -e "$M7" ]; then
  echo "expected matching artifacts to be removed" >&2
  exit 1
fi
if [ ! -e "$KEEP" ]; then
  echo "expected non-matching artifact to remain" >&2
  exit 1
fi
if [ ! -e "$LIVE" ]; then
  echo "expected live-postmaster artifact to remain" >&2
  exit 1
fi
if [ ! -e "$RECENT" ]; then
  echo "expected recent artifact to remain when min_age blocks deletion" >&2
  exit 1
fi
if ! printf "%s\n" "$out" | grep -Fq "skip_live $LIVE"; then
  echo "expected skip_live marker for active postmaster directory" >&2
  exit 1
fi
if ! printf "%s\n" "$out" | grep -Fq "skip_recent"; then
  echo "expected skip_recent marker" >&2
  exit 1
fi
if ! printf "%s\n" "$out" | grep -Fq "tmp_clean root=$WORKDIR removed=7 skipped_live=1 skipped_recent=1 min_age_s=3600"; then
  echo "expected cleanup summary with removed/skipped counters" >&2
  exit 1
fi
if ! printf "%s\n" "$out" | grep -Eq 'removed_kb=[0-9]+'; then
  echo "expected cleanup summary to include removed_kb metric" >&2
  exit 1
fi

if "$CLEAN_SCRIPT" "$WORKDIR" "bad_value" >/dev/null 2>&1; then
  echo "expected non-zero exit for invalid min_age_seconds" >&2
  exit 1
fi

echo "selftest_tmp_clean_pg_sorted_heap status=ok"
