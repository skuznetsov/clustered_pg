#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <planner_probe_log> [min_off_over_on_ratio]" >&2
  exit 2
fi

LOG_PATH="$1"
MIN_RATIO="${2:-100.0}"

if [ ! -f "$LOG_PATH" ]; then
  echo "planner probe log not found: $LOG_PATH" >&2
  exit 2
fi

if ! [[ "$MIN_RATIO" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  echo "min_off_over_on_ratio must be numeric: $MIN_RATIO" >&2
  exit 2
fi

mapfile -t compare_lines < <(grep '^planner_probe_compare|' "$LOG_PATH" || true)
if [ "${#compare_lines[@]}" -eq 0 ]; then
  echo "no planner_probe_compare lines found in log: $LOG_PATH" >&2
  exit 2
fi

worst_ratio=""
worst_rows=""
fail_count=0

for line in "${compare_lines[@]}"; do
  rows="$(printf "%s" "$line" | sed -nE 's/.*\|rows=([0-9]+)\|.*/\1/p')"
  ratio="$(printf "%s" "$line" | sed -nE 's/.*\|off_over_on=([0-9]+(\.[0-9]+)?).*/\1/p')"

  if [ -z "$rows" ] || [ -z "$ratio" ]; then
    echo "malformed planner_probe_compare line: $line" >&2
    exit 2
  fi

  if [ -z "$worst_ratio" ] || awk -v a="$ratio" -v b="$worst_ratio" 'BEGIN { exit (a < b) ? 0 : 1 }'; then
    worst_ratio="$ratio"
    worst_rows="$rows"
  fi

  if awk -v r="$ratio" -v m="$MIN_RATIO" 'BEGIN { exit (r < m) ? 0 : 1 }'; then
    echo "planner_probe_ratio violation: rows=$rows ratio=$ratio min_required=$MIN_RATIO" >&2
    fail_count=$((fail_count + 1))
  fi
done

echo "planner_probe_ratio_check log=$LOG_PATH min_required=$MIN_RATIO samples=${#compare_lines[@]} worst_rows=$worst_rows worst_ratio=$worst_ratio"

if [ "$fail_count" -gt 0 ]; then
  echo "planner_probe_ratio_check status=regression failed_samples=$fail_count" >&2
  exit 1
fi

echo "planner_probe_ratio_check status=ok"
