#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <planner_probe_log> [min_rows_for_default_index]" >&2
  exit 2
fi

LOG_PATH="$1"
MIN_ROWS="${2:-10000}"

if [ ! -f "$LOG_PATH" ]; then
  echo "planner probe log not found: $LOG_PATH" >&2
  exit 2
fi

if ! [[ "$MIN_ROWS" =~ ^[0-9]+$ ]] || [ "$MIN_ROWS" -le 0 ]; then
  echo "min_rows_for_default_index must be a positive integer: $MIN_ROWS" >&2
  exit 2
fi

mapfile -t probe_lines < <(grep '^planner_probe|' "$LOG_PATH" || true)
if [ "${#probe_lines[@]}" -eq 0 ]; then
  echo "no planner_probe lines found in log: $LOG_PATH" >&2
  exit 2
fi

checked_samples=0
violations=0

for line in "${probe_lines[@]}"; do
  rows="$(printf "%s" "$line" | sed -nE 's/.*\|rows=([0-9]+)\|.*/\1/p')"
  fastpath="$(printf "%s" "$line" | sed -nE 's/.*\|fastpath=([^|]+)\|query=.*/\1/p')"
  query="$(printf "%s" "$line" | sed -nE 's/.*\|query=([^|]+)\|plan=.*/\1/p')"
  plan="$(printf "%s" "$line" | sed -nE 's/.*\|plan=([^|]+)\|startup_cost=.*/\1/p')"

  if [ -z "$rows" ] || [ -z "$fastpath" ] || [ -z "$query" ] || [ -z "$plan" ]; then
    echo "malformed planner_probe line: $line" >&2
    exit 2
  fi

  if [ "$fastpath" != "on" ] || [ "$query" != "point_default" ]; then
    continue
  fi

  if [ "$rows" -lt "$MIN_ROWS" ]; then
    continue
  fi

  checked_samples=$((checked_samples + 1))
  if [[ "$plan" != *Index* ]]; then
    echo "planner_probe_default_path violation: rows=$rows fastpath=on query=point_default plan=$plan min_rows=$MIN_ROWS" >&2
    violations=$((violations + 1))
  fi
done

if [ "$checked_samples" -le 0 ]; then
  echo "no eligible point_default fastpath=on samples at rows >= $MIN_ROWS in log: $LOG_PATH" >&2
  exit 2
fi

echo "planner_probe_default_path_check log=$LOG_PATH min_rows=$MIN_ROWS checked_samples=$checked_samples"

if [ "$violations" -gt 0 ]; then
  echo "planner_probe_default_path_check status=regression failed_samples=$violations" >&2
  exit 1
fi

echo "planner_probe_default_path_check status=ok"
