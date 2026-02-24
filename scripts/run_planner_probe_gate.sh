#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 7 ]; then
  echo "usage: $0 <rows_csv> <port> [probe_out] [summary_format=json|csv] [summary_out] [min_off_over_on_ratio] [min_default_index_rows]" >&2
  exit 2
fi

ROWS_CSV="$1"
PORT="$2"
PROBE_OUT="${3:-}"
SUMMARY_FORMAT="${4:-json}"
SUMMARY_OUT="${5:-}"
MIN_OFF_OVER_ON="${6:-100.0}"
MIN_DEFAULT_INDEX_ROWS="${7:-10000}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROBE_SUMMARY_SCRIPT="${PLANNER_PROBE_SUMMARY_SCRIPT:-$SCRIPT_DIR/run_planner_probe_with_summary.sh}"
CHECK_SCRIPT="${PLANNER_RATIO_CHECK_SCRIPT:-$SCRIPT_DIR/check_planner_probe_cost_ratio.sh}"
DEFAULT_CHECK_SCRIPT="${PLANNER_DEFAULT_PATH_CHECK_SCRIPT:-$SCRIPT_DIR/check_planner_probe_default_path.sh}"

if [ ! -x "$PROBE_SUMMARY_SCRIPT" ]; then
  echo "probe-summary script not executable: $PROBE_SUMMARY_SCRIPT" >&2
  exit 2
fi
if [ ! -x "$CHECK_SCRIPT" ]; then
  echo "ratio check script not executable: $CHECK_SCRIPT" >&2
  exit 2
fi
if [ ! -x "$DEFAULT_CHECK_SCRIPT" ]; then
  echo "default-path check script not executable: $DEFAULT_CHECK_SCRIPT" >&2
  exit 2
fi

out="$("$PROBE_SUMMARY_SCRIPT" "$ROWS_CSV" "$PORT" "$PROBE_OUT" "$SUMMARY_FORMAT" "$SUMMARY_OUT")"
printf "%s\n" "$out"

log_path="$(printf "%s\n" "$out" | sed -nE 's/^planner_probe_output: (.*)$/\1/p' | tail -n 1)"
if [ -z "$log_path" ]; then
  echo "failed to capture planner probe log path from probe-summary output" >&2
  exit 2
fi

"$CHECK_SCRIPT" "$log_path" "$MIN_OFF_OVER_ON"
"$DEFAULT_CHECK_SCRIPT" "$log_path" "$MIN_DEFAULT_INDEX_ROWS"
echo "planner_probe_gate_status=ok"
