#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 5 ]; then
  echo "usage: $0 <rows_csv> <port> [probe_out] [summary_format=json|csv] [summary_out]" >&2
  exit 2
fi

ROWS_CSV="$1"
PORT="$2"
PROBE_OUT="${3:-}"
SUMMARY_FORMAT="${4:-json}"
SUMMARY_OUT="${5:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROBE_SCRIPT="${PLANNER_PROBE_SCRIPT:-$SCRIPT_DIR/run_planner_cost_probe.sh}"
SUMMARY_SCRIPT="${PLANNER_SUMMARY_SCRIPT:-$SCRIPT_DIR/summarize_planner_probe_log.sh}"

if [ ! -x "$PROBE_SCRIPT" ]; then
  echo "probe script not executable: $PROBE_SCRIPT" >&2
  exit 2
fi
if [ ! -x "$SUMMARY_SCRIPT" ]; then
  echo "summary script not executable: $SUMMARY_SCRIPT" >&2
  exit 2
fi
if [ "$SUMMARY_FORMAT" != "json" ] && [ "$SUMMARY_FORMAT" != "csv" ]; then
  echo "unsupported summary_format: $SUMMARY_FORMAT (supported: json|csv)" >&2
  exit 2
fi

if [ -z "$PROBE_OUT" ]; then
  PROBE_OUT="auto:/private/tmp"
fi

probe_output="$("$PROBE_SCRIPT" "$ROWS_CSV" "$PORT" "$PROBE_OUT")"
printf "%s\n" "$probe_output"

log_path="$(printf "%s\n" "$probe_output" | sed -nE 's/^planner_probe_output: (.*)$/\1/p' | tail -n 1)"
if [ -z "$log_path" ]; then
  echo "failed to capture planner probe output path from probe run" >&2
  exit 2
fi

if [ -z "$SUMMARY_OUT" ] || [ "$SUMMARY_OUT" = "auto" ]; then
  base="${log_path%.log}"
  SUMMARY_OUT="${base}.summary.${SUMMARY_FORMAT}"
elif [[ "$SUMMARY_OUT" == auto:* ]]; then
  summary_dir="${SUMMARY_OUT#auto:}"
  if [ -z "$summary_dir" ] || [[ "$summary_dir" != /* ]]; then
    echo "summary auto directory must be an absolute path: $SUMMARY_OUT" >&2
    exit 2
  fi
  stem="$(basename "${log_path%.log}")"
  SUMMARY_OUT="${summary_dir}/${stem}.summary.${SUMMARY_FORMAT}"
fi

"$SUMMARY_SCRIPT" "$log_path" "$SUMMARY_FORMAT" "$SUMMARY_OUT"
echo "planner_probe_summary_status=ok"
