#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <planner_probe_log> [format=json|csv] [output_path]" >&2
  exit 2
fi

LOG_PATH="$1"
FORMAT="${2:-json}"
OUT_PATH="${3:-}"

if [ ! -f "$LOG_PATH" ]; then
  echo "planner probe log not found: $LOG_PATH" >&2
  exit 2
fi

if [ "$FORMAT" != "json" ] && [ "$FORMAT" != "csv" ]; then
  echo "unsupported format: $FORMAT (supported: json|csv)" >&2
  exit 2
fi

ratios=()
forced_hits=""
forced_cases=""
probe_status=""
worst_rows=""
worst_ratio=""

while IFS= read -r line; do
  if [[ "$line" == planner_probe_compare\|* ]]; then
    rows="$(printf "%s" "$line" | sed -nE 's/.*\|rows=([0-9]+)\|.*/\1/p')"
    ratio="$(printf "%s" "$line" | sed -nE 's/.*\|off_over_on=([0-9]+(\.[0-9]+)?).*/\1/p')"
    if [ -z "$rows" ] || [ -z "$ratio" ]; then
      echo "malformed planner_probe_compare line: $line" >&2
      exit 2
    fi
    ratios+=("$ratio")
    if [ -z "$worst_ratio" ] || awk -v a="$ratio" -v b="$worst_ratio" 'BEGIN { exit (a < b) ? 0 : 1 }'; then
      worst_ratio="$ratio"
      worst_rows="$rows"
    fi
  elif [[ "$line" == planner_probe_summary\|* ]]; then
    forced_hits="$(printf "%s" "$line" | sed -nE 's/.*\|forced_index_hits=([0-9]+)\|.*/\1/p')"
    forced_cases="$(printf "%s" "$line" | sed -nE 's/.*\|forced_index_cases=([0-9]+).*/\1/p')"
  elif [[ "$line" == planner_probe_status=* ]]; then
    probe_status="${line#planner_probe_status=}"
  fi
done < "$LOG_PATH"

if [ "${#ratios[@]}" -eq 0 ]; then
  echo "no planner_probe_compare lines found in log: $LOG_PATH" >&2
  exit 2
fi

if [ -z "$forced_hits" ]; then
  forced_hits="0"
fi
if [ -z "$forced_cases" ]; then
  forced_cases="0"
fi
if [ -z "$probe_status" ]; then
  probe_status="unknown"
fi

sorted="$(printf "%s\n" "${ratios[@]}" | sort -g)"
ratio_min="$(printf "%s\n" "$sorted" | head -n 1)"
ratio_max="$(printf "%s\n" "$sorted" | tail -n 1)"
ratio_median="$(printf "%s\n" "$sorted" | awk '
  {
    arr[NR] = $1
  }
  END {
    if (NR % 2 == 1)
      printf "%.6f", arr[(NR + 1) / 2];
    else
      printf "%.6f", (arr[NR / 2] + arr[NR / 2 + 1]) / 2.0;
  }')"
ratio_mean="$(printf "%s\n" "$sorted" | awk '
  {
    sum += $1;
    n += 1;
  }
  END {
    if (n == 0)
      printf "0.000000";
    else
      printf "%.6f", sum / n;
  }')"

if [ "$FORMAT" = "json" ]; then
  log_json="$(printf "%s" "$LOG_PATH" | sed 's/\\/\\\\/g; s/"/\\"/g')"
  payload="$(cat <<EOF
{
  "log": "$log_json",
  "samples": ${#ratios[@]},
  "ratio": {
    "min": $ratio_min,
    "median": $ratio_median,
    "mean": $ratio_mean,
    "max": $ratio_max
  },
  "worst_case": {
    "rows": $worst_rows,
    "ratio": $worst_ratio
  },
  "forced_index": {
    "hits": $forced_hits,
    "cases": $forced_cases
  },
  "status": "$probe_status"
}
EOF
)"
else
  payload="log,samples,ratio_min,ratio_median,ratio_mean,ratio_max,worst_rows,worst_ratio,forced_index_hits,forced_index_cases,status
$LOG_PATH,${#ratios[@]},$ratio_min,$ratio_median,$ratio_mean,$ratio_max,$worst_rows,$worst_ratio,$forced_hits,$forced_cases,$probe_status"
fi

if [ -n "$OUT_PATH" ]; then
  mkdir -p "$(dirname "$OUT_PATH")"
  printf "%s\n" "$payload" > "$OUT_PATH"
  echo "planner_probe_summary_output: $OUT_PATH"
else
  printf "%s\n" "$payload"
fi
