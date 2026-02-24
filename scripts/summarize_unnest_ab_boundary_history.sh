#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <log_file_or_dir> [strict_min_observations]" >&2
  exit 2
fi

INPUT_PATH="$1"
STRICT_MIN_OBS="${2:-48}"
STRICT_KEY="lift_min${STRICT_MIN_OBS}"

if ! [[ "$STRICT_MIN_OBS" =~ ^[0-9]+$ ]] || [ "$STRICT_MIN_OBS" -le 0 ]; then
  echo "strict_min_observations must be a positive integer: $STRICT_MIN_OBS" >&2
  exit 2
fi

field_value() {
  local line="$1"
  local key="$2"
  printf '%s\n' "$line" | awk -F'|' -v key="$key" '
    {
      for (i = 1; i <= NF; i++) {
        if (index($i, key "=") == 1) {
          sub("^" key "=", "", $i)
          print $i
          exit
        }
      }
    }
  '
}

is_non_negative_int() {
  local value="$1"
  [[ "$value" =~ ^[0-9]+$ ]]
}

declare -a LOG_FILES=()
if [ -f "$INPUT_PATH" ]; then
  LOG_FILES=("$INPUT_PATH")
elif [ -d "$INPUT_PATH" ]; then
  while IFS= read -r file_path; do
    [ -n "$file_path" ] || continue
    LOG_FILES+=("$file_path")
  done < <(find "$INPUT_PATH" -maxdepth 1 -type f | sort)
else
  echo "log_file_or_dir not found: $INPUT_PATH" >&2
  exit 2
fi

if [ "${#LOG_FILES[@]}" -eq 0 ]; then
  echo "no regular files found in input path: $INPUT_PATH" >&2
  exit 2
fi

declare -A runs_by_scenario=()
declare -A samples_total_by_scenario=()
declare -A lift32_total_by_scenario=()
declare -A liftstrict_total_by_scenario=()

run_count=0
baseline_scenario_set=""
baseline_source=""

for log_file in "${LOG_FILES[@]}"; do
  if [ ! -r "$log_file" ]; then
    echo "log file is not readable: $log_file" >&2
    exit 2
  fi

  mapfile -t summary_lines < <(grep -E '^nightly_boundary_summary\|' "$log_file" || true)
  if [ "${#summary_lines[@]}" -eq 0 ]; then
    echo "no nightly_boundary_summary lines found in file: $log_file" >&2
    exit 2
  fi

  declare -A seen_in_file=()
  run_count=$((run_count + 1))

  for summary_line in "${summary_lines[@]}"; do
    scenario="$(field_value "$summary_line" "scenario")"
    samples="$(field_value "$summary_line" "samples")"
    lift32="$(field_value "$summary_line" "lift_min32")"
    liftstrict="$(field_value "$summary_line" "$STRICT_KEY")"

    if [ -z "$scenario" ]; then
      echo "invalid summary line without scenario in $log_file: $summary_line" >&2
      exit 2
    fi
    if [ -z "$samples" ] || [ -z "$lift32" ] || [ -z "$liftstrict" ]; then
      echo "summary line missing required field $STRICT_KEY in $log_file: $summary_line" >&2
      exit 2
    fi
    if ! is_non_negative_int "$samples" || ! is_non_negative_int "$lift32" || ! is_non_negative_int "$liftstrict"; then
      echo "summary line contains non-integer numeric fields in $log_file: $summary_line" >&2
      exit 2
    fi
    if [ "$lift32" -gt "$samples" ]; then
      echo "summary line has lift_min32 greater than samples in $log_file: $summary_line" >&2
      exit 2
    fi
    if [ "$liftstrict" -gt "$samples" ]; then
      echo "summary line has $STRICT_KEY greater than samples in $log_file: $summary_line" >&2
      exit 2
    fi
    if [ -n "${seen_in_file[$scenario]:-}" ]; then
      echo "duplicate scenario '$scenario' in file: $log_file" >&2
      exit 2
    fi

    seen_in_file["$scenario"]=1
    runs_by_scenario["$scenario"]=$(( ${runs_by_scenario[$scenario]:-0} + 1 ))
    samples_total_by_scenario["$scenario"]=$(( ${samples_total_by_scenario[$scenario]:-0} + samples ))
    lift32_total_by_scenario["$scenario"]=$(( ${lift32_total_by_scenario[$scenario]:-0} + lift32 ))
    liftstrict_total_by_scenario["$scenario"]=$(( ${liftstrict_total_by_scenario[$scenario]:-0} + liftstrict ))
  done

  current_scenario_set="$(
    for scenario in "${!seen_in_file[@]}"; do
      printf '%s\n' "$scenario"
    done | sort
  )"
  if [ -z "$current_scenario_set" ]; then
    echo "no scenarios discovered in file: $log_file" >&2
    exit 2
  fi

  if [ -z "$baseline_scenario_set" ]; then
    baseline_scenario_set="$current_scenario_set"
    baseline_source="$log_file"
  elif [ "$current_scenario_set" != "$baseline_scenario_set" ]; then
    echo "scenario set mismatch between files: baseline=$baseline_source current=$log_file" >&2
    exit 2
  fi
done

if [ "${#runs_by_scenario[@]}" -eq 0 ]; then
  echo "no scenario aggregates produced from input: $INPUT_PATH" >&2
  exit 2
fi

while IFS= read -r scenario; do
  [ -n "$scenario" ] || continue
  runs="${runs_by_scenario[$scenario]}"
  samples_total="${samples_total_by_scenario[$scenario]}"
  lift32_total="${lift32_total_by_scenario[$scenario]}"
  liftstrict_total="${liftstrict_total_by_scenario[$scenario]}"
  lift32_rate="$(awk -v num="$lift32_total" -v den="$samples_total" 'BEGIN { if (den == 0) printf "0.000000"; else printf "%.6f", num / den }')"
  liftstrict_rate="$(awk -v num="$liftstrict_total" -v den="$samples_total" 'BEGIN { if (den == 0) printf "0.000000"; else printf "%.6f", num / den }')"
  printf 'boundary_history|scenario=%s|runs=%s|samples_total=%s|lift_min32_total=%s|lift_min%d_total=%s|lift_min32_rate=%s|lift_min%d_rate=%s\n' \
    "$scenario" "$runs" "$samples_total" "$lift32_total" "$STRICT_MIN_OBS" "$liftstrict_total" "$lift32_rate" "$STRICT_MIN_OBS" "$liftstrict_rate"
done < <(
  for scenario in "${!runs_by_scenario[@]}"; do
    printf '%s\n' "$scenario"
  done | sort
)

echo "boundary_history_status|status=ok|runs=$run_count|scenarios=${#runs_by_scenario[@]}|strict_min_observations=$STRICT_MIN_OBS|input=$INPUT_PATH"
