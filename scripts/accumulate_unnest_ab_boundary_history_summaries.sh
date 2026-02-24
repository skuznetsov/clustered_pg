#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <history_summary_file_or_dir> [strict_min_observations]" >&2
  exit 2
fi

INPUT_PATH="$1"
STRICT_MIN_OBS="${2:-48}"
STRICT_TOTAL_KEY="lift_min${STRICT_MIN_OBS}_total"
STRICT_RATE_KEY="lift_min${STRICT_MIN_OBS}_rate"

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

declare -a SUMMARY_FILES=()
if [ -f "$INPUT_PATH" ]; then
  SUMMARY_FILES=("$INPUT_PATH")
elif [ -d "$INPUT_PATH" ]; then
  while IFS= read -r file_path; do
    [ -n "$file_path" ] || continue
    SUMMARY_FILES+=("$file_path")
  done < <(find "$INPUT_PATH" -maxdepth 1 -type f | sort)
else
  echo "history_summary_file_or_dir not found: $INPUT_PATH" >&2
  exit 2
fi

if [ "${#SUMMARY_FILES[@]}" -eq 0 ]; then
  echo "no regular files found in input path: $INPUT_PATH" >&2
  exit 2
fi

declare -A runs_total_by_scenario=()
declare -A samples_total_by_scenario=()
declare -A lift32_total_by_scenario=()
declare -A liftstrict_total_by_scenario=()

source_files=0
baseline_scenario_set=""
baseline_source=""

for summary_file in "${SUMMARY_FILES[@]}"; do
  if [ ! -r "$summary_file" ]; then
    echo "history summary file is not readable: $summary_file" >&2
    exit 2
  fi
  source_files=$((source_files + 1))

  mapfile -t history_lines < <(grep -E '^boundary_history\|' "$summary_file" || true)
  if [ "${#history_lines[@]}" -eq 0 ]; then
    echo "no boundary_history lines found in file: $summary_file" >&2
    exit 2
  fi

  status_line="$(grep -E '^boundary_history_status\|' "$summary_file" | tail -n 1 || true)"
  if [ -n "$status_line" ]; then
    status_strict="$(field_value "$status_line" "strict_min_observations")"
    if [ -n "$status_strict" ] && [ "$status_strict" != "$STRICT_MIN_OBS" ]; then
      echo "strict_min_observations mismatch in status line for file $summary_file: expected=$STRICT_MIN_OBS actual=$status_strict" >&2
      exit 2
    fi
  fi

  declare -A seen_in_file=()
  for history_line in "${history_lines[@]}"; do
    scenario="$(field_value "$history_line" "scenario")"
    runs="$(field_value "$history_line" "runs")"
    samples_total="$(field_value "$history_line" "samples_total")"
    lift32_total="$(field_value "$history_line" "lift_min32_total")"
    liftstrict_total="$(field_value "$history_line" "$STRICT_TOTAL_KEY")"

    if [ -z "$scenario" ] || [ -z "$runs" ] || [ -z "$samples_total" ] || [ -z "$lift32_total" ] || [ -z "$liftstrict_total" ]; then
      echo "history line missing required fields ($STRICT_TOTAL_KEY) in file $summary_file: $history_line" >&2
      exit 2
    fi
    if ! is_non_negative_int "$runs" || ! is_non_negative_int "$samples_total" || ! is_non_negative_int "$lift32_total" || ! is_non_negative_int "$liftstrict_total"; then
      echo "history line contains non-integer numeric fields in file $summary_file: $history_line" >&2
      exit 2
    fi
    if [ "$lift32_total" -gt "$samples_total" ]; then
      echo "history line has lift_min32_total greater than samples_total in file $summary_file: $history_line" >&2
      exit 2
    fi
    if [ "$liftstrict_total" -gt "$samples_total" ]; then
      echo "history line has $STRICT_TOTAL_KEY greater than samples_total in file $summary_file: $history_line" >&2
      exit 2
    fi
    if [ -n "${seen_in_file[$scenario]:-}" ]; then
      echo "duplicate scenario '$scenario' in file: $summary_file" >&2
      exit 2
    fi

    seen_in_file["$scenario"]=1
    runs_total_by_scenario["$scenario"]=$(( ${runs_total_by_scenario[$scenario]:-0} + runs ))
    samples_total_by_scenario["$scenario"]=$(( ${samples_total_by_scenario[$scenario]:-0} + samples_total ))
    lift32_total_by_scenario["$scenario"]=$(( ${lift32_total_by_scenario[$scenario]:-0} + lift32_total ))
    liftstrict_total_by_scenario["$scenario"]=$(( ${liftstrict_total_by_scenario[$scenario]:-0} + liftstrict_total ))
  done

  scenario_set_current="$(
    for scenario in "${!seen_in_file[@]}"; do
      printf '%s\n' "$scenario"
    done | sort
  )"
  if [ -z "$scenario_set_current" ]; then
    echo "no scenarios discovered in file: $summary_file" >&2
    exit 2
  fi

  if [ -z "$baseline_scenario_set" ]; then
    baseline_scenario_set="$scenario_set_current"
    baseline_source="$summary_file"
  elif [ "$scenario_set_current" != "$baseline_scenario_set" ]; then
    echo "scenario set mismatch between files: baseline=$baseline_source current=$summary_file" >&2
    exit 2
  fi
done

if [ "${#runs_total_by_scenario[@]}" -eq 0 ]; then
  echo "no aggregated scenarios produced from input: $INPUT_PATH" >&2
  exit 2
fi

aggregate_runs=""
while IFS= read -r scenario; do
  [ -n "$scenario" ] || continue
  runs_total="${runs_total_by_scenario[$scenario]}"
  samples_total="${samples_total_by_scenario[$scenario]}"
  lift32_total="${lift32_total_by_scenario[$scenario]}"
  liftstrict_total="${liftstrict_total_by_scenario[$scenario]}"
  lift32_rate="$(awk -v num="$lift32_total" -v den="$samples_total" 'BEGIN { if (den == 0) printf "0.000000"; else printf "%.6f", num / den }')"
  liftstrict_rate="$(awk -v num="$liftstrict_total" -v den="$samples_total" 'BEGIN { if (den == 0) printf "0.000000"; else printf "%.6f", num / den }')"

  if [ -z "$aggregate_runs" ]; then
    aggregate_runs="$runs_total"
  elif [ "$aggregate_runs" != "$runs_total" ]; then
    echo "aggregated runs mismatch across scenarios: reference=$aggregate_runs scenario=$scenario runs=$runs_total" >&2
    exit 2
  fi

  printf 'boundary_history|scenario=%s|runs=%s|samples_total=%s|lift_min32_total=%s|lift_min%d_total=%s|lift_min32_rate=%s|lift_min%d_rate=%s\n' \
    "$scenario" "$runs_total" "$samples_total" "$lift32_total" "$STRICT_MIN_OBS" "$liftstrict_total" "$lift32_rate" "$STRICT_MIN_OBS" "$liftstrict_rate"
done < <(
  for scenario in "${!runs_total_by_scenario[@]}"; do
    printf '%s\n' "$scenario"
  done | sort
)

echo "boundary_history_status|status=ok|runs=$aggregate_runs|scenarios=${#runs_total_by_scenario[@]}|strict_min_observations=$STRICT_MIN_OBS|source=$INPUT_PATH|files=$source_files"
