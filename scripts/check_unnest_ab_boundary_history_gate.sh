#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 7 ]; then
  echo "usage: $0 <history_summary_log> [strict_min_observations] [min_samples_total] [balanced_max_strict_rate] [boundary40_max_strict_rate] [boundary56_min_strict_rate] [pressure_min_strict_rate]" >&2
  exit 2
fi

SUMMARY_PATH="$1"
STRICT_MIN_OBS="${2:-48}"
MIN_SAMPLES_TOTAL="${3:-8}"
BALANCED_MAX_STRICT_RATE="${4:-0.05}"
BOUNDARY40_MAX_STRICT_RATE="${5:-0.25}"
BOUNDARY56_MIN_STRICT_RATE="${6:-0.90}"
PRESSURE_MIN_STRICT_RATE="${7:-0.90}"
STRICT_RATE_KEY="lift_min${STRICT_MIN_OBS}_rate"

if [ ! -f "$SUMMARY_PATH" ]; then
  echo "history_summary_log not found: $SUMMARY_PATH" >&2
  exit 2
fi
if [ ! -r "$SUMMARY_PATH" ]; then
  echo "history_summary_log is not readable: $SUMMARY_PATH" >&2
  exit 2
fi
if ! [[ "$STRICT_MIN_OBS" =~ ^[0-9]+$ ]] || [ "$STRICT_MIN_OBS" -le 0 ]; then
  echo "strict_min_observations must be a positive integer: $STRICT_MIN_OBS" >&2
  exit 2
fi
if ! [[ "$MIN_SAMPLES_TOTAL" =~ ^[0-9]+$ ]] || [ "$MIN_SAMPLES_TOTAL" -le 0 ]; then
  echo "min_samples_total must be a positive integer: $MIN_SAMPLES_TOTAL" >&2
  exit 2
fi

is_ratio_literal() {
  local value="$1"
  [[ "$value" =~ ^[0-9]+(\.[0-9]+)?$ ]]
}

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

for ratio in \
  "$BALANCED_MAX_STRICT_RATE" \
  "$BOUNDARY40_MAX_STRICT_RATE" \
  "$BOUNDARY56_MIN_STRICT_RATE" \
  "$PRESSURE_MIN_STRICT_RATE"; do
  if ! is_ratio_literal "$ratio"; then
    echo "invalid ratio literal: $ratio" >&2
    exit 2
  fi
  if ! awk -v x="$ratio" 'BEGIN { exit (x >= 0 && x <= 1) ? 0 : 1 }'; then
    echo "ratio must be in [0,1]: $ratio" >&2
    exit 2
  fi
done

declare -A samples_by_scenario=()
declare -A strict_rate_by_scenario=()

while IFS= read -r line; do
  [ -n "$line" ] || continue
  scenario="$(field_value "$line" "scenario")"
  samples_total="$(field_value "$line" "samples_total")"
  strict_rate="$(field_value "$line" "$STRICT_RATE_KEY")"

  if [ -z "$scenario" ] || [ -z "$samples_total" ] || [ -z "$strict_rate" ]; then
    echo "history summary line missing required fields ($STRICT_RATE_KEY): $line" >&2
    exit 2
  fi
  if ! [[ "$samples_total" =~ ^[0-9]+$ ]]; then
    echo "samples_total is not an integer in line: $line" >&2
    exit 2
  fi
  if ! is_ratio_literal "$strict_rate"; then
    echo "$STRICT_RATE_KEY is not a ratio literal in line: $line" >&2
    exit 2
  fi
  if ! awk -v x="$strict_rate" 'BEGIN { exit (x >= 0 && x <= 1) ? 0 : 1 }'; then
    echo "$STRICT_RATE_KEY must be in [0,1] in line: $line" >&2
    exit 2
  fi
  if [ -n "${samples_by_scenario[$scenario]:-}" ]; then
    echo "duplicate scenario in history summary: $scenario" >&2
    exit 2
  fi

  samples_by_scenario["$scenario"]="$samples_total"
  strict_rate_by_scenario["$scenario"]="$strict_rate"
done < <(grep -E '^boundary_history\|' "$SUMMARY_PATH" || true)

if [ "${#samples_by_scenario[@]}" -eq 0 ]; then
  echo "no boundary_history lines found in history summary: $SUMMARY_PATH" >&2
  exit 2
fi

require_scenario() {
  local scenario="$1"
  if [ -z "${samples_by_scenario[$scenario]:-}" ]; then
    echo "required scenario missing in history summary: $scenario" >&2
    exit 2
  fi
}

require_scenario "balanced_wide"
require_scenario "boundary_40"
require_scenario "boundary_56"
require_scenario "pressure_wide"

for scenario in "balanced_wide" "boundary_40" "boundary_56" "pressure_wide"; do
  samples_total="${samples_by_scenario[$scenario]}"
  if [ "$samples_total" -lt "$MIN_SAMPLES_TOTAL" ]; then
    echo "history gate min_samples_total violation|scenario=$scenario|samples_total=$samples_total|min_samples_total=$MIN_SAMPLES_TOTAL" >&2
    exit 1
  fi
done

r_balanced="${strict_rate_by_scenario[balanced_wide]}"
r_boundary40="${strict_rate_by_scenario[boundary_40]}"
r_boundary56="${strict_rate_by_scenario[boundary_56]}"
r_pressure="${strict_rate_by_scenario[pressure_wide]}"

if ! awk -v v="$r_balanced" -v max="$BALANCED_MAX_STRICT_RATE" 'BEGIN { exit (v <= max) ? 0 : 1 }'; then
  echo "history gate rate violation|scenario=balanced_wide|$STRICT_RATE_KEY=$r_balanced|max=$BALANCED_MAX_STRICT_RATE" >&2
  exit 1
fi
if ! awk -v v="$r_boundary40" -v max="$BOUNDARY40_MAX_STRICT_RATE" 'BEGIN { exit (v <= max) ? 0 : 1 }'; then
  echo "history gate rate violation|scenario=boundary_40|$STRICT_RATE_KEY=$r_boundary40|max=$BOUNDARY40_MAX_STRICT_RATE" >&2
  exit 1
fi
if ! awk -v v="$r_boundary56" -v min="$BOUNDARY56_MIN_STRICT_RATE" 'BEGIN { exit (v >= min) ? 0 : 1 }'; then
  echo "history gate rate violation|scenario=boundary_56|$STRICT_RATE_KEY=$r_boundary56|min=$BOUNDARY56_MIN_STRICT_RATE" >&2
  exit 1
fi
if ! awk -v v="$r_pressure" -v min="$PRESSURE_MIN_STRICT_RATE" 'BEGIN { exit (v >= min) ? 0 : 1 }'; then
  echo "history gate rate violation|scenario=pressure_wide|$STRICT_RATE_KEY=$r_pressure|min=$PRESSURE_MIN_STRICT_RATE" >&2
  exit 1
fi

if ! awk -v a="$r_balanced" -v b="$r_boundary40" -v c="$r_boundary56" -v d="$r_pressure" 'BEGIN { exit (a <= b && b <= c && c <= d) ? 0 : 1 }'; then
  echo "history gate monotonicity violation|strict_min_observations=$STRICT_MIN_OBS|balanced_wide=$r_balanced|boundary_40=$r_boundary40|boundary_56=$r_boundary56|pressure_wide=$r_pressure" >&2
  exit 1
fi

echo "boundary_history_gate|status=ok|strict_min_observations=$STRICT_MIN_OBS|min_samples_total=$MIN_SAMPLES_TOTAL|balanced_wide_rate=$r_balanced|boundary_40_rate=$r_boundary40|boundary_56_rate=$r_boundary56|pressure_wide_rate=$r_pressure"
