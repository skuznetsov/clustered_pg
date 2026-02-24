#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 4 ]; then
  echo "usage: $0 <history_summary_log> [strict_min_observations] [max_headroom] [min_floor_margin]" >&2
  exit 2
fi

SUMMARY_PATH="$1"
STRICT_MIN_OBS="${2:-48}"
MAX_HEADROOM="${3:-0.02}"
MIN_FLOOR_MARGIN="${4:-0.02}"
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

for ratio in "$MAX_HEADROOM" "$MIN_FLOOR_MARGIN"; do
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

for scenario in "balanced_wide" "boundary_40" "boundary_56" "pressure_wide"; do
  if [ -z "${samples_by_scenario[$scenario]:-}" ]; then
    echo "required scenario missing in history summary: $scenario" >&2
    exit 2
  fi
done

balanced_rate="${strict_rate_by_scenario[balanced_wide]}"
boundary40_rate="${strict_rate_by_scenario[boundary_40]}"
boundary56_rate="${strict_rate_by_scenario[boundary_56]}"
pressure_rate="${strict_rate_by_scenario[pressure_wide]}"

min_samples_total="${samples_by_scenario[balanced_wide]}"
for scenario in "boundary_40" "boundary_56" "pressure_wide"; do
  if [ "${samples_by_scenario[$scenario]}" -lt "$min_samples_total" ]; then
    min_samples_total="${samples_by_scenario[$scenario]}"
  fi
done

balanced_max="$(awk -v x="$balanced_rate" -v h="$MAX_HEADROOM" 'BEGIN { v = x + h; if (v > 1) v = 1; printf "%.6f", v }')"
boundary40_max="$(awk -v x="$boundary40_rate" -v h="$MAX_HEADROOM" 'BEGIN { v = x + h; if (v > 1) v = 1; printf "%.6f", v }')"
boundary56_min="$(awk -v x="$boundary56_rate" -v m="$MIN_FLOOR_MARGIN" 'BEGIN { v = x - m; if (v < 0) v = 0; printf "%.6f", v }')"
pressure_min="$(awk -v x="$pressure_rate" -v m="$MIN_FLOOR_MARGIN" 'BEGIN { v = x - m; if (v < 0) v = 0; printf "%.6f", v }')"

echo "boundary_history_gate_thresholds|strict_min_observations=$STRICT_MIN_OBS|min_samples_total=$min_samples_total|balanced_max_strict_rate=$balanced_max|boundary40_max_strict_rate=$boundary40_max|boundary56_min_strict_rate=$boundary56_min|pressure_min_strict_rate=$pressure_min|max_headroom=$MAX_HEADROOM|min_floor_margin=$MIN_FLOOR_MARGIN"
