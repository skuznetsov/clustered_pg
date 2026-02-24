#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 10 ]; then
  echo "usage: $0 <history_summary_log> [strict_min_observations] [current_balanced_max] [current_boundary40_max] [current_boundary56_min] [current_pressure_min] [derive_max_headroom] [derive_min_floor_margin] [delta_tolerance] [enforce_on_review=off|on]" >&2
  exit 2
fi

HISTORY_SUMMARY_PATH="$1"
STRICT_MIN_OBS="${2:-48}"
CURRENT_BALANCED_MAX="${3:-0.25}"
CURRENT_BOUNDARY40_MAX="${4:-0.50}"
CURRENT_BOUNDARY56_MIN="${5:-0.75}"
CURRENT_PRESSURE_MIN="${6:-0.75}"
DERIVE_MAX_HEADROOM="${7:-0.02}"
DERIVE_MIN_FLOOR_MARGIN="${8:-0.02}"
DELTA_TOLERANCE="${9:-0.05}"
ENFORCE_ON_REVIEW="${10:-off}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DERIVE_SCRIPT="$ROOT_DIR/scripts/derive_unnest_ab_boundary_history_gate_thresholds.sh"

if [ ! -f "$HISTORY_SUMMARY_PATH" ]; then
  echo "history_summary_log not found: $HISTORY_SUMMARY_PATH" >&2
  exit 2
fi
if [ ! -r "$HISTORY_SUMMARY_PATH" ]; then
  echo "history_summary_log is not readable: $HISTORY_SUMMARY_PATH" >&2
  exit 2
fi
if [ ! -x "$DERIVE_SCRIPT" ]; then
  echo "derive script not executable: $DERIVE_SCRIPT" >&2
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

require_ratio_01() {
  local value="$1"
  local name="$2"
  if ! is_ratio_literal "$value"; then
    echo "$name must be a ratio literal in [0,1]: $value" >&2
    exit 2
  fi
  if ! awk -v x="$value" 'BEGIN { exit (x >= 0 && x <= 1) ? 0 : 1 }'; then
    echo "$name must be in [0,1]: $value" >&2
    exit 2
  fi
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

require_ratio_01 "$CURRENT_BALANCED_MAX" "current_balanced_max"
require_ratio_01 "$CURRENT_BOUNDARY40_MAX" "current_boundary40_max"
require_ratio_01 "$CURRENT_BOUNDARY56_MIN" "current_boundary56_min"
require_ratio_01 "$CURRENT_PRESSURE_MIN" "current_pressure_min"
require_ratio_01 "$DERIVE_MAX_HEADROOM" "derive_max_headroom"
require_ratio_01 "$DERIVE_MIN_FLOOR_MARGIN" "derive_min_floor_margin"
require_ratio_01 "$DELTA_TOLERANCE" "delta_tolerance"

case "$ENFORCE_ON_REVIEW" in
  off|on|true|false|1|0) ;;
  *)
    echo "enforce_on_review must be one of: off|on|true|false|1|0 (got: $ENFORCE_ON_REVIEW)" >&2
    exit 2
    ;;
esac

derive_line="$(
  bash "$DERIVE_SCRIPT" "$HISTORY_SUMMARY_PATH" "$STRICT_MIN_OBS" "$DERIVE_MAX_HEADROOM" "$DERIVE_MIN_FLOOR_MARGIN"
)"
if ! printf '%s\n' "$derive_line" | grep -Eq '^boundary_history_gate_thresholds\|'; then
  echo "derive script output missing expected prefix: $derive_line" >&2
  exit 2
fi

RECOMMENDED_MIN_SAMPLES_TOTAL="$(field_value "$derive_line" "min_samples_total")"
RECOMMENDED_BALANCED_MAX="$(field_value "$derive_line" "balanced_max_strict_rate")"
RECOMMENDED_BOUNDARY40_MAX="$(field_value "$derive_line" "boundary40_max_strict_rate")"
RECOMMENDED_BOUNDARY56_MIN="$(field_value "$derive_line" "boundary56_min_strict_rate")"
RECOMMENDED_PRESSURE_MIN="$(field_value "$derive_line" "pressure_min_strict_rate")"

if [ -z "$RECOMMENDED_MIN_SAMPLES_TOTAL" ] || [ -z "$RECOMMENDED_BALANCED_MAX" ] || [ -z "$RECOMMENDED_BOUNDARY40_MAX" ] || [ -z "$RECOMMENDED_BOUNDARY56_MIN" ] || [ -z "$RECOMMENDED_PRESSURE_MIN" ]; then
  echo "derive output missing required fields: $derive_line" >&2
  exit 2
fi

classify_metric() {
  local kind="$1"
  local current="$2"
  local recommended="$3"
  local tolerance="$4"
  local upper
  local lower

  upper="$(awk -v v="$recommended" -v t="$tolerance" 'BEGIN { x = v + t; if (x > 1) x = 1; printf "%.6f", x }')"
  lower="$(awk -v v="$recommended" -v t="$tolerance" 'BEGIN { x = v - t; if (x < 0) x = 0; printf "%.6f", x }')"

  case "$kind" in
    max)
      if awk -v c="$current" -v u="$upper" 'BEGIN { exit (c > u) ? 0 : 1 }'; then
        echo "looser_than_recommended"
      elif awk -v c="$current" -v l="$lower" 'BEGIN { exit (c < l) ? 0 : 1 }'; then
        echo "stricter_than_recommended"
      else
        echo "aligned"
      fi
      ;;
    min)
      if awk -v c="$current" -v l="$lower" 'BEGIN { exit (c < l) ? 0 : 1 }'; then
        echo "looser_than_recommended"
      elif awk -v c="$current" -v u="$upper" 'BEGIN { exit (c > u) ? 0 : 1 }'; then
        echo "stricter_than_recommended"
      else
        echo "aligned"
      fi
      ;;
    *)
      echo "unsupported metric kind: $kind" >&2
      exit 2
      ;;
  esac
}

aligned_count=0
looser_count=0
stricter_count=0
max_abs_delta="0.000000"

emit_metric() {
  local metric="$1"
  local kind="$2"
  local current="$3"
  local recommended="$4"
  local class
  local delta
  local abs_delta

  class="$(classify_metric "$kind" "$current" "$recommended" "$DELTA_TOLERANCE")"
  delta="$(awk -v c="$current" -v r="$recommended" 'BEGIN { printf "%.6f", c - r }')"
  abs_delta="$(awk -v d="$delta" 'BEGIN { x = d; if (x < 0) x = -x; printf "%.6f", x }')"
  if awk -v a="$abs_delta" -v b="$max_abs_delta" 'BEGIN { exit (a > b) ? 0 : 1 }'; then
    max_abs_delta="$abs_delta"
  fi

  case "$class" in
    aligned) aligned_count=$((aligned_count + 1)) ;;
    looser_than_recommended) looser_count=$((looser_count + 1)) ;;
    stricter_than_recommended) stricter_count=$((stricter_count + 1)) ;;
    *)
      echo "unsupported classification: $class" >&2
      exit 2
      ;;
  esac

  echo "boundary_history_policy_delta|metric=$metric|kind=$kind|current=$current|recommended=$recommended|delta=$delta|abs_delta=$abs_delta|classification=$class"
}

emit_metric "balanced_max_strict_rate" "max" "$CURRENT_BALANCED_MAX" "$RECOMMENDED_BALANCED_MAX"
emit_metric "boundary40_max_strict_rate" "max" "$CURRENT_BOUNDARY40_MAX" "$RECOMMENDED_BOUNDARY40_MAX"
emit_metric "boundary56_min_strict_rate" "min" "$CURRENT_BOUNDARY56_MIN" "$RECOMMENDED_BOUNDARY56_MIN"
emit_metric "pressure_min_strict_rate" "min" "$CURRENT_PRESSURE_MIN" "$RECOMMENDED_PRESSURE_MIN"

status="aligned"
if [ "$looser_count" -gt 0 ] || [ "$stricter_count" -gt 0 ]; then
  status="review"
fi

echo "boundary_history_policy_delta_status|status=$status|strict_min_observations=$STRICT_MIN_OBS|min_samples_total_recommended=$RECOMMENDED_MIN_SAMPLES_TOTAL|delta_tolerance=$DELTA_TOLERANCE|max_abs_delta=$max_abs_delta|aligned=$aligned_count|looser_than_recommended=$looser_count|stricter_than_recommended=$stricter_count"

case "$ENFORCE_ON_REVIEW" in
  on|true|1)
    if [ "$status" != "aligned" ]; then
      exit 1
    fi
    ;;
esac
