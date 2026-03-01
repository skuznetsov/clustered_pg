#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [tmp_root_abs_dir]" >&2
  exit 2
fi

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp_root_abs_dir not found: $TMP_ROOT" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SENSITIVITY_SCRIPT="${UNNEST_STARTUP_SENTINEL_PROBE_SCRIPT:-$SCRIPT_DIR/run_unnest_ab_startup_sensitivity_probe.sh}"

if [ ! -x "$SENSITIVITY_SCRIPT" ]; then
  echo "startup sensitivity sentinel script not executable: $SENSITIVITY_SCRIPT" >&2
  exit 2
fi

set +e
SENSITIVITY_OUTPUT="$("$SENSITIVITY_SCRIPT" "$TMP_ROOT" 2>&1)"
SENSITIVITY_STATUS=$?
set -e

if [ "$SENSITIVITY_STATUS" -ne 0 ]; then
  printf '%s\n' "$SENSITIVITY_OUTPUT" >&2
  echo "unnest_ab_startup_sensitivity_sentinel status=error stage=sensitivity_exit exit_code=$SENSITIVITY_STATUS" >&2
  exit "$SENSITIVITY_STATUS"
fi

RESULT_LINE="$(printf '%s\n' "$SENSITIVITY_OUTPUT" | rg -m1 '^unnest_ab_startup_sensitivity status=ok\|' || true)"
if [ -z "$RESULT_LINE" ]; then
  echo "missing startup sensitivity status line in output" >&2
  printf '%s\n' "$SENSITIVITY_OUTPUT" >&2
  exit 1
fi

extract_value() {
  local key="$1"
  local line="$2"
  printf '%s\n' "$line" | awk -F'|' -v key="$key" '
    {
      for (i = 1; i <= NF; i++) {
        if ($i ~ ("^" key "=")) {
          sub(("^" key "="), "", $i)
          print $i
          exit
        }
      }
    }'
}

is_decimal() {
  local v="$1"
  awk -v x="$v" 'BEGIN { exit (x ~ /^[0-9]+([.][0-9]+)?$/) ? 0 : 1 }'
}

INSERT_COLD="$(extract_value insert_cold "$RESULT_LINE")"
INSERT_WARM="$(extract_value insert_warm "$RESULT_LINE")"
INSERT_WARM_OVER_COLD="$(extract_value insert_warm_over_cold "$RESULT_LINE")"
JOIN_UNNEST_COLD="$(extract_value join_unnest_cold "$RESULT_LINE")"
JOIN_UNNEST_WARM="$(extract_value join_unnest_warm "$RESULT_LINE")"
JOIN_UNNEST_WARM_OVER_COLD="$(extract_value join_unnest_warm_over_cold "$RESULT_LINE")"
ANY_ARRAY_COLD="$(extract_value any_array_cold "$RESULT_LINE")"
ANY_ARRAY_WARM="$(extract_value any_array_warm "$RESULT_LINE")"
ANY_ARRAY_WARM_OVER_COLD="$(extract_value any_array_warm_over_cold "$RESULT_LINE")"
COLD_WARMUP_SELECTS="$(extract_value cold_warmup_selects "$RESULT_LINE")"
WARM_WARMUP_SELECTS="$(extract_value warm_warmup_selects "$RESULT_LINE")"

for pair in \
  "insert_cold:$INSERT_COLD" \
  "insert_warm:$INSERT_WARM" \
  "insert_warm_over_cold:$INSERT_WARM_OVER_COLD" \
  "join_unnest_cold:$JOIN_UNNEST_COLD" \
  "join_unnest_warm:$JOIN_UNNEST_WARM" \
  "join_unnest_warm_over_cold:$JOIN_UNNEST_WARM_OVER_COLD" \
  "any_array_cold:$ANY_ARRAY_COLD" \
  "any_array_warm:$ANY_ARRAY_WARM" \
  "any_array_warm_over_cold:$ANY_ARRAY_WARM_OVER_COLD"; do
  key="${pair%%:*}"
  val="${pair#*:}"
  if ! is_decimal "$val"; then
    echo "startup sensitivity sentinel value is not numeric for $key: $val" >&2
    printf '%s\n' "$RESULT_LINE" >&2
    exit 1
  fi
done

if ! [[ "$COLD_WARMUP_SELECTS" =~ ^[0-9]+$ ]] || ! [[ "$WARM_WARMUP_SELECTS" =~ ^[0-9]+$ ]]; then
  echo "startup sensitivity sentinel warmup fields must be non-negative integers: cold=$COLD_WARMUP_SELECTS warm=$WARM_WARMUP_SELECTS" >&2
  printf '%s\n' "$RESULT_LINE" >&2
  exit 1
fi
if [ "$COLD_WARMUP_SELECTS" -ne 0 ] || [ "$WARM_WARMUP_SELECTS" -ne 1 ]; then
  echo "startup sensitivity sentinel expected cold_warmup_selects=0 and warm_warmup_selects=1: cold=$COLD_WARMUP_SELECTS warm=$WARM_WARMUP_SELECTS" >&2
  printf '%s\n' "$RESULT_LINE" >&2
  exit 1
fi

echo "unnest_ab_startup_sensitivity_sentinel status=observe|insert_cold=$INSERT_COLD|insert_warm=$INSERT_WARM|insert_warm_over_cold=$INSERT_WARM_OVER_COLD|join_unnest_cold=$JOIN_UNNEST_COLD|join_unnest_warm=$JOIN_UNNEST_WARM|join_unnest_warm_over_cold=$JOIN_UNNEST_WARM_OVER_COLD|any_array_cold=$ANY_ARRAY_COLD|any_array_warm=$ANY_ARRAY_WARM|any_array_warm_over_cold=$ANY_ARRAY_WARM_OVER_COLD|cold_warmup_selects=$COLD_WARMUP_SELECTS|warm_warmup_selects=$WARM_WARMUP_SELECTS|gating=off"
