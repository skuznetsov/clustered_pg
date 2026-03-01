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
SENSITIVITY_SCRIPT="${UNNEST_STARTUP_GUARD_PROBE_SCRIPT:-$SCRIPT_DIR/run_unnest_ab_startup_sensitivity_probe.sh}"

MAX_INSERT_WARM_OVER_COLD="${UNNEST_STARTUP_GUARD_MAX_INSERT_WARM_OVER_COLD:-2.00}"
MAX_JOIN_UNNEST_WARM_OVER_COLD="${UNNEST_STARTUP_GUARD_MAX_JOIN_UNNEST_WARM_OVER_COLD:-2.00}"
MAX_ANY_ARRAY_WARM_OVER_COLD="${UNNEST_STARTUP_GUARD_MAX_ANY_ARRAY_WARM_OVER_COLD:-1.80}"

if [ ! -x "$SENSITIVITY_SCRIPT" ]; then
  echo "startup sensitivity script not executable: $SENSITIVITY_SCRIPT" >&2
  exit 2
fi

is_positive_decimal() {
  local v="$1"
  awk -v x="$v" 'BEGIN { exit (x ~ /^[0-9]+([.][0-9]+)?$/ && x > 0) ? 0 : 1 }'
}

for pair in \
  "insert:$MAX_INSERT_WARM_OVER_COLD" \
  "join_unnest:$MAX_JOIN_UNNEST_WARM_OVER_COLD" \
  "any_array:$MAX_ANY_ARRAY_WARM_OVER_COLD"; do
  key="${pair%%:*}"
  val="${pair#*:}"
  if ! is_positive_decimal "$val"; then
    echo "startup sensitivity max thresholds must be positive decimals: key=$key value=$val" >&2
    exit 2
  fi
done

set +e
SENSITIVITY_OUTPUT="$("$SENSITIVITY_SCRIPT" "$TMP_ROOT" 2>&1)"
SENSITIVITY_STATUS=$?
set -e
if [ "$SENSITIVITY_STATUS" -ne 0 ]; then
  printf '%s\n' "$SENSITIVITY_OUTPUT" >&2
  echo "unnest_ab_startup_sensitivity_guard status=error stage=sensitivity_exit exit_code=$SENSITIVITY_STATUS" >&2
  exit "$SENSITIVITY_STATUS"
fi

RESULT_LINE="$(printf '%s\n' "$SENSITIVITY_OUTPUT" | rg -m1 '^unnest_ab_startup_sensitivity status=ok\|')"
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

check_le() {
  local actual="$1"
  local max_allowed="$2"
  awk -v a="$actual" -v m="$max_allowed" 'BEGIN { exit (a <= m) ? 0 : 1 }'
}

INSERT_WARM_OVER_COLD="$(extract_value insert_warm_over_cold "$RESULT_LINE")"
JOIN_UNNEST_WARM_OVER_COLD="$(extract_value join_unnest_warm_over_cold "$RESULT_LINE")"
ANY_ARRAY_WARM_OVER_COLD="$(extract_value any_array_warm_over_cold "$RESULT_LINE")"

for pair in \
  "insert_warm_over_cold:$INSERT_WARM_OVER_COLD" \
  "join_unnest_warm_over_cold:$JOIN_UNNEST_WARM_OVER_COLD" \
  "any_array_warm_over_cold:$ANY_ARRAY_WARM_OVER_COLD"; do
  key="${pair%%:*}"
  val="${pair#*:}"
  if ! is_decimal "$val"; then
    echo "startup sensitivity value is not numeric for $key: $val" >&2
    printf '%s\n' "$RESULT_LINE" >&2
    exit 1
  fi
done

if ! check_le "$INSERT_WARM_OVER_COLD" "$MAX_INSERT_WARM_OVER_COLD" ||
   ! check_le "$JOIN_UNNEST_WARM_OVER_COLD" "$MAX_JOIN_UNNEST_WARM_OVER_COLD" ||
   ! check_le "$ANY_ARRAY_WARM_OVER_COLD" "$MAX_ANY_ARRAY_WARM_OVER_COLD"; then
  echo "unnest_ab_startup_sensitivity_guard status=regression|insert_warm_over_cold=$INSERT_WARM_OVER_COLD|max_insert_warm_over_cold=$MAX_INSERT_WARM_OVER_COLD|join_unnest_warm_over_cold=$JOIN_UNNEST_WARM_OVER_COLD|max_join_unnest_warm_over_cold=$MAX_JOIN_UNNEST_WARM_OVER_COLD|any_array_warm_over_cold=$ANY_ARRAY_WARM_OVER_COLD|max_any_array_warm_over_cold=$MAX_ANY_ARRAY_WARM_OVER_COLD" >&2
  exit 1
fi

echo "unnest_ab_startup_sensitivity_guard status=ok|insert_warm_over_cold=$INSERT_WARM_OVER_COLD|max_insert_warm_over_cold=$MAX_INSERT_WARM_OVER_COLD|join_unnest_warm_over_cold=$JOIN_UNNEST_WARM_OVER_COLD|max_join_unnest_warm_over_cold=$MAX_JOIN_UNNEST_WARM_OVER_COLD|any_array_warm_over_cold=$ANY_ARRAY_WARM_OVER_COLD|max_any_array_warm_over_cold=$MAX_ANY_ARRAY_WARM_OVER_COLD"
