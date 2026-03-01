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
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
MAKEFILE="$ROOT_DIR/Makefile"

if [ ! -f "$MAKEFILE" ]; then
  echo "Makefile not found: $MAKEFILE" >&2
  exit 2
fi

extract_default() {
  local key="$1"
  local value
  value="$(
    awk -v k="$key" '
      BEGIN { found = 0 }
      $1 == k && $2 == "?=" {
        found = 1
        print $3
        exit
      }
      END {
        if (!found)
          exit 1
      }' "$MAKEFILE"
  )" || {
    echo "unable to extract default for $key from $MAKEFILE" >&2
    exit 1
  }
  printf '%s\n' "$value"
}

extract_positive_int_default() {
  local key="$1"
  local value
  value="$(extract_default "$key")"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || [ "$value" -le 0 ]; then
    echo "$key default must be a positive integer, got: $value" >&2
    exit 1
  fi
  printf '%s\n' "$value"
}

extract_port_default() {
  local key="$1"
  local value
  value="$(extract_default "$key")"
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "$key default must be numeric, got: $value" >&2
    exit 1
  fi
  if [ "$value" -le 1024 ] || [ "$value" -ge 65535 ]; then
    echo "$key default must be in range 1025..65534, got: $value" >&2
    exit 1
  fi
  printf '%s\n' "$value"
}

parse_positive_csv_count_default() {
  local key="$1"
  local csv value count
  local entries=()
  csv="$(extract_default "$key")"
  IFS=',' read -r -a entries <<< "$csv"
  if [ "${#entries[@]}" -eq 0 ]; then
    echo "$key default list must not be empty" >&2
    exit 1
  fi
  count=0
  for value in "${entries[@]}"; do
    value="${value//[[:space:]]/}"
    if ! [[ "$value" =~ ^[0-9]+$ ]] || [ "$value" -le 0 ]; then
      echo "$key default list entry must be a positive integer, got: '$value' (source: '$csv')" >&2
      exit 1
    fi
    count=$((count + 1))
  done
  if [ "$count" -le 0 ]; then
    echo "$key default list must contain at least one positive integer entry" >&2
    exit 1
  fi
  printf '%s\n' "$count"
}

PORT_KEYS=(
  "PERF_RUNTIME_SELFTEST_PORT"
  "PERF_RUNTIME_SELFTEST_HIGH_PORT"
  "PLANNER_PROBE_PORT"
  "UNNEST_AB_PORT"
  "UNNEST_GATE_BASE_PORT"
  "UNNEST_TUNE_BASE_PORT"
)
PORT_VALUES=()

for key in "${PORT_KEYS[@]}"; do
  PORT_VALUES+=("$(extract_port_default "$key")")
done

for ((i = 0; i < ${#PORT_KEYS[@]}; i++)); do
  for ((j = i + 1; j < ${#PORT_KEYS[@]}; j++)); do
    if [ "${PORT_VALUES[$i]}" -eq "${PORT_VALUES[$j]}" ]; then
      echo "default port collision: ${PORT_KEYS[$i]}=${PORT_VALUES[$i]} conflicts with ${PORT_KEYS[$j]}=${PORT_VALUES[$j]}" >&2
      exit 1
    fi
  done
done

value_for_key() {
  local key="$1"
  local idx
  for ((idx = 0; idx < ${#PORT_KEYS[@]}; idx++)); do
    if [ "${PORT_KEYS[$idx]}" = "$key" ]; then
      printf '%s\n' "${PORT_VALUES[$idx]}"
      return 0
    fi
  done
  echo "internal error: key not found in PORT_KEYS: $key" >&2
  exit 1
}

RUNTIME_BASE_PORT="$(value_for_key "PERF_RUNTIME_SELFTEST_PORT")"
RUNTIME_HIGH_PORT="$(value_for_key "PERF_RUNTIME_SELFTEST_HIGH_PORT")"
if [ "$RUNTIME_HIGH_PORT" -le "$RUNTIME_BASE_PORT" ]; then
  echo "runtime selftest port ordering invalid: PERF_RUNTIME_SELFTEST_HIGH_PORT=$RUNTIME_HIGH_PORT must be > PERF_RUNTIME_SELFTEST_PORT=$RUNTIME_BASE_PORT" >&2
  exit 1
fi

GATE_BASE_PORT="$(value_for_key "UNNEST_GATE_BASE_PORT")"
GATE_REF_RUNS="$(extract_positive_int_default "UNNEST_GATE_REF_RUNS")"
GATE_NEW_RUNS="$(extract_positive_int_default "UNNEST_GATE_NEW_RUNS")"
GATE_REF_MAX_PORT=$((GATE_BASE_PORT + GATE_REF_RUNS - 1))
GATE_NEW_MAX_PORT=$((GATE_BASE_PORT + 100 + GATE_NEW_RUNS - 1))
GATE_MAX_PORT="$GATE_NEW_MAX_PORT"
if [ "$GATE_REF_MAX_PORT" -gt "$GATE_MAX_PORT" ]; then
  GATE_MAX_PORT="$GATE_REF_MAX_PORT"
fi
if [ "$GATE_MAX_PORT" -ge 65535 ]; then
  echo "UNNEST_GATE_BASE_PORT default violates gate headroom: base=$GATE_BASE_PORT ref_runs=$GATE_REF_RUNS new_runs=$GATE_NEW_RUNS ref_derived_max=$GATE_REF_MAX_PORT new_derived_max=$GATE_NEW_MAX_PORT derived_max=$GATE_MAX_PORT" >&2
  exit 1
fi

TUNE_BASE_PORT="$(value_for_key "UNNEST_TUNE_BASE_PORT")"
TUNE_TRIGGER_COUNT="$(parse_positive_csv_count_default "UNNEST_TUNE_TRIGGER_LIST")"
TUNE_MIN_DISTINCT_COUNT="$(parse_positive_csv_count_default "UNNEST_TUNE_MIN_DISTINCT_LIST")"
TUNE_MAX_TIDS_COUNT="$(parse_positive_csv_count_default "UNNEST_TUNE_MAX_TIDS_LIST")"
TUNE_CASE_COUNT=$((TUNE_TRIGGER_COUNT * TUNE_MIN_DISTINCT_COUNT * TUNE_MAX_TIDS_COUNT))
if [ "$TUNE_CASE_COUNT" -le 0 ]; then
  echo "UNNEST_TUNE default matrix case count must be > 0, got: $TUNE_CASE_COUNT" >&2
  exit 1
fi
TUNE_MAX_PORT=$((TUNE_BASE_PORT + TUNE_CASE_COUNT - 1))
if [ "$TUNE_MAX_PORT" -ge 65535 ]; then
  echo "UNNEST_TUNE_BASE_PORT default violates tuning headroom: base=$TUNE_BASE_PORT cases=$TUNE_CASE_COUNT derived_max=$TUNE_MAX_PORT" >&2
  exit 1
fi

echo "selftest_unnest_gate_make_defaults status=ok runtime_base_port=$RUNTIME_BASE_PORT runtime_high_port=$RUNTIME_HIGH_PORT gate_base_port=$GATE_BASE_PORT gate_ref_runs=$GATE_REF_RUNS gate_new_runs=$GATE_NEW_RUNS gate_ref_derived_max_port=$GATE_REF_MAX_PORT gate_new_derived_max_port=$GATE_NEW_MAX_PORT gate_derived_max_port=$GATE_MAX_PORT tune_base_port=$TUNE_BASE_PORT tune_case_count=$TUNE_CASE_COUNT tune_derived_max_port=$TUNE_MAX_PORT"
