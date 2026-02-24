#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [tmp_root_abs_dir]" >&2
  exit 2
fi

TMP_ROOT="${1:-/private/tmp}"
if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp_root_abs_dir not found: $TMP_ROOT" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROBE_SCRIPT="${UNNEST_STARTUP_PROBE_SCRIPT:-$SCRIPT_DIR/run_unnest_ab_probe.sh}"

RUNS="${UNNEST_STARTUP_RUNS:-1}"
BATCH_SIZE="${UNNEST_STARTUP_BATCH_SIZE:-400}"
BATCHES="${UNNEST_STARTUP_BATCHES:-20}"
SELECT_ITERS="${UNNEST_STARTUP_SELECT_ITERS:-60}"
PROBE_SIZE="${UNNEST_STARTUP_PROBE_SIZE:-64}"
PORT="${UNNEST_STARTUP_PORT:-65486}"

if [ ! -x "$PROBE_SCRIPT" ]; then
  echo "probe script not executable: $PROBE_SCRIPT" >&2
  exit 2
fi

for v in "$RUNS" "$BATCH_SIZE" "$BATCHES" "$SELECT_ITERS" "$PROBE_SIZE"; do
  if ! [[ "$v" =~ ^[0-9]+$ ]] || [ "$v" -le 0 ]; then
    echo "startup runs/batch_size/batches/select_iters/probe_size must be positive integers" >&2
    exit 2
  fi
done
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ] || [ "$PORT" -ge 65534 ]; then
  echo "startup port must be an integer in range 1025..65533" >&2
  exit 2
fi

extract_ratio() {
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

run_probe_and_extract_ratio() {
  local warmup="$1"
  local port="$2"
  local output status ratio_line
  set +e
  output="$(
    UNNEST_AB_WARMUP_SELECTS="$warmup" \
      "$PROBE_SCRIPT" \
        "$RUNS" \
        "$BATCH_SIZE" \
        "$BATCHES" \
        "$SELECT_ITERS" \
        "$PROBE_SIZE" \
        "$port" \
        "" \
        2>&1
  )"
  status=$?
  set -e
  if [ "$status" -ne 0 ]; then
    printf '%s\n' "$output" >&2
    echo "unnest_ab_startup_sensitivity status=error stage=probe_exit warmup=$warmup exit_code=$status" >&2
    exit "$status"
  fi
  ratio_line="$(printf '%s\n' "$output" | rg -m1 '^ ?ratio_kv\|' || true)"
  if [ -z "$ratio_line" ]; then
    echo "missing ratio_kv line in probe output (warmup=$warmup)" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
  printf '%s\n' "$ratio_line"
}

is_numeric_decimal() {
  local v="$1"
  awk -v x="$v" 'BEGIN { exit (x ~ /^[0-9]+([.][0-9]+)?$/) ? 0 : 1 }'
}

div_ratio() {
  local num="$1"
  local den="$2"
  awk -v n="$num" -v d="$den" 'BEGIN { if (d == 0) exit 1; printf "%.6f\n", n / d }'
}

COLD_LINE="$(run_probe_and_extract_ratio 0 "$PORT")"
WARM_LINE="$(run_probe_and_extract_ratio 1 "$((PORT + 1))")"

INSERT_COLD="$(extract_ratio insert "$COLD_LINE")"
JOIN_COLD="$(extract_ratio join_unnest "$COLD_LINE")"
ANY_COLD="$(extract_ratio any_array "$COLD_LINE")"
INSERT_WARM="$(extract_ratio insert "$WARM_LINE")"
JOIN_WARM="$(extract_ratio join_unnest "$WARM_LINE")"
ANY_WARM="$(extract_ratio any_array "$WARM_LINE")"

for pair in \
  "insert_cold:$INSERT_COLD" \
  "join_cold:$JOIN_COLD" \
  "any_cold:$ANY_COLD" \
  "insert_warm:$INSERT_WARM" \
  "join_warm:$JOIN_WARM" \
  "any_warm:$ANY_WARM"; do
  key="${pair%%:*}"
  val="${pair#*:}"
  if ! is_numeric_decimal "$val"; then
    echo "ratio value is not numeric for $key: $val" >&2
    echo "cold_line: $COLD_LINE" >&2
    echo "warm_line: $WARM_LINE" >&2
    exit 1
  fi
done

INSERT_WARM_OVER_COLD="$(div_ratio "$INSERT_WARM" "$INSERT_COLD")"
JOIN_WARM_OVER_COLD="$(div_ratio "$JOIN_WARM" "$JOIN_COLD")"
ANY_WARM_OVER_COLD="$(div_ratio "$ANY_WARM" "$ANY_COLD")"

echo "unnest_ab_startup_sensitivity status=ok|insert_cold=$INSERT_COLD|insert_warm=$INSERT_WARM|insert_warm_over_cold=$INSERT_WARM_OVER_COLD|join_unnest_cold=$JOIN_COLD|join_unnest_warm=$JOIN_WARM|join_unnest_warm_over_cold=$JOIN_WARM_OVER_COLD|any_array_cold=$ANY_COLD|any_array_warm=$ANY_WARM|any_array_warm_over_cold=$ANY_WARM_OVER_COLD|cold_warmup_selects=0|warm_warmup_selects=1"
