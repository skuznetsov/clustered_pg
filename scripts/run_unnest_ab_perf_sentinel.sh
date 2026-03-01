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
PROBE_SCRIPT="${UNNEST_SENTINEL_PROBE_SCRIPT:-$SCRIPT_DIR/run_unnest_ab_probe.sh}"

RUNS="${UNNEST_SENTINEL_RUNS:-1}"
BATCH_SIZE="${UNNEST_SENTINEL_BATCH_SIZE:-400}"
BATCHES="${UNNEST_SENTINEL_BATCHES:-20}"
SELECT_ITERS="${UNNEST_SENTINEL_SELECT_ITERS:-60}"
PROBE_SIZE="${UNNEST_SENTINEL_PROBE_SIZE:-64}"
PORT="${UNNEST_SENTINEL_PORT:-65488}"
WARMUP_SELECTS="${UNNEST_SENTINEL_WARMUP_SELECTS:-1}"
ENFORCE_THRESHOLDS="${UNNEST_SENTINEL_ENFORCE_THRESHOLDS:-1}"

MIN_INSERT_RATIO="${UNNEST_SENTINEL_MIN_INSERT_RATIO:-0.90}"
MIN_JOIN_UNNEST_RATIO="${UNNEST_SENTINEL_MIN_JOIN_UNNEST_RATIO:-1.10}"
MIN_ANY_ARRAY_RATIO="${UNNEST_SENTINEL_MIN_ANY_ARRAY_RATIO:-1.00}"

if [ ! -x "$PROBE_SCRIPT" ]; then
  echo "probe script not executable: $PROBE_SCRIPT" >&2
  exit 2
fi

for v in "$RUNS" "$BATCH_SIZE" "$BATCHES" "$SELECT_ITERS" "$PROBE_SIZE"; do
  if ! [[ "$v" =~ ^[0-9]+$ ]] || [ "$v" -le 0 ]; then
    echo "sentinel runs/batch_size/batches/select_iters/probe_size must be positive integers" >&2
    exit 2
  fi
done

if ! [[ "$WARMUP_SELECTS" =~ ^[0-9]+$ ]]; then
  echo "sentinel warmup_selects must be a non-negative integer" >&2
  exit 2
fi

if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ] || [ "$PORT" -ge 65535 ]; then
  echo "sentinel port must be an integer in range 1025..65534" >&2
  exit 2
fi

case "$ENFORCE_THRESHOLDS" in
  1|on|true|yes)
    ENFORCE_THRESHOLDS=1
    ;;
  0|off|false|no)
    ENFORCE_THRESHOLDS=0
    ;;
  *)
    echo "sentinel enforce_thresholds must be boolean (1|0|on|off|true|false|yes|no): $ENFORCE_THRESHOLDS" >&2
    exit 2
    ;;
esac

for ratio in "$MIN_INSERT_RATIO" "$MIN_JOIN_UNNEST_RATIO" "$MIN_ANY_ARRAY_RATIO"; do
  if ! awk -v v="$ratio" 'BEGIN { exit (v ~ /^[0-9]+([.][0-9]+)?$/ && v > 0) ? 0 : 1 }'; then
    echo "sentinel min ratios must be positive decimals: insert=$MIN_INSERT_RATIO join_unnest=$MIN_JOIN_UNNEST_RATIO any_array=$MIN_ANY_ARRAY_RATIO" >&2
    exit 2
  fi
done

set +e
PROBE_OUTPUT="$(
  UNNEST_AB_WARMUP_SELECTS="$WARMUP_SELECTS" \
    "$PROBE_SCRIPT" \
      "$RUNS" \
      "$BATCH_SIZE" \
      "$BATCHES" \
      "$SELECT_ITERS" \
      "$PROBE_SIZE" \
      "$PORT" \
      "" \
      2>&1
)"
PROBE_STATUS=$?
set -e

if [ "$PROBE_STATUS" -ne 0 ]; then
  printf '%s\n' "$PROBE_OUTPUT" >&2
  echo "unnest_ab_perf_sentinel status=error stage=probe_exit exit_code=$PROBE_STATUS" >&2
  exit "$PROBE_STATUS"
fi

RATIO_LINE="$(printf '%s\n' "$PROBE_OUTPUT" | rg -m1 '^ ?ratio_kv\|' || true)"
if [ -z "$RATIO_LINE" ]; then
  echo "missing ratio_kv line in probe output" >&2
  printf '%s\n' "$PROBE_OUTPUT" >&2
  exit 1
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

INSERT_RATIO="$(extract_ratio insert "$RATIO_LINE")"
JOIN_RATIO="$(extract_ratio join_unnest "$RATIO_LINE")"
ANY_RATIO="$(extract_ratio any_array "$RATIO_LINE")"

for pair in "insert:$INSERT_RATIO" "join_unnest:$JOIN_RATIO" "any_array:$ANY_RATIO"; do
  key="${pair%%:*}"
  val="${pair#*:}"
  if ! awk -v v="$val" 'BEGIN { exit (v ~ /^[0-9]+([.][0-9]+)?$/) ? 0 : 1 }'; then
    echo "ratio value is not numeric for $key: $val" >&2
    printf '%s\n' "$RATIO_LINE" >&2
    exit 1
  fi
done

check_ratio_ge() {
  local actual="$1"
  local min_expected="$2"
  awk -v a="$actual" -v m="$min_expected" 'BEGIN { exit (a >= m) ? 0 : 1 }'
}

if [ "$ENFORCE_THRESHOLDS" -eq 1 ]; then
  if ! check_ratio_ge "$INSERT_RATIO" "$MIN_INSERT_RATIO" ||
     ! check_ratio_ge "$JOIN_RATIO" "$MIN_JOIN_UNNEST_RATIO" ||
     ! check_ratio_ge "$ANY_RATIO" "$MIN_ANY_ARRAY_RATIO"; then
    echo "unnest_ab_perf_sentinel status=regression|insert=$INSERT_RATIO|min_insert=$MIN_INSERT_RATIO|join_unnest=$JOIN_RATIO|min_join_unnest=$MIN_JOIN_UNNEST_RATIO|any_array=$ANY_RATIO|min_any_array=$MIN_ANY_ARRAY_RATIO|warmup_selects=$WARMUP_SELECTS|enforce_thresholds=1" >&2
    exit 1
  fi
  echo "unnest_ab_perf_sentinel status=ok|insert=$INSERT_RATIO|min_insert=$MIN_INSERT_RATIO|join_unnest=$JOIN_RATIO|min_join_unnest=$MIN_JOIN_UNNEST_RATIO|any_array=$ANY_RATIO|min_any_array=$MIN_ANY_ARRAY_RATIO|warmup_selects=$WARMUP_SELECTS|enforce_thresholds=1"
  exit 0
fi

echo "unnest_ab_perf_sentinel status=observe|insert=$INSERT_RATIO|min_insert=$MIN_INSERT_RATIO|join_unnest=$JOIN_RATIO|min_join_unnest=$MIN_JOIN_UNNEST_RATIO|any_array=$ANY_RATIO|min_any_array=$MIN_ANY_ARRAY_RATIO|warmup_selects=$WARMUP_SELECTS|enforce_thresholds=0"
