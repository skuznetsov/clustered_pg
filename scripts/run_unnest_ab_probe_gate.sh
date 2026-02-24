#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 10 ] || [ "$#" -gt 11 ]; then
  echo "usage: $0 <ref_runs> <new_runs> <batch_size> <batches> <select_iters> <probe_size> <base_port> <out_root_abs_dir> <min_fraction> <stat_mode> [min_samples]" >&2
  echo "optional env: UNNEST_GATE_EXISTING_REF_DIR=<abs_dir_with_clustered_pg_unnest_ab_logs> UNNEST_GATE_KEEP_NEW_DIR=<on|off> UNNEST_GATE_ALLOW_OPTIMISTIC_TAIL=<on|off>" >&2
  exit 2
fi

REF_RUNS="$1"
NEW_RUNS="$2"
BATCH_SIZE="$3"
BATCHES="$4"
SELECT_ITERS="$5"
PROBE_SIZE="$6"
BASE_PORT="$7"
OUT_ROOT="$8"
MIN_FRACTION="$9"
STAT_MODE="${10}"
MIN_SAMPLES="${11:-}"
EXISTING_REF_DIR="${UNNEST_GATE_EXISTING_REF_DIR:-}"
KEEP_NEW_DIR="${UNNEST_GATE_KEEP_NEW_DIR:-off}"
ALLOW_OPTIMISTIC_TAIL="${UNNEST_GATE_ALLOW_OPTIMISTIC_TAIL:-off}"

for v in "$NEW_RUNS" "$BATCH_SIZE" "$BATCHES" "$SELECT_ITERS" "$PROBE_SIZE"; do
  if ! [[ "$v" =~ ^[0-9]+$ ]] || [ "$v" -le 0 ]; then
    echo "new_runs/batch_size/batches/select_iters/probe_size must be positive integers" >&2
    exit 2
  fi
done

if ! [[ "$REF_RUNS" =~ ^[0-9]+$ ]]; then
  echo "ref_runs must be a non-negative integer" >&2
  exit 2
fi

if [ -z "$EXISTING_REF_DIR" ] && [ "$REF_RUNS" -le 0 ]; then
  echo "ref_runs must be > 0 when UNNEST_GATE_EXISTING_REF_DIR is not set" >&2
  exit 2
fi

if [ -n "$EXISTING_REF_DIR" ]; then
  if [[ "$EXISTING_REF_DIR" != /* ]]; then
    echo "UNNEST_GATE_EXISTING_REF_DIR must be absolute: $EXISTING_REF_DIR" >&2
    exit 2
  fi
  if [ ! -d "$EXISTING_REF_DIR" ]; then
    echo "UNNEST_GATE_EXISTING_REF_DIR not found: $EXISTING_REF_DIR" >&2
    exit 2
  fi
fi

if ! [[ "$BASE_PORT" =~ ^[0-9]+$ ]]; then
  echo "base_port must be numeric" >&2
  exit 2
fi
if [ "$BASE_PORT" -le 1024 ] || [ "$BASE_PORT" -ge 65535 ]; then
  echo "base_port must be in range 1025..65534" >&2
  exit 2
fi

if [ "$REF_RUNS" -gt 0 ]; then
  ref_max_port=$((BASE_PORT + REF_RUNS - 1))
else
  ref_max_port=$BASE_PORT
fi
new_max_port=$((BASE_PORT + 100 + NEW_RUNS - 1))
max_port="$new_max_port"
if [ "$ref_max_port" -gt "$max_port" ]; then
  max_port="$ref_max_port"
fi
if [ "$max_port" -ge 65535 ]; then
  echo "base_port too high for run counts: ref_max_port=$ref_max_port new_max_port=$new_max_port max_derived_port=$max_port" >&2
  exit 2
fi

if [[ "$OUT_ROOT" != /* ]]; then
  echo "out_root_abs_dir must be absolute: $OUT_ROOT" >&2
  exit 2
fi
if [ ! -d "$OUT_ROOT" ]; then
  echo "out_root_abs_dir not found: $OUT_ROOT" >&2
  exit 2
fi

case "$KEEP_NEW_DIR" in
  on|off|true|false|1|0) ;;
  *)
    echo "UNNEST_GATE_KEEP_NEW_DIR must be one of: on/off/true/false/1/0" >&2
    exit 2
    ;;
esac

case "$ALLOW_OPTIMISTIC_TAIL" in
  on|off|true|false|1|0) ;;
  *)
    echo "UNNEST_GATE_ALLOW_OPTIMISTIC_TAIL must be one of: on/off/true/false/1/0" >&2
    exit 2
    ;;
esac

if ! [[ "$MIN_FRACTION" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  echo "min_fraction must be numeric: $MIN_FRACTION" >&2
  exit 2
fi
if ! awk -v v="$MIN_FRACTION" 'BEGIN { exit (v > 0 && v <= 1.0) ? 0 : 1 }'; then
  echo "min_fraction must be in range (0, 1]: $MIN_FRACTION" >&2
  exit 2
fi

case "$STAT_MODE" in
  median|p05|p95|trimmed-mean) ;;
  *)
    echo "stat_mode must be one of: median|p05|p95|trimmed-mean" >&2
    exit 2
    ;;
esac

if [ "$STAT_MODE" = "p95" ]; then
  case "$ALLOW_OPTIMISTIC_TAIL" in
    on|true|1) ;;
    *)
      echo "stat_mode 'p95' is optimistic for throughput ratios; use median|p05|trimmed-mean or set UNNEST_GATE_ALLOW_OPTIMISTIC_TAIL=on to override" >&2
      exit 2
      ;;
  esac
fi

if [ -n "$MIN_SAMPLES" ]; then
  if ! [[ "$MIN_SAMPLES" =~ ^[0-9]+$ ]] || [ "$MIN_SAMPLES" -le 0 ]; then
    echo "min_samples must be a positive integer when provided" >&2
    exit 2
  fi
fi

if [ "$STAT_MODE" != "median" ] && [ -n "$MIN_SAMPLES" ] && [ "$MIN_SAMPLES" -lt 3 ]; then
  echo "min_samples must be >= 3 for stat_mode '$STAT_MODE'" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROBE_SCRIPT="${UNNEST_GATE_PROBE_SCRIPT:-$SCRIPT_DIR/run_unnest_ab_probe.sh}"
COMPARE_SCRIPT="${UNNEST_GATE_COMPARE_SCRIPT:-$SCRIPT_DIR/compare_unnest_ab_logsets.sh}"

if [ ! -x "$PROBE_SCRIPT" ]; then
  echo "probe script not executable: $PROBE_SCRIPT" >&2
  exit 2
fi
if [ ! -x "$COMPARE_SCRIPT" ]; then
  echo "compare script not executable: $COMPARE_SCRIPT" >&2
  exit 2
fi

NEW_DIR="$(mktemp -d "$OUT_ROOT/clustered_pg_unnest_ab_new_set.XXXXXX")"
REF_SOURCE="generated"
REF_RETAINED=1

run_set() {
  local runs="$1"
  local start_port="$2"
  local out_dir="$3"
  local i
  local port

  i=1
  while [ "$i" -le "$runs" ]; do
    port=$((start_port + i - 1))
    "$PROBE_SCRIPT" \
      1 \
      "$BATCH_SIZE" \
      "$BATCHES" \
      "$SELECT_ITERS" \
      "$PROBE_SIZE" \
      "$port" \
      "auto:$out_dir"
    i=$((i + 1))
  done
}

if [ -n "$EXISTING_REF_DIR" ]; then
  REF_DIR="$EXISTING_REF_DIR"
  REF_SOURCE="existing"
else
  REF_DIR="$(mktemp -d "$OUT_ROOT/clustered_pg_unnest_ab_ref_set.XXXXXX")"
  run_set "$REF_RUNS" "$BASE_PORT" "$REF_DIR"
fi

run_set "$NEW_RUNS" "$((BASE_PORT + 100))" "$NEW_DIR"

maybe_cleanup_generated_ref() {
  if [ "$REF_SOURCE" != "generated" ]; then
    REF_RETAINED=1
    return
  fi

  case "$KEEP_NEW_DIR" in
    on|true|1)
      if [ -d "$REF_DIR" ]; then
        rm -rf "$REF_DIR"
      fi
      REF_RETAINED=0
      ;;
    *)
      REF_RETAINED=1
      ;;
  esac
}

compare_output=""
if [ -n "$MIN_SAMPLES" ]; then
  if ! compare_output="$("$COMPARE_SCRIPT" "$REF_DIR" "$NEW_DIR" "$MIN_FRACTION" "$STAT_MODE" "$MIN_SAMPLES" 2>&1)"; then
    echo "$compare_output" >&2
    maybe_cleanup_generated_ref
    echo "unnest_ab_probe_gate_output|reference_dir=$REF_DIR|candidate_dir=$NEW_DIR|reference_source=$REF_SOURCE|reference_retained=$REF_RETAINED" >&2
    echo "unnest_ab_probe_gate_status=regression" >&2
    exit 1
  fi
else
  if ! compare_output="$("$COMPARE_SCRIPT" "$REF_DIR" "$NEW_DIR" "$MIN_FRACTION" "$STAT_MODE" 2>&1)"; then
    echo "$compare_output" >&2
    maybe_cleanup_generated_ref
    echo "unnest_ab_probe_gate_output|reference_dir=$REF_DIR|candidate_dir=$NEW_DIR|reference_source=$REF_SOURCE|reference_retained=$REF_RETAINED" >&2
    echo "unnest_ab_probe_gate_status=regression" >&2
    exit 1
  fi
fi

maybe_cleanup_generated_ref
echo "$compare_output"
echo "unnest_ab_probe_gate_output|reference_dir=$REF_DIR|candidate_dir=$NEW_DIR|reference_source=$REF_SOURCE|reference_retained=$REF_RETAINED"
echo "unnest_ab_probe_gate_status=ok"
