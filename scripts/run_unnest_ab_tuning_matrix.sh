#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUNS="${1:-1}"
BATCH_SIZE="${2:-400}"
BATCHES="${3:-20}"
SELECT_ITERS="${4:-60}"
PROBE_SIZE="${5:-64}"
BASE_PORT="${6:-65520}"
OUT_PATH="${7:-auto:${TMPDIR:-/tmp}}"
TRIGGER_LIST="${8:-1,2,4}"
MIN_DISTINCT_LIST="${9:-2,4,8}"
MAX_TIDS_LIST="${10:-65536,262144}"
PROBE_SCRIPT="${UNNEST_TUNE_PROBE_SCRIPT:-$ROOT_DIR/scripts/run_unnest_ab_probe.sh}"
PROBE_OUT_ROOT="${UNNEST_TUNE_PROBE_OUT_ROOT:-${TMPDIR:-/tmp}}"

for v in "$RUNS" "$BATCH_SIZE" "$BATCHES" "$SELECT_ITERS" "$PROBE_SIZE" "$BASE_PORT"; do
  if ! [[ "$v" =~ ^[0-9]+$ ]] || [ "$v" -le 0 ]; then
    echo "runs/batch_size/batches/select_iters/probe_size/base_port must be positive integers" >&2
    exit 2
  fi
done

if [ "$BASE_PORT" -le 1024 ] || [ "$BASE_PORT" -ge 65535 ]; then
  echo "base_port must be in range 1025..65534" >&2
  exit 2
fi

if [ ! -x "$PROBE_SCRIPT" ]; then
  echo "probe script not executable: $PROBE_SCRIPT" >&2
  exit 2
fi

if [[ "$PROBE_OUT_ROOT" != /* ]]; then
  echo "probe_out_root must be an absolute path: $PROBE_OUT_ROOT" >&2
  exit 2
fi
if [ ! -d "$PROBE_OUT_ROOT" ]; then
  echo "probe_out_root not found: $PROBE_OUT_ROOT" >&2
  exit 2
fi

IFS=',' read -r -a TRIGGERS <<< "$TRIGGER_LIST"
IFS=',' read -r -a MIN_DISTINCTS <<< "$MIN_DISTINCT_LIST"
IFS=',' read -r -a MAX_TIDS <<< "$MAX_TIDS_LIST"

if [ "${#TRIGGERS[@]}" -eq 0 ] || [ "${#MIN_DISTINCTS[@]}" -eq 0 ] || [ "${#MAX_TIDS[@]}" -eq 0 ]; then
  echo "trigger/min_distinct/max_tids lists must be non-empty CSVs" >&2
  exit 2
fi

for v in "${TRIGGERS[@]}" "${MIN_DISTINCTS[@]}" "${MAX_TIDS[@]}"; do
  if ! [[ "$v" =~ ^[0-9]+$ ]] || [ "$v" -le 0 ]; then
    echo "trigger/min_distinct/max_tids entries must be positive integers" >&2
    exit 2
  fi
done

CASE_COUNT=$(( ${#TRIGGERS[@]} * ${#MIN_DISTINCTS[@]} * ${#MAX_TIDS[@]} ))
if [ "$CASE_COUNT" -le 0 ]; then
  echo "derived tuning matrix case_count must be > 0, got: $CASE_COUNT" >&2
  exit 2
fi
DERIVED_MAX_PORT=$((BASE_PORT + CASE_COUNT - 1))
if [ "$DERIVED_MAX_PORT" -ge 65535 ]; then
  MAX_SAFE_BASE_PORT=$((65534 - CASE_COUNT + 1))
  echo "base_port too high for tuning matrix case-count: base_port=$BASE_PORT case_count=$CASE_COUNT derived_max_port=$DERIVED_MAX_PORT max_safe_base_port=$MAX_SAFE_BASE_PORT" >&2
  exit 2
fi

if [ "$OUT_PATH" = "auto" ]; then
  OUT_PATH="${TMPDIR:-/tmp}/pg_sorted_heap_unnest_tuning_matrix_$(date +%Y%m%d_%H%M%S)_$$.log"
elif [[ "$OUT_PATH" == auto:* ]]; then
  OUT_DIR="${OUT_PATH#auto:}"
  if [ -z "$OUT_DIR" ]; then
    echo "auto output directory must not be empty" >&2
    exit 2
  fi
  if [[ "$OUT_DIR" != /* ]]; then
    echo "auto output directory must be an absolute path" >&2
    exit 2
  fi
  OUT_PATH="$OUT_DIR/pg_sorted_heap_unnest_tuning_matrix_$(date +%Y%m%d_%H%M%S)_$$.log"
fi

if [ -n "$OUT_PATH" ]; then
  mkdir -p "$(dirname "$OUT_PATH")"
fi

case_index=0
best_join="0"
best_case=""

if [ -n "$OUT_PATH" ]; then
  : > "$OUT_PATH"
fi

echo "matrix_begin|runs=$RUNS|batch_size=$BATCH_SIZE|batches=$BATCHES|select_iters=$SELECT_ITERS|probe_size=$PROBE_SIZE|base_port=$BASE_PORT|case_count=$CASE_COUNT|derived_max_port=$DERIVED_MAX_PORT|probe_out_root=$PROBE_OUT_ROOT"
printf "trigger|min_distinct|max_tids|insert_ratio|join_unnest_ratio|any_array_ratio\n"

for trigger in "${TRIGGERS[@]}"; do
  for min_distinct in "${MIN_DISTINCTS[@]}"; do
    for max_tids in "${MAX_TIDS[@]}"; do
      port=$((BASE_PORT + case_index))
      if [ "$port" -ge 65535 ]; then
        echo "computed port out of range: $port (base_port=$BASE_PORT case_index=$case_index)" >&2
        exit 2
      fi

      case_output="$("$PROBE_SCRIPT" \
        "$RUNS" "$BATCH_SIZE" "$BATCHES" "$SELECT_ITERS" "$PROBE_SIZE" "$port" \
        "auto:$PROBE_OUT_ROOT" "$trigger" "$min_distinct" "$max_tids" 2>&1)"

      if [ -n "$OUT_PATH" ]; then
        {
          echo "=== case trigger=$trigger min_distinct=$min_distinct max_tids=$max_tids port=$port ==="
          printf '%s\n' "$case_output"
        } >> "$OUT_PATH"
      fi

      ratio_line="$(printf '%s\n' "$case_output" | rg -m1 'ratio_kv\|' || true)"
      if [ -z "$ratio_line" ]; then
        echo "missing ratio_kv line for case trigger=$trigger min_distinct=$min_distinct max_tids=$max_tids" >&2
        exit 1
      fi
      ratio_line="$(printf '%s\n' "$ratio_line" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')"

      insert_ratio="$(printf '%s\n' "$ratio_line" | sed -E 's/.*insert=([0-9.]+).*/\1/')"
      join_ratio="$(printf '%s\n' "$ratio_line" | sed -E 's/.*join_unnest=([0-9.]+).*/\1/')"
      any_ratio="$(printf '%s\n' "$ratio_line" | sed -E 's/.*any_array=([0-9.]+).*/\1/')"

      printf "%s|%s|%s|%s|%s|%s\n" "$trigger" "$min_distinct" "$max_tids" "$insert_ratio" "$join_ratio" "$any_ratio"

      if awk "BEGIN { exit !($join_ratio > $best_join) }"; then
        best_join="$join_ratio"
        best_case="trigger=$trigger|min_distinct=$min_distinct|max_tids=$max_tids"
      fi

      case_index=$((case_index + 1))
    done
  done
done

echo "matrix_best_join_unnest|$best_case|join_unnest_ratio=$best_join"
if [ -n "$OUT_PATH" ]; then
  echo "unnest_tuning_matrix_output: $OUT_PATH"
fi
