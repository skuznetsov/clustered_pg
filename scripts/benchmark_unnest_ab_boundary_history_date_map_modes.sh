#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 6 ]; then
  echo "usage: $0 [tmp_root_abs_dir] [map_entries] [iterations] [mode=pipeline|map-only] [samples] [timer_mode=auto|ms|seconds]" >&2
  exit 2
fi

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
MAP_ENTRIES="${2:-20000}"
ITERATIONS="${3:-20}"
BENCH_MODE="${4:-pipeline}"
SAMPLES="${5:-1}"
TIMER_MODE="${6:-auto}"
BENCH_DISABLE_MS_BACKEND="${UNNEST_AB_BENCH_DISABLE_MS_BACKEND:-0}"
SCHEMA_VERSION=1

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp_root_abs_dir not found: $TMP_ROOT" >&2
  exit 2
fi
if ! [[ "$MAP_ENTRIES" =~ ^[0-9]+$ ]]; then
  echo "map_entries must be a non-negative integer: $MAP_ENTRIES" >&2
  exit 2
fi
if ! [[ "$ITERATIONS" =~ ^[0-9]+$ ]] || [ "$ITERATIONS" -le 0 ]; then
  echo "iterations must be a positive integer: $ITERATIONS" >&2
  exit 2
fi
if [ "$BENCH_MODE" != "pipeline" ] && [ "$BENCH_MODE" != "map-only" ]; then
  echo "mode must be pipeline or map-only: $BENCH_MODE" >&2
  exit 2
fi
if ! [[ "$SAMPLES" =~ ^[0-9]+$ ]] || [ "$SAMPLES" -le 0 ]; then
  echo "samples must be a positive integer: $SAMPLES" >&2
  exit 2
fi
if [ "$TIMER_MODE" != "auto" ] && [ "$TIMER_MODE" != "ms" ] && [ "$TIMER_MODE" != "seconds" ]; then
  echo "timer_mode must be auto, ms, or seconds: $TIMER_MODE" >&2
  exit 2
fi
if [ "$BENCH_DISABLE_MS_BACKEND" != "0" ] && [ "$BENCH_DISABLE_MS_BACKEND" != "1" ]; then
  echo "UNNEST_AB_BENCH_DISABLE_MS_BACKEND must be 0 or 1: $BENCH_DISABLE_MS_BACKEND" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REVIEW_SCRIPT="$SCRIPT_DIR/run_unnest_ab_boundary_history_policy_review_window.sh"
INDEX_BUILD_SCRIPT="$SCRIPT_DIR/build_unnest_ab_boundary_history_date_map_index.sh"

if [ ! -x "$REVIEW_SCRIPT" ]; then
  echo "review-window script not executable: $REVIEW_SCRIPT" >&2
  exit 2
fi
if [ ! -x "$INDEX_BUILD_SCRIPT" ]; then
  echo "date-map-index build script not executable: $INDEX_BUILD_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_date_map_mode_bench.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

SUMMARY_DIR="$WORKDIR/summaries"
mkdir -p "$SUMMARY_DIR"

cat >"$SUMMARY_DIR/no_date_target.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/archive/no_date_target_run
EOF

MAP_FILE="$WORKDIR/date_map.csv"
target_slot=$((MAP_ENTRIES / 2))
{
  echo "# key,date"
  i=0
  while [ "$i" -lt "$MAP_ENTRIES" ]; do
    if [ "$i" -eq "$target_slot" ]; then
      echo "no_date_target.summary.log,2026-02-22"
    fi
    printf 'filler_%06d.summary.log,2026-02-21\n' "$i"
    i=$((i + 1))
  done
  if [ "$MAP_ENTRIES" -eq 0 ]; then
    echo "no_date_target.summary.log,2026-02-22"
  fi
} >"$MAP_FILE"

MAP_INDEX_FILE="$WORKDIR/date_map.index.tsv"
bash "$INDEX_BUILD_SCRIPT" "$MAP_FILE" "$MAP_INDEX_FILE" >/dev/null

USE_MS_TIMER=0
TIMER_BACKEND="seconds"
TIMER_EFFECTIVE_MODE="seconds"

normalize_date_key() {
  local raw="$1"
  local compact
  local month
  local day

  if [[ "$raw" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    compact="${raw//-/}"
  elif [[ "$raw" =~ ^[0-9]{8}$ ]]; then
    compact="$raw"
  else
    return 1
  fi
  month=$((10#${compact:4:2}))
  day=$((10#${compact:6:2}))
  if [ "$month" -lt 1 ] || [ "$month" -gt 12 ] || [ "$day" -lt 1 ] || [ "$day" -gt 31 ]; then
    return 1
  fi
  echo "$compact"
}

trim_whitespace() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

parse_csv_lookup_once() {
  local csv_file="$1"
  local wanted_key="$2"
  local line
  local line_no
  local key_raw
  local date_raw
  local key
  local date_key
  local sorted_key
  local sorted_date
  local prev_key
  local prev_date
  local found_date
  local -a parsed_entries=()

  line_no=0
  while IFS= read -r line || [ -n "$line" ]; do
    line_no=$((line_no + 1))
    line="$(trim_whitespace "$line")"
    [ -z "$line" ] && continue
    [[ "$line" == \#* ]] && continue
    if [[ "$line" != *,* ]]; then
      return 1
    fi
    key_raw="${line%%,*}"
    date_raw="${line#*,}"
    key="$(trim_whitespace "$key_raw")"
    date_raw="$(trim_whitespace "$date_raw")"
    if [ -z "$key" ] || [ -z "$date_raw" ]; then
      return 1
    fi
    date_key="$(normalize_date_key "$date_raw" || true)"
    if [ -z "$date_key" ]; then
      return 1
    fi
    parsed_entries+=("${key}"$'\t'"${date_key}")
  done < "$csv_file"

  prev_key=""
  prev_date=""
  found_date=""
  while IFS=$'\t' read -r sorted_key sorted_date; do
    [ -n "$sorted_key" ] || continue
    [ -n "$sorted_date" ] || continue
    if [ "$sorted_key" = "$prev_key" ]; then
      if [ "$sorted_date" != "$prev_date" ]; then
        return 1
      fi
      continue
    fi
    if [ "$sorted_key" = "$wanted_key" ]; then
      found_date="$sorted_date"
    fi
    prev_key="$sorted_key"
    prev_date="$sorted_date"
  done < <(printf '%s\n' "${parsed_entries[@]}" | LC_ALL=C sort -t $'\t' -k1,1 -k2,2)

  [ -n "$found_date" ]
}

parse_index_lookup_once() {
  local index_file="$1"
  local wanted_key="$2"
  local line
  local key_raw
  local date_raw
  local key
  local date_key
  local prev_key
  local prev_date
  local found_date

  prev_key=""
  prev_date=""
  found_date=""
  while IFS= read -r line || [ -n "$line" ]; do
    line="$(trim_whitespace "$line")"
    [ -z "$line" ] && continue
    [[ "$line" == \#* ]] && continue
    if [[ "$line" != *$'\t'* ]]; then
      return 1
    fi
    key_raw="${line%%$'\t'*}"
    date_raw="${line#*$'\t'}"
    key="$(trim_whitespace "$key_raw")"
    date_raw="$(trim_whitespace "$date_raw")"
    if [ -z "$key" ] || [ -z "$date_raw" ]; then
      return 1
    fi
    date_key="$(normalize_date_key "$date_raw" || true)"
    if [ -z "$date_key" ]; then
      return 1
    fi
    if [ -n "$prev_key" ] && [[ "$key" < "$prev_key" ]]; then
      return 1
    fi
    if [ "$key" = "$prev_key" ]; then
      if [ "$date_key" != "$prev_date" ]; then
        return 1
      fi
      continue
    fi
    if [ "$key" = "$wanted_key" ]; then
      found_date="$date_key"
    fi
    prev_key="$key"
    prev_date="$date_key"
  done < "$index_file"

  [ -n "$found_date" ]
}

run_csv_pipeline_once() {
  UNNEST_AB_POLICY_REVIEW_DATE_MAP="$MAP_FILE" \
    bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2026-02-22 2026-02-22 >/dev/null
}

run_index_pipeline_once() {
  UNNEST_AB_POLICY_REVIEW_DATE_MAP_INDEX="$MAP_INDEX_FILE" \
    bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2026-02-22 2026-02-22 >/dev/null
}

run_csv_map_only_once() {
  parse_csv_lookup_once "$MAP_FILE" "no_date_target.summary.log"
}

run_index_map_only_once() {
  parse_index_lookup_once "$MAP_INDEX_FILE" "no_date_target.summary.log"
}

init_timer_backend() {
  if [ "$TIMER_MODE" = "seconds" ]; then
    USE_MS_TIMER=0
    TIMER_BACKEND="seconds"
    TIMER_EFFECTIVE_MODE="seconds"
    return 0
  fi
  if [ "$BENCH_DISABLE_MS_BACKEND" = "1" ]; then
    USE_MS_TIMER=0
    TIMER_BACKEND="forced_seconds_fallback"
    TIMER_EFFECTIVE_MODE="seconds"
    return 0
  fi

  if command -v python3 >/dev/null 2>&1; then
    USE_MS_TIMER=1
    TIMER_BACKEND="python3"
    TIMER_EFFECTIVE_MODE="ms"
    return 0
  fi
  if command -v perl >/dev/null 2>&1; then
    USE_MS_TIMER=1
    TIMER_BACKEND="perl"
    TIMER_EFFECTIVE_MODE="ms"
    return 0
  fi

  USE_MS_TIMER=0
  TIMER_BACKEND="seconds_fallback"
  TIMER_EFFECTIVE_MODE="seconds"
}

timestamp_ms_now() {
  if [ "$TIMER_BACKEND" = "python3" ]; then
    python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
    return 0
  fi
  if [ "$TIMER_BACKEND" = "perl" ]; then
    perl -MTime::HiRes=time -e 'print int(time() * 1000), "\n"'
    return 0
  fi
  return 1
}

measure_mode_duration() {
  local mode="$1"
  local iter=0
  local start_ms=0
  local end_ms=0

  if [ "$USE_MS_TIMER" -eq 1 ]; then
    start_ms="$(timestamp_ms_now)"
  else
    SECONDS=0
  fi
  while [ "$iter" -lt "$ITERATIONS" ]; do
    if [ "$mode" = "csv" ]; then
      if [ "$BENCH_MODE" = "pipeline" ]; then
        run_csv_pipeline_once
      else
        run_csv_map_only_once
      fi
    else
      if [ "$BENCH_MODE" = "pipeline" ]; then
        run_index_pipeline_once
      else
        run_index_map_only_once
      fi
    fi
    iter=$((iter + 1))
  done
  if [ "$USE_MS_TIMER" -eq 1 ]; then
    end_ms="$(timestamp_ms_now)"
    echo $((end_ms - start_ms))
  else
    echo "$SECONDS"
  fi
}

measure_mode_median_duration() {
  local mode="$1"
  local sample=0
  local run_duration
  local median_duration
  local -a values=()

  while [ "$sample" -lt "$SAMPLES" ]; do
    run_duration="$(measure_mode_duration "$mode")"
    values+=("$run_duration")
    sample=$((sample + 1))
  done

  median_duration="$(
    printf '%s\n' "${values[@]}" | LC_ALL=C sort -n | awk -v n="${#values[@]}" '
      {
        a[NR] = $1
      }
      END {
        if (n % 2 == 1) {
          print a[(n + 1) / 2]
        } else {
          mid = n / 2
          print int((a[mid] + a[mid + 1]) / 2)
        }
      }
    '
  )"
  echo "$median_duration"
}

# Warm-up
init_timer_backend
if [ "$BENCH_MODE" = "pipeline" ]; then
  run_csv_pipeline_once
  run_index_pipeline_once
else
  run_csv_map_only_once
  run_index_map_only_once
fi

csv_duration="$(measure_mode_median_duration csv)"
index_duration="$(measure_mode_median_duration index)"

if [ "$USE_MS_TIMER" -eq 1 ]; then
  csv_millis="$csv_duration"
  index_millis="$index_duration"
  csv_seconds=$((csv_duration / 1000))
  index_seconds=$((index_duration / 1000))
else
  csv_seconds="$csv_duration"
  index_seconds="$index_duration"
  csv_millis=$((csv_duration * 1000))
  index_millis=$((index_duration * 1000))
fi

speedup_x="inf"
if [ "$index_duration" -gt 0 ]; then
  speedup_bp=$((csv_duration * 10000 / index_duration))
  speedup_x="$(printf '%d.%02d' $((speedup_bp / 100)) $((speedup_bp % 100)))"
fi

echo "unnest_ab_boundary_history_date_map_mode_bench|status=ok|mode=$BENCH_MODE|map_entries=$MAP_ENTRIES|iterations=$ITERATIONS|samples=$SAMPLES|timer_mode=$TIMER_MODE|timer_effective=$TIMER_EFFECTIVE_MODE|timer_backend=$TIMER_BACKEND|ms_backend_disabled=$BENCH_DISABLE_MS_BACKEND|csv_seconds=$csv_seconds|index_seconds=$index_seconds|csv_millis=$csv_millis|index_millis=$index_millis|schema_version=$SCHEMA_VERSION|speedup_x=$speedup_x"
