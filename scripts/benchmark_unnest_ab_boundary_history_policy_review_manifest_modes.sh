#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 7 ]; then
  echo "usage: $0 [tmp_root_abs_dir] [summary_files] [iterations] [samples] [timer_mode=auto|ms|seconds] [manifest_mode=safe|trusted] [stale_percent=0..100]" >&2
  exit 2
fi

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
SUMMARY_FILES="${2:-2000}"
ITERATIONS="${3:-10}"
SAMPLES="${4:-1}"
TIMER_MODE="${5:-auto}"
MANIFEST_MODE="${6:-trusted}"
STALE_PERCENT="${7:-0}"
BENCH_DISABLE_MS_BACKEND="${UNNEST_AB_BENCH_DISABLE_MS_BACKEND:-0}"
SCHEMA_VERSION=2

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp_root_abs_dir not found: $TMP_ROOT" >&2
  exit 2
fi
if ! [[ "$SUMMARY_FILES" =~ ^[0-9]+$ ]] || [ "$SUMMARY_FILES" -le 0 ]; then
  echo "summary_files must be a positive integer: $SUMMARY_FILES" >&2
  exit 2
fi
if ! [[ "$ITERATIONS" =~ ^[0-9]+$ ]] || [ "$ITERATIONS" -le 0 ]; then
  echo "iterations must be a positive integer: $ITERATIONS" >&2
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
if [ "$MANIFEST_MODE" != "safe" ] && [ "$MANIFEST_MODE" != "trusted" ]; then
  echo "manifest_mode must be safe or trusted: $MANIFEST_MODE" >&2
  exit 2
fi
if ! [[ "$STALE_PERCENT" =~ ^[0-9]+$ ]]; then
  echo "stale_percent must be an integer between 0 and 100: $STALE_PERCENT" >&2
  exit 2
fi
if [ "$STALE_PERCENT" -lt 0 ] || [ "$STALE_PERCENT" -gt 100 ]; then
  echo "stale_percent must be an integer between 0 and 100: $STALE_PERCENT" >&2
  exit 2
fi
if [ "$BENCH_DISABLE_MS_BACKEND" != "0" ] && [ "$BENCH_DISABLE_MS_BACKEND" != "1" ]; then
  echo "UNNEST_AB_BENCH_DISABLE_MS_BACKEND must be 0 or 1: $BENCH_DISABLE_MS_BACKEND" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REVIEW_SCRIPT="$SCRIPT_DIR/run_unnest_ab_boundary_history_policy_review_window.sh"
MANIFEST_BUILD_SCRIPT="$SCRIPT_DIR/build_unnest_ab_boundary_history_policy_review_manifest.sh"

if [ ! -x "$REVIEW_SCRIPT" ]; then
  echo "review-window script not executable: $REVIEW_SCRIPT" >&2
  exit 2
fi
if [ ! -x "$MANIFEST_BUILD_SCRIPT" ]; then
  echo "manifest build script not executable: $MANIFEST_BUILD_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_policy_review_manifest_mode_bench.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

SUMMARY_DIR="$WORKDIR/summaries"
mkdir -p "$SUMMARY_DIR"

i=0
while [ "$i" -lt "$SUMMARY_FILES" ]; do
  day=$((20 + (i % 9)))
  day_padded="$(printf '%02d' "$day")"
  file_path="$(printf '%s/%06d_2026-02-%s.summary.log' "$SUMMARY_DIR" "$i" "$day_padded")"
  cat >"$file_path" <<EOF
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=2|lift_min48_total=1|lift_min32_rate=0.500000|lift_min48_rate=0.250000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/archive/review_run_$i
EOF
  i=$((i + 1))
done

MANIFEST_FILE="$WORKDIR/review.manifest.tsv"
bash "$MANIFEST_BUILD_SCRIPT" "$SUMMARY_DIR" "$MANIFEST_FILE" >/dev/null

stale_files=$((SUMMARY_FILES * STALE_PERCENT / 100))
if [ "$STALE_PERCENT" -gt 0 ] && [ "$stale_files" -eq 0 ]; then
  stale_files=1
fi
if [ "$stale_files" -gt "$SUMMARY_FILES" ]; then
  stale_files="$SUMMARY_FILES"
fi
if [ "$stale_files" -gt 0 ]; then
  stale_done=0
  while IFS= read -r stale_file; do
    [ -n "$stale_file" ] || continue
    printf '# stale marker %s\n' "$stale_done" >>"$stale_file"
    stale_done=$((stale_done + 1))
    if [ "$stale_done" -ge "$stale_files" ]; then
      break
    fi
  done < <(find "$SUMMARY_DIR" -maxdepth 1 -type f | sort)
fi

USE_MS_TIMER=0
TIMER_BACKEND="seconds"
TIMER_EFFECTIVE_MODE="seconds"

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

run_live_pipeline_once() {
  bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 14 0.25 0.50 0.75 0.75 0.02 0.02 0.05 off >/dev/null
}

run_manifest_pipeline_once() {
  manifest_trusted="off"
  if [ "$MANIFEST_MODE" = "trusted" ]; then
    manifest_trusted="on"
  fi
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED="$manifest_trusted" \
    bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 14 0.25 0.50 0.75 0.75 0.02 0.02 0.05 off >/dev/null
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
    if [ "$mode" = "live" ]; then
      run_live_pipeline_once
    else
      run_manifest_pipeline_once
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
run_live_pipeline_once
run_manifest_pipeline_once

live_duration="$(measure_mode_median_duration live)"
manifest_duration="$(measure_mode_median_duration manifest)"

if [ "$USE_MS_TIMER" -eq 1 ]; then
  live_millis="$live_duration"
  manifest_millis="$manifest_duration"
  live_seconds=$((live_duration / 1000))
  manifest_seconds=$((manifest_duration / 1000))
else
  live_seconds="$live_duration"
  manifest_seconds="$manifest_duration"
  live_millis=$((live_duration * 1000))
  manifest_millis=$((manifest_duration * 1000))
fi

speedup_x="inf"
if [ "$manifest_duration" -gt 0 ]; then
  speedup_bp=$((live_duration * 100 / manifest_duration))
  speedup_x="$(printf '%d.%02d' $((speedup_bp / 100)) $((speedup_bp % 100)))"
fi

echo "unnest_ab_boundary_history_policy_review_manifest_mode_bench|status=ok|mode=pipeline|manifest_mode=$MANIFEST_MODE|summary_files=$SUMMARY_FILES|iterations=$ITERATIONS|samples=$SAMPLES|stale_percent=$STALE_PERCENT|stale_files=$stale_files|timer_mode=$TIMER_MODE|timer_effective=$TIMER_EFFECTIVE_MODE|timer_backend=$TIMER_BACKEND|ms_backend_disabled=$BENCH_DISABLE_MS_BACKEND|live_seconds=$live_seconds|manifest_seconds=$manifest_seconds|live_millis=$live_millis|manifest_millis=$manifest_millis|schema_version=$SCHEMA_VERSION|speedup_x=$speedup_x"
