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
SUMMARY_SCRIPT="$SCRIPT_DIR/summarize_unnest_ab_boundary_history.sh"
if [ ! -x "$SUMMARY_SCRIPT" ]; then
  echo "summary script not executable: $SUMMARY_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_boundary_history_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

LOG_DIR_OK="$WORKDIR/logs_ok"
mkdir -p "$LOG_DIR_OK"

cat >"$LOG_DIR_OK/run1.log" <<'EOF'
nightly_boundary_summary|scenario=balanced_wide|samples=4|lift_min32=0|lift_min48=0
nightly_boundary_summary|scenario=boundary_40|samples=4|lift_min32=4|lift_min48=0
nightly_boundary_summary|scenario=boundary_56|samples=4|lift_min32=4|lift_min48=4
nightly_boundary_summary|scenario=pressure_wide|samples=4|lift_min32=4|lift_min48=4
EOF

cat >"$LOG_DIR_OK/run2.log" <<'EOF'
nightly_boundary_summary|scenario=balanced_wide|samples=4|lift_min32=0|lift_min48=0
nightly_boundary_summary|scenario=boundary_40|samples=4|lift_min32=3|lift_min48=0
nightly_boundary_summary|scenario=boundary_56|samples=4|lift_min32=4|lift_min48=4
nightly_boundary_summary|scenario=pressure_wide|samples=4|lift_min32=4|lift_min48=4
EOF

OUT_OK="$(
  bash "$SUMMARY_SCRIPT" "$LOG_DIR_OK" 48
)"
if ! printf '%s\n' "$OUT_OK" | grep -Fq "boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48"; then
  echo "expected ok status line with runs/scenarios/strict_min_observations in positive case" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "boundary_history|scenario=boundary_40|runs=2|samples_total=8|lift_min32_total=7|lift_min48_total=0|lift_min32_rate=0.875000|lift_min48_rate=0.000000"; then
  echo "expected boundary_40 aggregate line in positive case" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi

LOG_DIR_MISMATCH="$WORKDIR/logs_mismatch"
mkdir -p "$LOG_DIR_MISMATCH"
cp "$LOG_DIR_OK/run1.log" "$LOG_DIR_MISMATCH/"
cat >"$LOG_DIR_MISMATCH/run_bad.log" <<'EOF'
nightly_boundary_summary|scenario=balanced_wide|samples=4|lift_min32=0|lift_min48=0
nightly_boundary_summary|scenario=boundary_56|samples=4|lift_min32=4|lift_min48=4
nightly_boundary_summary|scenario=pressure_wide|samples=4|lift_min32=4|lift_min48=4
EOF

set +e
OUT_MISMATCH="$(
  bash "$SUMMARY_SCRIPT" "$LOG_DIR_MISMATCH" 48 2>&1
)"
STATUS_MISMATCH=$?
set -e
if [ "$STATUS_MISMATCH" -eq 0 ]; then
  echo "expected non-zero exit for scenario-set mismatch case" >&2
  printf '%s\n' "$OUT_MISMATCH" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MISMATCH" | grep -Fq "scenario set mismatch"; then
  echo "expected scenario-set mismatch marker in negative case" >&2
  printf '%s\n' "$OUT_MISMATCH" >&2
  exit 1
fi

set +e
OUT_STRICT_MISSING="$(
  bash "$SUMMARY_SCRIPT" "$LOG_DIR_OK" 64 2>&1
)"
STATUS_STRICT_MISSING=$?
set -e
if [ "$STATUS_STRICT_MISSING" -eq 0 ]; then
  echo "expected non-zero exit when strict lift field is missing" >&2
  printf '%s\n' "$OUT_STRICT_MISSING" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_STRICT_MISSING" | grep -Fq "missing required field lift_min64"; then
  echo "expected strict-field missing marker in negative case" >&2
  printf '%s\n' "$OUT_STRICT_MISSING" >&2
  exit 1
fi

echo "selftest_summarize_unnest_ab_boundary_history status=ok"
