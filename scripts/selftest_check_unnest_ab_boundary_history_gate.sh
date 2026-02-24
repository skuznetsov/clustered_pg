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
GATE_SCRIPT="$SCRIPT_DIR/check_unnest_ab_boundary_history_gate.sh"
if [ ! -x "$GATE_SCRIPT" ]; then
  echo "gate script not executable: $GATE_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_boundary_history_gate_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

SUMMARY_OK="$WORKDIR/history_ok.log"
cat >"$SUMMARY_OK" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=8|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=8|lift_min32_total=7|lift_min48_total=1|lift_min32_rate=0.875000|lift_min48_rate=0.125000
boundary_history|scenario=boundary_56|runs=2|samples_total=8|lift_min32_total=8|lift_min48_total=8|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=8|lift_min32_total=8|lift_min48_total=8|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/tmp/fake
EOF

OUT_OK="$(
  bash "$GATE_SCRIPT" "$SUMMARY_OK" 48 8 0.05 0.25 0.90 0.90
)"
if ! printf '%s\n' "$OUT_OK" | grep -Fq "boundary_history_gate|status=ok|strict_min_observations=48|min_samples_total=8"; then
  echo "expected ok status line in positive gate case" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi

SUMMARY_RATE_FAIL="$WORKDIR/history_rate_fail.log"
cat >"$SUMMARY_RATE_FAIL" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=8|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=8|lift_min32_total=6|lift_min48_total=1|lift_min32_rate=0.750000|lift_min48_rate=0.125000
boundary_history|scenario=boundary_56|runs=2|samples_total=8|lift_min32_total=8|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=0.500000
boundary_history|scenario=pressure_wide|runs=2|samples_total=8|lift_min32_total=8|lift_min48_total=8|lift_min32_rate=1.000000|lift_min48_rate=1.000000
EOF

set +e
OUT_RATE_FAIL="$(
  bash "$GATE_SCRIPT" "$SUMMARY_RATE_FAIL" 48 8 0.05 0.25 0.90 0.90 2>&1
)"
STATUS_RATE_FAIL=$?
set -e
if [ "$STATUS_RATE_FAIL" -eq 0 ]; then
  echo "expected non-zero exit for strict rate threshold violation" >&2
  printf '%s\n' "$OUT_RATE_FAIL" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_RATE_FAIL" | grep -Fq "scenario=boundary_56"; then
  echo "expected boundary_56 violation marker in strict rate negative case" >&2
  printf '%s\n' "$OUT_RATE_FAIL" >&2
  exit 1
fi

SUMMARY_MISSING_SCENARIO="$WORKDIR/history_missing_scenario.log"
cat >"$SUMMARY_MISSING_SCENARIO" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=8|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=8|lift_min32_total=7|lift_min48_total=1|lift_min32_rate=0.875000|lift_min48_rate=0.125000
boundary_history|scenario=pressure_wide|runs=2|samples_total=8|lift_min32_total=8|lift_min48_total=8|lift_min32_rate=1.000000|lift_min48_rate=1.000000
EOF

set +e
OUT_MISSING_SCENARIO="$(
  bash "$GATE_SCRIPT" "$SUMMARY_MISSING_SCENARIO" 48 8 0.05 0.25 0.90 0.90 2>&1
)"
STATUS_MISSING_SCENARIO=$?
set -e
if [ "$STATUS_MISSING_SCENARIO" -eq 0 ]; then
  echo "expected non-zero exit for missing required scenario" >&2
  printf '%s\n' "$OUT_MISSING_SCENARIO" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MISSING_SCENARIO" | grep -Fq "required scenario missing"; then
  echo "expected required-scenario marker in negative case" >&2
  printf '%s\n' "$OUT_MISSING_SCENARIO" >&2
  exit 1
fi

echo "selftest_check_unnest_ab_boundary_history_gate status=ok"
