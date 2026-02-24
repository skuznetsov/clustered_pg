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
DERIVE_SCRIPT="$SCRIPT_DIR/derive_unnest_ab_boundary_history_gate_thresholds.sh"
if [ ! -x "$DERIVE_SCRIPT" ]; then
  echo "derive script not executable: $DERIVE_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_boundary_history_thresholds_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

SUMMARY_OK="$WORKDIR/history_ok.log"
cat >"$SUMMARY_OK" <<'EOF'
boundary_history|scenario=balanced_wide|runs=3|samples_total=12|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.010000
boundary_history|scenario=boundary_40|runs=3|samples_total=12|lift_min32_total=8|lift_min48_total=2|lift_min32_rate=0.666667|lift_min48_rate=0.200000
boundary_history|scenario=boundary_56|runs=3|samples_total=12|lift_min32_total=12|lift_min48_total=11|lift_min32_rate=1.000000|lift_min48_rate=0.950000
boundary_history|scenario=pressure_wide|runs=3|samples_total=12|lift_min32_total=12|lift_min48_total=12|lift_min32_rate=1.000000|lift_min48_rate=0.980000
boundary_history_status|status=ok|runs=3|scenarios=4|strict_min_observations=48|input=/tmp/fake
EOF

OUT_OK="$(
  bash "$DERIVE_SCRIPT" "$SUMMARY_OK" 48 0.02 0.02
)"
EXPECTED="boundary_history_gate_thresholds|strict_min_observations=48|min_samples_total=12|balanced_max_strict_rate=0.030000|boundary40_max_strict_rate=0.220000|boundary56_min_strict_rate=0.930000|pressure_min_strict_rate=0.960000|max_headroom=0.02|min_floor_margin=0.02"
if ! printf '%s\n' "$OUT_OK" | grep -Fq "$EXPECTED"; then
  echo "expected exact threshold derivation output in positive case" >&2
  printf 'expected: %s\n' "$EXPECTED" >&2
  printf 'actual:   %s\n' "$OUT_OK" >&2
  exit 1
fi

SUMMARY_MISSING="$WORKDIR/history_missing.log"
cat >"$SUMMARY_MISSING" <<'EOF'
boundary_history|scenario=balanced_wide|runs=3|samples_total=12|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=3|samples_total=12|lift_min32_total=8|lift_min48_total=2|lift_min32_rate=0.666667|lift_min48_rate=0.200000
boundary_history|scenario=pressure_wide|runs=3|samples_total=12|lift_min32_total=12|lift_min48_total=12|lift_min32_rate=1.000000|lift_min48_rate=0.980000
EOF

set +e
OUT_MISSING="$(
  bash "$DERIVE_SCRIPT" "$SUMMARY_MISSING" 48 0.02 0.02 2>&1
)"
STATUS_MISSING=$?
set -e
if [ "$STATUS_MISSING" -eq 0 ]; then
  echo "expected non-zero exit for missing required scenario" >&2
  printf '%s\n' "$OUT_MISSING" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MISSING" | grep -Fq "required scenario missing"; then
  echo "expected required-scenario marker in negative case" >&2
  printf '%s\n' "$OUT_MISSING" >&2
  exit 1
fi

echo "selftest_derive_unnest_ab_boundary_history_gate_thresholds status=ok"
