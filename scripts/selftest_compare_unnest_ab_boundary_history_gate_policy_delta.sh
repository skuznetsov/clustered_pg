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
COMPARE_SCRIPT="$SCRIPT_DIR/compare_unnest_ab_boundary_history_gate_policy_delta.sh"
if [ ! -x "$COMPARE_SCRIPT" ]; then
  echo "compare script not executable: $COMPARE_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_boundary_policy_delta_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

SUMMARY="$WORKDIR/history_summary.log"
cat >"$SUMMARY" <<'EOF'
boundary_history|scenario=balanced_wide|runs=3|samples_total=12|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.010000
boundary_history|scenario=boundary_40|runs=3|samples_total=12|lift_min32_total=8|lift_min48_total=2|lift_min32_rate=0.666667|lift_min48_rate=0.200000
boundary_history|scenario=boundary_56|runs=3|samples_total=12|lift_min32_total=12|lift_min48_total=11|lift_min32_rate=1.000000|lift_min48_rate=0.950000
boundary_history|scenario=pressure_wide|runs=3|samples_total=12|lift_min32_total=12|lift_min48_total=12|lift_min32_rate=1.000000|lift_min48_rate=0.980000
boundary_history_status|status=ok|runs=3|scenarios=4|strict_min_observations=48|input=/tmp/fake
EOF

# Derived recommendations with headroom/floor 0.02:
# balanced_max=0.030000, boundary40_max=0.220000, boundary56_min=0.930000, pressure_min=0.960000
OUT_ALIGNED="$(
  bash "$COMPARE_SCRIPT" "$SUMMARY" 48 0.03 0.22 0.93 0.96 0.02 0.02 0.01 off
)"
if ! printf '%s\n' "$OUT_ALIGNED" | grep -Fq "boundary_history_policy_delta_status|status=aligned|strict_min_observations=48|min_samples_total_recommended=12"; then
  echo "expected aligned status in aligned case" >&2
  printf '%s\n' "$OUT_ALIGNED" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_ALIGNED" | grep -Fq "looser_than_recommended=0|stricter_than_recommended=0"; then
  echo "expected zero looser/stricter counts in aligned case" >&2
  printf '%s\n' "$OUT_ALIGNED" >&2
  exit 1
fi

OUT_REVIEW="$(
  bash "$COMPARE_SCRIPT" "$SUMMARY" 48 0.25 0.50 0.75 0.75 0.02 0.02 0.01 off
)"
if ! printf '%s\n' "$OUT_REVIEW" | grep -Fq "boundary_history_policy_delta_status|status=review|strict_min_observations=48"; then
  echo "expected review status in non-aligned case" >&2
  printf '%s\n' "$OUT_REVIEW" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_REVIEW" | grep -Fq "looser_than_recommended=4"; then
  echo "expected looser-than-recommended count in non-aligned case" >&2
  printf '%s\n' "$OUT_REVIEW" >&2
  exit 1
fi

set +e
OUT_ENFORCE="$(
  bash "$COMPARE_SCRIPT" "$SUMMARY" 48 0.25 0.50 0.75 0.75 0.02 0.02 0.01 on 2>&1
)"
STATUS_ENFORCE=$?
set -e
if [ "$STATUS_ENFORCE" -eq 0 ]; then
  echo "expected non-zero exit for enforce-on-review mode when status=review" >&2
  printf '%s\n' "$OUT_ENFORCE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_ENFORCE" | grep -Fq "boundary_history_policy_delta_status|status=review"; then
  echo "expected review status marker in enforce failure case" >&2
  printf '%s\n' "$OUT_ENFORCE" >&2
  exit 1
fi

echo "selftest_compare_unnest_ab_boundary_history_gate_policy_delta status=ok"
