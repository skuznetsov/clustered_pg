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
ACCUMULATE_SCRIPT="$SCRIPT_DIR/accumulate_unnest_ab_boundary_history_summaries.sh"
if [ ! -x "$ACCUMULATE_SCRIPT" ]; then
  echo "accumulate script not executable: $ACCUMULATE_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_accumulate_boundary_history_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

DIR_OK="$WORKDIR/ok"
mkdir -p "$DIR_OK"

cat >"$DIR_OK/a.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=3|lift_min48_total=0|lift_min32_rate=0.750000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/tmp/a
EOF

cat >"$DIR_OK/b.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=0|lift_min32_rate=1.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/tmp/b
EOF

OUT_OK="$(
  bash "$ACCUMULATE_SCRIPT" "$DIR_OK" 48
)"
if ! printf '%s\n' "$OUT_OK" | grep -Fq "boundary_history|scenario=boundary_40|runs=4|samples_total=8|lift_min32_total=7|lift_min48_total=0|lift_min32_rate=0.875000|lift_min48_rate=0.000000"; then
  echo "expected boundary_40 aggregate line in positive case" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "boundary_history_status|status=ok|runs=4|scenarios=4|strict_min_observations=48|source=$DIR_OK|files=2"; then
  echo "expected status aggregate line in positive case" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi

DIR_MISMATCH="$WORKDIR/mismatch"
mkdir -p "$DIR_MISMATCH"
cp "$DIR_OK/a.log" "$DIR_MISMATCH/"
cat >"$DIR_MISMATCH/bad.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
EOF

set +e
OUT_MISMATCH="$(
  bash "$ACCUMULATE_SCRIPT" "$DIR_MISMATCH" 48 2>&1
)"
STATUS_MISMATCH=$?
set -e
if [ "$STATUS_MISMATCH" -eq 0 ]; then
  echo "expected non-zero exit for scenario set mismatch case" >&2
  printf '%s\n' "$OUT_MISMATCH" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MISMATCH" | grep -Fq "scenario set mismatch"; then
  echo "expected scenario set mismatch marker in negative case" >&2
  printf '%s\n' "$OUT_MISMATCH" >&2
  exit 1
fi

DIR_STRICT="$WORKDIR/strict_mismatch"
mkdir -p "$DIR_STRICT"
cat >"$DIR_STRICT/strict.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=3|lift_min48_total=0|lift_min32_rate=0.750000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=64|input=/tmp/strict
EOF

set +e
OUT_STRICT="$(
  bash "$ACCUMULATE_SCRIPT" "$DIR_STRICT" 48 2>&1
)"
STATUS_STRICT=$?
set -e
if [ "$STATUS_STRICT" -eq 0 ]; then
  echo "expected non-zero exit for strict_min_observations status mismatch case" >&2
  printf '%s\n' "$OUT_STRICT" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_STRICT" | grep -Fq "strict_min_observations mismatch"; then
  echo "expected strict mismatch marker in negative case" >&2
  printf '%s\n' "$OUT_STRICT" >&2
  exit 1
fi

echo "selftest_accumulate_unnest_ab_boundary_history_summaries status=ok"
