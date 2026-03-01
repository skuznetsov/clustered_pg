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
MANIFEST_BUILD_SCRIPT="$SCRIPT_DIR/build_unnest_ab_boundary_history_policy_review_manifest.sh"
FRESHNESS_CHECK_SCRIPT="$SCRIPT_DIR/check_unnest_ab_boundary_history_policy_review_manifest_freshness.sh"

if [ ! -x "$MANIFEST_BUILD_SCRIPT" ]; then
  echo "manifest build script not executable: $MANIFEST_BUILD_SCRIPT" >&2
  exit 2
fi
if [ ! -x "$FRESHNESS_CHECK_SCRIPT" ]; then
  echo "manifest freshness check script not executable: $FRESHNESS_CHECK_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_manifest_freshness_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

SUMMARY_DIR="$WORKDIR/summaries"
mkdir -p "$SUMMARY_DIR"
cat >"$SUMMARY_DIR/20260222.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=2|lift_min48_total=1|lift_min32_rate=0.500000|lift_min48_rate=0.250000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/archive/manifest_freshness_case
EOF

MANIFEST_FILE="$WORKDIR/review.manifest.tsv"
OUT_BUILD="$(
  bash "$MANIFEST_BUILD_SCRIPT" "$SUMMARY_DIR" "$MANIFEST_FILE"
)"
if ! printf '%s\n' "$OUT_BUILD" | grep -Fq "unnest_ab_boundary_history_policy_review_manifest|status=ok"; then
  echo "expected manifest build status output" >&2
  printf '%s\n' "$OUT_BUILD" >&2
  exit 1
fi
if [ ! -s "$MANIFEST_FILE" ]; then
  echo "expected non-empty manifest file" >&2
  exit 1
fi

GENERATED_EPOCH="$(
  awk -F= '
    /^# generated_at_epoch=/ {
      value = $2
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      print value
      exit
    }
  ' "$MANIFEST_FILE"
)"
if ! [[ "$GENERATED_EPOCH" =~ ^[0-9]+$ ]]; then
  echo "expected integer generated_at_epoch in manifest header" >&2
  exit 1
fi

OUT_OK="$(
  bash "$FRESHNESS_CHECK_SCRIPT" "$MANIFEST_FILE" 120 $((GENERATED_EPOCH + 60))
)"
if ! printf '%s\n' "$OUT_OK" | grep -Fq "unnest_ab_boundary_history_policy_review_manifest_freshness|status=ok|"; then
  echo "expected freshness check success for age <= max_age" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "|age_seconds=60|max_age_seconds=120"; then
  echo "expected deterministic age/max_age fields in freshness success output" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi

set +e
OUT_STALE="$(
  bash "$FRESHNESS_CHECK_SCRIPT" "$MANIFEST_FILE" 120 $((GENERATED_EPOCH + 121)) 2>&1
)"
STATUS_STALE=$?
set -e
if [ "$STATUS_STALE" -eq 0 ]; then
  echo "expected non-zero exit for stale manifest age > max_age" >&2
  printf '%s\n' "$OUT_STALE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_STALE" | grep -Fq "|status=error|reason=stale|"; then
  echo "expected stale reason marker in freshness stale output" >&2
  printf '%s\n' "$OUT_STALE" >&2
  exit 1
fi

set +e
OUT_FUTURE="$(
  bash "$FRESHNESS_CHECK_SCRIPT" "$MANIFEST_FILE" 120 $((GENERATED_EPOCH - 1)) 2>&1
)"
STATUS_FUTURE=$?
set -e
if [ "$STATUS_FUTURE" -eq 0 ]; then
  echo "expected non-zero exit for future generated epoch relative to now override" >&2
  printf '%s\n' "$OUT_FUTURE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_FUTURE" | grep -Fq "|status=error|reason=future_generated_epoch|"; then
  echo "expected future-generated-epoch reason marker" >&2
  printf '%s\n' "$OUT_FUTURE" >&2
  exit 1
fi

BAD_MANIFEST="$WORKDIR/review.bad_manifest.tsv"
grep -v '^# generated_at_epoch=' "$MANIFEST_FILE" >"$BAD_MANIFEST"
set +e
OUT_MISSING_HEADER="$(
  bash "$FRESHNESS_CHECK_SCRIPT" "$BAD_MANIFEST" 120 "$GENERATED_EPOCH" 2>&1
)"
STATUS_MISSING_HEADER=$?
set -e
if [ "$STATUS_MISSING_HEADER" -eq 0 ]; then
  echo "expected non-zero exit for missing generated_at_epoch header" >&2
  printf '%s\n' "$OUT_MISSING_HEADER" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MISSING_HEADER" | grep -Fq "missing generated_at_epoch"; then
  echo "expected explicit missing generated_at_epoch header error" >&2
  printf '%s\n' "$OUT_MISSING_HEADER" >&2
  exit 1
fi

set +e
OUT_BAD_MAX_AGE="$(
  bash "$FRESHNESS_CHECK_SCRIPT" "$MANIFEST_FILE" invalid "$GENERATED_EPOCH" 2>&1
)"
STATUS_BAD_MAX_AGE=$?
set -e
if [ "$STATUS_BAD_MAX_AGE" -eq 0 ]; then
  echo "expected non-zero exit for invalid max_age_seconds option" >&2
  printf '%s\n' "$OUT_BAD_MAX_AGE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_BAD_MAX_AGE" | grep -Fq "max_age_seconds must be a non-negative integer"; then
  echo "expected explicit invalid max_age_seconds validation error" >&2
  printf '%s\n' "$OUT_BAD_MAX_AGE" >&2
  exit 1
fi

echo "selftest_check_unnest_ab_boundary_history_policy_review_manifest_freshness status=ok"
