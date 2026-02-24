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
REVIEW_SCRIPT="$SCRIPT_DIR/run_unnest_ab_boundary_history_policy_review_window.sh"
if [ ! -x "$REVIEW_SCRIPT" ]; then
  echo "review-window script not executable: $REVIEW_SCRIPT" >&2
  exit 2
fi
MANIFEST_BUILD_SCRIPT="$SCRIPT_DIR/build_unnest_ab_boundary_history_policy_review_manifest.sh"
if [ ! -x "$MANIFEST_BUILD_SCRIPT" ]; then
  echo "policy-review manifest build script not executable: $MANIFEST_BUILD_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_policy_review_prechecked_matrix_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

SUMMARY_DIR="$WORKDIR/summaries"
mkdir -p "$SUMMARY_DIR"
cat >"$SUMMARY_DIR/20260222.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/tmp/prechecked_matrix
EOF

MANIFEST_FILE="$WORKDIR/review.manifest.tsv"
OUT_MANIFEST_BUILD="$(
  bash "$MANIFEST_BUILD_SCRIPT" "$SUMMARY_DIR" "$MANIFEST_FILE"
)"
if ! printf '%s\n' "$OUT_MANIFEST_BUILD" | grep -Fq "unnest_ab_boundary_history_policy_review_manifest|status=ok|"; then
  echo "expected manifest build status output for prechecked matrix selftest" >&2
  printf '%s\n' "$OUT_MANIFEST_BUILD" >&2
  exit 1
fi

MANIFEST_GENERATED_EPOCH="$(
  awk -F= '
    /^# generated_at_epoch=/ {
      value = $2
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      print value
      exit
    }
  ' "$MANIFEST_FILE"
)"
if ! [[ "$MANIFEST_GENERATED_EPOCH" =~ ^[0-9]+$ ]]; then
  echo "expected integer generated_at_epoch header in manifest file for prechecked matrix selftest" >&2
  exit 1
fi

set +e
OUT_PRECHECKED_UNTRUSTED="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=on bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2>&1
)"
STATUS_PRECHECKED_UNTRUSTED=$?
set -e
if [ "$STATUS_PRECHECKED_UNTRUSTED" -eq 0 ]; then
  echo "expected non-zero exit for prechecked manifest mode without trusted manifest mode" >&2
  printf '%s\n' "$OUT_PRECHECKED_UNTRUSTED" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_PRECHECKED_UNTRUSTED" | grep -Fq "UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=on requires UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on"; then
  echo "expected explicit validation error for prechecked manifest mode without trusted manifest mode" >&2
  printf '%s\n' "$OUT_PRECHECKED_UNTRUSTED" >&2
  exit 1
fi

set +e
OUT_PRECHECKED_NO_MAX_AGE="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=on bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2>&1
)"
STATUS_PRECHECKED_NO_MAX_AGE=$?
set -e
if [ "$STATUS_PRECHECKED_NO_MAX_AGE" -eq 0 ]; then
  echo "expected non-zero exit for prechecked trusted manifest mode without max-age SLA" >&2
  printf '%s\n' "$OUT_PRECHECKED_NO_MAX_AGE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_PRECHECKED_NO_MAX_AGE" | grep -Fq "requires UNNEST_AB_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS"; then
  echo "expected explicit validation error for prechecked trusted manifest mode without max-age SLA" >&2
  printf '%s\n' "$OUT_PRECHECKED_NO_MAX_AGE" >&2
  exit 1
fi

OUT_PRECHECKED_OK="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" \
  UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on \
  UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=on \
  UNNEST_AB_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS=120 \
  UNNEST_AB_POLICY_REVIEW_MANIFEST_NOW_EPOCH="$((MANIFEST_GENERATED_EPOCH + 60))" \
  bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off
)"
if ! printf '%s\n' "$OUT_PRECHECKED_OK" | grep -Fq "boundary_history_policy_review_window|status=ok|"; then
  echo "expected successful review-window status for trusted prechecked manifest mode" >&2
  printf '%s\n' "$OUT_PRECHECKED_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_PRECHECKED_OK" | grep -Fq "|manifest_trusted=1"; then
  echo "expected trusted marker in final status for trusted prechecked manifest mode" >&2
  printf '%s\n' "$OUT_PRECHECKED_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_PRECHECKED_OK" | grep -Fq "|manifest_freshness_checked=1|manifest_freshness_status=prechecked"; then
  echo "expected prechecked freshness marker in final status for trusted prechecked manifest mode" >&2
  printf '%s\n' "$OUT_PRECHECKED_OK" >&2
  exit 1
fi
if printf '%s\n' "$OUT_PRECHECKED_OK" | grep -Fq "unnest_ab_boundary_history_policy_review_manifest_freshness|status=ok|"; then
  echo "expected direct prechecked path to avoid duplicate in-process freshness checker output" >&2
  printf '%s\n' "$OUT_PRECHECKED_OK" >&2
  exit 1
fi

echo "selftest_policy_review_manifest_prechecked_matrix status=ok"
