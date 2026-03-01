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
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_trusted_orchestrator_selftest.XXXXXX")"
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
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/archive/trusted_orchestrator_selftest
EOF

set +e
OUT_MISSING_MANIFEST="$(
  make -s -C "$ROOT_DIR" --no-print-directory unnest-ab-profile-boundary-history-policy-review-window-trusted \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_INPUT="$SUMMARY_DIR" \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_WINDOW_FILES=1 2>&1
)"
STATUS_MISSING_MANIFEST=$?
set -e
if [ "$STATUS_MISSING_MANIFEST" -eq 0 ]; then
  echo "expected non-zero exit when trusted orchestrator target is called without manifest output path" >&2
  printf '%s\n' "$OUT_MISSING_MANIFEST" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MISSING_MANIFEST" | grep -Fq "UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_OUT must be set"; then
  echo "expected explicit missing manifest output validation error from trusted orchestrator target" >&2
  printf '%s\n' "$OUT_MISSING_MANIFEST" >&2
  exit 1
fi

MANIFEST_FILE="$WORKDIR/review.manifest.tsv"
OUT_OK="$(
  make -s -C "$ROOT_DIR" --no-print-directory unnest-ab-profile-boundary-history-policy-review-window-trusted \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_INPUT="$SUMMARY_DIR" \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_OUT="$MANIFEST_FILE" \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS=3600 \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_WINDOW_FILES=1 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BALANCED_MAX=0.90 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BOUNDARY40_MAX=0.90 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BOUNDARY56_MIN=0.50 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_PRESSURE_MIN=0.50
)"
if ! printf '%s\n' "$OUT_OK" | grep -Fq "unnest_ab_boundary_history_policy_review_manifest|status=ok|"; then
  echo "expected manifest build status in trusted orchestrator output" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "unnest_ab_boundary_history_policy_review_manifest_freshness|status=ok|"; then
  echo "expected freshness check status in trusted orchestrator output" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
FRESHNESS_LINE_COUNT="$(printf '%s\n' "$OUT_OK" | grep -c '^unnest_ab_boundary_history_policy_review_manifest_freshness|' || true)"
if [ "$FRESHNESS_LINE_COUNT" -ne 1 ]; then
  echo "expected exactly one manifest freshness status line in trusted orchestrator output (prechecked internal path should skip duplicate check)" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "boundary_history_policy_review_window|status=ok|"; then
  echo "expected review-window success status in trusted orchestrator output" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "|manifest_trusted=1"; then
  echo "expected trusted marker in final review-window status line" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "|manifest_freshness_checked=1|manifest_freshness_status=prechecked"; then
  echo "expected final review-window status to report prechecked freshness path in trusted orchestrator output" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if [ ! -s "$MANIFEST_FILE" ]; then
  echo "expected non-empty manifest file produced by trusted orchestrator target" >&2
  exit 1
fi

OUT_PRECHECKED_UNTRUSTED_FORCED="$(
  make -s -C "$ROOT_DIR" --no-print-directory unnest-ab-profile-boundary-history-policy-review-window \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_INPUT="$SUMMARY_DIR" \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" \
    UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=on \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_WINDOW_FILES=1 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BALANCED_MAX=0.90 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BOUNDARY40_MAX=0.90 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BOUNDARY56_MIN=0.50 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_PRESSURE_MIN=0.50
)"
if ! printf '%s\n' "$OUT_PRECHECKED_UNTRUSTED_FORCED" | grep -Fq "boundary_history_policy_review_window|status=ok|"; then
  echo "expected non-trusted make workflow to complete when prechecked flag is forced externally" >&2
  printf '%s\n' "$OUT_PRECHECKED_UNTRUSTED_FORCED" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_PRECHECKED_UNTRUSTED_FORCED" | grep -Fq "|manifest_trusted=0|"; then
  echo "expected non-trusted marker in non-trusted make workflow when prechecked flag is forced externally" >&2
  printf '%s\n' "$OUT_PRECHECKED_UNTRUSTED_FORCED" >&2
  exit 1
fi
if printf '%s\n' "$OUT_PRECHECKED_UNTRUSTED_FORCED" | grep -Fq "|manifest_freshness_status=prechecked"; then
  echo "expected non-trusted make workflow to never surface prechecked freshness status" >&2
  printf '%s\n' "$OUT_PRECHECKED_UNTRUSTED_FORCED" >&2
  exit 1
fi

MANIFEST_FILE_STALE="$WORKDIR/review.stale.manifest.tsv"
set +e
OUT_STALE="$(
  make -s -C "$ROOT_DIR" --no-print-directory unnest-ab-profile-boundary-history-policy-review-window-trusted \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_INPUT="$SUMMARY_DIR" \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_OUT="$MANIFEST_FILE_STALE" \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS=120 \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_NOW_EPOCH=4102444800 \
    UNNEST_AB_NIGHTLY_POLICY_REVIEW_WINDOW_FILES=1 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BALANCED_MAX=0.90 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BOUNDARY40_MAX=0.90 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BOUNDARY56_MIN=0.50 \
    UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_PRESSURE_MIN=0.50 2>&1
)"
STATUS_STALE=$?
set -e
if [ "$STATUS_STALE" -eq 0 ]; then
  echo "expected non-zero exit for stale manifest freshness SLA violation in trusted orchestrator target" >&2
  printf '%s\n' "$OUT_STALE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_STALE" | grep -Fq "unnest_ab_boundary_history_policy_review_manifest_freshness|status=error|reason=stale|"; then
  echo "expected stale freshness error marker in trusted orchestrator stale contract case" >&2
  printf '%s\n' "$OUT_STALE" >&2
  exit 1
fi

echo "selftest_make_unnest_ab_boundary_history_policy_review_window_trusted_workflow status=ok"
