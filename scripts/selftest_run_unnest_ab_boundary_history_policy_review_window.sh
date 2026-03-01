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
MAP_INDEX_BUILD_SCRIPT="$SCRIPT_DIR/build_unnest_ab_boundary_history_date_map_index.sh"
if [ ! -x "$MAP_INDEX_BUILD_SCRIPT" ]; then
  echo "date-map-index build script not executable: $MAP_INDEX_BUILD_SCRIPT" >&2
  exit 2
fi
MANIFEST_BUILD_SCRIPT="$SCRIPT_DIR/build_unnest_ab_boundary_history_policy_review_manifest.sh"
if [ ! -x "$MANIFEST_BUILD_SCRIPT" ]; then
  echo "policy-review manifest build script not executable: $MANIFEST_BUILD_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_boundary_policy_review_window_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

SUMMARY_DIR="$WORKDIR/summaries"
mkdir -p "$SUMMARY_DIR"

cat >"$SUMMARY_DIR/20260220.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=2|lift_min48_total=1|lift_min32_rate=0.500000|lift_min48_rate=0.250000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/tmp/f1
EOF

cat >"$SUMMARY_DIR/20260221.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=3|lift_min48_total=2|lift_min32_rate=0.750000|lift_min48_rate=0.500000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/tmp/f2
EOF

cat >"$SUMMARY_DIR/20260222.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/tmp/f3
EOF

AGG_OUT="$WORKDIR/aggregate.summary.log"
OUT_OK="$(
  UNNEST_AB_POLICY_REVIEW_AGGREGATE_OUT="$AGG_OUT" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off
)"
if ! printf '%s\n' "$OUT_OK" | grep -Fq "boundary_history|scenario=boundary_40|runs=4|samples_total=8|lift_min32_total=7|lift_min48_total=5|lift_min32_rate=0.875000|lift_min48_rate=0.625000"; then
  echo "expected boundary_40 aggregate for selected latest 2 files" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=2|candidate_files=3|selected_files=2|from_date=none|to_date=none|date_map_active=0|policy_delta_status=review|aggregate_summary=$AGG_OUT|aggregate_summary_persisted=1"; then
  echo "expected review-window status line with persisted aggregate output" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if [ ! -s "$AGG_OUT" ]; then
  echo "expected persisted aggregate summary output file: $AGG_OUT" >&2
  exit 1
fi

MANIFEST_FILE="$WORKDIR/review.manifest.tsv"
OUT_MANIFEST_BUILD="$(
  bash "$MANIFEST_BUILD_SCRIPT" "$SUMMARY_DIR" "$MANIFEST_FILE"
)"
if ! printf '%s\n' "$OUT_MANIFEST_BUILD" | grep -Fq "unnest_ab_boundary_history_policy_review_manifest|status=ok|schema_version=1|input=$SUMMARY_DIR|output=$MANIFEST_FILE|scanned_files=3|written_entries=3"; then
  echo "expected successful policy-review manifest build status output" >&2
  printf '%s\n' "$OUT_MANIFEST_BUILD" >&2
  exit 1
fi
if [ ! -s "$MANIFEST_FILE" ]; then
  echo "expected generated policy-review manifest file: $MANIFEST_FILE" >&2
  exit 1
fi

OUT_MANIFEST="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off
)"
if ! printf '%s\n' "$OUT_MANIFEST" | grep -Fq "boundary_history|scenario=boundary_40|runs=4|samples_total=8|lift_min32_total=7|lift_min48_total=5|lift_min32_rate=0.875000|lift_min48_rate=0.625000"; then
  echo "expected manifest-backed selection to preserve baseline aggregate behavior" >&2
  printf '%s\n' "$OUT_MANIFEST" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=2|candidate_files=3|selected_files=2|from_date=none|to_date=none|date_map_active=0|policy_delta_status=review|aggregate_summary=ephemeral|aggregate_summary_persisted=0|manifest_active=1|manifest_entries=3|manifest_fresh_hits=3|manifest_stale_entries=0|manifest_missing_entries=0"; then
  echo "expected manifest metadata counters for all-fresh manifest-backed run" >&2
  printf '%s\n' "$OUT_MANIFEST" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST" | grep -Fq "|manifest_trusted=0"; then
  echo "expected default manifest mode to report manifest_trusted=0" >&2
  printf '%s\n' "$OUT_MANIFEST" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST" | grep -Fq "|manifest_freshness_checked=0|manifest_freshness_status=skipped"; then
  echo "expected default manifest run to report skipped freshness checks when max-age SLA is not configured" >&2
  printf '%s\n' "$OUT_MANIFEST" >&2
  exit 1
fi

OUT_MANIFEST_TRUSTED="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off
)"
if ! printf '%s\n' "$OUT_MANIFEST_TRUSTED" | grep -Fq "boundary_history|scenario=boundary_40|runs=4|samples_total=8|lift_min32_total=7|lift_min48_total=5|lift_min32_rate=0.875000|lift_min48_rate=0.625000"; then
  echo "expected trusted-manifest mode to preserve aggregate behavior for fresh manifest" >&2
  printf '%s\n' "$OUT_MANIFEST_TRUSTED" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST_TRUSTED" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=2|candidate_files=3|selected_files=2|from_date=none|to_date=none|date_map_active=0|policy_delta_status=review|aggregate_summary=ephemeral|aggregate_summary_persisted=0|manifest_active=1|manifest_entries=3|manifest_fresh_hits=3|manifest_stale_entries=0|manifest_missing_entries=0|manifest_trusted=1"; then
  echo "expected trusted-manifest status counters and marker in final status line" >&2
  printf '%s\n' "$OUT_MANIFEST_TRUSTED" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST_TRUSTED" | grep -Fq "|manifest_freshness_checked=0|manifest_freshness_status=skipped"; then
  echo "expected trusted-manifest run to report skipped freshness checks when max-age SLA is not configured" >&2
  printf '%s\n' "$OUT_MANIFEST_TRUSTED" >&2
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
  echo "expected integer generated_at_epoch header in manifest file for freshness-enforced run case" >&2
  exit 1
fi

OUT_MANIFEST_TRUSTED_FRESHNESS_OK="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on UNNEST_AB_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS=120 UNNEST_AB_POLICY_REVIEW_MANIFEST_NOW_EPOCH="$((MANIFEST_GENERATED_EPOCH + 60))" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off
)"
if ! printf '%s\n' "$OUT_MANIFEST_TRUSTED_FRESHNESS_OK" | grep -Fq "unnest_ab_boundary_history_policy_review_manifest_freshness|status=ok|"; then
  echo "expected manifest freshness checker status output in freshness-enforced trusted run" >&2
  printf '%s\n' "$OUT_MANIFEST_TRUSTED_FRESHNESS_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST_TRUSTED_FRESHNESS_OK" | grep -Fq "|manifest_freshness_checked=1|manifest_freshness_status=ok"; then
  echo "expected trusted run status to report successful freshness check when max-age SLA is configured" >&2
  printf '%s\n' "$OUT_MANIFEST_TRUSTED_FRESHNESS_OK" >&2
  exit 1
fi

set +e
OUT_MANIFEST_TRUSTED_FRESHNESS_STALE="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on UNNEST_AB_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS=120 UNNEST_AB_POLICY_REVIEW_MANIFEST_NOW_EPOCH="$((MANIFEST_GENERATED_EPOCH + 121))" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2>&1
)"
STATUS_MANIFEST_TRUSTED_FRESHNESS_STALE=$?
set -e
if [ "$STATUS_MANIFEST_TRUSTED_FRESHNESS_STALE" -eq 0 ]; then
  echo "expected non-zero exit for stale trusted manifest when max-age SLA is configured" >&2
  printf '%s\n' "$OUT_MANIFEST_TRUSTED_FRESHNESS_STALE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST_TRUSTED_FRESHNESS_STALE" | grep -Fq "unnest_ab_boundary_history_policy_review_manifest_freshness|status=error|reason=stale|"; then
  echo "expected stale manifest freshness error marker in freshness-enforced trusted run" >&2
  printf '%s\n' "$OUT_MANIFEST_TRUSTED_FRESHNESS_STALE" >&2
  exit 1
fi

set +e
OUT_MANIFEST_TRUSTED_WITHOUT_MANIFEST="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2>&1
)"
STATUS_MANIFEST_TRUSTED_WITHOUT_MANIFEST=$?
set -e
if [ "$STATUS_MANIFEST_TRUSTED_WITHOUT_MANIFEST" -eq 0 ]; then
  echo "expected non-zero exit for trusted manifest mode without manifest input" >&2
  printf '%s\n' "$OUT_MANIFEST_TRUSTED_WITHOUT_MANIFEST" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST_TRUSTED_WITHOUT_MANIFEST" | grep -Fq "requires UNNEST_AB_POLICY_REVIEW_MANIFEST"; then
  echo "expected explicit error for trusted manifest mode without manifest input" >&2
  printf '%s\n' "$OUT_MANIFEST_TRUSTED_WITHOUT_MANIFEST" >&2
  exit 1
fi

set +e
OUT_MANIFEST_TRUSTED_INVALID="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=invalid bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2>&1
)"
STATUS_MANIFEST_TRUSTED_INVALID=$?
set -e
if [ "$STATUS_MANIFEST_TRUSTED_INVALID" -eq 0 ]; then
  echo "expected non-zero exit for invalid manifest trusted option value" >&2
  printf '%s\n' "$OUT_MANIFEST_TRUSTED_INVALID" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST_TRUSTED_INVALID" | grep -Fq "UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED must be off or on"; then
  echo "expected explicit validation error for invalid trusted option value" >&2
  printf '%s\n' "$OUT_MANIFEST_TRUSTED_INVALID" >&2
  exit 1
fi

set +e
OUT_MANIFEST_PRECHECKED_UNTRUSTED="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=on bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2>&1
)"
STATUS_MANIFEST_PRECHECKED_UNTRUSTED=$?
set -e
if [ "$STATUS_MANIFEST_PRECHECKED_UNTRUSTED" -eq 0 ]; then
  echo "expected non-zero exit for prechecked manifest mode without trusted manifest mode" >&2
  printf '%s\n' "$OUT_MANIFEST_PRECHECKED_UNTRUSTED" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST_PRECHECKED_UNTRUSTED" | grep -Fq "UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=on requires UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on"; then
  echo "expected explicit validation error for prechecked manifest mode without trusted manifest mode" >&2
  printf '%s\n' "$OUT_MANIFEST_PRECHECKED_UNTRUSTED" >&2
  exit 1
fi

set +e
OUT_MANIFEST_PRECHECKED_NO_MAX_AGE="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=on bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2>&1
)"
STATUS_MANIFEST_PRECHECKED_NO_MAX_AGE=$?
set -e
if [ "$STATUS_MANIFEST_PRECHECKED_NO_MAX_AGE" -eq 0 ]; then
  echo "expected non-zero exit for prechecked trusted manifest mode without max-age SLA" >&2
  printf '%s\n' "$OUT_MANIFEST_PRECHECKED_NO_MAX_AGE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST_PRECHECKED_NO_MAX_AGE" | grep -Fq "requires UNNEST_AB_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS"; then
  echo "expected explicit validation error for prechecked trusted manifest mode without max-age SLA" >&2
  printf '%s\n' "$OUT_MANIFEST_PRECHECKED_NO_MAX_AGE" >&2
  exit 1
fi

printf '# stale marker\n' >>"$SUMMARY_DIR/20260222.summary.log"
OUT_MANIFEST_STALE="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$MANIFEST_FILE" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off
)"
if ! printf '%s\n' "$OUT_MANIFEST_STALE" | grep -Fq "boundary_history|scenario=boundary_40|runs=4|samples_total=8|lift_min32_total=7|lift_min48_total=5|lift_min32_rate=0.875000|lift_min48_rate=0.625000"; then
  echo "expected stale-manifest fallback to preserve aggregate behavior" >&2
  printf '%s\n' "$OUT_MANIFEST_STALE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MANIFEST_STALE" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=2|candidate_files=3|selected_files=2|from_date=none|to_date=none|date_map_active=0|policy_delta_status=review|aggregate_summary=ephemeral|aggregate_summary_persisted=0|manifest_active=1|manifest_entries=3|manifest_fresh_hits=2|manifest_stale_entries=1|manifest_missing_entries=0"; then
  echo "expected stale-manifest counter to increment and fallback to live parse" >&2
  printf '%s\n' "$OUT_MANIFEST_STALE" >&2
  exit 1
fi

BAD_MANIFEST="$WORKDIR/bad_review.manifest.tsv"
cat >"$BAD_MANIFEST" <<'EOF'
invalid_manifest_line_without_tabs
EOF

set +e
OUT_BAD_MANIFEST="$(
  UNNEST_AB_POLICY_REVIEW_MANIFEST="$BAD_MANIFEST" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2>&1
)"
STATUS_BAD_MANIFEST=$?
set -e
if [ "$STATUS_BAD_MANIFEST" -eq 0 ]; then
  echo "expected non-zero exit for malformed policy-review manifest input" >&2
  printf '%s\n' "$OUT_BAD_MANIFEST" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_BAD_MANIFEST" | grep -Fq "invalid policy review manifest line"; then
  echo "expected malformed-manifest parser error" >&2
  printf '%s\n' "$OUT_BAD_MANIFEST" >&2
  exit 1
fi

BAD_MAP_NO_RANGE="$WORKDIR/date_map_bad_no_range.csv"
cat >"$BAD_MAP_NO_RANGE" <<'EOF'
invalid_line_without_comma
EOF

OUT_NO_RANGE_BAD_MAP="$(
  UNNEST_AB_POLICY_REVIEW_DATE_MAP="$BAD_MAP_NO_RANGE" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off
)"
if ! printf '%s\n' "$OUT_NO_RANGE_BAD_MAP" | grep -Fq "boundary_history|scenario=boundary_40|runs=4|samples_total=8|lift_min32_total=7|lift_min48_total=5|lift_min32_rate=0.875000|lift_min48_rate=0.625000"; then
  echo "expected no-range flow to ignore date map and keep baseline aggregate behavior" >&2
  printf '%s\n' "$OUT_NO_RANGE_BAD_MAP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_NO_RANGE_BAD_MAP" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=2|candidate_files=3|selected_files=2|from_date=none|to_date=none|date_map_active=0|policy_delta_status=review"; then
  echo "expected no-range flow to report date_map_active=0 even when map env is provided" >&2
  printf '%s\n' "$OUT_NO_RANGE_BAD_MAP" >&2
  exit 1
fi

set +e
OUT_ENFORCE="$(
  bash "$REVIEW_SCRIPT" "$SUMMARY_DIR" 48 2 0.90 0.90 0.50 0.50 0.02 0.02 0.05 on 2>&1
)"
STATUS_ENFORCE=$?
set -e
if [ "$STATUS_ENFORCE" -eq 0 ]; then
  echo "expected non-zero exit with enforce_on_review=on and review delta status" >&2
  printf '%s\n' "$OUT_ENFORCE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_ENFORCE" | grep -Fq "boundary_history_policy_review_window|status=error"; then
  echo "expected error status marker in enforce failure case" >&2
  printf '%s\n' "$OUT_ENFORCE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_ENFORCE" | grep -Fq "policy_delta_status=review"; then
  echo "expected review policy delta status in enforce failure case" >&2
  printf '%s\n' "$OUT_ENFORCE" >&2
  exit 1
fi

SUMMARY_DIR_RANGE="$WORKDIR/summaries_range"
mkdir -p "$SUMMARY_DIR_RANGE"

cat >"$SUMMARY_DIR_RANGE/zzz_2026-02-20_keep.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=1|lift_min48_total=1|lift_min32_rate=0.250000|lift_min48_rate=0.250000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/tmp/r1
EOF

cat >"$SUMMARY_DIR_RANGE/mid_20260221_keep.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=2|lift_min48_total=2|lift_min32_rate=0.500000|lift_min48_rate=0.500000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/tmp/r2
EOF

cat >"$SUMMARY_DIR_RANGE/alpha_2026-02-22_keep.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/tmp/r3
EOF

OUT_RANGE="$(
  bash "$REVIEW_SCRIPT" "$SUMMARY_DIR_RANGE" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2026-02-21 2026-02-22
)"
if ! printf '%s\n' "$OUT_RANGE" | grep -Fq "boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000"; then
  echo "expected date-range selection to choose latest in-range artifact (2026-02-22) with window_files=1" >&2
  printf '%s\n' "$OUT_RANGE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_RANGE" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=1|candidate_files=2|selected_files=1|from_date=20260221|to_date=20260222|date_map_active=0|policy_delta_status=review"; then
  echo "expected date-range metadata in final status line for range case" >&2
  printf '%s\n' "$OUT_RANGE" >&2
  exit 1
fi

SUMMARY_DIR_META="$WORKDIR/summaries_meta"
mkdir -p "$SUMMARY_DIR_META"

cat >"$SUMMARY_DIR_META/no_date_a.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=1|lift_min48_total=1|lift_min32_rate=0.250000|lift_min48_rate=0.250000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/archive/run_2026-02-21
EOF

cat >"$SUMMARY_DIR_META/no_date_b.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|source=/archive/run_20260222
EOF

OUT_META="$(
  bash "$REVIEW_SCRIPT" "$SUMMARY_DIR_META" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2026-02-22 2026-02-22
)"
if ! printf '%s\n' "$OUT_META" | grep -Fq "boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000"; then
  echo "expected date-range fallback via status metadata to select in-range artifact without date in filename" >&2
  printf '%s\n' "$OUT_META" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_META" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=1|candidate_files=1|selected_files=1|from_date=20260222|to_date=20260222|date_map_active=0|policy_delta_status=review"; then
  echo "expected date-range metadata for status-metadata fallback case" >&2
  printf '%s\n' "$OUT_META" >&2
  exit 1
fi

SUMMARY_DIR_MAP="$WORKDIR/summaries_map"
mkdir -p "$SUMMARY_DIR_MAP"

cat >"$SUMMARY_DIR_MAP/no_date_map_a.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=1|lift_min48_total=1|lift_min32_rate=0.250000|lift_min48_rate=0.250000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/archive/no_date_run_a
EOF

cat >"$SUMMARY_DIR_MAP/no_date_map_b.summary.log" <<'EOF'
boundary_history|scenario=balanced_wide|runs=2|samples_total=4|lift_min32_total=0|lift_min48_total=0|lift_min32_rate=0.000000|lift_min48_rate=0.000000
boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000
boundary_history|scenario=boundary_56|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history|scenario=pressure_wide|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=4|lift_min32_rate=1.000000|lift_min48_rate=1.000000
boundary_history_status|status=ok|runs=2|scenarios=4|strict_min_observations=48|input=/archive/no_date_run_b
EOF

MAP_FILE="$WORKDIR/date_map.csv"
cat >"$MAP_FILE" <<'EOF'
# key,date
no_date_map_a.summary.log,2026-02-21
no_date_map_b.summary.log,20260222
EOF

OUT_MAP="$(
  UNNEST_AB_POLICY_REVIEW_DATE_MAP="$MAP_FILE" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR_MAP" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2026-02-22 2026-02-22
)"
if ! printf '%s\n' "$OUT_MAP" | grep -Fq "boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000"; then
  echo "expected date-range fallback via explicit map file to select in-range artifact without date in filename/status metadata" >&2
  printf '%s\n' "$OUT_MAP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MAP" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=1|candidate_files=1|selected_files=1|from_date=20260222|to_date=20260222|date_map_active=1|policy_delta_status=review"; then
  echo "expected final status metadata for explicit date map fallback case" >&2
  printf '%s\n' "$OUT_MAP" >&2
  exit 1
fi

MAP_FILE_FOR_BUILD="$WORKDIR/date_map_for_build.csv"
cat >"$MAP_FILE_FOR_BUILD" <<'EOF'
# unsorted + duplicate-same-date
no_date_map_b.summary.log,2026-02-22
no_date_map_a.summary.log,20260221
no_date_map_b.summary.log,20260222
EOF

MAP_INDEX_FROM_BUILD="$WORKDIR/date_map_from_build.index.tsv"
BUILD_OUT="$(
  bash "$MAP_INDEX_BUILD_SCRIPT" "$MAP_FILE_FOR_BUILD" "$MAP_INDEX_FROM_BUILD"
)"
if ! printf '%s\n' "$BUILD_OUT" | grep -Fq "unnest_ab_boundary_history_date_map_index|status=ok"; then
  echo "expected successful date map index build status output" >&2
  printf '%s\n' "$BUILD_OUT" >&2
  exit 1
fi
if [ ! -s "$MAP_INDEX_FROM_BUILD" ]; then
  echo "expected generated date map index file: $MAP_INDEX_FROM_BUILD" >&2
  exit 1
fi

OUT_MAP_INDEX_FROM_BUILD="$(
  UNNEST_AB_POLICY_REVIEW_DATE_MAP_INDEX="$MAP_INDEX_FROM_BUILD" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR_MAP" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2026-02-22 2026-02-22
)"
if ! printf '%s\n' "$OUT_MAP_INDEX_FROM_BUILD" | grep -Fq "boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000"; then
  echo "expected generated date-map index to be usable by review-window selector" >&2
  printf '%s\n' "$OUT_MAP_INDEX_FROM_BUILD" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MAP_INDEX_FROM_BUILD" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=1|candidate_files=1|selected_files=1|from_date=20260222|to_date=20260222|date_map_active=1|policy_delta_status=review"; then
  echo "expected success status for generated date-map index selection case" >&2
  printf '%s\n' "$OUT_MAP_INDEX_FROM_BUILD" >&2
  exit 1
fi

MAP_FILE_BUILD_CONFLICT="$WORKDIR/date_map_build_conflict.csv"
cat >"$MAP_FILE_BUILD_CONFLICT" <<'EOF'
no_date_map_b.summary.log,20260221
no_date_map_b.summary.log,20260222
EOF

set +e
OUT_BUILD_CONFLICT="$(
  bash "$MAP_INDEX_BUILD_SCRIPT" "$MAP_FILE_BUILD_CONFLICT" "$WORKDIR/unused_conflict.index.tsv" 2>&1
)"
STATUS_BUILD_CONFLICT=$?
set -e
if [ "$STATUS_BUILD_CONFLICT" -eq 0 ]; then
  echo "expected non-zero exit for conflicting duplicate key while building date-map index" >&2
  printf '%s\n' "$OUT_BUILD_CONFLICT" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_BUILD_CONFLICT" | grep -Fq "conflicting date map key"; then
  echo "expected conflict error for date-map index build with duplicate key and different date" >&2
  printf '%s\n' "$OUT_BUILD_CONFLICT" >&2
  exit 1
fi

MAP_INDEX_FILE="$WORKDIR/date_map.index.tsv"
printf '# key\tdate\nno_date_map_a.summary.log\t20260221\nno_date_map_b.summary.log\t20260222\n' >"$MAP_INDEX_FILE"

OUT_MAP_INDEX="$(
  UNNEST_AB_POLICY_REVIEW_DATE_MAP_INDEX="$MAP_INDEX_FILE" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR_MAP" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2026-02-22 2026-02-22
)"
if ! printf '%s\n' "$OUT_MAP_INDEX" | grep -Fq "boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000"; then
  echo "expected date-range fallback via pre-indexed map file to select in-range artifact" >&2
  printf '%s\n' "$OUT_MAP_INDEX" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MAP_INDEX" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=1|candidate_files=1|selected_files=1|from_date=20260222|to_date=20260222|date_map_active=1|policy_delta_status=review"; then
  echo "expected final status metadata for pre-indexed date map fallback case" >&2
  printf '%s\n' "$OUT_MAP_INDEX" >&2
  exit 1
fi

set +e
OUT_MAP_BOTH="$(
  UNNEST_AB_POLICY_REVIEW_DATE_MAP="$MAP_FILE" UNNEST_AB_POLICY_REVIEW_DATE_MAP_INDEX="$MAP_INDEX_FILE" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR_MAP" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2026-02-22 2026-02-22 2>&1
)"
STATUS_MAP_BOTH=$?
set -e
if [ "$STATUS_MAP_BOTH" -eq 0 ]; then
  echo "expected non-zero exit when both csv and indexed date-map inputs are provided" >&2
  printf '%s\n' "$OUT_MAP_BOTH" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MAP_BOTH" | grep -Fq "cannot be set together"; then
  echo "expected explicit conflict error when both date-map sources are provided" >&2
  printf '%s\n' "$OUT_MAP_BOTH" >&2
  exit 1
fi

MAP_FILE_PATH_PRECEDENCE="$WORKDIR/date_map_path_precedence.csv"
cat >"$MAP_FILE_PATH_PRECEDENCE" <<EOF
no_date_map_b.summary.log,20260221
$SUMMARY_DIR_MAP/no_date_map_b.summary.log,20260222
EOF

OUT_MAP_PATH_PRECEDENCE="$(
  UNNEST_AB_POLICY_REVIEW_DATE_MAP="$MAP_FILE_PATH_PRECEDENCE" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR_MAP" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2026-02-22 2026-02-22
)"
if ! printf '%s\n' "$OUT_MAP_PATH_PRECEDENCE" | grep -Fq "boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000"; then
  echo "expected full-path date-map key to take precedence over basename key" >&2
  printf '%s\n' "$OUT_MAP_PATH_PRECEDENCE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MAP_PATH_PRECEDENCE" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=1|candidate_files=1|selected_files=1|from_date=20260222|to_date=20260222|date_map_active=1|policy_delta_status=review"; then
  echo "expected success status for map full-path precedence case" >&2
  printf '%s\n' "$OUT_MAP_PATH_PRECEDENCE" >&2
  exit 1
fi

MAP_FILE_DUP_SAME="$WORKDIR/date_map_duplicate_same_date.csv"
cat >"$MAP_FILE_DUP_SAME" <<'EOF'
no_date_map_b.summary.log,20260222
no_date_map_b.summary.log,2026-02-22
EOF

OUT_MAP_DUP_SAME="$(
  UNNEST_AB_POLICY_REVIEW_DATE_MAP="$MAP_FILE_DUP_SAME" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR_MAP" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2026-02-22 2026-02-22
)"
if ! printf '%s\n' "$OUT_MAP_DUP_SAME" | grep -Fq "boundary_history|scenario=boundary_40|runs=2|samples_total=4|lift_min32_total=4|lift_min48_total=3|lift_min32_rate=1.000000|lift_min48_rate=0.750000"; then
  echo "expected duplicate same-date map key to remain idempotent and keep selection behavior" >&2
  printf '%s\n' "$OUT_MAP_DUP_SAME" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MAP_DUP_SAME" | grep -Fq "boundary_history_policy_review_window|status=ok|strict_min_observations=48|requested_window_files=1|candidate_files=1|selected_files=1|from_date=20260222|to_date=20260222|date_map_active=1|policy_delta_status=review"; then
  echo "expected success status for duplicate same-date map key case" >&2
  printf '%s\n' "$OUT_MAP_DUP_SAME" >&2
  exit 1
fi

MAP_FILE_CONFLICT="$WORKDIR/date_map_conflict.csv"
cat >"$MAP_FILE_CONFLICT" <<'EOF'
no_date_map_b.summary.log,20260221
no_date_map_b.summary.log,20260222
EOF

set +e
OUT_MAP_CONFLICT="$(
  UNNEST_AB_POLICY_REVIEW_DATE_MAP="$MAP_FILE_CONFLICT" bash "$REVIEW_SCRIPT" "$SUMMARY_DIR_MAP" 48 1 0.90 0.90 0.50 0.50 0.02 0.02 0.05 off 2026-02-22 2026-02-22 2>&1
)"
STATUS_MAP_CONFLICT=$?
set -e
if [ "$STATUS_MAP_CONFLICT" -eq 0 ]; then
  echo "expected non-zero exit for conflicting duplicate date-map key" >&2
  printf '%s\n' "$OUT_MAP_CONFLICT" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MAP_CONFLICT" | grep -Fq "conflicting date map key"; then
  echo "expected conflicting date-map key error message" >&2
  printf '%s\n' "$OUT_MAP_CONFLICT" >&2
  exit 1
fi

echo "selftest_run_unnest_ab_boundary_history_policy_review_window status=ok"
