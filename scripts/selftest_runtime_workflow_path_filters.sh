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
WORKFLOW="$ROOT_DIR/.github/workflows/fastpath-runtime-selftest.yml"

if [ ! -f "$WORKFLOW" ]; then
  echo "workflow not found: $WORKFLOW" >&2
  exit 2
fi

require_path_filter() {
  local path="$1"
  local hits_unquoted
  local hits_quoted
  local hits
  hits_unquoted="$(grep -F --count "      - $path" "$WORKFLOW" || true)"
  hits_quoted="$(grep -F --count "      - '$path'" "$WORKFLOW" || true)"
  hits=$((hits_unquoted + hits_quoted))
  if [ "$hits" -lt 2 ]; then
    echo "expected path filter '$path' in both pull_request and push blocks (hits=$hits)" >&2
    exit 1
  fi
}

require_literal() {
  local literal="$1"
  if ! grep -Fq "$literal" "$WORKFLOW"; then
    echo "expected literal '$literal' in $WORKFLOW" >&2
    exit 1
  fi
}

require_literal_count_at_least() {
  local literal="$1"
  local min_count="$2"
  local hits
  hits="$(grep -F --count "$literal" "$WORKFLOW" || true)"
  if [ "$hits" -lt "$min_count" ]; then
    echo "expected literal '$literal' at least $min_count time(s) in $WORKFLOW (hits=$hits)" >&2
    exit 1
  fi
}

extract_paths_for_event() {
  local event="$1"
  awk -v event="$event" '
    function unquote(s) {
      if (s ~ /^'\''.*'\''$/) {
        sub(/^'\''/, "", s)
        sub(/'\''$/, "", s)
      }
      return s
    }
    $0 == "  " event ":" {
      in_event = 1
      in_paths = 0
      next
    }
    in_event && $0 ~ /^  [A-Za-z_]+:/ && $0 != "    paths:" {
      if ($0 !~ /^    /) {
        in_event = 0
        in_paths = 0
      }
      next
    }
    in_event && $0 == "    paths:" {
      in_paths = 1
      next
    }
    in_event && in_paths && $0 ~ /^      - / {
      path = $0
      sub(/^      - /, "", path)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", path)
      path = unquote(path)
      print path
      next
    }
    in_event && in_paths && $0 !~ /^      - / && $0 !~ /^[[:space:]]*$/ {
      in_paths = 0
      next
    }
  ' "$WORKFLOW"
}

assert_event_path_parity() {
  local tmpdir
  local dup_count
  tmpdir="$(mktemp -d "$TMP_ROOT/runtime_workflow_path_parity.XXXXXX")"
  extract_paths_for_event "pull_request" >"$tmpdir/pull_request.raw"
  extract_paths_for_event "push" >"$tmpdir/push.raw"

  if [ ! -s "$tmpdir/pull_request.raw" ] || [ ! -s "$tmpdir/push.raw" ]; then
    rm -rf "$tmpdir"
    echo "expected non-empty pull_request and push path filters in $WORKFLOW" >&2
    exit 1
  fi

  sort "$tmpdir/pull_request.raw" >"$tmpdir/pull_request.sorted"
  sort "$tmpdir/push.raw" >"$tmpdir/push.sorted"

  dup_count="$(uniq -d "$tmpdir/pull_request.sorted" | wc -l | tr -d ' ')"
  if [ "${dup_count:-0}" -gt 0 ]; then
    rm -rf "$tmpdir"
    echo "unexpected duplicate path filter entries in pull_request block for $WORKFLOW" >&2
    exit 1
  fi
  dup_count="$(uniq -d "$tmpdir/push.sorted" | wc -l | tr -d ' ')"
  if [ "${dup_count:-0}" -gt 0 ]; then
    rm -rf "$tmpdir"
    echo "unexpected duplicate path filter entries in push block for $WORKFLOW" >&2
    exit 1
  fi

  if ! cmp -s "$tmpdir/pull_request.sorted" "$tmpdir/push.sorted"; then
    rm -rf "$tmpdir"
    echo "path filter mismatch between pull_request and push blocks for $WORKFLOW" >&2
    exit 1
  fi

  rm -rf "$tmpdir"
}

assert_event_path_parity

require_path_filter "clustered_pg.c"
require_path_filter "Makefile"
require_path_filter "scripts/extract_clustered_pg_define.sh"
require_path_filter "scripts/run_unnest_ab_probe_mixed_shapes.sh"
require_path_filter "scripts/summarize_unnest_ab_boundary_history.sh"
require_path_filter "scripts/check_unnest_ab_boundary_history_gate.sh"
require_path_filter "scripts/compare_unnest_ab_boundary_history_gate_policy_delta.sh"
require_path_filter "sql/clustered_pg--0.1.0.sql"
require_path_filter ".github/workflows/fastpath-runtime-selftest.yml"

require_literal "workflow_dispatch:"
require_literal "schedule:"
require_literal "if: github.event_name == 'schedule'"
require_literal "runs-on: macos-14"
require_literal "make fastpath-perf-probe-selftest PERF_RUNTIME_SELFTEST_TMP_ROOT=/tmp | tee /tmp/clustered_pg_runtime_perf_selftest.log"
require_literal "make fastpath-perf-probe-selftest-high-churn PERF_RUNTIME_SELFTEST_TMP_ROOT=/tmp | tee /tmp/clustered_pg_runtime_perf_selftest_high_churn.log"
require_literal "make -s --no-print-directory unnest-ab-profile-boundary-compare-baseline UNNEST_AB_NIGHTLY_OUT_ROOT=/tmp UNNEST_AB_NIGHTLY_BASE_PORT=65430 UNNEST_AB_NIGHTLY_REPS=4 UNNEST_AB_NIGHTLY_STRICT_MIN_OBS=48 | tee /tmp/clustered_pg_boundary_baseline_compare.log"
require_literal "make -s --no-print-directory unnest-ab-profile-boundary-history-window-gate UNNEST_AB_NIGHTLY_OUT_ROOT=/tmp UNNEST_AB_NIGHTLY_BASE_PORT=65450 UNNEST_AB_NIGHTLY_REPS=2 UNNEST_AB_NIGHTLY_STRICT_MIN_OBS=48 UNNEST_AB_NIGHTLY_WINDOW_RUNS=2 UNNEST_AB_NIGHTLY_WINDOW_MIN_SAMPLES_TOTAL=4 UNNEST_AB_NIGHTLY_WINDOW_SUMMARY_OUT=/tmp/clustered_pg_boundary_history_window_summary.log UNNEST_AB_NIGHTLY_HISTORY_GATE_BALANCED_MAX_STRICT_RATE=0.25 UNNEST_AB_NIGHTLY_HISTORY_GATE_BOUNDARY40_MAX_STRICT_RATE=0.50 UNNEST_AB_NIGHTLY_HISTORY_GATE_BOUNDARY56_MIN_STRICT_RATE=0.75 UNNEST_AB_NIGHTLY_HISTORY_GATE_PRESSURE_MIN_STRICT_RATE=0.75 | tee /tmp/clustered_pg_boundary_history_window_gate.log"
require_literal "make -s --no-print-directory unnest-ab-profile-boundary-history-policy-delta UNNEST_AB_NIGHTLY_POLICY_DELTA_INPUT=/tmp/clustered_pg_boundary_history_window_summary.log UNNEST_AB_NIGHTLY_POLICY_DELTA_STRICT_MIN_OBS=48 UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BALANCED_MAX=0.25 UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BOUNDARY40_MAX=0.50 UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_BOUNDARY56_MIN=0.75 UNNEST_AB_NIGHTLY_POLICY_DELTA_CURRENT_PRESSURE_MIN=0.75 UNNEST_AB_NIGHTLY_POLICY_DELTA_DERIVE_MAX_HEADROOM=0.02 UNNEST_AB_NIGHTLY_POLICY_DELTA_DERIVE_MIN_FLOOR_MARGIN=0.02 UNNEST_AB_NIGHTLY_POLICY_DELTA_TOLERANCE=0.05 UNNEST_AB_NIGHTLY_POLICY_DELTA_ENFORCE=off | tee /tmp/clustered_pg_boundary_history_policy_delta.log"
require_literal "/tmp/clustered_pg_boundary_history_window_summary.log"
require_literal "/tmp/clustered_pg_boundary_history_policy_delta.log"
require_literal_count_at_least "uses: actions/upload-artifact@v4" 4
require_literal_count_at_least "if: always()" 4
require_literal_count_at_least "if-no-files-found: error" 4

forbidden_base_override_hits="$(grep -F --count "PERF_RUNTIME_SELFTEST_CHURN_ROWS=" "$WORKFLOW" || true)"
if [ "${forbidden_base_override_hits:-0}" -gt 0 ]; then
  echo "runtime workflow must not hardcode PERF_RUNTIME_SELFTEST_CHURN_ROWS; use Makefile defaults" >&2
  exit 1
fi

forbidden_high_override_hits="$(grep -F --count "PERF_RUNTIME_SELFTEST_HIGH_CHURN_ROWS=" "$WORKFLOW" || true)"
if [ "${forbidden_high_override_hits:-0}" -gt 0 ]; then
  echo "runtime workflow must not hardcode PERF_RUNTIME_SELFTEST_HIGH_CHURN_ROWS; use Makefile defaults" >&2
  exit 1
fi

for forbidden_token in \
  "PERF_RUNTIME_SELFTEST_PORT=" \
  "PERF_RUNTIME_SELFTEST_ROWS=" \
  "PERF_RUNTIME_SELFTEST_ITERS=" \
  "PERF_RUNTIME_SELFTEST_CHURN_ITERS=" \
  "PERF_RUNTIME_SELFTEST_HIGH_PORT=" \
  "PERF_RUNTIME_SELFTEST_HIGH_ROWS=" \
  "PERF_RUNTIME_SELFTEST_HIGH_ITERS=" \
  "PERF_RUNTIME_SELFTEST_HIGH_CHURN_ITERS="; do
  forbidden_token_hits="$(grep -F --count "$forbidden_token" "$WORKFLOW" || true)"
  if [ "${forbidden_token_hits:-0}" -gt 0 ]; then
    echo "runtime workflow must not hardcode $forbidden_token; use Makefile defaults" >&2
    exit 1
  fi
done

echo "selftest_runtime_workflow_path_filters status=ok"
