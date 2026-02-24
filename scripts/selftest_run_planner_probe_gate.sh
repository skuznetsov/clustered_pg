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
GATE_SCRIPT="$SCRIPT_DIR/run_planner_probe_gate.sh"
if [ ! -x "$GATE_SCRIPT" ]; then
  echo "gate script not executable: $GATE_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_planner_gate_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

MOCK_PROBE_OK="$WORKDIR/mock_probe_ok.sh"
MOCK_PROBE_BAD="$WORKDIR/mock_probe_bad.sh"
MOCK_CHECK="$WORKDIR/mock_check.sh"
MOCK_DEFAULT_CHECK="$WORKDIR/mock_default_check.sh"
MOCK_LOG="$WORKDIR/mock_probe.log"
CALLS_LOG="$WORKDIR/mock_calls.log"

cat >"$MOCK_PROBE_OK" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo "probe|rows_csv=\$1|port=\$2|out=\${3:-}|format=\${4:-}|summary_out=\${5:-}" >>"$CALLS_LOG"
echo "planner_probe_begin|rows_csv=\$1|port=\$2"
echo "planner_probe_status=ok"
echo "planner_probe_output: $MOCK_LOG"
echo "planner_probe_summary_status=ok"
EOF

cat >"$MOCK_PROBE_BAD" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo "probe|rows_csv=\$1|port=\$2|out=\${3:-}|format=\${4:-}|summary_out=\${5:-}" >>"$CALLS_LOG"
echo "planner_probe_begin|rows_csv=\$1|port=\$2"
echo "planner_probe_status=ok"
EOF

cat >"$MOCK_CHECK" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo "ratio_check|log=\$1|min_ratio=\$2" >>"$CALLS_LOG"
echo "mock_ratio_check args=\$1,\$2"
EOF

cat >"$MOCK_DEFAULT_CHECK" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo "default_check|log=\$1|min_rows=\$2" >>"$CALLS_LOG"
echo "mock_default_check args=\$1,\$2"
EOF

chmod +x "$MOCK_PROBE_OK" "$MOCK_PROBE_BAD" "$MOCK_CHECK" "$MOCK_DEFAULT_CHECK"
touch "$MOCK_LOG" "$CALLS_LOG"

expect_fail_contains() {
  local expected="$1"
  shift
  local out="$WORKDIR/expect_fail.out"
  if "$@" >"$out" 2>&1; then
    echo "expected failure but command succeeded: $*" >&2
    cat "$out" >&2
    exit 1
  fi
  if ! grep -Fq "$expected" "$out"; then
    echo "expected failure output to contain: $expected" >&2
    cat "$out" >&2
    exit 1
  fi
}

out="$(
  PLANNER_PROBE_SUMMARY_SCRIPT="$MOCK_PROBE_OK" \
  PLANNER_RATIO_CHECK_SCRIPT="$MOCK_CHECK" \
  PLANNER_DEFAULT_PATH_CHECK_SCRIPT="$MOCK_DEFAULT_CHECK" \
  "$GATE_SCRIPT" "100,200" "65508" "auto:/private/tmp" "json" "auto:/private/tmp" "123.0" "10000"
)"
printf "%s\n" "$out"

if ! printf "%s\n" "$out" | grep -Fq "planner_probe_gate_status=ok"; then
  echo "expected planner_probe_gate_status=ok" >&2
  exit 1
fi
if ! printf "%s\n" "$out" | grep -Fq "mock_ratio_check args=$MOCK_LOG,123.0"; then
  echo "expected mocked ratio checker invocation" >&2
  exit 1
fi
if ! printf "%s\n" "$out" | grep -Fq "mock_default_check args=$MOCK_LOG,10000"; then
  echo "expected mocked default-path checker invocation" >&2
  exit 1
fi
if ! grep -Fq "probe|rows_csv=100,200|port=65508|out=auto:/private/tmp|format=json|summary_out=auto:/private/tmp" "$CALLS_LOG"; then
  echo "expected probe-summary invocation in success path" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi
if ! grep -Fq "ratio_check|log=$MOCK_LOG|min_ratio=123.0" "$CALLS_LOG"; then
  echo "expected ratio check invocation in success path" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi
if ! grep -Fq "default_check|log=$MOCK_LOG|min_rows=10000" "$CALLS_LOG"; then
  echo "expected default-path check invocation in success path" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

: >"$CALLS_LOG"
expect_fail_contains "failed to capture planner probe log path from probe-summary output" \
  env PLANNER_PROBE_SUMMARY_SCRIPT="$MOCK_PROBE_BAD" PLANNER_RATIO_CHECK_SCRIPT="$MOCK_CHECK" PLANNER_DEFAULT_PATH_CHECK_SCRIPT="$MOCK_DEFAULT_CHECK" \
  "$GATE_SCRIPT" "100,200" "65508" "auto:/private/tmp" "json" "auto:/private/tmp" "123.0" "10000"
if ! grep -Fq "probe|rows_csv=100,200|port=65508|out=auto:/private/tmp|format=json|summary_out=auto:/private/tmp" "$CALLS_LOG"; then
  echo "expected probe-summary invocation before missing-log-path failure" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi
if grep -Fq "ratio_check|" "$CALLS_LOG" || grep -Fq "default_check|" "$CALLS_LOG"; then
  echo "expected no ratio/default checks when probe log path extraction fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

MISSING_PROBE_SUMMARY="$WORKDIR/missing_probe_summary.sh"
: >"$CALLS_LOG"
expect_fail_contains "probe-summary script not executable: $MISSING_PROBE_SUMMARY" \
  env PLANNER_PROBE_SUMMARY_SCRIPT="$MISSING_PROBE_SUMMARY" PLANNER_RATIO_CHECK_SCRIPT="$MOCK_CHECK" PLANNER_DEFAULT_PATH_CHECK_SCRIPT="$MOCK_DEFAULT_CHECK" \
  "$GATE_SCRIPT" "100,200" "65508" "auto:/private/tmp" "json" "auto:/private/tmp" "123.0" "10000"
if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/check calls when probe-summary preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

MISSING_RATIO_CHECK="$WORKDIR/missing_ratio_check.sh"
: >"$CALLS_LOG"
expect_fail_contains "ratio check script not executable: $MISSING_RATIO_CHECK" \
  env PLANNER_PROBE_SUMMARY_SCRIPT="$MOCK_PROBE_OK" PLANNER_RATIO_CHECK_SCRIPT="$MISSING_RATIO_CHECK" PLANNER_DEFAULT_PATH_CHECK_SCRIPT="$MOCK_DEFAULT_CHECK" \
  "$GATE_SCRIPT" "100,200" "65508" "auto:/private/tmp" "json" "auto:/private/tmp" "123.0" "10000"
if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/check calls when ratio-check preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

MISSING_DEFAULT_CHECK="$WORKDIR/missing_default_check.sh"
: >"$CALLS_LOG"
expect_fail_contains "default-path check script not executable: $MISSING_DEFAULT_CHECK" \
  env PLANNER_PROBE_SUMMARY_SCRIPT="$MOCK_PROBE_OK" PLANNER_RATIO_CHECK_SCRIPT="$MOCK_CHECK" PLANNER_DEFAULT_PATH_CHECK_SCRIPT="$MISSING_DEFAULT_CHECK" \
  "$GATE_SCRIPT" "100,200" "65508" "auto:/private/tmp" "json" "auto:/private/tmp" "123.0" "10000"
if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/check calls when default-check preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

echo "selftest_run_planner_probe_gate status=ok"
