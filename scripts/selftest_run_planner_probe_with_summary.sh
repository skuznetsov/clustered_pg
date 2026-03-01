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
RUN_SCRIPT="$SCRIPT_DIR/run_planner_probe_with_summary.sh"
if [ ! -x "$RUN_SCRIPT" ]; then
  echo "planner probe+summary script not executable: $RUN_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_planner_probe_with_summary_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

CALLS_LOG="$WORKDIR/mock_calls.log"
MOCK_LOG="$WORKDIR/mock_probe.log"
SUMMARY_DIR="$WORKDIR/summary_dir"

MOCK_PROBE_OK="$WORKDIR/mock_probe_ok.sh"
MOCK_PROBE_BAD="$WORKDIR/mock_probe_bad.sh"
MOCK_SUMMARY="$WORKDIR/mock_summary.sh"

cat >"$MOCK_PROBE_OK" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo "probe_call|rows_csv=\$1|port=\$2|out=\${3:-}" >>"$CALLS_LOG"
echo "planner_probe_begin|rows_csv=\$1|port=\$2"
echo "planner_probe_status=ok"
echo "planner_probe_output: $MOCK_LOG"
EOF

cat >"$MOCK_PROBE_BAD" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo "probe_call|rows_csv=\$1|port=\$2|out=\${3:-}" >>"$CALLS_LOG"
echo "planner_probe_begin|rows_csv=\$1|port=\$2"
echo "planner_probe_status=ok"
EOF

cat >"$MOCK_SUMMARY" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo "summary_call|log=\$1|format=\$2|out=\${3:-}" >>"$CALLS_LOG"
if [ "\$#" -ge 3 ] && [ -n "\${3:-}" ]; then
  mkdir -p "\$(dirname "\$3")"
  printf 'summary_format=%s\n' "\$2" >"\$3"
fi
echo "mock_summary_status=ok"
EOF

chmod +x "$MOCK_PROBE_OK" "$MOCK_PROBE_BAD" "$MOCK_SUMMARY"
touch "$MOCK_LOG"
mkdir -p "$SUMMARY_DIR"

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

: >"$CALLS_LOG"
OUT_OK_DEFAULT="$WORKDIR/out_ok_default.log"
PLANNER_PROBE_SCRIPT="$MOCK_PROBE_OK" \
PLANNER_SUMMARY_SCRIPT="$MOCK_SUMMARY" \
  "$RUN_SCRIPT" "100,200" "65508" "auto:/private/tmp" "json" >"$OUT_OK_DEFAULT"

if ! grep -Fq "planner_probe_summary_status=ok" "$OUT_OK_DEFAULT"; then
  echo "expected planner_probe_summary_status=ok in default-summary run output" >&2
  cat "$OUT_OK_DEFAULT" >&2
  exit 1
fi
EXPECTED_DEFAULT_SUMMARY="${MOCK_LOG%.log}.summary.json"
if ! grep -Fq "summary_call|log=$MOCK_LOG|format=json|out=$EXPECTED_DEFAULT_SUMMARY" "$CALLS_LOG"; then
  echo "expected summary invocation with derived default output path" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi
if [ ! -f "$EXPECTED_DEFAULT_SUMMARY" ]; then
  echo "expected default derived summary output file: $EXPECTED_DEFAULT_SUMMARY" >&2
  exit 1
fi

: >"$CALLS_LOG"
OUT_OK_AUTO_DIR="$WORKDIR/out_ok_auto_dir.log"
PLANNER_PROBE_SCRIPT="$MOCK_PROBE_OK" \
PLANNER_SUMMARY_SCRIPT="$MOCK_SUMMARY" \
  "$RUN_SCRIPT" "100,200" "65508" "auto:/private/tmp" "csv" "auto:$SUMMARY_DIR" >"$OUT_OK_AUTO_DIR"

EXPECTED_AUTO_SUMMARY="$SUMMARY_DIR/$(basename "${MOCK_LOG%.log}").summary.csv"
if ! grep -Fq "summary_call|log=$MOCK_LOG|format=csv|out=$EXPECTED_AUTO_SUMMARY" "$CALLS_LOG"; then
  echo "expected summary invocation with auto:<abs_dir> derived output path" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi
if [ ! -f "$EXPECTED_AUTO_SUMMARY" ]; then
  echo "expected auto:<abs_dir> derived summary output file: $EXPECTED_AUTO_SUMMARY" >&2
  exit 1
fi

: >"$CALLS_LOG"
expect_fail_contains "unsupported summary_format: bad (supported: json|csv)" \
  env PLANNER_PROBE_SCRIPT="$MOCK_PROBE_OK" PLANNER_SUMMARY_SCRIPT="$MOCK_SUMMARY" \
  "$RUN_SCRIPT" "100,200" "65508" "auto:/private/tmp" "bad"
if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/summary calls when summary_format preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

: >"$CALLS_LOG"
expect_fail_contains "failed to capture planner probe output path from probe run" \
  env PLANNER_PROBE_SCRIPT="$MOCK_PROBE_BAD" PLANNER_SUMMARY_SCRIPT="$MOCK_SUMMARY" \
  "$RUN_SCRIPT" "100,200" "65508" "auto:/private/tmp" "json"
if ! grep -Fq "probe_call|rows_csv=100,200|port=65508|out=auto:/private/tmp" "$CALLS_LOG"; then
  echo "expected probe invocation before missing-log-path failure" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi
if grep -Fq "summary_call|" "$CALLS_LOG"; then
  echo "expected no summary invocation when probe output path extraction fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

MISSING_PROBE="$WORKDIR/missing_probe.sh"
: >"$CALLS_LOG"
expect_fail_contains "probe script not executable: $MISSING_PROBE" \
  env PLANNER_PROBE_SCRIPT="$MISSING_PROBE" PLANNER_SUMMARY_SCRIPT="$MOCK_SUMMARY" \
  "$RUN_SCRIPT" "100,200" "65508"
if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/summary calls when probe script preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

MISSING_SUMMARY="$WORKDIR/missing_summary.sh"
: >"$CALLS_LOG"
expect_fail_contains "summary script not executable: $MISSING_SUMMARY" \
  env PLANNER_PROBE_SCRIPT="$MOCK_PROBE_OK" PLANNER_SUMMARY_SCRIPT="$MISSING_SUMMARY" \
  "$RUN_SCRIPT" "100,200" "65508"
if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/summary calls when summary script preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

: >"$CALLS_LOG"
expect_fail_contains "summary auto directory must be an absolute path: auto:relative/path" \
  env PLANNER_PROBE_SCRIPT="$MOCK_PROBE_OK" PLANNER_SUMMARY_SCRIPT="$MOCK_SUMMARY" \
  "$RUN_SCRIPT" "100,200" "65508" "auto:/private/tmp" "json" "auto:relative/path"
if ! grep -Fq "probe_call|rows_csv=100,200|port=65508|out=auto:/private/tmp" "$CALLS_LOG"; then
  echo "expected probe invocation before relative auto-summary-dir failure" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi
if grep -Fq "summary_call|" "$CALLS_LOG"; then
  echo "expected no summary invocation when auto summary dir is invalid" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

echo "selftest_run_planner_probe_with_summary status=ok"
