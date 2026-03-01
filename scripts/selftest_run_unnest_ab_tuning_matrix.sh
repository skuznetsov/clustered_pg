#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [tmp_root_abs_dir]" >&2
  exit 2
fi

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp_root_abs_dir not found: $TMP_ROOT" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MATRIX_SCRIPT="$SCRIPT_DIR/run_unnest_ab_tuning_matrix.sh"
if [ ! -x "$MATRIX_SCRIPT" ]; then
  echo "matrix script not executable: $MATRIX_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_unnest_tuning_matrix_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

CALLS_LOG="$WORKDIR/calls.log"
MOCK_PROBE="$WORKDIR/mock_probe.sh"

cat >"$MOCK_PROBE" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

calls_log="${UNNEST_TUNE_CALLS_LOG:?}"
if [ "$#" -lt 10 ]; then
  echo "mock probe expected >= 10 args, got: $#" >&2
  exit 2
fi
port="$6"
out="$7"
trigger="$8"
min_distinct="$9"
max_tids="${10}"
echo "probe|port=$port|out=$out|trigger=$trigger|min_distinct=$min_distinct|max_tids=$max_tids" >> "$calls_log"
echo "ratio_kv|insert=1.050000|join_unnest=1.100000|any_array=1.020000"
EOF
chmod +x "$MOCK_PROBE"

OUT_OK="$WORKDIR/out_ok.log"
OUT_MATRIX="$WORKDIR/matrix.log"
PROBE_OUT_ROOT="$WORKDIR/probe_out"
mkdir -p "$PROBE_OUT_ROOT"
: >"$CALLS_LOG"
UNNEST_TUNE_CALLS_LOG="$CALLS_LOG" \
UNNEST_TUNE_PROBE_SCRIPT="$MOCK_PROBE" \
UNNEST_TUNE_PROBE_OUT_ROOT="$PROBE_OUT_ROOT" \
"$MATRIX_SCRIPT" 1 20 5 10 8 65400 "$OUT_MATRIX" "1,2" "2" "32,64" >"$OUT_OK" 2>&1

if ! grep -Fq "matrix_begin|" "$OUT_OK"; then
  echo "expected matrix_begin marker in tuning-matrix output" >&2
  cat "$OUT_OK" >&2
  exit 1
fi
if ! grep -Fq "case_count=4" "$OUT_OK"; then
  echo "expected case_count=4 in matrix_begin output" >&2
  cat "$OUT_OK" >&2
  exit 1
fi
if ! grep -Fq "derived_max_port=65403" "$OUT_OK"; then
  echo "expected derived_max_port=65403 in matrix_begin output" >&2
  cat "$OUT_OK" >&2
  exit 1
fi
if ! grep -Fq "probe_out_root=$PROBE_OUT_ROOT" "$OUT_OK"; then
  echo "expected probe_out_root marker in matrix_begin output" >&2
  cat "$OUT_OK" >&2
  exit 1
fi
if ! grep -Fq "matrix_best_join_unnest|" "$OUT_OK"; then
  echo "expected matrix_best_join_unnest marker in tuning-matrix output" >&2
  cat "$OUT_OK" >&2
  exit 1
fi
if [ ! -f "$OUT_MATRIX" ]; then
  echo "expected matrix output log file: $OUT_MATRIX" >&2
  exit 1
fi

probe_calls="$(grep -c '^probe|' "$CALLS_LOG" || true)"
if [ "$probe_calls" -ne 4 ]; then
  echo "expected 4 probe calls for 2x1x2 matrix, got: $probe_calls" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

for expected_port in 65400 65401 65402 65403; do
  if ! grep -Fq "probe|port=$expected_port|out=auto:$PROBE_OUT_ROOT|" "$CALLS_LOG"; then
    echo "expected probe call for port=$expected_port" >&2
    cat "$CALLS_LOG" >&2
    exit 1
  fi
done

OUT_FAIL_OVERFLOW="$WORKDIR/out_fail_overflow.log"
: >"$CALLS_LOG"
if UNNEST_TUNE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_TUNE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_TUNE_PROBE_OUT_ROOT="$PROBE_OUT_ROOT" \
  "$MATRIX_SCRIPT" 1 20 5 10 8 65533 "$WORKDIR/matrix_fail.log" "1,2" "2" "32,64" >"$OUT_FAIL_OVERFLOW" 2>&1; then
  echo "expected failure for derived tuning-matrix port overflow" >&2
  cat "$OUT_FAIL_OVERFLOW" >&2
  exit 1
fi
if ! grep -Fq "base_port too high for tuning matrix case-count:" "$OUT_FAIL_OVERFLOW"; then
  echo "expected overflow validation marker in tuning-matrix output" >&2
  cat "$OUT_FAIL_OVERFLOW" >&2
  exit 1
fi
if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe calls when overflow preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

OUT_FAIL_PROBE="$WORKDIR/out_fail_probe.log"
: >"$CALLS_LOG"
if UNNEST_TUNE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_TUNE_PROBE_SCRIPT="$WORKDIR/missing_probe.sh" \
  UNNEST_TUNE_PROBE_OUT_ROOT="$PROBE_OUT_ROOT" \
  "$MATRIX_SCRIPT" 1 20 5 10 8 65400 "$WORKDIR/matrix_fail_probe.log" "1,2" "2" "32,64" >"$OUT_FAIL_PROBE" 2>&1; then
  echo "expected failure for non-executable probe override" >&2
  cat "$OUT_FAIL_PROBE" >&2
  exit 1
fi
if ! grep -Fq "probe script not executable:" "$OUT_FAIL_PROBE"; then
  echo "expected probe-script executable validation marker" >&2
  cat "$OUT_FAIL_PROBE" >&2
  exit 1
fi
if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe invocations when probe-script preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

OUT_FAIL_PROBE_OUT_ROOT="$WORKDIR/out_fail_probe_out_root.log"
: >"$CALLS_LOG"
if UNNEST_TUNE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_TUNE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_TUNE_PROBE_OUT_ROOT="relative/path" \
  "$MATRIX_SCRIPT" 1 20 5 10 8 65400 "$WORKDIR/matrix_fail_probe_out_root.log" "1,2" "2" "32,64" >"$OUT_FAIL_PROBE_OUT_ROOT" 2>&1; then
  echo "expected failure for non-absolute probe_out_root override" >&2
  cat "$OUT_FAIL_PROBE_OUT_ROOT" >&2
  exit 1
fi
if ! grep -Fq "probe_out_root must be an absolute path:" "$OUT_FAIL_PROBE_OUT_ROOT"; then
  echo "expected probe_out_root absolute-path validation marker" >&2
  cat "$OUT_FAIL_PROBE_OUT_ROOT" >&2
  exit 1
fi
if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe invocations when probe_out_root absolute-path preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

OUT_FAIL_PROBE_OUT_ROOT_MISSING="$WORKDIR/out_fail_probe_out_root_missing.log"
MISSING_PROBE_OUT_ROOT="$WORKDIR/missing_probe_out_root"
: >"$CALLS_LOG"
if UNNEST_TUNE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_TUNE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_TUNE_PROBE_OUT_ROOT="$MISSING_PROBE_OUT_ROOT" \
  "$MATRIX_SCRIPT" 1 20 5 10 8 65400 "$WORKDIR/matrix_fail_probe_out_root_missing.log" "1,2" "2" "32,64" >"$OUT_FAIL_PROBE_OUT_ROOT_MISSING" 2>&1; then
  echo "expected failure for non-existent absolute probe_out_root override" >&2
  cat "$OUT_FAIL_PROBE_OUT_ROOT_MISSING" >&2
  exit 1
fi
if ! grep -Fq "probe_out_root not found: $MISSING_PROBE_OUT_ROOT" "$OUT_FAIL_PROBE_OUT_ROOT_MISSING"; then
  echo "expected probe_out_root existence validation marker" >&2
  cat "$OUT_FAIL_PROBE_OUT_ROOT_MISSING" >&2
  exit 1
fi
if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe invocations when probe_out_root existence preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

echo "selftest_run_unnest_ab_tuning_matrix status=ok"
