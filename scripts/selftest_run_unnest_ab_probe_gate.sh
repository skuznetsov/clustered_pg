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
GATE_SCRIPT="$SCRIPT_DIR/run_unnest_ab_probe_gate.sh"
if [ ! -x "$GATE_SCRIPT" ]; then
  echo "gate script not executable: $GATE_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_unnest_gate_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

CALLS_LOG="$WORKDIR/calls.log"
MOCK_PROBE="$WORKDIR/mock_probe.sh"
MOCK_COMPARE="$WORKDIR/mock_compare.sh"

cat >"$MOCK_PROBE" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

calls_log="${UNNEST_GATE_CALLS_LOG:?}"
if [ "$#" -lt 7 ]; then
  echo "mock_probe expected >= 7 args, got $#" >&2
  exit 2
fi
port="$6"
out="$7"
echo "probe|port=$port|out=$out" >> "$calls_log"

case "$out" in
  auto:*)
    out_dir="${out#auto:}"
    mkdir -p "$out_dir"
    cat >"$out_dir/clustered_pg_unnest_ab_mock_${port}.log" <<EOM
ratio_kv|insert=1.00|join_unnest=1.00|any_array=1.00
EOM
    ;;
  *)
    echo "mock_probe expected auto:<dir> output target, got: $out" >&2
    exit 2
    ;;
esac
EOF

cat >"$MOCK_COMPARE" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

calls_log="${UNNEST_GATE_CALLS_LOG:?}"
ref_dir="$1"
new_dir="$2"
min_fraction="$3"
stat_mode="$4"
min_samples="${5:-}"
echo "compare|ref=$ref_dir|new=$new_dir|min_fraction=$min_fraction|stat_mode=$stat_mode|min_samples=$min_samples" >> "$calls_log"

ref_count="$(find "$ref_dir" -maxdepth 1 -type f -name 'clustered_pg_unnest_ab_*.log' | wc -l | tr -d ' ')"
new_count="$(find "$new_dir" -maxdepth 1 -type f -name 'clustered_pg_unnest_ab_*.log' | wc -l | tr -d ' ')"
if [ "$ref_count" -lt 2 ] || [ "$new_count" -lt 3 ]; then
  echo "unexpected log counts ref=$ref_count new=$new_count" >&2
  exit 1
fi

echo "unnest_ab_set_compare status=ok"
EOF

chmod +x "$MOCK_PROBE" "$MOCK_COMPARE"

extract_gate_field() {
  local line="$1"
  local key="$2"

  printf '%s\n' "$line" | tr '|' '\n' | awk -F= -v k="$key" '$1 == k { print substr($0, length(k) + 2); exit }'
}

OUT_OK="$WORKDIR/out_ok.log"
: >"$CALLS_LOG"
UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
"$GATE_SCRIPT" 2 3 200 10 20 32 65400 "$WORKDIR" 0.90 median 1 >"$OUT_OK" 2>&1

if ! grep -Fq "unnest_ab_probe_gate_status=ok" "$OUT_OK"; then
  echo "expected unnest_ab_probe_gate_status=ok in gate output" >&2
  cat "$OUT_OK" >&2
  exit 1
fi

ok_gate_line="$(grep -F 'unnest_ab_probe_gate_output|' "$OUT_OK" | tail -n 1 || true)"
if [ -z "$ok_gate_line" ]; then
  echo "expected gate output line in normal run" >&2
  cat "$OUT_OK" >&2
  exit 1
fi

ok_ref_dir="$(extract_gate_field "$ok_gate_line" "reference_dir")"
ok_new_dir="$(extract_gate_field "$ok_gate_line" "candidate_dir")"
ok_ref_retained="$(extract_gate_field "$ok_gate_line" "reference_retained")"
if [ -z "$ok_ref_dir" ] || [ -z "$ok_new_dir" ] || [ -z "$ok_ref_retained" ]; then
  echo "expected reference_dir/candidate_dir/reference_retained fields in normal run" >&2
  echo "$ok_gate_line" >&2
  exit 1
fi
if [ ! -d "$ok_ref_dir" ] || [ ! -d "$ok_new_dir" ]; then
  echo "expected both reference and candidate directories to exist in default mode" >&2
  echo "$ok_gate_line" >&2
  exit 1
fi
if [ "$ok_ref_retained" != "1" ]; then
  echo "expected reference_retained=1 in default mode, got: $ok_ref_retained" >&2
  echo "$ok_gate_line" >&2
  exit 1
fi

probe_calls="$(grep -c '^probe|' "$CALLS_LOG" || true)"
if [ "$probe_calls" -ne 5 ]; then
  echo "expected 5 probe calls, got $probe_calls" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

compare_calls="$(grep -c '^compare|' "$CALLS_LOG" || true)"
if [ "$compare_calls" -ne 1 ]; then
  echo "expected 1 compare call, got $compare_calls" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

OUT_P05="$WORKDIR/out_p05.log"
: >"$CALLS_LOG"
UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
"$GATE_SCRIPT" 2 3 200 10 20 32 65400 "$WORKDIR" 0.90 p05 3 >"$OUT_P05" 2>&1

if ! grep -Fq "unnest_ab_probe_gate_status=ok" "$OUT_P05"; then
  echo "expected unnest_ab_probe_gate_status=ok for p05 mode" >&2
  cat "$OUT_P05" >&2
  exit 1
fi

if ! grep -Fq "stat_mode=p05" "$CALLS_LOG"; then
  echo "expected compare call with stat_mode=p05" >&2
  cat "$CALLS_LOG" >&2
  cat "$OUT_P05" >&2
  exit 1
fi

OUT_KEEP_NEW="$WORKDIR/out_keep_new.log"
: >"$CALLS_LOG"
UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
UNNEST_GATE_KEEP_NEW_DIR=on \
"$GATE_SCRIPT" 2 3 200 10 20 32 65400 "$WORKDIR" 0.90 median 1 >"$OUT_KEEP_NEW" 2>&1

if ! grep -Fq "unnest_ab_probe_gate_status=ok" "$OUT_KEEP_NEW"; then
  echo "expected unnest_ab_probe_gate_status=ok in keep-new-only mode" >&2
  cat "$OUT_KEEP_NEW" >&2
  exit 1
fi

keep_gate_line="$(grep -F 'unnest_ab_probe_gate_output|' "$OUT_KEEP_NEW" | tail -n 1 || true)"
if [ -z "$keep_gate_line" ]; then
  echo "expected gate output line in keep-new-only mode" >&2
  cat "$OUT_KEEP_NEW" >&2
  exit 1
fi

keep_ref_dir="$(extract_gate_field "$keep_gate_line" "reference_dir")"
keep_new_dir="$(extract_gate_field "$keep_gate_line" "candidate_dir")"
keep_ref_retained="$(extract_gate_field "$keep_gate_line" "reference_retained")"
keep_ref_source="$(extract_gate_field "$keep_gate_line" "reference_source")"
if [ -z "$keep_ref_dir" ] || [ -z "$keep_new_dir" ] || [ -z "$keep_ref_retained" ] || [ -z "$keep_ref_source" ]; then
  echo "expected reference_dir/candidate_dir/reference_source/reference_retained fields in keep-new-only mode" >&2
  echo "$keep_gate_line" >&2
  exit 1
fi
if [ "$keep_ref_source" != "generated" ]; then
  echo "expected reference_source=generated in keep-new-only mode, got: $keep_ref_source" >&2
  echo "$keep_gate_line" >&2
  exit 1
fi
if [ "$keep_ref_retained" != "0" ]; then
  echo "expected reference_retained=0 in keep-new-only mode, got: $keep_ref_retained" >&2
  echo "$keep_gate_line" >&2
  exit 1
fi
if [ -d "$keep_ref_dir" ]; then
  echo "expected generated reference directory to be removed in keep-new-only mode" >&2
  echo "$keep_gate_line" >&2
  exit 1
fi
if [ ! -d "$keep_new_dir" ]; then
  echo "expected candidate directory to exist in keep-new-only mode" >&2
  echo "$keep_gate_line" >&2
  exit 1
fi

EXISTING_REF="$WORKDIR/existing_ref"
mkdir -p "$EXISTING_REF"
cat >"$EXISTING_REF/clustered_pg_unnest_ab_existing_1.log" <<'EOF'
ratio_kv|insert=1.00|join_unnest=1.00|any_array=1.00
EOF
cat >"$EXISTING_REF/clustered_pg_unnest_ab_existing_2.log" <<'EOF'
ratio_kv|insert=1.01|join_unnest=1.01|any_array=1.01
EOF

OUT_EXISTING="$WORKDIR/out_existing.log"
: >"$CALLS_LOG"
UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
UNNEST_GATE_EXISTING_REF_DIR="$EXISTING_REF" \
"$GATE_SCRIPT" 0 3 200 10 20 32 65400 "$WORKDIR" 0.90 median 1 >"$OUT_EXISTING" 2>&1

if ! grep -Fq "reference_source=existing" "$OUT_EXISTING"; then
  echo "expected reference_source=existing in gate output" >&2
  cat "$OUT_EXISTING" >&2
  exit 1
fi

if ! grep -Fq "reference_retained=1" "$OUT_EXISTING"; then
  echo "expected reference_retained=1 with existing reference dir" >&2
  cat "$OUT_EXISTING" >&2
  exit 1
fi

probe_calls_existing="$(grep -c '^probe|' "$CALLS_LOG" || true)"
if [ "$probe_calls_existing" -ne 3 ]; then
  echo "expected 3 probe calls with existing reference dir, got $probe_calls_existing" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

OUT_FAIL_REF_RUNS="$WORKDIR/out_fail_ref_runs.log"
if UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
  "$GATE_SCRIPT" 0 1 200 10 20 32 65400 "$WORKDIR" 0.90 median 1 >"$OUT_FAIL_REF_RUNS" 2>&1; then
  echo "expected failure for ref_runs=0 without existing reference dir" >&2
  cat "$OUT_FAIL_REF_RUNS" >&2
  exit 1
fi

if ! grep -Fq "ref_runs must be > 0 when UNNEST_GATE_EXISTING_REF_DIR is not set" "$OUT_FAIL_REF_RUNS"; then
  echo "expected ref_runs validation error" >&2
  cat "$OUT_FAIL_REF_RUNS" >&2
  exit 1
fi

OUT_FAIL_EXISTING_REF_REL="$WORKDIR/out_fail_existing_ref_rel.log"
: >"$CALLS_LOG"
if UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
  UNNEST_GATE_EXISTING_REF_DIR="relative/ref" \
  "$GATE_SCRIPT" 0 1 200 10 20 32 65400 "$WORKDIR" 0.90 median 1 >"$OUT_FAIL_EXISTING_REF_REL" 2>&1; then
  echo "expected failure for non-absolute UNNEST_GATE_EXISTING_REF_DIR" >&2
  cat "$OUT_FAIL_EXISTING_REF_REL" >&2
  exit 1
fi

if ! grep -Fq "UNNEST_GATE_EXISTING_REF_DIR must be absolute: relative/ref" "$OUT_FAIL_EXISTING_REF_REL"; then
  echo "expected UNNEST_GATE_EXISTING_REF_DIR absolute-path validation error" >&2
  cat "$OUT_FAIL_EXISTING_REF_REL" >&2
  exit 1
fi

if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/compare calls when UNNEST_GATE_EXISTING_REF_DIR absolute-path validation fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

OUT_FAIL_EXISTING_REF_MISSING="$WORKDIR/out_fail_existing_ref_missing.log"
MISSING_EXISTING_REF="$WORKDIR/missing_existing_ref"
: >"$CALLS_LOG"
if UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
  UNNEST_GATE_EXISTING_REF_DIR="$MISSING_EXISTING_REF" \
  "$GATE_SCRIPT" 0 1 200 10 20 32 65400 "$WORKDIR" 0.90 median 1 >"$OUT_FAIL_EXISTING_REF_MISSING" 2>&1; then
  echo "expected failure for missing UNNEST_GATE_EXISTING_REF_DIR" >&2
  cat "$OUT_FAIL_EXISTING_REF_MISSING" >&2
  exit 1
fi

if ! grep -Fq "UNNEST_GATE_EXISTING_REF_DIR not found: $MISSING_EXISTING_REF" "$OUT_FAIL_EXISTING_REF_MISSING"; then
  echo "expected UNNEST_GATE_EXISTING_REF_DIR existence validation error" >&2
  cat "$OUT_FAIL_EXISTING_REF_MISSING" >&2
  exit 1
fi

if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/compare calls when UNNEST_GATE_EXISTING_REF_DIR existence validation fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

OUT_FAIL_OUT_ROOT_REL="$WORKDIR/out_fail_out_root_rel.log"
: >"$CALLS_LOG"
if UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
  "$GATE_SCRIPT" 1 1 200 10 20 32 65400 "relative_out_root" 0.90 median 1 >"$OUT_FAIL_OUT_ROOT_REL" 2>&1; then
  echo "expected failure for non-absolute out_root_abs_dir" >&2
  cat "$OUT_FAIL_OUT_ROOT_REL" >&2
  exit 1
fi

if ! grep -Fq "out_root_abs_dir must be absolute: relative_out_root" "$OUT_FAIL_OUT_ROOT_REL"; then
  echo "expected out_root absolute-path validation error" >&2
  cat "$OUT_FAIL_OUT_ROOT_REL" >&2
  exit 1
fi

if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/compare calls when out_root absolute-path validation fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

OUT_FAIL_OUT_ROOT_MISSING="$WORKDIR/out_fail_out_root_missing.log"
MISSING_OUT_ROOT="$WORKDIR/missing_out_root"
: >"$CALLS_LOG"
if UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
  "$GATE_SCRIPT" 1 1 200 10 20 32 65400 "$MISSING_OUT_ROOT" 0.90 median 1 >"$OUT_FAIL_OUT_ROOT_MISSING" 2>&1; then
  echo "expected failure for missing out_root_abs_dir" >&2
  cat "$OUT_FAIL_OUT_ROOT_MISSING" >&2
  exit 1
fi

if ! grep -Fq "out_root_abs_dir not found: $MISSING_OUT_ROOT" "$OUT_FAIL_OUT_ROOT_MISSING"; then
  echo "expected out_root existence validation error" >&2
  cat "$OUT_FAIL_OUT_ROOT_MISSING" >&2
  exit 1
fi

if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/compare calls when out_root existence validation fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

OUT_FAIL_PROBE_SCRIPT="$WORKDIR/out_fail_probe_script.log"
MISSING_PROBE_SCRIPT="$WORKDIR/missing_probe.sh"
: >"$CALLS_LOG"
if UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_GATE_PROBE_SCRIPT="$MISSING_PROBE_SCRIPT" \
  UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
  "$GATE_SCRIPT" 1 1 200 10 20 32 65400 "$WORKDIR" 0.90 median 1 >"$OUT_FAIL_PROBE_SCRIPT" 2>&1; then
  echo "expected failure for non-executable probe script override" >&2
  cat "$OUT_FAIL_PROBE_SCRIPT" >&2
  exit 1
fi

if ! grep -Fq "probe script not executable: $MISSING_PROBE_SCRIPT" "$OUT_FAIL_PROBE_SCRIPT"; then
  echo "expected probe script preflight error" >&2
  cat "$OUT_FAIL_PROBE_SCRIPT" >&2
  exit 1
fi

if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/compare calls when probe script preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

OUT_FAIL_COMPARE_SCRIPT="$WORKDIR/out_fail_compare_script.log"
MISSING_COMPARE_SCRIPT="$WORKDIR/missing_compare.sh"
: >"$CALLS_LOG"
if UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_GATE_COMPARE_SCRIPT="$MISSING_COMPARE_SCRIPT" \
  "$GATE_SCRIPT" 1 1 200 10 20 32 65400 "$WORKDIR" 0.90 median 1 >"$OUT_FAIL_COMPARE_SCRIPT" 2>&1; then
  echo "expected failure for non-executable compare script override" >&2
  cat "$OUT_FAIL_COMPARE_SCRIPT" >&2
  exit 1
fi

if ! grep -Fq "compare script not executable: $MISSING_COMPARE_SCRIPT" "$OUT_FAIL_COMPARE_SCRIPT"; then
  echo "expected compare script preflight error" >&2
  cat "$OUT_FAIL_COMPARE_SCRIPT" >&2
  exit 1
fi

if [ -s "$CALLS_LOG" ]; then
  echo "expected no probe/compare calls when compare script preflight fails" >&2
  cat "$CALLS_LOG" >&2
  exit 1
fi

OUT_FAIL_REF_PORT_HEADROOM="$WORKDIR/out_fail_ref_port_headroom.log"
if UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
  "$GATE_SCRIPT" 140 1 200 10 20 32 65400 "$WORKDIR" 0.90 median 1 >"$OUT_FAIL_REF_PORT_HEADROOM" 2>&1; then
  echo "expected failure for ref-run-derived port overflow" >&2
  cat "$OUT_FAIL_REF_PORT_HEADROOM" >&2
  exit 1
fi

if ! grep -Fq "base_port too high for run counts: ref_max_port=" "$OUT_FAIL_REF_PORT_HEADROOM"; then
  echo "expected derived port-headroom validation error for ref runs" >&2
  cat "$OUT_FAIL_REF_PORT_HEADROOM" >&2
  exit 1
fi

OUT_FAIL="$WORKDIR/out_fail.log"
if UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
  "$GATE_SCRIPT" 1 1 200 10 20 32 65400 "$WORKDIR" 0.90 p95 3 >"$OUT_FAIL" 2>&1; then
  echo "expected failure for p95 without explicit optimistic-tail override" >&2
  cat "$OUT_FAIL" >&2
  exit 1
fi

if ! grep -Fq "stat_mode 'p95' is optimistic for throughput ratios; use median|p05|trimmed-mean or set UNNEST_GATE_ALLOW_OPTIMISTIC_TAIL=on to override" "$OUT_FAIL"; then
  echo "expected optimistic-tail guard error" >&2
  cat "$OUT_FAIL" >&2
  exit 1
fi

OUT_FAIL_MIN_SAMPLES="$WORKDIR/out_fail_min_samples.log"
if UNNEST_GATE_CALLS_LOG="$CALLS_LOG" \
  UNNEST_GATE_PROBE_SCRIPT="$MOCK_PROBE" \
  UNNEST_GATE_COMPARE_SCRIPT="$MOCK_COMPARE" \
  UNNEST_GATE_ALLOW_OPTIMISTIC_TAIL=on \
  "$GATE_SCRIPT" 1 1 200 10 20 32 65400 "$WORKDIR" 0.90 p95 2 >"$OUT_FAIL_MIN_SAMPLES" 2>&1; then
  echo "expected failure for p95 with min_samples=2" >&2
  cat "$OUT_FAIL_MIN_SAMPLES" >&2
  exit 1
fi

if ! grep -Fq "min_samples must be >= 3 for stat_mode 'p95'" "$OUT_FAIL_MIN_SAMPLES"; then
  echo "expected p95 min_samples validation error" >&2
  cat "$OUT_FAIL_MIN_SAMPLES" >&2
  exit 1
fi

echo "selftest_run_unnest_ab_probe_gate status=ok"
