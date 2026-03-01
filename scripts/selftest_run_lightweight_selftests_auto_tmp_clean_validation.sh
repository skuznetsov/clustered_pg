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
RUNNER_SCRIPT="$SCRIPT_DIR/run_lightweight_selftests.sh"
TMP_CLEAN_SCRIPT="$SCRIPT_DIR/tmp_clean_pg_sorted_heap.sh"

if [ ! -x "$RUNNER_SCRIPT" ]; then
  echo "runner script not executable: $RUNNER_SCRIPT" >&2
  exit 2
fi
if [ ! -f "$TMP_CLEAN_SCRIPT" ]; then
  echo "tmp-clean script not found: $TMP_CLEAN_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_lightweight_auto_clean_validation.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

OUT_MODE="$WORKDIR/invalid_mode.out"
set +e
LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN="maybe" "$RUNNER_SCRIPT" "$TMP_ROOT" jsonl >"$OUT_MODE" 2>&1
rc_mode=$?
set -e
if [ "$rc_mode" -ne 2 ]; then
  echo "expected runner to fail with exit code 2 for invalid auto-clean mode, got rc=$rc_mode" >&2
  cat "$OUT_MODE" >&2
  exit 1
fi
if ! grep -Fq "LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN must be one of: off|on|0|1|false|true|no|yes (got: maybe)" "$OUT_MODE"; then
  echo "expected invalid auto-clean mode error marker" >&2
  cat "$OUT_MODE" >&2
  exit 1
fi
if grep -Fq '"event":"' "$OUT_MODE"; then
  echo "unexpected JSONL events for invalid auto-clean mode preflight failure" >&2
  cat "$OUT_MODE" >&2
  exit 1
fi

OUT_AGE="$WORKDIR/invalid_age.out"
set +e
LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN="on" LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN_MIN_AGE_S="bad" "$RUNNER_SCRIPT" "$TMP_ROOT" jsonl >"$OUT_AGE" 2>&1
rc_age=$?
set -e
if [ "$rc_age" -ne 2 ]; then
  echo "expected runner to fail with exit code 2 for invalid auto-clean min-age, got rc=$rc_age" >&2
  cat "$OUT_AGE" >&2
  exit 1
fi
if ! grep -Fq "LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN_MIN_AGE_S must be a non-negative integer when auto-clean is enabled: bad" "$OUT_AGE"; then
  echo "expected invalid auto-clean min-age error marker" >&2
  cat "$OUT_AGE" >&2
  exit 1
fi
if grep -Fq '"event":"' "$OUT_AGE"; then
  echo "unexpected JSONL events for invalid auto-clean min-age preflight failure" >&2
  cat "$OUT_AGE" >&2
  exit 1
fi

LOCAL_RUNNER="$WORKDIR/run_lightweight_selftests.sh"
LOCAL_TMP_CLEAN="$WORKDIR/tmp_clean_pg_sorted_heap.sh"
cp "$RUNNER_SCRIPT" "$LOCAL_RUNNER"
cp "$TMP_CLEAN_SCRIPT" "$LOCAL_TMP_CLEAN"
chmod +x "$LOCAL_RUNNER"
chmod 0644 "$LOCAL_TMP_CLEAN"

OUT_TMP_CLEAN="$WORKDIR/non_exec_tmp_clean.out"
set +e
(cd "$WORKDIR" && LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN="on" LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN_MIN_AGE_S="0" ./run_lightweight_selftests.sh "$TMP_ROOT" jsonl >"$OUT_TMP_CLEAN" 2>&1)
rc_tmp_clean=$?
set -e
if [ "$rc_tmp_clean" -ne 2 ]; then
  echo "expected runner to fail with exit code 2 for non-executable tmp-clean script, got rc=$rc_tmp_clean" >&2
  cat "$OUT_TMP_CLEAN" >&2
  exit 1
fi
if ! grep -Fq "tmp-clean script not executable: $LOCAL_TMP_CLEAN" "$OUT_TMP_CLEAN"; then
  echo "expected non-executable tmp-clean preflight marker" >&2
  cat "$OUT_TMP_CLEAN" >&2
  exit 1
fi
if grep -Fq '"event":"' "$OUT_TMP_CLEAN"; then
  echo "unexpected JSONL events for non-executable tmp-clean preflight failure" >&2
  cat "$OUT_TMP_CLEAN" >&2
  exit 1
fi

echo "selftest_run_lightweight_selftests_auto_tmp_clean_validation status=ok"
