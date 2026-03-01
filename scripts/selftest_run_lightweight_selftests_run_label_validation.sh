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
RUNNER_SCRIPT="$SCRIPT_DIR/run_lightweight_selftests.sh"

if [ ! -x "$RUNNER_SCRIPT" ]; then
  echo "runner script not executable: $RUNNER_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_lightweight_run_label_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

OUT="$WORKDIR/runner.out"
set +e
LIGHTWEIGHT_SELFTEST_RUN_LABEL="bad label!" "$RUNNER_SCRIPT" "$TMP_ROOT" jsonl >"$OUT" 2>&1
rc=$?
set -e

if [ "$rc" -ne 2 ]; then
  echo "expected runner to fail with exit code 2 for invalid run label, got rc=$rc" >&2
  cat "$OUT" >&2
  exit 1
fi

if ! grep -Fq "LIGHTWEIGHT_SELFTEST_RUN_LABEL contains unsupported characters: bad label!" "$OUT"; then
  echo "expected invalid run label error message" >&2
  cat "$OUT" >&2
  exit 1
fi

if grep -Fq '"event":"' "$OUT"; then
  echo "unexpected JSONL events for invalid run label preflight failure" >&2
  cat "$OUT" >&2
  exit 1
fi

OUT_FORMAT="$WORKDIR/runner_invalid_format.out"
set +e
"$RUNNER_SCRIPT" "$TMP_ROOT" badfmt >"$OUT_FORMAT" 2>&1
rc_format=$?
set -e
if [ "$rc_format" -ne 2 ]; then
  echo "expected runner to fail with exit code 2 for invalid output format, got rc=$rc_format" >&2
  cat "$OUT_FORMAT" >&2
  exit 1
fi
if ! grep -Fq "unsupported format: badfmt (supported: text|jsonl)" "$OUT_FORMAT"; then
  echo "expected invalid format preflight marker" >&2
  cat "$OUT_FORMAT" >&2
  exit 1
fi
if grep -Fq '"event":"' "$OUT_FORMAT"; then
  echo "unexpected JSONL events for invalid format preflight failure" >&2
  cat "$OUT_FORMAT" >&2
  exit 1
fi

OUT_TMP_REL="$WORKDIR/runner_invalid_tmp_rel.out"
set +e
"$RUNNER_SCRIPT" "relative_tmp_root" jsonl >"$OUT_TMP_REL" 2>&1
rc_tmp_rel=$?
set -e
if [ "$rc_tmp_rel" -ne 2 ]; then
  echo "expected runner to fail with exit code 2 for non-absolute tmp_root, got rc=$rc_tmp_rel" >&2
  cat "$OUT_TMP_REL" >&2
  exit 1
fi
if ! grep -Fq "tmp_root_abs_dir must be absolute: relative_tmp_root" "$OUT_TMP_REL"; then
  echo "expected non-absolute tmp_root preflight marker" >&2
  cat "$OUT_TMP_REL" >&2
  exit 1
fi
if grep -Fq '"event":"' "$OUT_TMP_REL"; then
  echo "unexpected JSONL events for non-absolute tmp_root preflight failure" >&2
  cat "$OUT_TMP_REL" >&2
  exit 1
fi

MISSING_TMP_ROOT="$WORKDIR/missing_tmp_root"
OUT_TMP_MISSING="$WORKDIR/runner_missing_tmp.out"
set +e
"$RUNNER_SCRIPT" "$MISSING_TMP_ROOT" jsonl >"$OUT_TMP_MISSING" 2>&1
rc_tmp_missing=$?
set -e
if [ "$rc_tmp_missing" -ne 2 ]; then
  echo "expected runner to fail with exit code 2 for missing tmp_root, got rc=$rc_tmp_missing" >&2
  cat "$OUT_TMP_MISSING" >&2
  exit 1
fi
if ! grep -Fq "tmp_root_abs_dir not found: $MISSING_TMP_ROOT" "$OUT_TMP_MISSING"; then
  echo "expected missing tmp_root preflight marker" >&2
  cat "$OUT_TMP_MISSING" >&2
  exit 1
fi
if grep -Fq '"event":"' "$OUT_TMP_MISSING"; then
  echo "unexpected JSONL events for missing tmp_root preflight failure" >&2
  cat "$OUT_TMP_MISSING" >&2
  exit 1
fi

echo "selftest_run_lightweight_selftests_run_label_validation status=ok"
