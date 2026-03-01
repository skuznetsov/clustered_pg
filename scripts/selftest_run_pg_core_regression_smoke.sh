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
SMOKE_SCRIPT="$SCRIPT_DIR/run_pg_core_regression_smoke.sh"
if [ ! -x "$SMOKE_SCRIPT" ]; then
  echo "pg core regression smoke script not executable: $SMOKE_SCRIPT" >&2
  exit 2
fi

OUT="$("$SMOKE_SCRIPT" "$TMP_ROOT")"

if ! printf '%s\n' "$OUT" | grep -Fq "pg_core_regression_smoke status=ok"; then
  echo "expected pg core regression smoke status=ok" >&2
  printf '%s\n' "$OUT" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT" | grep -Fq "installcheck_target=present"; then
  echo "expected installcheck_target=present in pg core regression smoke output" >&2
  printf '%s\n' "$OUT" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT" | rg -q 'tap_prove=(present|missing)'; then
  echo "expected tap_prove marker in pg core regression smoke output" >&2
  printf '%s\n' "$OUT" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT" | rg -q 'isolation_regress=(present|missing)'; then
  echo "expected isolation_regress marker in pg core regression smoke output" >&2
  printf '%s\n' "$OUT" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT" | grep -Fq "entrypoints=make_installcheck,make_prove_check,pg_isolation_regress"; then
  echo "expected pg core entrypoints marker in output" >&2
  printf '%s\n' "$OUT" >&2
  exit 1
fi

echo "selftest_run_pg_core_regression_smoke status=ok"
