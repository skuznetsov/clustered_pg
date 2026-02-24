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
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if ! make -s -C "$REPO_ROOT" --no-print-directory -n installcheck >/dev/null 2>&1; then
  echo "pg_core_regression_smoke status=error stage=installcheck_target_missing" >&2
  exit 1
fi

if command -v prove >/dev/null 2>&1; then
  TAP_PROVE_STATUS="present"
else
  TAP_PROVE_STATUS="missing"
fi

if command -v pg_isolation_regress >/dev/null 2>&1; then
  ISOLATION_STATUS="present"
else
  ISOLATION_STATUS="missing"
fi

echo "pg_core_regression_smoke status=ok|installcheck_target=present|tap_prove=$TAP_PROVE_STATUS|isolation_regress=$ISOLATION_STATUS|entrypoints=make_installcheck,make_prove_check,pg_isolation_regress"
