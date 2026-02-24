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
README="$ROOT_DIR/README.md"
OPERATIONS="$ROOT_DIR/OPERATIONS.md"

if [ ! -f "$README" ]; then
  echo "missing README: $README" >&2
  exit 1
fi
if [ ! -f "$OPERATIONS" ]; then
  echo "missing OPERATIONS: $OPERATIONS" >&2
  exit 1
fi

if ! rg -n 'make policy-safety-selftest' "$README" >/dev/null; then
  echo "expected README to include make policy-safety-selftest quickstart command" >&2
  exit 1
fi
if ! rg -n 'make policy-safety-selftest' "$OPERATIONS" >/dev/null; then
  echo "expected OPERATIONS to include make policy-safety-selftest command" >&2
  exit 1
fi
if ! rg -n 'policy-safety-selftest\.yml' "$OPERATIONS" >/dev/null; then
  echo "expected OPERATIONS to mention policy-safety-selftest workflow file" >&2
  exit 1
fi

echo "selftest_docs_policy_safety_quickstart_contract status=ok"
