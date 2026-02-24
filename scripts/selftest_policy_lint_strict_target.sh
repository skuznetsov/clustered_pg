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
MAKEFILE="$ROOT_DIR/Makefile"

if [ ! -f "$MAKEFILE" ]; then
  echo "Makefile not found: $MAKEFILE" >&2
  exit 2
fi

target_hits="$(grep -F --count "policy-lint-strict:" "$MAKEFILE" || true)"
if [ "$target_hits" -ne 1 ]; then
  echo "expected exactly one policy-lint-strict target in Makefile (hits=$target_hits)" >&2
  exit 1
fi

recipe_hits="$(grep -F --count "POLICY_LINT_WARNINGS_MAX=0 ./scripts/lint_comparator_policy.sh" "$MAKEFILE" || true)"
if [ "$recipe_hits" -lt 1 ]; then
  echo "expected strict lint recipe with POLICY_LINT_WARNINGS_MAX=0 in Makefile" >&2
  exit 1
fi

help_hits="$(grep -F --count "make policy-lint-strict" "$MAKEFILE" || true)"
if [ "$help_hits" -lt 1 ]; then
  echo "expected help output to document 'make policy-lint-strict'" >&2
  exit 1
fi

echo "selftest_policy_lint_strict_target status=ok"
