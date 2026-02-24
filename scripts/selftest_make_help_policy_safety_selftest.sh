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

OUT_HELP="$(
  make -s -C "$ROOT_DIR" --no-print-directory help
)"

if ! printf '%s\n' "$OUT_HELP" | grep -Fq "make policy-safety-selftest"; then
  echo "expected help output to include policy-safety-selftest target" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_HELP" | grep -Fq "UNNEST_AB_SELFTEST_TMP_ROOT=<abs_tmp_dir>"; then
  echo "expected help output to include UNNEST_AB_SELFTEST_TMP_ROOT contract for policy-safety-selftest" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_HELP" | grep -Fq "UNNEST_GATE_SELFTEST_TMP_ROOT=<abs_tmp_dir>"; then
  echo "expected help output to include UNNEST_GATE_SELFTEST_TMP_ROOT contract for policy-safety-selftest" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi

echo "selftest_make_help_policy_safety_selftest status=ok"
