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

MAKEFILE="$REPO_ROOT/Makefile"
OPERATIONS="$REPO_ROOT/OPERATIONS.md"

if [ ! -f "$MAKEFILE" ]; then
  echo "missing Makefile: $MAKEFILE" >&2
  exit 1
fi
if [ ! -f "$OPERATIONS" ]; then
  echo "missing operations doc: $OPERATIONS" >&2
  exit 1
fi

if ! rg -n '^unnest-ab-perf-sentinel-cold-observe:' "$MAKEFILE" >/dev/null; then
  echo "missing make target definition: unnest-ab-perf-sentinel-cold-observe" >&2
  exit 1
fi

HELP_OUT="$(make -s -C "$REPO_ROOT" --no-print-directory help)"
if ! printf '%s\n' "$HELP_OUT" | grep -Fq "make unnest-ab-perf-sentinel-cold-observe"; then
  echo "missing help usage entry for unnest-ab-perf-sentinel-cold-observe" >&2
  exit 1
fi

if ! rg -n 'make unnest-ab-perf-sentinel-cold-observe' "$OPERATIONS" >/dev/null; then
  echo "missing OPERATIONS command for unnest-ab-perf-sentinel-cold-observe" >&2
  exit 1
fi
if ! rg -n 'status=observe' "$OPERATIONS" >/dev/null; then
  echo "missing OPERATIONS observe-status semantics" >&2
  exit 1
fi

echo "selftest_docs_make_sentinel_cold_observe_contract status=ok"
