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

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_runtime_profile_export_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

expect_fail_contains() {
  local expected="$1"
  shift
  local out="$WORKDIR/expect_fail.out"
  if "$@" >"$out" 2>&1; then
    echo "expected failure but command succeeded: $*" >&2
    cat "$out" >&2
    exit 1
  fi
  if ! grep -Fq "$expected" "$out"; then
    echo "expected failure output to contain: $expected" >&2
    cat "$out" >&2
    exit 1
  fi
}

KV_OUT="$(make -C "$ROOT_DIR" --no-print-directory fastpath-perf-probe-selftest-profiles RUNTIME_PROFILE_FORMAT=kv)"
if ! grep -Fq "runtime_profile|name=base|" <<<"$KV_OUT"; then
  echo "expected kv output to contain base profile" >&2
  echo "$KV_OUT" >&2
  exit 1
fi
if ! grep -Fq "runtime_profile|name=high_churn|" <<<"$KV_OUT"; then
  echo "expected kv output to contain high_churn profile" >&2
  echo "$KV_OUT" >&2
  exit 1
fi

JSON_OUT="$(make -C "$ROOT_DIR" --no-print-directory fastpath-perf-probe-selftest-profiles RUNTIME_PROFILE_FORMAT=json)"
if ! grep -Eq '^\{"schema_version":[0-9]+,"profiles":\[\{"name":"base",' <<<"$JSON_OUT"; then
  echo "expected json output prefix with schema_version and base profile" >&2
  echo "$JSON_OUT" >&2
  exit 1
fi
if ! grep -Fq '"name":"high_churn"' <<<"$JSON_OUT"; then
  echo "expected json output to contain high_churn profile" >&2
  echo "$JSON_OUT" >&2
  exit 1
fi

expect_fail_contains "unsupported RUNTIME_PROFILE_FORMAT: bad" \
  make -C "$ROOT_DIR" --no-print-directory fastpath-perf-probe-selftest-profiles RUNTIME_PROFILE_FORMAT=bad

expect_fail_contains "invalid RUNTIME_PROFILE_SCHEMA_VERSION: 0" \
  make -C "$ROOT_DIR" --no-print-directory fastpath-perf-probe-selftest-profiles RUNTIME_PROFILE_FORMAT=json RUNTIME_PROFILE_SCHEMA_VERSION=0

echo "selftest_runtime_profile_export status=ok"
