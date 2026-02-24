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

extract_default() {
  local key="$1"
  awk -v k="$key" '
    $1 == k && $2 == "?=" { print $3; exit }
  ' "$MAKEFILE"
}

OUT_HELP="$(
  make -s -C "$ROOT_DIR" --no-print-directory help
)"

for key in \
  UNNEST_STARTUP_RUNS \
  UNNEST_STARTUP_BATCH_SIZE \
  UNNEST_STARTUP_BATCHES \
  UNNEST_STARTUP_SELECT_ITERS \
  UNNEST_STARTUP_PROBE_SIZE \
  UNNEST_STARTUP_PORT; do
  val="$(extract_default "$key")"
  if [ -z "$val" ]; then
    echo "unable to extract default for $key from Makefile" >&2
    exit 1
  fi
  if ! printf '%s\n' "$OUT_HELP" | grep -Fq "$key=$val"; then
    echo "expected help defaults output to include $key=$val" >&2
    printf '%s\n' "$OUT_HELP" >&2
    exit 1
  fi
done

echo "selftest_make_defaults_unnest_ab_startup_sensitivity status=ok"
