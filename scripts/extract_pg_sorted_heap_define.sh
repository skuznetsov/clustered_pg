#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <source_c_path> <define_name>" >&2
  exit 2
fi

SOURCE_C="$1"
DEFINE_NAME="$2"

if [[ "$DEFINE_NAME" =~ [^A-Za-z0-9_] ]] || [[ ! "$DEFINE_NAME" =~ ^[A-Za-z_] ]]; then
  echo "invalid define_name: $DEFINE_NAME" >&2
  exit 2
fi

if [ ! -f "$SOURCE_C" ]; then
  echo "source file not found: $SOURCE_C" >&2
  exit 2
fi

value="$(sed -nE "s/^#define[[:space:]]+$DEFINE_NAME[[:space:]]+([0-9]+).*/\\1/p" "$SOURCE_C" | head -n1 || true)"
if [ -z "$value" ] || ! [[ "$value" =~ ^[0-9]+$ ]] || [ "$value" -le 0 ]; then
  echo "failed to parse $DEFINE_NAME from $SOURCE_C" >&2
  exit 2
fi

echo "$value"
