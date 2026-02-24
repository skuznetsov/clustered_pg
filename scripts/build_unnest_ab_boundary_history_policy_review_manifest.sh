#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <history_summary_dir_abs_path> <manifest_tsv_abs_path>" >&2
  exit 2
fi

INPUT_DIR="$1"
MANIFEST_OUT="$2"

if [[ "$INPUT_DIR" != /* ]]; then
  echo "history_summary_dir_abs_path must be absolute: $INPUT_DIR" >&2
  exit 2
fi
if [[ "$MANIFEST_OUT" != /* ]]; then
  echo "manifest_tsv_abs_path must be absolute: $MANIFEST_OUT" >&2
  exit 2
fi
if [ ! -d "$INPUT_DIR" ]; then
  echo "history summary directory not found: $INPUT_DIR" >&2
  exit 2
fi
if [ ! -r "$INPUT_DIR" ]; then
  echo "history summary directory is not readable: $INPUT_DIR" >&2
  exit 2
fi
MANIFEST_OUT_DIR="$(dirname "$MANIFEST_OUT")"
if [ ! -d "$MANIFEST_OUT_DIR" ]; then
  echo "manifest output directory not found: $MANIFEST_OUT_DIR" >&2
  exit 2
fi
if [ ! -w "$MANIFEST_OUT_DIR" ]; then
  echo "manifest output directory is not writable: $MANIFEST_OUT_DIR" >&2
  exit 2
fi

file_mtime_size() {
  local file_path="$1"
  local out
  if out="$(stat -f '%m %z' "$file_path" 2>/dev/null)"; then
    printf '%s\n' "$out"
    return 0
  fi
  if out="$(stat -c '%Y %s' "$file_path" 2>/dev/null)"; then
    printf '%s\n' "$out"
    return 0
  fi
  return 1
}

TMP_DIR="$(mktemp -d "/private/tmp/clustered_pg_policy_review_manifest_build.XXXXXX")"
TMP_OUT="$TMP_DIR/policy_review.manifest.tsv"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

schema_version=1
scanned_files=0
written_entries=0
generated_epoch="$(date +%s)"
generated_at_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

printf '# schema_version=%s\n' "$schema_version" >"$TMP_OUT"
printf '# input_dir=%s\n' "$INPUT_DIR" >>"$TMP_OUT"
printf '# generated_at_utc=%s\n' "$generated_at_utc" >>"$TMP_OUT"
printf '# generated_at_epoch=%s\n' "$generated_epoch" >>"$TMP_OUT"
while IFS= read -r file_path; do
  [ -n "$file_path" ] || continue
  scanned_files=$((scanned_files + 1))
  if [ ! -r "$file_path" ]; then
    continue
  fi
  status_line="$(grep -E '^boundary_history_status\|' "$file_path" | tail -n 1 || true)"
  if [ -z "$status_line" ]; then
    continue
  fi
  meta="$(file_mtime_size "$file_path" || true)"
  if [ -z "$meta" ]; then
    echo "failed to stat file for manifest entry: $file_path" >&2
    exit 2
  fi
  read -r mtime size <<< "$meta"
  if ! [[ "$mtime" =~ ^[0-9]+$ ]]; then
    echo "failed to parse mtime for manifest entry: file=$file_path mtime=$mtime" >&2
    exit 2
  fi
  if ! [[ "$size" =~ ^[0-9]+$ ]]; then
    echo "failed to parse size for manifest entry: file=$file_path size=$size" >&2
    exit 2
  fi
  printf '%s\t%s\t%s\t%s\n' "$file_path" "$mtime" "$size" "$status_line" >>"$TMP_OUT"
  written_entries=$((written_entries + 1))
done < <(find "$INPUT_DIR" -maxdepth 1 -type f | sort)

mv "$TMP_OUT" "$MANIFEST_OUT"
echo "unnest_ab_boundary_history_policy_review_manifest|status=ok|schema_version=$schema_version|input=$INPUT_DIR|output=$MANIFEST_OUT|scanned_files=$scanned_files|written_entries=$written_entries"
