#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 2 ]; then
  echo "usage: $0 [tmp_root_abs_dir] [min_age_seconds]" >&2
  exit 2
fi

ROOT="${1:-/private/tmp}"
MIN_AGE_SECONDS="${2:-0}"
if [[ "$ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $ROOT" >&2
  exit 2
fi
if [ ! -d "$ROOT" ]; then
  echo "tmp_root_abs_dir not found: $ROOT" >&2
  exit 2
fi
if ! [[ "$MIN_AGE_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "min_age_seconds must be a non-negative integer: $MIN_AGE_SECONDS" >&2
  exit 2
fi

stat_mtime_epoch() {
  local path="$1"
  if stat -f %m "$path" >/dev/null 2>&1; then
    stat -f %m "$path"
    return 0
  fi
  if stat -c %Y "$path" >/dev/null 2>&1; then
    stat -c %Y "$path"
    return 0
  fi
  return 1
}

is_live_postmaster_dir() {
  local path="$1"
  local pid_file="$path/postmaster.pid"
  local pid
  if [ ! -f "$pid_file" ]; then
    return 1
  fi
  pid="$(awk 'NR==1 {print $1}' "$pid_file" 2>/dev/null || true)"
  if ! [[ "$pid" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  if kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  return 1
}

matches=()
while IFS= read -r p; do
  matches+=("$p")
done < <(find "$ROOT" -mindepth 1 -maxdepth 1 \
  \( -name 'clustered_pg_*' \
     -o -name 'workflow_runner_guard.*' \
     -o -name 'workflow_path_parity.*' \
     -o -name 'runtime_workflow_path_parity.*' \) | sort)

removed=0
removed_kb=0
skipped_live=0
skipped_recent=0
now_epoch="$(date +%s)"

for p in "${matches[@]}"; do
  if is_live_postmaster_dir "$p"; then
    echo "skip_live $p"
    skipped_live=$((skipped_live + 1))
    continue
  fi
  if [ "$MIN_AGE_SECONDS" -gt 0 ]; then
    mtime_epoch="$(stat_mtime_epoch "$p" || true)"
    if [[ "$mtime_epoch" =~ ^[0-9]+$ ]]; then
      age_seconds=$((now_epoch - mtime_epoch))
      if [ "$age_seconds" -lt "$MIN_AGE_SECONDS" ]; then
        echo "skip_recent age_s=$age_seconds min_age_s=$MIN_AGE_SECONDS $p"
        skipped_recent=$((skipped_recent + 1))
        continue
      fi
    fi
  fi
  echo "$p"
  size_kb="$(du -sk "$p" 2>/dev/null | awk '{print $1}' || true)"
  if [[ "$size_kb" =~ ^[0-9]+$ ]]; then
    removed_kb=$((removed_kb + size_kb))
  fi
  rm -rf -- "$p"
  removed=$((removed + 1))
done

echo "tmp_clean root=$ROOT removed=$removed skipped_live=$skipped_live skipped_recent=$skipped_recent min_age_s=$MIN_AGE_SECONDS removed_kb=$removed_kb"
