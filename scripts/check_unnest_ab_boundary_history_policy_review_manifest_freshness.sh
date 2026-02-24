#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <manifest_tsv_abs_path> [max_age_seconds] [now_epoch_override]" >&2
  exit 2
fi

MANIFEST_PATH="$1"
MAX_AGE_SECONDS="${2:-86400}"
NOW_EPOCH_OVERRIDE="${3:-}"

if [[ "$MANIFEST_PATH" != /* ]]; then
  echo "manifest_tsv_abs_path must be absolute: $MANIFEST_PATH" >&2
  exit 2
fi
if [ ! -f "$MANIFEST_PATH" ]; then
  echo "manifest file not found: $MANIFEST_PATH" >&2
  exit 2
fi
if [ ! -r "$MANIFEST_PATH" ]; then
  echo "manifest file is not readable: $MANIFEST_PATH" >&2
  exit 2
fi
if ! [[ "$MAX_AGE_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "max_age_seconds must be a non-negative integer: $MAX_AGE_SECONDS" >&2
  exit 2
fi
if [ -n "$NOW_EPOCH_OVERRIDE" ] && ! [[ "$NOW_EPOCH_OVERRIDE" =~ ^[0-9]+$ ]]; then
  echo "now_epoch_override must be a non-negative integer when provided: $NOW_EPOCH_OVERRIDE" >&2
  exit 2
fi

generated_epoch="$(
  awk -F= '
    /^# generated_at_epoch=/ {
      value = $2
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      print value
      exit
    }
  ' "$MANIFEST_PATH"
)"
generated_at_utc="$(
  awk -F= '
    /^# generated_at_utc=/ {
      value = $2
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      print value
      exit
    }
  ' "$MANIFEST_PATH"
)"

if [ -z "$generated_epoch" ]; then
  echo "manifest header missing generated_at_epoch: $MANIFEST_PATH" >&2
  exit 2
fi
if ! [[ "$generated_epoch" =~ ^[0-9]+$ ]]; then
  echo "manifest header generated_at_epoch must be an integer: $generated_epoch" >&2
  exit 2
fi
if [ -n "$NOW_EPOCH_OVERRIDE" ]; then
  now_epoch="$NOW_EPOCH_OVERRIDE"
else
  now_epoch="$(date +%s)"
fi

if [ "$generated_epoch" -gt "$now_epoch" ]; then
  echo "unnest_ab_boundary_history_policy_review_manifest_freshness|status=error|reason=future_generated_epoch|input=$MANIFEST_PATH|generated_at_utc=${generated_at_utc:-unknown}|generated_epoch=$generated_epoch|now_epoch=$now_epoch|max_age_seconds=$MAX_AGE_SECONDS"
  exit 1
fi

age_seconds=$((now_epoch - generated_epoch))
if [ "$age_seconds" -gt "$MAX_AGE_SECONDS" ]; then
  echo "unnest_ab_boundary_history_policy_review_manifest_freshness|status=error|reason=stale|input=$MANIFEST_PATH|generated_at_utc=${generated_at_utc:-unknown}|generated_epoch=$generated_epoch|now_epoch=$now_epoch|age_seconds=$age_seconds|max_age_seconds=$MAX_AGE_SECONDS"
  exit 1
fi

echo "unnest_ab_boundary_history_policy_review_manifest_freshness|status=ok|input=$MANIFEST_PATH|generated_at_utc=${generated_at_utc:-unknown}|generated_epoch=$generated_epoch|now_epoch=$now_epoch|age_seconds=$age_seconds|max_age_seconds=$MAX_AGE_SECONDS"
