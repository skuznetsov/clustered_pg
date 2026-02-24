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

if ! printf '%s\n' "$OUT_HELP" | grep -Fq "make unnest-ab-profile-boundary-history-policy-review-window-trusted"; then
  echo "expected help output to include trusted policy review workflow target" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_HELP" | grep -Fq "UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_TRUSTED=<off|on>"; then
  echo "expected help output to include trusted manifest mode variable contract" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_HELP" | grep -Fq "make unnest-ab-profile-boundary-history-policy-review-manifest-freshness"; then
  echo "expected help output to include manifest freshness check target" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_HELP" | grep -Fq "UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS=<non_negative_seconds>"; then
  echo "expected help output to include manifest freshness SLA variable contract" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_HELP" | grep -Fq "UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_NOW_EPOCH=<unix_epoch_override>"; then
  echo "expected help output to include manifest freshness now-epoch override variable contract" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_HELP" | grep -Fq "UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=<off|on>"; then
  echo "expected help output to include manifest freshness prechecked variable contract" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi

echo "selftest_make_help_policy_review_trusted_workflow status=ok"
