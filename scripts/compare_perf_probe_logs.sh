#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 <reference_log> <candidate_log> [max_slowdown_ratio]" >&2
  exit 2
fi

REF_LOG="$1"
NEW_LOG="$2"
MAX_SLOWDOWN="${3:-1.25}"

if [ ! -f "$REF_LOG" ]; then
  echo "reference log not found: $REF_LOG" >&2
  exit 2
fi
if [ ! -f "$NEW_LOG" ]; then
  echo "candidate log not found: $NEW_LOG" >&2
  exit 2
fi

extract_elapsed() {
  local file="$1"
  local scenario="$2"
  local value

  value="$(awk -F'|' -v s="$scenario" '
    $1 ~ s {
      gsub(/[[:space:]]/, "", $2);
      print $2;
      found = 1;
      exit
    }
    END {
      if (!found)
        exit 1
    }' "$file")" || {
      echo "unable to extract scenario '$scenario' from $file" >&2
      exit 2
    }

  if ! [[ "$value" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    echo "invalid numeric value for '$scenario' in $file: $value" >&2
    exit 2
  fi

  echo "$value"
}

ref_baseline="$(extract_elapsed "$REF_LOG" "baseline_fastpath")"
ref_churn="$(extract_elapsed "$REF_LOG" "churn_fastpath")"
new_baseline="$(extract_elapsed "$NEW_LOG" "baseline_fastpath")"
new_churn="$(extract_elapsed "$NEW_LOG" "churn_fastpath")"

baseline_slowdown="$(awk -v n="$new_baseline" -v r="$ref_baseline" 'BEGIN { printf "%.6f", n / r }')"
churn_slowdown="$(awk -v n="$new_churn" -v r="$ref_churn" 'BEGIN { printf "%.6f", n / r }')"

echo "perf_compare reference=$REF_LOG candidate=$NEW_LOG max_slowdown=$MAX_SLOWDOWN"
echo "  baseline_fastpath: ref=$ref_baseline ms cand=$new_baseline ms slowdown=${baseline_slowdown}x"
echo "  churn_fastpath:    ref=$ref_churn ms cand=$new_churn ms slowdown=${churn_slowdown}x"

baseline_regress="$(awk -v s="$baseline_slowdown" -v m="$MAX_SLOWDOWN" 'BEGIN { print (s > m) ? 1 : 0 }')"
churn_regress="$(awk -v s="$churn_slowdown" -v m="$MAX_SLOWDOWN" 'BEGIN { print (s > m) ? 1 : 0 }')"

if [ "$baseline_regress" -eq 1 ] || [ "$churn_regress" -eq 1 ]; then
  echo "perf_compare status=regression" >&2
  exit 1
fi

echo "perf_compare status=ok"
