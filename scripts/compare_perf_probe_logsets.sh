#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 5 ]; then
  echo "usage: $0 <reference_dir> <candidate_dir> [max_slowdown_ratio] [stat_mode] [min_samples]" >&2
  exit 2
fi

REF_DIR="$1"
NEW_DIR="$2"
MAX_SLOWDOWN="${3:-1.25}"
STAT_MODE="${4:-median}"
MIN_SAMPLES_RAW="${5:-}"

if [ ! -d "$REF_DIR" ]; then
  echo "reference dir not found: $REF_DIR" >&2
  exit 2
fi
if [ ! -d "$NEW_DIR" ]; then
  echo "candidate dir not found: $NEW_DIR" >&2
  exit 2
fi

collect_logs() {
  local dir="$1"
  find "$dir" -maxdepth 1 -type f -name 'pg_sorted_heap_perf_probe_*.log' | sort
}

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
  if ! awk -v v="$value" 'BEGIN { exit (v > 0) ? 0 : 1 }'; then
    echo "non-positive value for '$scenario' in $file: $value" >&2
    exit 2
  fi

  echo "$value"
}

median() {
  if [ "$#" -eq 0 ]; then
    echo "median requires at least one value" >&2
    exit 2
  fi

  printf "%s\n" "$@" | sort -g | awk '
    {
      arr[NR] = $1
    }
    END {
      if (NR % 2 == 1)
        printf "%.6f", arr[(NR + 1) / 2];
      else
        printf "%.6f", (arr[NR / 2] + arr[NR / 2 + 1]) / 2.0;
    }'
}

mean() {
  if [ "$#" -eq 0 ]; then
    echo "mean requires at least one value" >&2
    exit 2
  fi

  awk '
    {
      sum += $1;
      n += 1;
    }
    END {
      if (n == 0)
        exit 1;
      printf "%.6f", sum / n;
    }' <<<"$(printf "%s\n" "$@")"
}

p95() {
  if [ "$#" -eq 0 ]; then
    echo "p95 requires at least one value" >&2
    exit 2
  fi

  printf "%s\n" "$@" | sort -g | awk '
    {
      arr[NR] = $1
    }
    END {
      idx = int((0.95 * NR) + 0.999999);
      if (idx < 1)
        idx = 1;
      if (idx > NR)
        idx = NR;
      printf "%.6f", arr[idx];
    }'
}

trimmed_mean() {
  if [ "$#" -eq 0 ]; then
    echo "trimmed_mean requires at least one value" >&2
    exit 2
  fi

  printf "%s\n" "$@" | sort -g | awk '
    {
      arr[NR] = $1
    }
    END {
      if (NR < 5) {
        sum = 0.0;
        for (i = 1; i <= NR; i++)
          sum += arr[i];
        printf "%.6f", sum / NR;
        exit
      }

      trim = int(NR * 0.10);
      if (trim * 2 >= NR)
        trim = 0;

      start = trim + 1;
      stop = NR - trim;
      sum = 0.0;
      count = 0;
      for (i = start; i <= stop; i++) {
        sum += arr[i];
        count += 1;
      }
      if (count == 0)
        count = NR;
      printf "%.6f", sum / count;
    }'
}

aggregate_stat() {
  local mode="$1"
  shift

  case "$mode" in
    median)
      median "$@"
      ;;
    p95)
      p95 "$@"
      ;;
    trimmed-mean)
      trimmed_mean "$@"
      ;;
    *)
      echo "unsupported stat_mode: $mode (supported: median|p95|trimmed-mean)" >&2
      exit 2
      ;;
  esac
}

if [ -z "$MIN_SAMPLES_RAW" ]; then
  if [ "$STAT_MODE" = "median" ]; then
    MIN_SAMPLES=1
  else
    MIN_SAMPLES=3
  fi
else
  if ! [[ "$MIN_SAMPLES_RAW" =~ ^[0-9]+$ ]] || [ "$MIN_SAMPLES_RAW" -le 0 ]; then
    echo "min_samples must be a positive integer" >&2
    exit 2
  fi
  MIN_SAMPLES="$MIN_SAMPLES_RAW"
fi

if [ "$STAT_MODE" != "median" ] && [ "$MIN_SAMPLES" -lt 3 ]; then
  echo "min_samples must be >= 3 for stat_mode '$STAT_MODE'" >&2
  exit 2
fi

declare -a ref_logs
declare -a new_logs
declare -a ref_baseline_values
declare -a ref_churn_values
declare -a new_baseline_values
declare -a new_churn_values

while IFS= read -r line; do
  ref_logs+=("$line")
done < <(collect_logs "$REF_DIR")

while IFS= read -r line; do
  new_logs+=("$line")
done < <(collect_logs "$NEW_DIR")

if [ "${#ref_logs[@]}" -eq 0 ]; then
  echo "no probe logs found in reference dir: $REF_DIR" >&2
  exit 2
fi
if [ "${#new_logs[@]}" -eq 0 ]; then
  echo "no probe logs found in candidate dir: $NEW_DIR" >&2
  exit 2
fi
if [ "${#ref_logs[@]}" -lt "$MIN_SAMPLES" ]; then
  echo "reference set too small: need >= $MIN_SAMPLES logs, got ${#ref_logs[@]}" >&2
  exit 2
fi
if [ "${#new_logs[@]}" -lt "$MIN_SAMPLES" ]; then
  echo "candidate set too small: need >= $MIN_SAMPLES logs, got ${#new_logs[@]}" >&2
  exit 2
fi

for f in "${ref_logs[@]}"; do
  ref_baseline_values+=("$(extract_elapsed "$f" "baseline_fastpath")")
  ref_churn_values+=("$(extract_elapsed "$f" "churn_fastpath")")
done

for f in "${new_logs[@]}"; do
  new_baseline_values+=("$(extract_elapsed "$f" "baseline_fastpath")")
  new_churn_values+=("$(extract_elapsed "$f" "churn_fastpath")")
done

ref_baseline_stat="$(aggregate_stat "$STAT_MODE" "${ref_baseline_values[@]}")"
ref_churn_stat="$(aggregate_stat "$STAT_MODE" "${ref_churn_values[@]}")"
new_baseline_stat="$(aggregate_stat "$STAT_MODE" "${new_baseline_values[@]}")"
new_churn_stat="$(aggregate_stat "$STAT_MODE" "${new_churn_values[@]}")"

baseline_slowdown="$(awk -v n="$new_baseline_stat" -v r="$ref_baseline_stat" 'BEGIN { printf "%.6f", n / r }')"
churn_slowdown="$(awk -v n="$new_churn_stat" -v r="$ref_churn_stat" 'BEGIN { printf "%.6f", n / r }')"

echo "perf_set_compare reference_dir=$REF_DIR candidate_dir=$NEW_DIR max_slowdown=$MAX_SLOWDOWN stat_mode=$STAT_MODE min_samples=$MIN_SAMPLES metric_polarity=lower_is_better"
echo "  baseline_fastpath_${STAT_MODE}: ref=$ref_baseline_stat ms cand=$new_baseline_stat ms slowdown=${baseline_slowdown}x"
echo "  churn_fastpath_${STAT_MODE}:    ref=$ref_churn_stat ms cand=$new_churn_stat ms slowdown=${churn_slowdown}x"
echo "  sample_sizes: ref=${#ref_logs[@]} cand=${#new_logs[@]}"

baseline_regress="$(awk -v s="$baseline_slowdown" -v m="$MAX_SLOWDOWN" 'BEGIN { print (s > m) ? 1 : 0 }')"
churn_regress="$(awk -v s="$churn_slowdown" -v m="$MAX_SLOWDOWN" 'BEGIN { print (s > m) ? 1 : 0 }')"

if [ "$baseline_regress" -eq 1 ] || [ "$churn_regress" -eq 1 ]; then
  echo "perf_set_compare status=regression" >&2
  exit 1
fi

echo "perf_set_compare status=ok"
