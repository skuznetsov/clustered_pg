#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 5 ]; then
  echo "usage: $0 <reference_dir> <candidate_dir> [min_fraction] [stat_mode] [min_samples]" >&2
  exit 2
fi

REF_DIR="$1"
NEW_DIR="$2"
MIN_FRACTION="${3:-0.90}"
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
if ! [[ "$MIN_FRACTION" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  echo "min_fraction must be numeric: $MIN_FRACTION" >&2
  exit 2
fi
if awk -v v="$MIN_FRACTION" 'BEGIN { exit (v > 0 && v <= 1.0) ? 0 : 1 }'; then
  :
else
  echo "min_fraction must be in range (0, 1]: $MIN_FRACTION" >&2
  exit 2
fi

collect_logs() {
  local dir="$1"
  find "$dir" -maxdepth 1 -type f -name 'pg_sorted_heap_planner_probe_*.log' | sort
}

extract_worst_ratio() {
  local file="$1"
  local ratio

  ratio="$(awk -F'|' '
    /^planner_probe_compare\|/ {
      found = 1;
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^off_over_on=/) {
          val = $i;
          sub(/^off_over_on=/, "", val);
          if (val !~ /^[0-9]+(\.[0-9]+)?$/) {
            bad = 1;
            exit;
          }
          n += 1;
          if (min == "" || val + 0 < min + 0)
            min = val;
          break;
        }
      }
    }
    END {
      if (!found || n == 0 || bad)
        exit 1;
      printf "%.6f", min + 0;
    }' "$file")" || {
      echo "unable to extract valid planner_probe_compare ratios from $file" >&2
      exit 2
    }

  echo "$ratio"
}

median() {
  if [ "$#" -eq 0 ]; then
    echo "median requires at least one value" >&2
    exit 2
  fi

  printf "%s\n" "$@" | sort -g | awk '
    { arr[NR] = $1 }
    END {
      if (NR % 2 == 1)
        printf "%.6f", arr[(NR + 1) / 2];
      else
        printf "%.6f", (arr[NR / 2] + arr[NR / 2 + 1]) / 2.0;
    }'
}

p95() {
  if [ "$#" -eq 0 ]; then
    echo "p95 requires at least one value" >&2
    exit 2
  fi

  printf "%s\n" "$@" | sort -g | awk '
    { arr[NR] = $1 }
    END {
      idx = int((0.95 * NR) + 0.999999);
      if (idx < 1)
        idx = 1;
      if (idx > NR)
        idx = NR;
      printf "%.6f", arr[idx];
    }'
}

p05() {
  if [ "$#" -eq 0 ]; then
    echo "p05 requires at least one value" >&2
    exit 2
  fi

  printf "%s\n" "$@" | sort -g | awk '
    { arr[NR] = $1 }
    END {
      idx = int((0.05 * NR) + 0.999999);
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
    { arr[NR] = $1 }
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
    p05)
      p05 "$@"
      ;;
    trimmed-mean)
      trimmed_mean "$@"
      ;;
    *)
      echo "unsupported stat_mode: $mode (supported: median|p05|p95|trimmed-mean)" >&2
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
declare -a ref_ratios
declare -a new_ratios

while IFS= read -r line; do
  ref_logs+=("$line")
done < <(collect_logs "$REF_DIR")
while IFS= read -r line; do
  new_logs+=("$line")
done < <(collect_logs "$NEW_DIR")

if [ "${#ref_logs[@]}" -lt "$MIN_SAMPLES" ]; then
  echo "reference set too small: need >= $MIN_SAMPLES logs, got ${#ref_logs[@]}" >&2
  exit 2
fi
if [ "${#new_logs[@]}" -lt "$MIN_SAMPLES" ]; then
  echo "candidate set too small: need >= $MIN_SAMPLES logs, got ${#new_logs[@]}" >&2
  exit 2
fi

for f in "${ref_logs[@]}"; do
  ref_ratios+=("$(extract_worst_ratio "$f")")
done
for f in "${new_logs[@]}"; do
  new_ratios+=("$(extract_worst_ratio "$f")")
done

ref_stat="$(aggregate_stat "$STAT_MODE" "${ref_ratios[@]}")"
new_stat="$(aggregate_stat "$STAT_MODE" "${new_ratios[@]}")"
fraction="$(awk -v n="$new_stat" -v r="$ref_stat" 'BEGIN { if (r <= 0) { print "0.000000"; } else { printf "%.6f", n / r; } }')"

echo "planner_probe_set_compare reference_dir=$REF_DIR candidate_dir=$NEW_DIR min_fraction=$MIN_FRACTION stat_mode=$STAT_MODE min_samples=$MIN_SAMPLES metric_polarity=higher_is_better"
echo "  worst_ratio_${STAT_MODE}: ref=$ref_stat cand=$new_stat candidate_over_reference=$fraction"
echo "  sample_sizes: ref=${#ref_logs[@]} cand=${#new_logs[@]}"

if awk -v f="$fraction" -v m="$MIN_FRACTION" 'BEGIN { exit (f < m) ? 0 : 1 }'; then
  echo "planner_probe_set_compare status=regression" >&2
  exit 1
fi

echo "planner_probe_set_compare status=ok"
