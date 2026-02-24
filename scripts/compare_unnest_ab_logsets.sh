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
  find "$dir" -maxdepth 1 -type f -name 'clustered_pg_unnest_ab_*.log' | sort
}

extract_ratio_metric() {
  local file="$1"
  local metric="$2"
  local value

  value="$(awk -F'|' -v metric="$metric" '
    /ratio_kv\|/ {
      found_line = 1;
      for (i = 1; i <= NF; i++) {
        gsub(/[[:space:]]/, "", $i);
        if ($i ~ ("^" metric "=")) {
          val = $i;
          sub("^" metric "=", "", val);
          print val;
          found = 1;
          exit;
        }
      }
    }
    END {
      if (!found)
        exit 1;
    }' "$file")" || {
      echo "unable to extract metric '$metric' from $file" >&2
      exit 2
    }

  if ! [[ "$value" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    echo "invalid numeric value for metric '$metric' in $file: $value" >&2
    exit 2
  fi
  if ! awk -v v="$value" 'BEGIN { exit (v > 0) ? 0 : 1 }'; then
    echo "non-positive value for metric '$metric' in $file: $value" >&2
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
declare -a ref_insert_values
declare -a ref_join_values
declare -a ref_any_values
declare -a new_insert_values
declare -a new_join_values
declare -a new_any_values

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
  ref_insert_values+=("$(extract_ratio_metric "$f" "insert")")
  ref_join_values+=("$(extract_ratio_metric "$f" "join_unnest")")
  ref_any_values+=("$(extract_ratio_metric "$f" "any_array")")
done

for f in "${new_logs[@]}"; do
  new_insert_values+=("$(extract_ratio_metric "$f" "insert")")
  new_join_values+=("$(extract_ratio_metric "$f" "join_unnest")")
  new_any_values+=("$(extract_ratio_metric "$f" "any_array")")
done

ref_insert_stat="$(aggregate_stat "$STAT_MODE" "${ref_insert_values[@]}")"
ref_join_stat="$(aggregate_stat "$STAT_MODE" "${ref_join_values[@]}")"
ref_any_stat="$(aggregate_stat "$STAT_MODE" "${ref_any_values[@]}")"
new_insert_stat="$(aggregate_stat "$STAT_MODE" "${new_insert_values[@]}")"
new_join_stat="$(aggregate_stat "$STAT_MODE" "${new_join_values[@]}")"
new_any_stat="$(aggregate_stat "$STAT_MODE" "${new_any_values[@]}")"

insert_fraction="$(awk -v n="$new_insert_stat" -v r="$ref_insert_stat" 'BEGIN { printf "%.6f", n / r }')"
join_fraction="$(awk -v n="$new_join_stat" -v r="$ref_join_stat" 'BEGIN { printf "%.6f", n / r }')"
any_fraction="$(awk -v n="$new_any_stat" -v r="$ref_any_stat" 'BEGIN { printf "%.6f", n / r }')"

echo "unnest_ab_set_compare reference_dir=$REF_DIR candidate_dir=$NEW_DIR min_fraction=$MIN_FRACTION stat_mode=$STAT_MODE min_samples=$MIN_SAMPLES metric_polarity=higher_is_better"
echo "  insert_${STAT_MODE}:      ref=$ref_insert_stat cand=$new_insert_stat candidate_over_reference=$insert_fraction"
echo "  join_unnest_${STAT_MODE}: ref=$ref_join_stat cand=$new_join_stat candidate_over_reference=$join_fraction"
echo "  any_array_${STAT_MODE}:   ref=$ref_any_stat cand=$new_any_stat candidate_over_reference=$any_fraction"
echo "  sample_sizes: ref=${#ref_logs[@]} cand=${#new_logs[@]}"

insert_regress="$(awk -v f="$insert_fraction" -v m="$MIN_FRACTION" 'BEGIN { print (f < m) ? 1 : 0 }')"
join_regress="$(awk -v f="$join_fraction" -v m="$MIN_FRACTION" 'BEGIN { print (f < m) ? 1 : 0 }')"
any_regress="$(awk -v f="$any_fraction" -v m="$MIN_FRACTION" 'BEGIN { print (f < m) ? 1 : 0 }')"

if [ "$insert_regress" -eq 1 ] || [ "$join_regress" -eq 1 ] || [ "$any_regress" -eq 1 ]; then
  echo "unnest_ab_set_compare status=regression" >&2
  exit 1
fi

echo "unnest_ab_set_compare status=ok"
