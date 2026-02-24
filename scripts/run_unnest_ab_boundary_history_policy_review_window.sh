#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 13 ]; then
  echo "usage: $0 <history_summary_file_or_dir> [strict_min_observations] [window_files] [current_balanced_max] [current_boundary40_max] [current_boundary56_min] [current_pressure_min] [derive_max_headroom] [derive_min_floor_margin] [delta_tolerance] [enforce_on_review=off|on] [from_date_yyyymmdd_or_yyyy-mm-dd] [to_date_yyyymmdd_or_yyyy-mm-dd]" >&2
  exit 2
fi

INPUT_PATH="$1"
STRICT_MIN_OBS="${2:-48}"
WINDOW_FILES="${3:-14}"
CURRENT_BALANCED_MAX="${4:-0.25}"
CURRENT_BOUNDARY40_MAX="${5:-0.50}"
CURRENT_BOUNDARY56_MIN="${6:-0.75}"
CURRENT_PRESSURE_MIN="${7:-0.75}"
DERIVE_MAX_HEADROOM="${8:-0.02}"
DERIVE_MIN_FLOOR_MARGIN="${9:-0.02}"
DELTA_TOLERANCE="${10:-0.05}"
ENFORCE_ON_REVIEW="${11:-off}"
FROM_DATE_RAW="${12:-}"
TO_DATE_RAW="${13:-}"
AGGREGATE_OUT="${UNNEST_AB_POLICY_REVIEW_AGGREGATE_OUT:-}"
DATE_MAP_PATH="${UNNEST_AB_POLICY_REVIEW_DATE_MAP:-}"
DATE_MAP_INDEX_PATH="${UNNEST_AB_POLICY_REVIEW_DATE_MAP_INDEX:-}"
MANIFEST_PATH="${UNNEST_AB_POLICY_REVIEW_MANIFEST:-}"
MANIFEST_TRUSTED_MODE="${UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED:-off}"
MANIFEST_MAX_AGE_SECONDS="${UNNEST_AB_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS:-}"
MANIFEST_NOW_EPOCH="${UNNEST_AB_POLICY_REVIEW_MANIFEST_NOW_EPOCH:-}"
MANIFEST_FRESHNESS_PRECHECKED="${UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED:-off}"
FROM_DATE_KEY=""
TO_DATE_KEY=""
DATE_RANGE_ACTIVE=0
DATE_MAP_ACTIVE=0
MANIFEST_ACTIVE=0
MANIFEST_TRUSTED_ACTIVE=0
MANIFEST_FRESHNESS_CHECKED=0
MANIFEST_FRESHNESS_STATUS="skipped"
MANIFEST_ENTRIES=0
MANIFEST_FRESH_HITS=0
MANIFEST_STALE_ENTRIES=0
MANIFEST_MISSING_ENTRIES=0
declare -a DATE_MAP_KEYS=()
declare -a DATE_MAP_DATES=()
declare -a MANIFEST_PATHS=()
declare -a MANIFEST_MTIMES=()
declare -a MANIFEST_SIZES=()
declare -a MANIFEST_STATUS_LINES=()

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ACCUMULATE_SCRIPT="$ROOT_DIR/scripts/accumulate_unnest_ab_boundary_history_summaries.sh"
POLICY_DELTA_SCRIPT="$ROOT_DIR/scripts/compare_unnest_ab_boundary_history_gate_policy_delta.sh"
MANIFEST_FRESHNESS_SCRIPT="$ROOT_DIR/scripts/check_unnest_ab_boundary_history_policy_review_manifest_freshness.sh"
WORKDIR=""
STAGE_DIR=""
AGGREGATE_SUMMARY=""
AGGREGATE_SUMMARY_REPORTED=""
AGGREGATE_SUMMARY_PERSISTED=0

normalize_date_key() {
  local raw="$1"
  local compact
  local month
  local day
  if [ -z "$raw" ]; then
    echo ""
    return 0
  fi
  if [[ "$raw" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    compact="${raw//-/}"
  elif [[ "$raw" =~ ^[0-9]{8}$ ]]; then
    compact="$raw"
  else
    return 1
  fi
  month=$((10#${compact:4:2}))
  day=$((10#${compact:6:2}))
  if [ "$month" -lt 1 ] || [ "$month" -gt 12 ] || [ "$day" -lt 1 ] || [ "$day" -gt 31 ]; then
    return 1
  fi
  echo "$compact"
}

trim_whitespace() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

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

status_field_value() {
  local status_line="$1"
  local key="$2"
  local field
  local -a fields=()

  IFS='|' read -r -a fields <<< "$status_line"
  for field in "${fields[@]}"; do
    if [[ "$field" == "$key="* ]]; then
      printf '%s\n' "${field#"$key="}"
      return 0
    fi
  done
  return 1
}

extract_first_date_token() {
  local text="$1"
  if [[ "$text" =~ ([0-9]{4}-[0-9]{2}-[0-9]{2}) ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  if [[ "$text" =~ ([0-9]{8}) ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

extract_date_key_from_path() {
  local path="$1"
  local base
  local token
  local normalized
  base="$(basename "$path")"
  token="$(extract_first_date_token "$base" || true)"
  if [ -z "$token" ]; then
    return 1
  fi
  normalized="$(normalize_date_key "$token" || true)"
  if [ -z "$normalized" ]; then
    return 1
  fi
  echo "$normalized"
}

extract_date_key_from_status_metadata() {
  local file_path="$1"
  local status_line
  status_line="$(grep -E '^boundary_history_status\|' "$file_path" | tail -n 1 || true)"
  extract_date_key_from_status_line "$status_line"
}

extract_date_key_from_status_line() {
  local status_line="$1"
  local status_path
  local token
  local normalized

  if [ -z "$status_line" ]; then
    return 1
  fi

  status_path="$(status_field_value "$status_line" "source")"
  if [ -z "$status_path" ]; then
    status_path="$(status_field_value "$status_line" "input")"
  fi
  if [ -z "$status_path" ]; then
    return 1
  fi

  token="$(extract_first_date_token "$status_path" || true)"
  if [ -z "$token" ]; then
    return 1
  fi

  normalized="$(normalize_date_key "$token" || true)"
  if [ -z "$normalized" ]; then
    return 1
  fi
  echo "$normalized"
}

map_lookup_date_key() {
  local lookup_key="$1"
  local low
  local high
  local mid
  local mid_key

  if [ "${#DATE_MAP_KEYS[@]}" -eq 0 ]; then
    return 1
  fi

  low=0
  high=$((${#DATE_MAP_KEYS[@]} - 1))
  while [ "$low" -le "$high" ]; do
    mid=$(((low + high) / 2))
    mid_key="${DATE_MAP_KEYS[$mid]}"
    if [ "$mid_key" = "$lookup_key" ]; then
      echo "${DATE_MAP_DATES[$mid]}"
      return 0
    fi
    if [[ "$mid_key" < "$lookup_key" ]]; then
      low=$((mid + 1))
    else
      high=$((mid - 1))
    fi
  done
  return 1
}

validate_date_map_file() {
  local line
  local line_no
  local key_raw
  local date_raw
  local key
  local date_key
  local sorted_key
  local sorted_date
  local prev_key
  local prev_date
  local -a parsed_entries=()

  DATE_MAP_KEYS=()
  DATE_MAP_DATES=()
  line_no=0
  while IFS= read -r line || [ -n "$line" ]; do
    line_no=$((line_no + 1))
    line="$(trim_whitespace "$line")"
    [ -z "$line" ] && continue
    [[ "$line" == \#* ]] && continue
    if [[ "$line" != *,* ]]; then
      echo "invalid date map line (missing comma) at $DATE_MAP_PATH:$line_no: $line" >&2
      exit 2
    fi
    key_raw="${line%%,*}"
    date_raw="${line#*,}"
    key="$(trim_whitespace "$key_raw")"
    date_raw="$(trim_whitespace "$date_raw")"
    if [ -z "$key" ] || [ -z "$date_raw" ]; then
      echo "invalid date map line (empty key/date) at $DATE_MAP_PATH:$line_no: $line" >&2
      exit 2
    fi
    if ! date_key="$(normalize_date_key "$date_raw" 2>/dev/null)"; then
      echo "invalid date map line (bad date token) at $DATE_MAP_PATH:$line_no: $line" >&2
      exit 2
    fi
    if [ -z "$date_key" ]; then
      echo "invalid date map line (bad date token) at $DATE_MAP_PATH:$line_no: $line" >&2
      exit 2
    fi

    parsed_entries+=("${key}"$'\t'"${date_key}")
  done < "$DATE_MAP_PATH"

  if [ "${#parsed_entries[@]}" -eq 0 ]; then
    return 0
  fi

  prev_key=""
  prev_date=""
  while IFS=$'\t' read -r sorted_key sorted_date; do
    [ -n "$sorted_key" ] || continue
    [ -n "$sorted_date" ] || continue
    if [ "$sorted_key" = "$prev_key" ]; then
      if [ "$sorted_date" != "$prev_date" ]; then
        echo "conflicting date map key at $DATE_MAP_PATH: key=$sorted_key existing_date=$prev_date new_date=$sorted_date" >&2
        exit 2
      fi
      continue
    fi
    DATE_MAP_KEYS+=("$sorted_key")
    DATE_MAP_DATES+=("$sorted_date")
    prev_key="$sorted_key"
    prev_date="$sorted_date"
  done < <(printf '%s\n' "${parsed_entries[@]}" | LC_ALL=C sort -t $'\t' -k1,1 -k2,2)
}

validate_date_map_index_file() {
  local line
  local line_no
  local key_raw
  local date_raw
  local key
  local date_key
  local prev_key
  local prev_date

  DATE_MAP_KEYS=()
  DATE_MAP_DATES=()
  prev_key=""
  prev_date=""
  line_no=0
  while IFS= read -r line || [ -n "$line" ]; do
    line_no=$((line_no + 1))
    line="$(trim_whitespace "$line")"
    [ -z "$line" ] && continue
    [[ "$line" == \#* ]] && continue
    if [[ "$line" != *$'\t'* ]]; then
      echo "invalid date map index line (missing tab) at $DATE_MAP_INDEX_PATH:$line_no: $line" >&2
      exit 2
    fi
    key_raw="${line%%$'\t'*}"
    date_raw="${line#*$'\t'}"
    key="$(trim_whitespace "$key_raw")"
    date_raw="$(trim_whitespace "$date_raw")"
    if [ -z "$key" ] || [ -z "$date_raw" ]; then
      echo "invalid date map index line (empty key/date) at $DATE_MAP_INDEX_PATH:$line_no: $line" >&2
      exit 2
    fi
    if ! date_key="$(normalize_date_key "$date_raw" 2>/dev/null)"; then
      echo "invalid date map index line (bad date token) at $DATE_MAP_INDEX_PATH:$line_no: $line" >&2
      exit 2
    fi
    if [ -z "$date_key" ]; then
      echo "invalid date map index line (bad date token) at $DATE_MAP_INDEX_PATH:$line_no: $line" >&2
      exit 2
    fi

    if [ -n "$prev_key" ] && [[ "$key" < "$prev_key" ]]; then
      echo "date map index must be sorted by key at $DATE_MAP_INDEX_PATH:$line_no: key=$key prev_key=$prev_key" >&2
      exit 2
    fi
    if [ "$key" = "$prev_key" ]; then
      if [ "$date_key" != "$prev_date" ]; then
        echo "conflicting date map index key at $DATE_MAP_INDEX_PATH:$line_no: key=$key existing_date=$prev_date new_date=$date_key" >&2
        exit 2
      fi
      continue
    fi

    DATE_MAP_KEYS+=("$key")
    DATE_MAP_DATES+=("$date_key")
    prev_key="$key"
    prev_date="$date_key"
  done < "$DATE_MAP_INDEX_PATH"
}

validate_manifest_file() {
  local line
  local line_no
  local path_raw
  local mtime_raw
  local size_raw
  local status_line_raw
  local extra_raw
  local path
  local mtime
  local size
  local status_line
  local prev_path

  MANIFEST_PATHS=()
  MANIFEST_MTIMES=()
  MANIFEST_SIZES=()
  MANIFEST_STATUS_LINES=()
  line_no=0
  prev_path=""
  while IFS= read -r line || [ -n "$line" ]; do
    line_no=$((line_no + 1))
    line="$(trim_whitespace "$line")"
    [ -z "$line" ] && continue
    [[ "$line" == \#* ]] && continue

    IFS=$'\t' read -r path_raw mtime_raw size_raw status_line_raw extra_raw <<< "$line"
    if [ -n "${extra_raw:-}" ]; then
      echo "invalid policy review manifest line (expected 4 tab-separated fields) at $MANIFEST_PATH:$line_no: $line" >&2
      exit 2
    fi

    path="$(trim_whitespace "${path_raw:-}")"
    mtime="$(trim_whitespace "${mtime_raw:-}")"
    size="$(trim_whitespace "${size_raw:-}")"
    status_line="${status_line_raw:-}"

    if [ -z "$path" ] || [ -z "$mtime" ] || [ -z "$size" ] || [ -z "$status_line" ]; then
      echo "invalid policy review manifest line (empty field) at $MANIFEST_PATH:$line_no: $line" >&2
      exit 2
    fi
    if [[ "$path" != /* ]]; then
      echo "invalid policy review manifest line (path must be absolute) at $MANIFEST_PATH:$line_no: path=$path" >&2
      exit 2
    fi
    if [ "$MANIFEST_TRUSTED_ACTIVE" -eq 1 ]; then
      case "$path" in
        "$INPUT_PATH"/*) ;;
        *)
          echo "invalid policy review manifest line (path outside trusted input dir) at $MANIFEST_PATH:$line_no: path=$path input_dir=$INPUT_PATH" >&2
          exit 2
          ;;
      esac
    fi
    if ! [[ "$mtime" =~ ^[0-9]+$ ]]; then
      echo "invalid policy review manifest line (mtime must be integer) at $MANIFEST_PATH:$line_no: mtime=$mtime" >&2
      exit 2
    fi
    if ! [[ "$size" =~ ^[0-9]+$ ]]; then
      echo "invalid policy review manifest line (size must be integer) at $MANIFEST_PATH:$line_no: size=$size" >&2
      exit 2
    fi
    if [[ "$status_line" != boundary_history_status\|* ]]; then
      echo "invalid policy review manifest line (status must start with boundary_history_status|) at $MANIFEST_PATH:$line_no" >&2
      exit 2
    fi
    if [ -n "$prev_path" ] && [[ "$path" < "$prev_path" ]]; then
      echo "policy review manifest must be sorted by absolute path at $MANIFEST_PATH:$line_no: path=$path prev_path=$prev_path" >&2
      exit 2
    fi
    if [ "$path" = "$prev_path" ]; then
      echo "duplicate policy review manifest path at $MANIFEST_PATH:$line_no: $path" >&2
      exit 2
    fi

    MANIFEST_PATHS+=("$path")
    MANIFEST_MTIMES+=("$mtime")
    MANIFEST_SIZES+=("$size")
    MANIFEST_STATUS_LINES+=("$status_line")
    prev_path="$path"
  done < "$MANIFEST_PATH"

  MANIFEST_ENTRIES="${#MANIFEST_PATHS[@]}"
}

load_trusted_manifest_file() {
  local line
  local path_raw
  local mtime_raw
  local size_raw
  local status_line_raw
  local path
  local mtime
  local size
  local status_line

  MANIFEST_PATHS=()
  MANIFEST_MTIMES=()
  MANIFEST_SIZES=()
  MANIFEST_STATUS_LINES=()

  while IFS= read -r line || [ -n "$line" ]; do
    [ -n "$line" ] || continue
    case "$line" in
      \#*) continue ;;
    esac

    IFS=$'\t' read -r path_raw mtime_raw size_raw status_line_raw _ <<< "$line"
    path="${path_raw:-}"
    mtime="${mtime_raw:-}"
    size="${size_raw:-}"
    status_line="${status_line_raw:-}"
    [ -n "$path" ] || continue
    [ -n "$status_line" ] || continue
    case "$path" in
      "$INPUT_PATH"/*) ;;
      *) continue ;;
    esac
    MANIFEST_PATHS+=("$path")
    MANIFEST_MTIMES+=("$mtime")
    MANIFEST_SIZES+=("$size")
    MANIFEST_STATUS_LINES+=("$status_line")
  done < "$MANIFEST_PATH"

  MANIFEST_ENTRIES="${#MANIFEST_PATHS[@]}"
}

extract_date_key_from_map_file() {
  local file_path="$1"
  local base
  local date_key

  [ "$DATE_MAP_ACTIVE" -eq 1 ] || return 1
  if [ "${#DATE_MAP_KEYS[@]}" -eq 0 ]; then
    return 1
  fi

  date_key="$(map_lookup_date_key "$file_path" || true)"
  if [ -n "$date_key" ]; then
    echo "$date_key"
    return 0
  fi

  base="$(basename "$file_path")"
  date_key="$(map_lookup_date_key "$base" || true)"
  if [ -z "$date_key" ]; then
    return 1
  fi
  echo "$date_key"
}

candidate_in_date_range() {
  local date_key="$1"
  if [ -n "$FROM_DATE_KEY" ] && [[ "$date_key" < "$FROM_DATE_KEY" ]]; then
    return 1
  fi
  if [ -n "$TO_DATE_KEY" ] && [[ "$date_key" > "$TO_DATE_KEY" ]]; then
    return 1
  fi
  return 0
}

if [ -n "$FROM_DATE_RAW" ]; then
  FROM_DATE_KEY="$(normalize_date_key "$FROM_DATE_RAW" || true)"
  if [ -z "$FROM_DATE_KEY" ]; then
    echo "from_date has invalid format (expected YYYYMMDD or YYYY-MM-DD): $FROM_DATE_RAW" >&2
    exit 2
  fi
  DATE_RANGE_ACTIVE=1
fi
if [ -n "$TO_DATE_RAW" ]; then
  TO_DATE_KEY="$(normalize_date_key "$TO_DATE_RAW" || true)"
  if [ -z "$TO_DATE_KEY" ]; then
    echo "to_date has invalid format (expected YYYYMMDD or YYYY-MM-DD): $TO_DATE_RAW" >&2
    exit 2
  fi
  DATE_RANGE_ACTIVE=1
fi
if [ -n "$FROM_DATE_KEY" ] && [ -n "$TO_DATE_KEY" ] && [[ "$FROM_DATE_KEY" > "$TO_DATE_KEY" ]]; then
  echo "from_date must be <= to_date: from_date=$FROM_DATE_RAW to_date=$TO_DATE_RAW" >&2
  exit 2
fi
if [ "$DATE_RANGE_ACTIVE" -eq 1 ]; then
  if [ -n "$DATE_MAP_PATH" ] && [ -n "$DATE_MAP_INDEX_PATH" ]; then
    echo "UNNEST_AB_POLICY_REVIEW_DATE_MAP and UNNEST_AB_POLICY_REVIEW_DATE_MAP_INDEX cannot be set together" >&2
    exit 2
  fi
  if [ -n "$DATE_MAP_INDEX_PATH" ]; then
    DATE_MAP_ACTIVE=1
    if [[ "$DATE_MAP_INDEX_PATH" != /* ]]; then
      echo "UNNEST_AB_POLICY_REVIEW_DATE_MAP_INDEX must be an absolute path: $DATE_MAP_INDEX_PATH" >&2
      exit 2
    fi
    if [ ! -f "$DATE_MAP_INDEX_PATH" ]; then
      echo "date map index file not found: $DATE_MAP_INDEX_PATH" >&2
      exit 2
    fi
    if [ ! -r "$DATE_MAP_INDEX_PATH" ]; then
      echo "date map index file is not readable: $DATE_MAP_INDEX_PATH" >&2
      exit 2
    fi
    validate_date_map_index_file
  elif [ -n "$DATE_MAP_PATH" ]; then
    DATE_MAP_ACTIVE=1
    if [[ "$DATE_MAP_PATH" != /* ]]; then
      echo "UNNEST_AB_POLICY_REVIEW_DATE_MAP must be an absolute path: $DATE_MAP_PATH" >&2
      exit 2
    fi
    if [ ! -f "$DATE_MAP_PATH" ]; then
      echo "date map file not found: $DATE_MAP_PATH" >&2
      exit 2
    fi
    if [ ! -r "$DATE_MAP_PATH" ]; then
      echo "date map file is not readable: $DATE_MAP_PATH" >&2
      exit 2
    fi
    validate_date_map_file
  fi
fi

if [ ! -x "$ACCUMULATE_SCRIPT" ]; then
  echo "accumulate script not executable: $ACCUMULATE_SCRIPT" >&2
  exit 2
fi
if [ ! -x "$POLICY_DELTA_SCRIPT" ]; then
  echo "policy delta script not executable: $POLICY_DELTA_SCRIPT" >&2
  exit 2
fi
if ! [[ "$STRICT_MIN_OBS" =~ ^[0-9]+$ ]] || [ "$STRICT_MIN_OBS" -le 0 ]; then
  echo "strict_min_observations must be a positive integer: $STRICT_MIN_OBS" >&2
  exit 2
fi
if ! [[ "$WINDOW_FILES" =~ ^[0-9]+$ ]] || [ "$WINDOW_FILES" -le 0 ]; then
  echo "window_files must be a positive integer: $WINDOW_FILES" >&2
  exit 2
fi
if [ "$MANIFEST_TRUSTED_MODE" != "off" ] && [ "$MANIFEST_TRUSTED_MODE" != "on" ]; then
  echo "UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED must be off or on: $MANIFEST_TRUSTED_MODE" >&2
  exit 2
fi
if [ "$MANIFEST_FRESHNESS_PRECHECKED" != "off" ] && [ "$MANIFEST_FRESHNESS_PRECHECKED" != "on" ]; then
  echo "UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED must be off or on: $MANIFEST_FRESHNESS_PRECHECKED" >&2
  exit 2
fi
if [ "$MANIFEST_TRUSTED_MODE" = "on" ] && [ -z "$MANIFEST_PATH" ]; then
  echo "UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on requires UNNEST_AB_POLICY_REVIEW_MANIFEST" >&2
  exit 2
fi
if [ "$MANIFEST_FRESHNESS_PRECHECKED" = "on" ] && [ "$MANIFEST_TRUSTED_MODE" != "on" ]; then
  echo "UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=on requires UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on" >&2
  exit 2
fi
if [ -n "$MANIFEST_MAX_AGE_SECONDS" ] && [ -z "$MANIFEST_PATH" ]; then
  echo "UNNEST_AB_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS requires UNNEST_AB_POLICY_REVIEW_MANIFEST" >&2
  exit 2
fi
if [ "$MANIFEST_FRESHNESS_PRECHECKED" = "on" ] && [ -z "$MANIFEST_MAX_AGE_SECONDS" ]; then
  echo "UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=on requires UNNEST_AB_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS" >&2
  exit 2
fi
if [ -n "$AGGREGATE_OUT" ]; then
  if [[ "$AGGREGATE_OUT" != /* ]]; then
    echo "UNNEST_AB_POLICY_REVIEW_AGGREGATE_OUT must be an absolute path: $AGGREGATE_OUT" >&2
    exit 2
  fi
  aggregate_out_dir="$(dirname "$AGGREGATE_OUT")"
  if [ ! -d "$aggregate_out_dir" ]; then
    echo "aggregate output directory not found: $aggregate_out_dir" >&2
    exit 2
  fi
fi

declare -a CANDIDATE_LINES=()
add_candidate_file() {
  local file_path="$1"
  local status_line="${2:-}"
  local sort_key
  local date_key

  if [ "$DATE_RANGE_ACTIVE" -eq 1 ]; then
    date_key="$(extract_date_key_from_path "$file_path" || true)"
    if [ -z "$date_key" ]; then
      if [ -z "$status_line" ]; then
        status_line="$(grep -E '^boundary_history_status\|' "$file_path" | tail -n 1 || true)"
      fi
      date_key="$(extract_date_key_from_status_line "$status_line" || true)"
    fi
    if [ -z "$date_key" ]; then
      date_key="$(extract_date_key_from_map_file "$file_path" || true)"
    fi
    if [ -z "$date_key" ]; then
      return 0
    fi
    if ! candidate_in_date_range "$date_key"; then
      return 0
    fi
    sort_key="$date_key"
  else
    sort_key="$file_path"
  fi
  CANDIDATE_LINES+=("${sort_key}"$'\t'"${file_path}")
}

declare -a CANDIDATE_FILES=()
if [ -f "$INPUT_PATH" ]; then
  if [ -n "$MANIFEST_PATH" ]; then
    echo "UNNEST_AB_POLICY_REVIEW_MANIFEST is supported only when input is a directory: $INPUT_PATH" >&2
    exit 2
  fi
  if [ ! -r "$INPUT_PATH" ]; then
    echo "history summary file is not readable: $INPUT_PATH" >&2
    exit 2
  fi
  status_line="$(grep -E '^boundary_history_status\|' "$INPUT_PATH" | tail -n 1 || true)"
  if [ -n "$status_line" ]; then
    add_candidate_file "$INPUT_PATH" "$status_line"
  fi
elif [ -d "$INPUT_PATH" ]; then
  manifest_scan_idx=0
  if [ -n "$MANIFEST_PATH" ]; then
    MANIFEST_ACTIVE=1
    if [ "$MANIFEST_TRUSTED_MODE" = "on" ]; then
      MANIFEST_TRUSTED_ACTIVE=1
    fi
    if [[ "$MANIFEST_PATH" != /* ]]; then
      echo "UNNEST_AB_POLICY_REVIEW_MANIFEST must be an absolute path: $MANIFEST_PATH" >&2
      exit 2
    fi
    if [ ! -f "$MANIFEST_PATH" ]; then
      echo "policy review manifest file not found: $MANIFEST_PATH" >&2
      exit 2
    fi
    if [ ! -r "$MANIFEST_PATH" ]; then
      echo "policy review manifest file is not readable: $MANIFEST_PATH" >&2
      exit 2
    fi
    if [ -n "$MANIFEST_MAX_AGE_SECONDS" ]; then
      MANIFEST_FRESHNESS_CHECKED=1
      if [ "$MANIFEST_FRESHNESS_PRECHECKED" = "on" ]; then
        MANIFEST_FRESHNESS_STATUS="prechecked"
      else
        if [ ! -x "$MANIFEST_FRESHNESS_SCRIPT" ]; then
          echo "manifest freshness script not executable: $MANIFEST_FRESHNESS_SCRIPT" >&2
          exit 2
        fi
        set +e
        if [ -n "$MANIFEST_NOW_EPOCH" ]; then
          MANIFEST_FRESHNESS_OUTPUT="$(
            bash "$MANIFEST_FRESHNESS_SCRIPT" "$MANIFEST_PATH" "$MANIFEST_MAX_AGE_SECONDS" "$MANIFEST_NOW_EPOCH" 2>&1
          )"
        else
          MANIFEST_FRESHNESS_OUTPUT="$(
            bash "$MANIFEST_FRESHNESS_SCRIPT" "$MANIFEST_PATH" "$MANIFEST_MAX_AGE_SECONDS" 2>&1
          )"
        fi
        MANIFEST_FRESHNESS_EXIT=$?
        set -e
        printf '%s\n' "$MANIFEST_FRESHNESS_OUTPUT"
        if [ "$MANIFEST_FRESHNESS_EXIT" -ne 0 ]; then
          MANIFEST_FRESHNESS_STATUS="error"
          exit "$MANIFEST_FRESHNESS_EXIT"
        fi
        MANIFEST_FRESHNESS_STATUS="ok"
      fi
    fi
    if [ "$MANIFEST_TRUSTED_ACTIVE" -eq 1 ]; then
      load_trusted_manifest_file
    else
      validate_manifest_file
    fi
  fi
  if [ "$MANIFEST_ACTIVE" -eq 1 ] && [ "$MANIFEST_TRUSTED_ACTIVE" -eq 1 ]; then
    for ((i = 0; i < MANIFEST_ENTRIES; i++)); do
      file_path="${MANIFEST_PATHS[$i]}"
      status_line="${MANIFEST_STATUS_LINES[$i]}"
      add_candidate_file "$file_path" "$status_line"
      MANIFEST_FRESH_HITS=$((MANIFEST_FRESH_HITS + 1))
    done
  else
  while IFS= read -r file_path; do
    manifest_idx=""
    manifest_meta=""
    actual_mtime=""
    actual_size=""
    status_line=""
    [ -n "$file_path" ] || continue
    if [ ! -r "$file_path" ]; then
      continue
    fi

    if [ "$MANIFEST_ACTIVE" -eq 1 ]; then
      while [ "$manifest_scan_idx" -lt "$MANIFEST_ENTRIES" ] && [[ "${MANIFEST_PATHS[$manifest_scan_idx]}" < "$file_path" ]]; do
        manifest_scan_idx=$((manifest_scan_idx + 1))
      done
      if [ "$manifest_scan_idx" -lt "$MANIFEST_ENTRIES" ] && [ "${MANIFEST_PATHS[$manifest_scan_idx]}" = "$file_path" ]; then
        manifest_idx="$manifest_scan_idx"
        manifest_meta="$(file_mtime_size "$file_path" || true)"
        if [ -n "$manifest_meta" ]; then
          read -r actual_mtime actual_size <<< "$manifest_meta"
        fi
        if [ -n "$actual_mtime" ] && [ -n "$actual_size" ] && \
          [ "$actual_mtime" = "${MANIFEST_MTIMES[$manifest_idx]}" ] && \
          [ "$actual_size" = "${MANIFEST_SIZES[$manifest_idx]}" ]; then
          status_line="${MANIFEST_STATUS_LINES[$manifest_idx]}"
          MANIFEST_FRESH_HITS=$((MANIFEST_FRESH_HITS + 1))
        else
          MANIFEST_STALE_ENTRIES=$((MANIFEST_STALE_ENTRIES + 1))
        fi
      else
        MANIFEST_MISSING_ENTRIES=$((MANIFEST_MISSING_ENTRIES + 1))
      fi
    fi

    if [ -z "$status_line" ]; then
      status_line="$(grep -E '^boundary_history_status\|' "$file_path" | tail -n 1 || true)"
    fi
    if [ -n "$status_line" ]; then
      add_candidate_file "$file_path" "$status_line"
    fi
  done < <(find "$INPUT_PATH" -maxdepth 1 -type f | sort)
  fi
else
  echo "history_summary_file_or_dir not found: $INPUT_PATH" >&2
  exit 2
fi

if [ "${#CANDIDATE_LINES[@]}" -eq 0 ]; then
  echo "no readable boundary_history summary artifacts found in input after filters: input=$INPUT_PATH from_date=${FROM_DATE_KEY:-none} to_date=${TO_DATE_KEY:-none} date_map=${DATE_MAP_PATH:-none} date_map_index=${DATE_MAP_INDEX_PATH:-none}" >&2
  exit 2
fi

while IFS=$'\t' read -r _sort_key sorted_path; do
  [ -n "$sorted_path" ] || continue
  CANDIDATE_FILES+=("$sorted_path")
done < <(
  printf '%s\n' "${CANDIDATE_LINES[@]}" | sort -t $'\t' -k1,1 -k2,2
)

candidate_count="${#CANDIDATE_FILES[@]}"
start_index=$((candidate_count - WINDOW_FILES))
if [ "$start_index" -lt 0 ]; then
  start_index=0
fi

declare -a SELECTED_FILES=()
for ((i = start_index; i < candidate_count; i++)); do
  SELECTED_FILES+=("${CANDIDATE_FILES[$i]}")
done
selected_count="${#SELECTED_FILES[@]}"
if [ "$selected_count" -eq 0 ]; then
  echo "no selected files after window filter: window_files=$WINDOW_FILES candidates=$candidate_count" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "/private/tmp/clustered_pg_boundary_policy_review_window.XXXXXX")"
STAGE_DIR="$WORKDIR/selected"
AGGREGATE_SUMMARY="$WORKDIR/aggregate.summary"
mkdir -p "$STAGE_DIR"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

ordinal=0
for source_file in "${SELECTED_FILES[@]}"; do
  ordinal=$((ordinal + 1))
  stage_file="$(printf '%s/%03d_%s' "$STAGE_DIR" "$ordinal" "$(basename "$source_file")")"
  cp "$source_file" "$stage_file"
  echo "boundary_history_policy_review_selected|ordinal=$ordinal|source=$source_file|staged=$stage_file"
done

bash "$ACCUMULATE_SCRIPT" "$STAGE_DIR" "$STRICT_MIN_OBS" >"$AGGREGATE_SUMMARY"
cat "$AGGREGATE_SUMMARY"

if [ -n "$AGGREGATE_OUT" ]; then
  cp "$AGGREGATE_SUMMARY" "$AGGREGATE_OUT"
  AGGREGATE_SUMMARY_REPORTED="$AGGREGATE_OUT"
  AGGREGATE_SUMMARY_PERSISTED=1
else
  AGGREGATE_SUMMARY_REPORTED="ephemeral"
  AGGREGATE_SUMMARY_PERSISTED=0
fi

set +e
POLICY_DELTA_OUTPUT="$(
  bash "$POLICY_DELTA_SCRIPT" "$AGGREGATE_SUMMARY" "$STRICT_MIN_OBS" "$CURRENT_BALANCED_MAX" "$CURRENT_BOUNDARY40_MAX" "$CURRENT_BOUNDARY56_MIN" "$CURRENT_PRESSURE_MIN" "$DERIVE_MAX_HEADROOM" "$DERIVE_MIN_FLOOR_MARGIN" "$DELTA_TOLERANCE" "$ENFORCE_ON_REVIEW" 2>&1
)"
POLICY_DELTA_STATUS=$?
set -e
printf '%s\n' "$POLICY_DELTA_OUTPUT"

POLICY_DELTA_REVIEW_STATUS="$(
  printf '%s\n' "$POLICY_DELTA_OUTPUT" | awk -F'|' '
    /^boundary_history_policy_delta_status\|/ {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^status=/) {
          sub(/^status=/, "", $i)
          print $i
          exit
        }
      }
    }
  '
)"
if [ -z "$POLICY_DELTA_REVIEW_STATUS" ]; then
  POLICY_DELTA_REVIEW_STATUS="unknown"
fi

if [ "$POLICY_DELTA_STATUS" -ne 0 ]; then
  echo "boundary_history_policy_review_window|status=error|strict_min_observations=$STRICT_MIN_OBS|requested_window_files=$WINDOW_FILES|candidate_files=$candidate_count|selected_files=$selected_count|from_date=${FROM_DATE_KEY:-none}|to_date=${TO_DATE_KEY:-none}|date_map_active=$DATE_MAP_ACTIVE|policy_delta_status=$POLICY_DELTA_REVIEW_STATUS|policy_delta_exit_code=$POLICY_DELTA_STATUS|aggregate_summary=$AGGREGATE_SUMMARY_REPORTED|aggregate_summary_persisted=$AGGREGATE_SUMMARY_PERSISTED|manifest_active=$MANIFEST_ACTIVE|manifest_entries=$MANIFEST_ENTRIES|manifest_fresh_hits=$MANIFEST_FRESH_HITS|manifest_stale_entries=$MANIFEST_STALE_ENTRIES|manifest_missing_entries=$MANIFEST_MISSING_ENTRIES|manifest_trusted=$MANIFEST_TRUSTED_ACTIVE|manifest_freshness_checked=$MANIFEST_FRESHNESS_CHECKED|manifest_freshness_status=$MANIFEST_FRESHNESS_STATUS"
  exit "$POLICY_DELTA_STATUS"
fi

echo "boundary_history_policy_review_window|status=ok|strict_min_observations=$STRICT_MIN_OBS|requested_window_files=$WINDOW_FILES|candidate_files=$candidate_count|selected_files=$selected_count|from_date=${FROM_DATE_KEY:-none}|to_date=${TO_DATE_KEY:-none}|date_map_active=$DATE_MAP_ACTIVE|policy_delta_status=$POLICY_DELTA_REVIEW_STATUS|aggregate_summary=$AGGREGATE_SUMMARY_REPORTED|aggregate_summary_persisted=$AGGREGATE_SUMMARY_PERSISTED|manifest_active=$MANIFEST_ACTIVE|manifest_entries=$MANIFEST_ENTRIES|manifest_fresh_hits=$MANIFEST_FRESH_HITS|manifest_stale_entries=$MANIFEST_STALE_ENTRIES|manifest_missing_entries=$MANIFEST_MISSING_ENTRIES|manifest_trusted=$MANIFEST_TRUSTED_ACTIVE|manifest_freshness_checked=$MANIFEST_FRESHNESS_CHECKED|manifest_freshness_status=$MANIFEST_FRESHNESS_STATUS"
