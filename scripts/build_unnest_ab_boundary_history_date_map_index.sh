#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <date_map_csv_abs_path> <date_map_index_tsv_abs_path>" >&2
  exit 2
fi

DATE_MAP_CSV="$1"
DATE_MAP_INDEX_OUT="$2"

if [[ "$DATE_MAP_CSV" != /* ]]; then
  echo "date_map_csv_abs_path must be absolute: $DATE_MAP_CSV" >&2
  exit 2
fi
if [[ "$DATE_MAP_INDEX_OUT" != /* ]]; then
  echo "date_map_index_tsv_abs_path must be absolute: $DATE_MAP_INDEX_OUT" >&2
  exit 2
fi
if [ ! -f "$DATE_MAP_CSV" ]; then
  echo "date map csv file not found: $DATE_MAP_CSV" >&2
  exit 2
fi
if [ ! -r "$DATE_MAP_CSV" ]; then
  echo "date map csv file is not readable: $DATE_MAP_CSV" >&2
  exit 2
fi
DATE_MAP_INDEX_OUT_DIR="$(dirname "$DATE_MAP_INDEX_OUT")"
if [ ! -d "$DATE_MAP_INDEX_OUT_DIR" ]; then
  echo "date map index output directory not found: $DATE_MAP_INDEX_OUT_DIR" >&2
  exit 2
fi
if [ ! -w "$DATE_MAP_INDEX_OUT_DIR" ]; then
  echo "date map index output directory is not writable: $DATE_MAP_INDEX_OUT_DIR" >&2
  exit 2
fi

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

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pg_sorted_heap_date_map_index_build.XXXXXX")"
TMP_OUT="$TMP_DIR/date_map.index.tsv"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

declare -a PARSED_ENTRIES=()
line_no=0
while IFS= read -r line || [ -n "$line" ]; do
  line_no=$((line_no + 1))
  line="$(trim_whitespace "$line")"
  [ -z "$line" ] && continue
  [[ "$line" == \#* ]] && continue
  if [[ "$line" != *,* ]]; then
    echo "invalid date map line (missing comma) at $DATE_MAP_CSV:$line_no: $line" >&2
    exit 2
  fi
  key_raw="${line%%,*}"
  date_raw="${line#*,}"
  key="$(trim_whitespace "$key_raw")"
  date_raw="$(trim_whitespace "$date_raw")"
  if [ -z "$key" ] || [ -z "$date_raw" ]; then
    echo "invalid date map line (empty key/date) at $DATE_MAP_CSV:$line_no: $line" >&2
    exit 2
  fi
  if ! date_key="$(normalize_date_key "$date_raw" 2>/dev/null)"; then
    echo "invalid date map line (bad date token) at $DATE_MAP_CSV:$line_no: $line" >&2
    exit 2
  fi
  if [ -z "$date_key" ]; then
    echo "invalid date map line (bad date token) at $DATE_MAP_CSV:$line_no: $line" >&2
    exit 2
  fi
  PARSED_ENTRIES+=("${key}"$'\t'"${date_key}")
done < "$DATE_MAP_CSV"

printf '# key\tdate\n' > "$TMP_OUT"
written=0
if [ "${#PARSED_ENTRIES[@]}" -gt 0 ]; then
  prev_key=""
  prev_date=""
  while IFS=$'\t' read -r key date; do
    [ -n "$key" ] || continue
    [ -n "$date" ] || continue
    if [ "$key" = "$prev_key" ]; then
      if [ "$date" != "$prev_date" ]; then
        echo "conflicting date map key at $DATE_MAP_CSV: key=$key existing_date=$prev_date new_date=$date" >&2
        exit 2
      fi
      continue
    fi
    printf '%s\t%s\n' "$key" "$date" >> "$TMP_OUT"
    prev_key="$key"
    prev_date="$date"
    written=$((written + 1))
  done < <(printf '%s\n' "${PARSED_ENTRIES[@]}" | LC_ALL=C sort -t $'\t' -k1,1 -k2,2)
fi

mv "$TMP_OUT" "$DATE_MAP_INDEX_OUT"
echo "unnest_ab_boundary_history_date_map_index|status=ok|input=$DATE_MAP_CSV|output=$DATE_MAP_INDEX_OUT|parsed_entries=${#PARSED_ENTRIES[@]}|written_entries=$written"
