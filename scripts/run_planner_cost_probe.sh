#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ROWS_CSV="${1:-1000,10000,50000}"
PORT="${2:-65496}"
PROBE_OUT="${3:-}"
TMP_ROOT="${TMPDIR:-/private/tmp}"
TMP_DIR=""

if [[ "$PROBE_OUT" == auto:* ]]; then
  PROBE_OUT_DIR="${PROBE_OUT#auto:}"
  if [ -z "$PROBE_OUT_DIR" ]; then
    echo "auto output directory must not be empty" >&2
    exit 2
  fi
  if [[ "$PROBE_OUT_DIR" != /* ]]; then
    echo "auto output directory must be an absolute path" >&2
    exit 2
  fi
  PROBE_OUT="$PROBE_OUT_DIR/pg_sorted_heap_planner_probe_$(date +%Y%m%d_%H%M%S)_$$.log"
elif [ "$PROBE_OUT" = "auto" ]; then
  PROBE_OUT="/private/tmp/pg_sorted_heap_planner_probe_$(date +%Y%m%d_%H%M%S)_$$.log"
fi

if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ] || [ "$PORT" -ge 65535 ]; then
  echo "port must be an integer in range 1025..65534" >&2
  exit 2
fi

if [[ "$TMP_ROOT" != /* ]]; then
  echo "TMPDIR must be absolute when provided: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp root not found: $TMP_ROOT" >&2
  exit 2
fi

if command -v pg_config >/dev/null 2>&1; then
  PG_BINDIR="$(pg_config --bindir)"
else
  PG_BINDIR="/opt/homebrew/Cellar/postgresql@18/18.1_1/bin"
fi

cleanup() {
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/data" ]; then
    "$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_planner_probe.XXXXXX")"

rows_list=()
forced_index_hits=0
forced_index_cases=0
last_total_cost=""
IFS=',' read -r -a raw_rows <<<"$ROWS_CSV"
for r in "${raw_rows[@]}"; do
  r="${r//[[:space:]]/}"
  if [ -z "$r" ]; then
    continue
  fi
  if ! [[ "$r" =~ ^[0-9]+$ ]] || [ "$r" -le 0 ]; then
    echo "rows list must contain only positive integers: '$r'" >&2
    exit 2
  fi
  rows_list+=("$r")
done
if [ "${#rows_list[@]}" -eq 0 ]; then
  echo "rows list is empty; use e.g. 1000,10000,50000" >&2
  exit 2
fi

run_psql() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -X -qAt -c "$1"
}

parse_plan_line() {
  local line="$1"
  local trimmed node startup total rows width
  trimmed="$(printf "%s" "$line" | sed -E 's/^[[:space:]]+//')"
  node="$(printf "%s" "$trimmed" | sed -E 's/[[:space:]]+\(cost=.*$//')"
  startup="$(printf "%s" "$trimmed" | sed -nE 's/.*\(cost=([0-9.]+)\.\.([0-9.]+) rows=([0-9]+) width=([0-9]+)\).*/\1/p')"
  total="$(printf "%s" "$trimmed" | sed -nE 's/.*\(cost=([0-9.]+)\.\.([0-9.]+) rows=([0-9]+) width=([0-9]+)\).*/\2/p')"
  rows="$(printf "%s" "$trimmed" | sed -nE 's/.*\(cost=([0-9.]+)\.\.([0-9.]+) rows=([0-9]+) width=([0-9]+)\).*/\3/p')"
  width="$(printf "%s" "$trimmed" | sed -nE 's/.*\(cost=([0-9.]+)\.\.([0-9.]+) rows=([0-9]+) width=([0-9]+)\).*/\4/p')"

  if [ -z "$node" ] || [ -z "$startup" ] || [ -z "$total" ] || [ -z "$rows" ] || [ -z "$width" ]; then
    echo "failed to parse explain line: $line" >&2
    exit 2
  fi
  printf "%s|%s|%s|%s|%s" "$node" "$startup" "$total" "$rows" "$width"
}

emit_probe() {
  local row_count="$1"
  local fastpath="$2"
  local qtype="$3"
  local sql="$4"
  local explain_line parsed fastpath_setting planner_setting

  if [ "$fastpath" = "on" ]; then
    fastpath_setting="on"
  else
    fastpath_setting="off"
  fi

  if [ "$qtype" = "point_forced_index" ]; then
    planner_setting="SET enable_seqscan = off; SET enable_bitmapscan = off;"
  else
    planner_setting="SET enable_seqscan = on; SET enable_bitmapscan = on;"
  fi

  explain_line="$(run_psql "SET enable_indexscan = on; SET enable_indexonlyscan = on; $planner_setting EXPLAIN $sql;" | head -n 1)"
  parsed="$(parse_plan_line "$explain_line")"
  IFS='|' read -r node startup total rows width <<<"$parsed"
  last_total_cost="$total"
  echo "planner_probe|rows=$row_count|fastpath=$fastpath|query=$qtype|plan=$node|startup_cost=$startup|total_cost=$total|plan_rows=$rows|plan_width=$width"
  if [ "$qtype" = "point_forced_index" ]; then
    forced_index_cases=$((forced_index_cases + 1))
    if [[ "$node" == *"Index"* ]]; then
      forced_index_hits=$((forced_index_hits + 1))
    fi
  fi
}

run_single_case() {
  local row_count="$1"
  local range_high
  local off_forced_total=""
  local on_forced_total=""
  local off_over_on=""

  range_high=$(( row_count / 100 ))
  if [ "$range_high" -lt 10 ]; then
    range_high=10
  fi

  run_psql "DROP TABLE IF EXISTS pg_sorted_heap_planner_probe;"
  run_psql "CREATE TABLE pg_sorted_heap_planner_probe(id bigint) USING clustered_heap;"
  run_psql "CREATE INDEX pg_sorted_heap_planner_probe_idx ON pg_sorted_heap_planner_probe USING clustered_pk_index (id);"
  run_psql "INSERT INTO pg_sorted_heap_planner_probe(id) SELECT generate_series(1,$row_count);"
  run_psql "ANALYZE pg_sorted_heap_planner_probe;"
  emit_probe "$row_count" "off" "point_default" "SELECT id FROM pg_sorted_heap_planner_probe WHERE id = $row_count;"
  emit_probe "$row_count" "off" "range_default" "SELECT id FROM pg_sorted_heap_planner_probe WHERE id BETWEEN 1 AND $range_high;"
  emit_probe "$row_count" "off" "point_forced_index" "SELECT id FROM pg_sorted_heap_planner_probe WHERE id = $row_count;"
  off_forced_total="$last_total_cost"
  emit_probe "$row_count" "on" "point_default" "SELECT id FROM pg_sorted_heap_planner_probe WHERE id = $row_count;"
  emit_probe "$row_count" "on" "range_default" "SELECT id FROM pg_sorted_heap_planner_probe WHERE id BETWEEN 1 AND $range_high;"
  emit_probe "$row_count" "on" "point_forced_index" "SELECT id FROM pg_sorted_heap_planner_probe WHERE id = $row_count;"
  on_forced_total="$last_total_cost"

  off_over_on="$(awk -v off="$off_forced_total" -v on="$on_forced_total" 'BEGIN { if (on <= 0) { print "inf"; } else { printf "%.6f", off / on; } }')"
  echo "planner_probe_compare|rows=$row_count|forced_point_off_total=$off_forced_total|forced_point_on_total=$on_forced_total|off_over_on=$off_over_on"
  run_psql "RESET enable_seqscan; RESET enable_bitmapscan; RESET enable_indexscan; RESET enable_indexonlyscan;"
  run_psql "DROP TABLE pg_sorted_heap_planner_probe;"
}

run_probe_main() {
  local row_count

  echo "planner_probe_begin|rows_csv=$ROWS_CSV|port=$PORT"

  for row_count in "${rows_list[@]}"; do
    run_single_case "$row_count"
  done

  echo "planner_probe_summary|forced_index_hits=$forced_index_hits|forced_index_cases=$forced_index_cases"
  if [ "$forced_index_cases" -gt 0 ] && [ "$forced_index_hits" -eq 0 ]; then
    echo "planner_probe_warning|no_index_path_detected=1"
  fi
  echo "planner_probe_status=ok"
}

make -C "$ROOT_DIR" install >/dev/null
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null
"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" -o "-k $TMP_DIR -p $PORT" start >/dev/null
run_psql "CREATE EXTENSION pg_sorted_heap;"

if [ -n "$PROBE_OUT" ]; then
  mkdir -p "$(dirname "$PROBE_OUT")"
  run_probe_main | tee "$PROBE_OUT"
  echo "planner_probe_output: $PROBE_OUT"
else
  run_probe_main
fi
