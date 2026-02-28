#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# sorted_heap vs heap+btree — Comparative Benchmark
# ============================================================
#
# Measures INSERT rows/sec and SELECT queries/sec at scale,
# comparing sorted_heap (zone map pruning) with heap + btree PK.
#
# Usage: ./scripts/bench_sorted_heap.sh [tmp_root] [port] [scales]
#   scales: comma-separated list, e.g. "1000000,10000000,100000000"

TMP_ROOT="${1:-/private/tmp}"
PORT="${2:-65494}"
SCALES_CSV="${3:-1000000,10000000}"
PGBENCH_DURATION=10   # seconds per SELECT benchmark

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2; exit 2
fi
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ] || [ "$PORT" -ge 65535 ]; then
  echo "port must be 1025..65534" >&2; exit 2
fi

IFS=',' read -ra SCALES <<< "$SCALES_CSV"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if command -v pg_config >/dev/null 2>&1; then
  PG_BINDIR="$(pg_config --bindir)"
else
  PG_BINDIR="/opt/homebrew/Cellar/postgresql@18/18.1_1/bin"
fi

TMP_DIR=""

cleanup() {
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/data" ]; then
    "$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

# --- Create ephemeral cluster with tuned settings ---
TMP_DIR="$(mktemp -d "$TMP_ROOT/clustered_pg_bench.XXXXXX")"
make -C "$ROOT_DIR" install >/dev/null 2>&1
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1

cat >> "$TMP_DIR/data/postgresql.conf" <<'PGCONF'
shared_buffers = 4GB
work_mem = 256MB
maintenance_work_mem = 2GB
effective_cache_size = 48GB
max_wal_size = 8GB
checkpoint_timeout = 30min
wal_level = minimal
max_wal_senders = 0
fsync = on
synchronous_commit = off
log_min_messages = warning
PGCONF

"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

PSQL_TIMING() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -qtAX \
    -c "\\timing on" "$@" 2>&1
}

PSQL -c "CREATE EXTENSION clustered_pg"

# --- Helper: format number with commas ---
fmt() {
  printf "%'d" "$1" 2>/dev/null || echo "$1"
}

# --- Helper: extract ms from psql \timing output ---
extract_ms() {
  # psql outputs "Time: 1234.567 ms" or "Time: 12:34.567 (m:ss.ms)"
  local output="$1"
  # Try "Time: X ms" format first
  local ms
  ms=$(echo "$output" | grep -oE 'Time: [0-9]+(\.[0-9]+)? ms' | grep -oE '[0-9]+(\.[0-9]+)?' | head -1)
  if [ -n "$ms" ]; then
    echo "$ms"
    return
  fi
  # Try "Time: mm:ss.ms" format
  local mmss
  mmss=$(echo "$output" | grep -oE 'Time: [0-9]+:[0-9]+\.[0-9]+' | grep -oE '[0-9]+:[0-9]+\.[0-9]+' | head -1)
  if [ -n "$mmss" ]; then
    local min sec
    min=$(echo "$mmss" | cut -d: -f1)
    sec=$(echo "$mmss" | cut -d: -f2)
    echo "$min $sec" | awk '{printf "%.3f", $1*60000 + $2*1000}'
    return
  fi
  echo "0"
}

# --- Helper: run pgbench and extract TPS ---
run_pgbench() {
  local script_file="$1" scale="$2" label="$3"
  local output
  output=$("$PG_BINDIR/pgbench" -h "$TMP_DIR" -p "$PORT" postgres \
    -n -T "$PGBENCH_DURATION" -f "$script_file" \
    -D scale="$scale" -c 1 -j 1 2>&1)
  # Extract TPS from "tps = XXXX.YYYY (without initial connection time)"
  local tps
  tps=$(echo "$output" | grep -oE 'tps = [0-9]+(\.[0-9]+)?' | grep -oE '[0-9]+(\.[0-9]+)?' | head -1)
  if [ -z "$tps" ]; then
    tps="0"
  fi
  echo "$tps"
}

# --- Create pgbench scripts ---
mkdir -p "$TMP_DIR/bench"

# Point query
cat > "$TMP_DIR/bench/point_sh.sql" <<'SQL'
\set r random(1, :scale)
SELECT * FROM sh_bench WHERE id = :r;
SQL

cat > "$TMP_DIR/bench/point_heap.sql" <<'SQL'
\set r random(1, :scale)
SELECT * FROM heap_bench WHERE id = :r;
SQL

# Narrow range (100 rows)
cat > "$TMP_DIR/bench/narrow_sh.sql" <<'SQL'
\set r random(1, :scale - 100)
SELECT count(*) FROM sh_bench WHERE id BETWEEN :r AND :r + 100;
SQL

cat > "$TMP_DIR/bench/narrow_heap.sql" <<'SQL'
\set r random(1, :scale - 100)
SELECT count(*) FROM heap_bench WHERE id BETWEEN :r AND :r + 100;
SQL

# Medium range (5K rows)
cat > "$TMP_DIR/bench/medium_sh.sql" <<'SQL'
\set r random(1, :scale - 5000)
SELECT count(*) FROM sh_bench WHERE id BETWEEN :r AND :r + 5000;
SQL

cat > "$TMP_DIR/bench/medium_heap.sql" <<'SQL'
\set r random(1, :scale - 5000)
SELECT count(*) FROM heap_bench WHERE id BETWEEN :r AND :r + 5000;
SQL

# Wide range (100K rows)
cat > "$TMP_DIR/bench/wide_sh.sql" <<'SQL'
\set r random(1, :scale - 100000)
SELECT count(*) FROM sh_bench WHERE id BETWEEN :r AND :r + 100000;
SQL

cat > "$TMP_DIR/bench/wide_heap.sql" <<'SQL'
\set r random(1, :scale - 100000)
SELECT count(*) FROM heap_bench WHERE id BETWEEN :r AND :r + 100000;
SQL

echo "============================================================"
echo "sorted_heap vs heap+btree — Comparative Benchmark"
echo "============================================================"
echo "Hardware: $(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo '?') CPUs, $(sysctl -n hw.memsize 2>/dev/null | awk '{printf "%.0f GB", $1/1024/1024/1024}' 2>/dev/null || echo '? GB') RAM"
echo "PG config: shared_buffers=4GB, work_mem=256MB, maintenance_work_mem=2GB"
echo "pgbench: ${PGBENCH_DURATION}s per test, 1 client"
echo ""

for N in "${SCALES[@]}"; do
  echo "============================================================"
  echo "=== Scale: $(fmt "$N") rows ==="
  echo "============================================================"

  # --- Create tables ---
  PSQL -c "DROP TABLE IF EXISTS sh_bench CASCADE"
  PSQL -c "DROP TABLE IF EXISTS heap_bench CASCADE"

  PSQL -c "CREATE TABLE sh_bench(id bigint PRIMARY KEY, category int, val text) USING sorted_heap"
  PSQL -c "CREATE TABLE heap_bench(id bigint PRIMARY KEY, category int, val text)"

  # ============================================================
  # INSERT benchmark
  # ============================================================
  echo ""
  echo "--- INSERT (bulk load) ---"

  # sorted_heap INSERT
  sh_insert_output=$(PSQL -c "\\timing" -c "
    INSERT INTO sh_bench
      SELECT g, (g % 100)::int, 'row-' || g
      FROM generate_series(1, $N) g;
  " 2>&1)
  sh_insert_ms=$(extract_ms "$sh_insert_output")

  # heap INSERT
  heap_insert_output=$(PSQL -c "\\timing" -c "
    INSERT INTO heap_bench
      SELECT g, (g % 100)::int, 'row-' || g
      FROM generate_series(1, $N) g;
  " 2>&1)
  heap_insert_ms=$(extract_ms "$heap_insert_output")

  # Calculate rows/sec
  sh_insert_rps=$(echo "$N $sh_insert_ms" | awk '{if($2>0) printf "%.0f", $1/($2/1000); else print "?"}')
  heap_insert_rps=$(echo "$N $heap_insert_ms" | awk '{if($2>0) printf "%.0f", $1/($2/1000); else print "?"}')
  sh_insert_sec=$(echo "$sh_insert_ms" | awk '{printf "%.1f", $1/1000}')
  heap_insert_sec=$(echo "$heap_insert_ms" | awk '{printf "%.1f", $1/1000}')

  echo "  sorted_heap:  $(fmt "$sh_insert_rps") rows/sec  (${sh_insert_sec}s)"
  echo "  heap+btree:   $(fmt "$heap_insert_rps") rows/sec  (${heap_insert_sec}s)"

  # ============================================================
  # Compact
  # ============================================================
  echo ""
  echo "--- compact (sorted_heap only) ---"

  compact_output=$(PSQL -c "\\timing" -c "SELECT sorted_heap_compact('sh_bench'::regclass)" 2>&1)
  compact_ms=$(extract_ms "$compact_output")
  compact_sec=$(echo "$compact_ms" | awk '{printf "%.1f", $1/1000}')
  echo "  compact:      ${compact_sec}s"

  # ============================================================
  # Table sizes
  # ============================================================
  echo ""
  echo "--- Table size ---"

  sh_total=$(PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('sh_bench'::regclass))")
  heap_table=$(PSQL -c "SELECT pg_size_pretty(pg_relation_size('heap_bench'::regclass))")
  heap_idx=$(PSQL -c "SELECT pg_size_pretty(pg_indexes_size('heap_bench'::regclass))")
  heap_total=$(PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('heap_bench'::regclass))")

  echo "  sorted_heap:  $sh_total"
  echo "  heap+btree:   $heap_total  (table $heap_table + index $heap_idx)"

  # ============================================================
  # CHECKPOINT + warm cache
  # ============================================================
  PSQL -c "CHECKPOINT"
  # Warm cache with dummy scans
  PSQL -c "SELECT count(*) FROM sh_bench" >/dev/null
  PSQL -c "SELECT count(*) FROM heap_bench" >/dev/null

  # ============================================================
  # SELECT benchmarks (pgbench)
  # ============================================================
  echo ""
  echo "--- SELECT (1 client, ${PGBENCH_DURATION}s per test) ---"

  # Point query
  tps_sh=$(run_pgbench "$TMP_DIR/bench/point_sh.sql" "$N" "point_sh")
  tps_heap=$(run_pgbench "$TMP_DIR/bench/point_heap.sql" "$N" "point_heap")
  tps_sh_fmt=$(echo "$tps_sh" | awk '{printf "%.0f", $1}')
  tps_heap_fmt=$(echo "$tps_heap" | awk '{printf "%.0f", $1}')
  echo "  Point (1 row):    sorted_heap $(fmt "$tps_sh_fmt") tps | heap+btree $(fmt "$tps_heap_fmt") tps"

  # Narrow range
  tps_sh=$(run_pgbench "$TMP_DIR/bench/narrow_sh.sql" "$N" "narrow_sh")
  tps_heap=$(run_pgbench "$TMP_DIR/bench/narrow_heap.sql" "$N" "narrow_heap")
  tps_sh_fmt=$(echo "$tps_sh" | awk '{printf "%.0f", $1}')
  tps_heap_fmt=$(echo "$tps_heap" | awk '{printf "%.0f", $1}')
  echo "  Narrow (100):     sorted_heap $(fmt "$tps_sh_fmt") tps | heap+btree $(fmt "$tps_heap_fmt") tps"

  # Medium range
  tps_sh=$(run_pgbench "$TMP_DIR/bench/medium_sh.sql" "$N" "medium_sh")
  tps_heap=$(run_pgbench "$TMP_DIR/bench/medium_heap.sql" "$N" "medium_heap")
  tps_sh_fmt=$(echo "$tps_sh" | awk '{printf "%.0f", $1}')
  tps_heap_fmt=$(echo "$tps_heap" | awk '{printf "%.0f", $1}')
  echo "  Medium (5K):      sorted_heap $(fmt "$tps_sh_fmt") tps | heap+btree $(fmt "$tps_heap_fmt") tps"

  # Wide range (skip if scale < 200K)
  if [ "$N" -ge 200000 ]; then
    tps_sh=$(run_pgbench "$TMP_DIR/bench/wide_sh.sql" "$N" "wide_sh")
    tps_heap=$(run_pgbench "$TMP_DIR/bench/wide_heap.sql" "$N" "wide_heap")
    tps_sh_fmt=$(echo "$tps_sh" | awk '{printf "%.0f", $1}')
    tps_heap_fmt=$(echo "$tps_heap" | awk '{printf "%.0f", $1}')
    echo "  Wide (100K):      sorted_heap $(fmt "$tps_sh_fmt") tps | heap+btree $(fmt "$tps_heap_fmt") tps"
  fi

  echo ""
done

echo "============================================================"
echo "Benchmark complete."
echo "============================================================"
