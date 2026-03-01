#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUNS="${1:-5}"
BATCH_SIZE="${2:-500}"
BATCHES="${3:-40}"
SELECT_ITERS="${4:-200}"
PROBE_SIZE="${5:-128}"
PORT="${6:-65498}"
OUT_PATH="${7:-}"
PROBE_PATTERN_RAW="${8:-}"
EMIT_OBSERVABILITY_RAW="${UNNEST_AB_EMIT_OBSERVABILITY:-off}"
EMIT_OBSERVABILITY="off"
WARMUP_SELECTS_RAW="${UNNEST_AB_WARMUP_SELECTS:-1}"
WARMUP_SELECTS="1"
TMP_ROOT="${TMPDIR:-${TMPDIR:-/tmp}}"
TMP_DIR=""
PROBE_PATTERN=""

if [ "$OUT_PATH" = "auto" ]; then
  OUT_PATH="${TMPDIR:-/tmp}/pg_sorted_heap_unnest_ab_$(date +%Y%m%d_%H%M%S)_$$.log"
elif [[ "$OUT_PATH" == auto:* ]]; then
  OUT_DIR="${OUT_PATH#auto:}"
  if [ -z "$OUT_DIR" ]; then
    echo "auto output directory must not be empty" >&2
    exit 2
  fi
  if [[ "$OUT_DIR" != /* ]]; then
    echo "auto output directory must be an absolute path" >&2
    exit 2
  fi
  OUT_PATH="$OUT_DIR/pg_sorted_heap_unnest_ab_$(date +%Y%m%d_%H%M%S)_$$.log"
fi

for v in "$RUNS" "$BATCH_SIZE" "$BATCHES" "$SELECT_ITERS" "$PROBE_SIZE"; do
  if ! [[ "$v" =~ ^[0-9]+$ ]] || [ "$v" -le 0 ]; then
    echo "runs/batch_size/batches/select_iters/probe_size must be positive integers" >&2
    exit 2
  fi
done

if [ -n "$PROBE_PATTERN_RAW" ]; then
  PROBE_PATTERN="$(printf '%s' "$PROBE_PATTERN_RAW" | tr -d '[:space:]')"
  if ! [[ "$PROBE_PATTERN" =~ ^[0-9]+(,[0-9]+)*$ ]]; then
    echo "probe_pattern must be a comma-separated list of positive integers (example: 32,128,32)" >&2
    exit 2
  fi
  for pv in ${PROBE_PATTERN//,/ }; do
    if [ "$pv" -le 0 ]; then
      echo "probe_pattern entries must be positive integers" >&2
      exit 2
    fi
  done
else
  PROBE_PATTERN="$PROBE_SIZE"
fi

case "$EMIT_OBSERVABILITY_RAW" in
  on|true|1)
    EMIT_OBSERVABILITY="on"
    ;;
  off|false|0)
    EMIT_OBSERVABILITY="off"
    ;;
  *)
    echo "UNNEST_AB_EMIT_OBSERVABILITY must be one of: on/off/true/false/1/0" >&2
    exit 2
    ;;
esac

if ! [[ "$WARMUP_SELECTS_RAW" =~ ^[0-9]+$ ]]; then
  echo "UNNEST_AB_WARMUP_SELECTS must be a non-negative integer" >&2
  exit 2
fi
WARMUP_SELECTS="$WARMUP_SELECTS_RAW"

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

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_unnest_ab_probe.XXXXXX")"

make -C "$ROOT_DIR" install >/dev/null
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null
"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" -o "-k $TMP_DIR -p $PORT" start >/dev/null

run_probe() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 \
    -v runs="$RUNS" \
    -v batch_size="$BATCH_SIZE" \
    -v batches="$BATCHES" \
    -v select_iters="$SELECT_ITERS" \
    -v probe_size="$PROBE_SIZE" \
    -v probe_pattern="$PROBE_PATTERN" \
    -v warmup_selects="$WARMUP_SELECTS" \
    -v emit_observability="$EMIT_OBSERVABILITY" <<'SQL'
\set ON_ERROR_STOP on
CREATE EXTENSION pg_sorted_heap;
SET synchronous_commit = off;
SET jit = off;
SET enable_seqscan = off;
SET pg_sorted_heap.ab_runs = :'runs';
SET pg_sorted_heap.ab_batch_size = :'batch_size';
SET pg_sorted_heap.ab_batches = :'batches';
SET pg_sorted_heap.ab_select_iters = :'select_iters';
SET pg_sorted_heap.ab_probe_size = :'probe_size';
SET pg_sorted_heap.ab_probe_pattern = :'probe_pattern';
SET pg_sorted_heap.ab_warmup_selects = :'warmup_selects';

CREATE TEMP TABLE bench_results(
  run int NOT NULL,
  storage text NOT NULL,
  insert_ms double precision NOT NULL,
  insert_rows bigint NOT NULL,
  join_unnest_ms double precision NOT NULL,
  any_array_ms double precision NOT NULL,
  select_hits bigint NOT NULL
);

DO $$
DECLARE
  v_runs int := current_setting('pg_sorted_heap.ab_runs')::int;
  v_batch_size int := current_setting('pg_sorted_heap.ab_batch_size')::int;
  v_batches int := current_setting('pg_sorted_heap.ab_batches')::int;
  v_select_iters int := current_setting('pg_sorted_heap.ab_select_iters')::int;
  v_probe_size int := current_setting('pg_sorted_heap.ab_probe_size')::int;
  v_probe_pattern text := current_setting('pg_sorted_heap.ab_probe_pattern', true);
  v_warmup_selects int := current_setting('pg_sorted_heap.ab_warmup_selects')::int;
  v_probe_sizes int[];
  v_probe_count int;
  v_probe_idx int;
  v_cur_probe_size int;
  v_run int;
  v_storage text;
  v_started timestamptz;
  v_insert_ms double precision;
  v_join_unnest_ms double precision;
  v_any_array_ms double precision;
  v_insert_rows bigint;
  v_select_hits bigint;
  v_cnt bigint;
  v_probe_ids bigint[];
  v_probe_base bigint;
  v_ids bigint[];
  v_payloads text[];
  v_base bigint;
  b int;
  s int;
BEGIN
  IF v_runs <= 0 OR v_batch_size <= 0 OR v_batches <= 0 OR v_select_iters <= 0 OR v_probe_size <= 0 THEN
    RAISE EXCEPTION 'invalid benchmark settings';
  END IF;
  IF v_warmup_selects < 0 THEN
    RAISE EXCEPTION 'invalid warmup settings';
  END IF;

  IF v_probe_pattern IS NULL OR length(trim(v_probe_pattern)) = 0 THEN
    v_probe_sizes := ARRAY[v_probe_size];
  ELSE
    SELECT array_agg(part::int ORDER BY ord)
      INTO v_probe_sizes
    FROM (
      SELECT ord, trim(part) AS part
      FROM regexp_split_to_table(v_probe_pattern, ',') WITH ORDINALITY AS t(part, ord)
    ) s
    WHERE part <> '';
  END IF;

  v_probe_count := COALESCE(array_length(v_probe_sizes, 1), 0);
  IF v_probe_count <= 0 THEN
    RAISE EXCEPTION 'invalid probe pattern: %', v_probe_pattern;
  END IF;
  FOR s IN 1..v_probe_count LOOP
    IF v_probe_sizes[s] IS NULL OR v_probe_sizes[s] <= 0 THEN
      RAISE EXCEPTION 'invalid probe pattern entry at position %: %', s, v_probe_sizes[s];
    END IF;
  END LOOP;

  v_insert_rows := (v_batch_size::bigint * v_batches::bigint);

  FOR v_run IN 1..v_runs LOOP
    FOR v_storage IN SELECT unnest(ARRAY['heap','clustered']) LOOP
      EXECUTE 'DROP TABLE IF EXISTS bench_unnest_t';

      IF v_storage = 'heap' THEN
        EXECUTE 'CREATE TABLE bench_unnest_t(id bigint, payload text NOT NULL)';
        EXECUTE 'CREATE INDEX bench_unnest_t_idx ON bench_unnest_t(id)';
      ELSE
        EXECUTE 'CREATE TABLE bench_unnest_t(id bigint, payload text NOT NULL) USING clustered_heap';
        EXECUTE 'CREATE INDEX bench_unnest_t_idx ON bench_unnest_t USING clustered_pk_index (id)';
      END IF;

      v_started := clock_timestamp();
      FOR b IN 0..(v_batches - 1) LOOP
        v_base := ((v_run::bigint - 1) * v_insert_rows) + (b::bigint * v_batch_size::bigint);
        SELECT array_agg(x), array_agg(lpad((x % 1000000)::text, 32, '0'))
          INTO v_ids, v_payloads
        FROM generate_series(v_base + 1, v_base + v_batch_size) AS g(x);

        EXECUTE 'INSERT INTO bench_unnest_t(id, payload) SELECT * FROM unnest($1::bigint[], $2::text[])'
          USING v_ids, v_payloads;
      END LOOP;
      v_insert_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_started)) * 1000.0;

      v_select_hits := 0;
      FOR s IN 1..v_warmup_selects LOOP
        v_probe_idx := ((s - 1) % v_probe_count) + 1;
        v_cur_probe_size := v_probe_sizes[v_probe_idx];
        v_probe_base := (v_run::bigint - 1) * v_insert_rows + 1
                        + (((s - 1)::bigint * v_cur_probe_size::bigint)
                           % GREATEST(1, v_insert_rows - v_cur_probe_size::bigint + 1));
        SELECT array_agg(x)
          INTO v_probe_ids
        FROM generate_series(v_probe_base,
                             v_probe_base + v_cur_probe_size - 1) AS g(x);

        EXECUTE 'SELECT count(*) FROM bench_unnest_t t JOIN unnest($1::bigint[]) u(id) ON t.id = u.id'
          INTO v_cnt
          USING v_probe_ids;
        IF v_cnt <> v_cur_probe_size THEN
          RAISE EXCEPTION 'join_unnest warmup mismatch: storage %, run %, iter %, got %, expected %', v_storage, v_run, s, v_cnt, v_cur_probe_size;
        END IF;
      END LOOP;

      v_started := clock_timestamp();
      FOR s IN 1..v_select_iters LOOP
        v_probe_idx := ((s - 1) % v_probe_count) + 1;
        v_cur_probe_size := v_probe_sizes[v_probe_idx];
        v_probe_base := (v_run::bigint - 1) * v_insert_rows + 1
                        + (((s - 1)::bigint * v_cur_probe_size::bigint)
                           % GREATEST(1, v_insert_rows - v_cur_probe_size::bigint + 1));
        SELECT array_agg(x)
          INTO v_probe_ids
        FROM generate_series(v_probe_base,
                             v_probe_base + v_cur_probe_size - 1) AS g(x);

        EXECUTE 'SELECT count(*) FROM bench_unnest_t t JOIN unnest($1::bigint[]) u(id) ON t.id = u.id'
          INTO v_cnt
          USING v_probe_ids;
        IF v_cnt <> v_cur_probe_size THEN
          RAISE EXCEPTION 'join_unnest mismatch: storage %, run %, iter %, got %, expected %', v_storage, v_run, s, v_cnt, v_cur_probe_size;
        END IF;
        v_select_hits := v_select_hits + v_cur_probe_size;
      END LOOP;
      v_join_unnest_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_started)) * 1000.0;

      FOR s IN 1..v_warmup_selects LOOP
        v_probe_idx := ((s - 1) % v_probe_count) + 1;
        v_cur_probe_size := v_probe_sizes[v_probe_idx];
        v_probe_base := (v_run::bigint - 1) * v_insert_rows + 1
                        + (((s - 1)::bigint * v_cur_probe_size::bigint)
                           % GREATEST(1, v_insert_rows - v_cur_probe_size::bigint + 1));
        SELECT array_agg(x)
          INTO v_probe_ids
        FROM generate_series(v_probe_base,
                             v_probe_base + v_cur_probe_size - 1) AS g(x);

        EXECUTE 'SELECT count(*) FROM bench_unnest_t t WHERE t.id = ANY($1::bigint[])'
          INTO v_cnt
          USING v_probe_ids;
        IF v_cnt <> v_cur_probe_size THEN
          RAISE EXCEPTION 'any_array warmup mismatch: storage %, run %, iter %, got %, expected %', v_storage, v_run, s, v_cnt, v_cur_probe_size;
        END IF;
      END LOOP;

      v_started := clock_timestamp();
      FOR s IN 1..v_select_iters LOOP
        v_probe_idx := ((s - 1) % v_probe_count) + 1;
        v_cur_probe_size := v_probe_sizes[v_probe_idx];
        v_probe_base := (v_run::bigint - 1) * v_insert_rows + 1
                        + (((s - 1)::bigint * v_cur_probe_size::bigint)
                           % GREATEST(1, v_insert_rows - v_cur_probe_size::bigint + 1));
        SELECT array_agg(x)
          INTO v_probe_ids
        FROM generate_series(v_probe_base,
                             v_probe_base + v_cur_probe_size - 1) AS g(x);

        EXECUTE 'SELECT count(*) FROM bench_unnest_t t WHERE t.id = ANY($1::bigint[])'
          INTO v_cnt
          USING v_probe_ids;
        IF v_cnt <> v_cur_probe_size THEN
          RAISE EXCEPTION 'any_array mismatch: storage %, run %, iter %, got %, expected %', v_storage, v_run, s, v_cnt, v_cur_probe_size;
        END IF;
      END LOOP;
      v_any_array_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_started)) * 1000.0;

      INSERT INTO bench_results(run, storage, insert_ms, insert_rows, join_unnest_ms, any_array_ms, select_hits)
      VALUES (v_run, v_storage, v_insert_ms, v_insert_rows, v_join_unnest_ms, v_any_array_ms, v_select_hits);
    END LOOP;
  END LOOP;

  EXECUTE 'DROP TABLE IF EXISTS bench_unnest_t';
END $$;

\echo '--- aggregate avg ---'
WITH agg AS (
  SELECT storage,
         avg(insert_ms) AS avg_insert_ms,
         avg(insert_rows * 1000.0 / insert_ms) AS avg_insert_rps,
         avg(join_unnest_ms) AS avg_join_ms,
         avg(select_hits * 1000.0 / join_unnest_ms) AS avg_join_hps,
         avg(any_array_ms) AS avg_any_ms,
         avg(select_hits * 1000.0 / any_array_ms) AS avg_any_hps
  FROM bench_results
  GROUP BY storage
)
SELECT storage,
       round(avg_insert_ms::numeric, 3) AS avg_insert_ms,
       round(avg_insert_rps::numeric, 1) AS avg_insert_rows_per_s,
       round(avg_join_ms::numeric, 3) AS avg_join_unnest_ms,
       round(avg_join_hps::numeric, 1) AS avg_join_unnest_hits_per_s,
       round(avg_any_ms::numeric, 3) AS avg_any_array_ms,
       round(avg_any_hps::numeric, 1) AS avg_any_array_hits_per_s
FROM agg
ORDER BY storage;

\echo '--- ratio clustered/heap ---'
WITH agg AS (
  SELECT storage,
         avg(insert_rows * 1000.0 / insert_ms) AS avg_insert_rps,
         avg(select_hits * 1000.0 / join_unnest_ms) AS avg_join_hps,
         avg(select_hits * 1000.0 / any_array_ms) AS avg_any_hps
  FROM bench_results
  GROUP BY storage
)
SELECT round((c.avg_insert_rps / h.avg_insert_rps)::numeric, 3) AS insert_rps_ratio_clustered_vs_heap,
       round((c.avg_join_hps / h.avg_join_hps)::numeric, 3) AS join_unnest_hps_ratio_clustered_vs_heap,
       round((c.avg_any_hps / h.avg_any_hps)::numeric, 3) AS any_array_hps_ratio_clustered_vs_heap
FROM agg c
JOIN agg h ON h.storage = 'heap'
WHERE c.storage = 'clustered';

\echo '--- ratio kv ---'
\pset tuples_only on
WITH agg AS (
  SELECT storage,
         avg(insert_rows * 1000.0 / insert_ms) AS avg_insert_rps,
         avg(select_hits * 1000.0 / join_unnest_ms) AS avg_join_hps,
         avg(select_hits * 1000.0 / any_array_ms) AS avg_any_hps
  FROM bench_results
  GROUP BY storage
)
SELECT 'ratio_kv|insert=' ||
       round((c.avg_insert_rps / h.avg_insert_rps)::numeric, 6)::text ||
       '|join_unnest=' ||
       round((c.avg_join_hps / h.avg_join_hps)::numeric, 6)::text ||
       '|any_array=' ||
       round((c.avg_any_hps / h.avg_any_hps)::numeric, 6)::text
FROM agg c
JOIN agg h ON h.storage = 'heap'
WHERE c.storage = 'clustered';
\pset tuples_only off

\if :emit_observability
\echo '--- observability ---'
\pset tuples_only on
SELECT 'observability_kv|' || public.pg_sorted_heap_observability();
\pset tuples_only off
\endif

DROP EXTENSION pg_sorted_heap CASCADE;
SQL
}

if [ -n "$OUT_PATH" ]; then
  mkdir -p "$(dirname "$OUT_PATH")"
  run_probe | tee "$OUT_PATH"
  echo "unnest_ab_probe_output: $OUT_PATH"
else
  run_probe
fi

echo "unnest_ab_probe_mixed_shapes: runs=$RUNS batch_size=$BATCH_SIZE batches=$BATCHES select_iters=$SELECT_ITERS probe_size=$PROBE_SIZE probe_pattern=$PROBE_PATTERN status=ok"
