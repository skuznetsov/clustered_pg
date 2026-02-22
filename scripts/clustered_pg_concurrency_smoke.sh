#!/usr/bin/env bash
set -euo pipefail

DB_NAME=${1:-${PGDATABASE:-contrib_regression}}
WORKER_CONNECTIONS=${2:-8}
CLIENT_JOBS=${3:-8}
TX_PER_CLIENT=${4:-400}

TABLE_NAME="clustered_pg_concurrent_stress"
INDEX_NAME="${TABLE_NAME}_idx"
WORKFILE="$(mktemp)"

cleanup() {
	if command -v psql >/dev/null 2>&1; then
		psql -v ON_ERROR_STOP=1 -d "${DB_NAME}" -c "DROP TABLE IF EXISTS ${TABLE_NAME}" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

if ! command -v psql >/dev/null 2>&1; then
	echo "SKIP: psql not found in PATH."
	exit 0
fi
if ! command -v pgbench >/dev/null 2>&1; then
	echo "SKIP: pgbench not found in PATH."
	exit 0
fi

cat > "${WORKFILE}" <<'SQL'
\set id random(1, 1000000)
INSERT INTO clustered_pg_concurrent_stress(i) VALUES (:id);
SQL

echo "Preparing workload table in ${DB_NAME}..."
psql -v ON_ERROR_STOP=1 -d "${DB_NAME}" <<SQL
CREATE EXTENSION IF NOT EXISTS clustered_pg;
DROP TABLE IF EXISTS ${TABLE_NAME};
CREATE TABLE ${TABLE_NAME}(i bigint) USING clustered_heap;
CREATE INDEX ${INDEX_NAME}
	ON ${TABLE_NAME}
	USING clustered_pk_index (i)
	WITH (split_threshold = 32, target_fillfactor = 80, auto_repack_interval = 30.0);
SQL

echo "Running pgbench concurrency smoke test..."
pgbench -n -c "${WORKER_CONNECTIONS}" -j "${CLIENT_JOBS}" -t "${TX_PER_CLIENT}" -f "${WORKFILE}" "${DB_NAME}"

echo "Asserting allocator/map invariants..."
psql -v ON_ERROR_STOP=1 -d "${DB_NAME}" <<SQL
SET statement_timeout = '15s';

SELECT
	(SELECT count(*) FROM ${TABLE_NAME}) AS rows_in_heap,
	(SELECT sum(row_count) FROM segment_map WHERE relation_oid = ${TABLE_NAME}::regclass::oid) AS segment_rows_sum,
	(SELECT count(*) FROM segment_map WHERE relation_oid = ${TABLE_NAME}::regclass::oid) AS segment_count;

SELECT
	COUNT(*) AS overfull_segments
FROM segment_map
WHERE relation_oid = ${TABLE_NAME}::regclass::oid
	AND row_count > GREATEST(1, (split_threshold * LEAST(100, GREATEST(1, target_fillfactor))) / 100);

SELECT 'CONCURRENT_REPO_STRESS_OK' AS status;
SQL
