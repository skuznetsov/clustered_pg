CREATE EXTENSION clustered_pg;
SELECT public.version();

CREATE TABLE clustered_pg_tableam_smoke(i int) USING clustered_heap;
INSERT INTO clustered_pg_tableam_smoke(i) VALUES (1), (3), (7), (9);
SELECT am.amname AS tableam_name
FROM pg_class c
JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'clustered_pg_tableam_smoke';
SELECT array_agg(i ORDER BY i) AS tableam_values FROM clustered_pg_tableam_smoke;
CREATE INDEX clustered_pg_tableam_smoke_idx ON clustered_pg_tableam_smoke (i);
SELECT count(*) AS tableam_filter_count
FROM clustered_pg_tableam_smoke
WHERE i IN (3,9);
UPDATE clustered_pg_tableam_smoke
SET i = i + 1
WHERE i = 3;
SELECT count(*) AS tableam_rows_after_update FROM clustered_pg_tableam_smoke;
ANALYZE clustered_pg_tableam_smoke;
VACUUM clustered_pg_tableam_smoke;
DELETE FROM clustered_pg_tableam_smoke WHERE i > 4;
SELECT count(*) AS tableam_rows_after_delete FROM clustered_pg_tableam_smoke;
TRUNCATE clustered_pg_tableam_smoke;
SELECT count(*) AS tableam_rows_after_truncate FROM clustered_pg_tableam_smoke;
DROP TABLE clustered_pg_tableam_smoke;

SELECT locator_to_hex(locator_pack(4,7)) as packed_hex;
SELECT locator_pack_int8(12345) = locator_pack(0,12345) as pack_int8_matches_pair;
SELECT locator_major(locator_pack_int8(12345)) as major_part,
       locator_minor(locator_pack_int8(12345)) as minor_part;

SELECT locator_cmp(locator_pack(1, 10), locator_pack(1, 11)) AS cmp_1;
SELECT locator_cmp(locator_pack(2, 1), locator_pack(1, 1000000)) AS cmp_2;
SELECT locator_to_hex(locator_advance_major(locator_pack(4,7), 3)) AS advanced_hex;
SELECT locator_to_hex(locator_next_minor(locator_pack(4,7), 10)) AS next_minor_hex;

CREATE TEMP TABLE clustered_pg_fixture(id int);
SELECT segment_map_touch('clustered_pg_fixture'::regclass::oid, 4, 1, 100, 128, 85, 60.0) AS touched_count;
SELECT * FROM segment_map_stats('clustered_pg_fixture'::regclass::oid) ORDER BY major_key;

SELECT locator_to_hex(segment_map_allocate_locator('clustered_pg_fixture'::regclass::oid, 1, 1)) AS alloc_hex_1;
SELECT locator_to_hex(segment_map_allocate_locator('clustered_pg_fixture'::regclass::oid, 2, 1)) AS alloc_hex_2;
SELECT locator_to_hex(segment_map_allocate_locator('clustered_pg_fixture'::regclass::oid, 3, 1, 2)) AS alloc_hex_3;
SELECT locator_to_hex(segment_map_allocate_locator('clustered_pg_fixture'::regclass::oid, 4, 1, 2)) AS alloc_hex_4;
SELECT locator_to_hex(segment_map_allocate_locator('clustered_pg_fixture'::regclass::oid, 10, 1, 2)) AS alloc_hex_5;
SELECT locator_to_hex(segment_map_allocate_locator('clustered_pg_fixture'::regclass::oid, 10, 1, 2)) AS alloc_hex_6;
SELECT locator_to_hex(segment_map_allocate_locator_regclass('clustered_pg_fixture', 11, 1, 2)) AS alloc_hex_7;
SELECT locator_to_hex(segment_map_next_locator('clustered_pg_fixture', 12, 1, 2)) AS alloc_hex_8;

SELECT locator_major(locator_pack(0,10)) AS major_check,
       locator_minor(locator_pack(0,10)) AS minor_check;

CREATE TABLE clustered_pk_int8_table(id bigint);
CREATE INDEX clustered_pk_int8_table_idx ON clustered_pk_int8_table USING clustered_pk_index (id);
DROP TABLE clustered_pk_int8_table;

CREATE TABLE clustered_pk_int8_build_table(id bigint);
INSERT INTO clustered_pk_int8_build_table(id)
SELECT generate_series(1,18);
CREATE INDEX clustered_pk_int8_build_table_idx
	ON clustered_pk_int8_build_table USING clustered_pk_index (id)
		WITH (split_threshold=16, target_fillfactor=75, auto_repack_interval=30.0);
SELECT * FROM segment_map_stats('clustered_pk_int8_build_table'::regclass::oid) ORDER BY major_key;
DROP TABLE clustered_pk_int8_build_table;

CREATE TABLE clustered_pk_int8_table_opts(id bigint);
CREATE INDEX clustered_pk_int8_table_opts_idx
	ON clustered_pk_int8_table_opts USING clustered_pk_index (id)
		WITH (split_threshold=16, target_fillfactor=75, auto_repack_interval=30.0);
INSERT INTO clustered_pk_int8_table_opts(id)
SELECT generate_series(1,18);
SELECT * FROM segment_map_stats('clustered_pk_int8_table_opts'::regclass::oid) ORDER BY major_key;
DROP TABLE clustered_pk_int8_table_opts;

CREATE TABLE clustered_pk_int8_vacuum_table(id bigint);
INSERT INTO clustered_pk_int8_vacuum_table(id)
SELECT generate_series(1,18);
CREATE INDEX clustered_pk_int8_vacuum_table_idx
	ON clustered_pk_int8_vacuum_table USING clustered_pk_index (id)
		WITH (split_threshold=16, target_fillfactor=75, auto_repack_interval=30.0);
SELECT * FROM segment_map_stats('clustered_pk_int8_vacuum_table'::regclass::oid) ORDER BY major_key;
SELECT segment_map_count_repack_due('clustered_pk_int8_vacuum_table'::regclass::oid, 3600.0::double precision) AS due_repack_before_vacuum;
DELETE FROM clustered_pk_int8_vacuum_table WHERE id BETWEEN 1 AND 4;
VACUUM clustered_pk_int8_vacuum_table;
SELECT * FROM segment_map_stats('clustered_pk_int8_vacuum_table'::regclass::oid) ORDER BY major_key;
SELECT segment_map_count_repack_due('clustered_pk_int8_vacuum_table'::regclass::oid, 3600.0::double precision) AS due_repack_after_vacuum;
DROP TABLE clustered_pk_int8_vacuum_table;

CREATE TABLE clustered_pk_int8_rebuild_table(id bigint);
INSERT INTO clustered_pk_int8_rebuild_table(id)
SELECT generate_series(1,18);
CREATE INDEX clustered_pk_int8_rebuild_table_idx
	ON clustered_pk_int8_rebuild_table USING clustered_pk_index (id)
		WITH (split_threshold=16, target_fillfactor=75, auto_repack_interval=30.0);
DELETE FROM clustered_pk_int8_rebuild_table WHERE id BETWEEN 1 AND 4;
SELECT segment_map_count_repack_due('clustered_pk_int8_rebuild_table'::regclass::oid, 3600.0::double precision) AS due_repack_before_manual_rebuild;
SELECT segment_map_rebuild_from_index('clustered_pk_int8_rebuild_table_idx'::regclass, 1, 16, 75, 30.0::double precision) AS rebuilt_rows;
SELECT * FROM segment_map_stats('clustered_pk_int8_rebuild_table'::regclass::oid) ORDER BY major_key;
DROP TABLE clustered_pk_int8_rebuild_table;

CREATE TABLE clustered_pg_perf_smoke(locator bytea);
INSERT INTO clustered_pg_perf_smoke(locator)
SELECT segment_map_allocate_locator('clustered_pg_perf_smoke'::regclass::oid, g, 1, 100, 100)
FROM generate_series(1,10000) g;
SELECT count(*) AS perf_smoke_rows FROM clustered_pg_perf_smoke;
SELECT count(*) AS perf_smoke_segment_count,
       sum(row_count) AS perf_smoke_segment_rows,
       max(major_key) AS perf_smoke_max_major
FROM segment_map_stats('clustered_pg_perf_smoke'::regclass::oid);
DROP TABLE clustered_pg_perf_smoke;

CREATE TABLE clustered_pg_fillfactor_bounds(locator bytea);
INSERT INTO clustered_pg_fillfactor_bounds(locator)
SELECT segment_map_allocate_locator('clustered_pg_fillfactor_bounds'::regclass::oid, g, 1, 4, 100)
FROM generate_series(1,18) g;
SELECT * FROM segment_map_stats('clustered_pg_fillfactor_bounds'::regclass::oid) ORDER BY major_key;

CREATE TABLE clustered_pg_fillfactor_floor(locator bytea);
INSERT INTO clustered_pg_fillfactor_floor(locator)
SELECT segment_map_allocate_locator('clustered_pg_fillfactor_floor'::regclass::oid, g, 1, 4, 1)
FROM generate_series(1,6) g;
SELECT count(*) AS fillfactor_floor_segment_count,
       sum(row_count) AS fillfactor_floor_rows
FROM segment_map_stats('clustered_pg_fillfactor_floor'::regclass::oid);
DROP TABLE clustered_pg_fillfactor_floor;
DROP TABLE clustered_pg_fillfactor_bounds;

CREATE TABLE clustered_pg_am_smoke(id bigint);
CREATE INDEX clustered_pg_am_smoke_idx
	ON clustered_pg_am_smoke USING clustered_pk_index (id)
		WITH (split_threshold=128, target_fillfactor=75, auto_repack_interval=30.0);
INSERT INTO clustered_pg_am_smoke(id)
SELECT generate_series(1,10000);
SELECT count(*) AS am_smoke_rows
FROM clustered_pg_am_smoke;
SELECT count(*) AS am_smoke_segment_count,
       sum(row_count) AS am_smoke_segment_rows,
       max(major_key) AS am_smoke_max_major
FROM segment_map_stats('clustered_pg_am_smoke'::regclass::oid);
DROP TABLE clustered_pg_am_smoke;

CREATE TABLE clustered_pg_am_scale_smoke(id bigint);
CREATE INDEX clustered_pg_am_scale_smoke_idx
	ON clustered_pg_am_scale_smoke USING clustered_pk_index (id)
		WITH (split_threshold=256, target_fillfactor=70, auto_repack_interval=30.0);
INSERT INTO clustered_pg_am_scale_smoke(id)
SELECT generate_series(1,50000);
SELECT count(*) AS am_scale_rows FROM clustered_pg_am_scale_smoke;
SELECT count(*) AS am_scale_segment_count,
       sum(row_count) AS am_scale_segment_rows,
       max(major_key) AS am_scale_max_major
FROM segment_map_stats('clustered_pg_am_scale_smoke'::regclass::oid);
DROP TABLE clustered_pg_am_scale_smoke;

CREATE TABLE clustered_pg_am_desc_smoke(id bigint);
CREATE INDEX clustered_pg_am_desc_smoke_idx
	ON clustered_pg_am_desc_smoke USING clustered_pk_index (id)
		WITH (split_threshold=64, target_fillfactor=60, auto_repack_interval=30.0);
INSERT INTO clustered_pg_am_desc_smoke(id)
SELECT generate_series(10000,1,-1);
SELECT count(*) AS am_desc_rows FROM clustered_pg_am_desc_smoke;
SELECT count(*) AS am_desc_segment_count,
       sum(row_count) AS am_desc_segment_rows,
       min(major_key) AS am_desc_min_major,
       max(major_key) AS am_desc_max_major
FROM segment_map_stats('clustered_pg_am_desc_smoke'::regclass::oid);
DROP TABLE clustered_pg_am_desc_smoke;

CREATE TABLE clustered_pg_am_churn_smoke(id bigint);
CREATE INDEX clustered_pg_am_churn_smoke_idx
	ON clustered_pg_am_churn_smoke USING clustered_pk_index (id)
		WITH (split_threshold=32, target_fillfactor=80, auto_repack_interval=30.0);
INSERT INTO clustered_pg_am_churn_smoke(id)
SELECT generate_series(1,2000);
SELECT count(*) AS am_churn_initial_rows FROM clustered_pg_am_churn_smoke;
SELECT count(*) AS am_churn_initial_segment_count,
       max(major_key) AS am_churn_initial_max_major
FROM segment_map_stats('clustered_pg_am_churn_smoke'::regclass::oid);
DELETE FROM clustered_pg_am_churn_smoke WHERE id % 2 = 0;
INSERT INTO clustered_pg_am_churn_smoke(id)
SELECT generate_series(1,2000);
SELECT count(*) AS am_churn_after_rows FROM clustered_pg_am_churn_smoke;
SELECT count(*) AS am_churn_segment_count,
       sum(row_count) AS am_churn_segment_rows,
       max(major_key) AS am_churn_max_major
FROM segment_map_stats('clustered_pg_am_churn_smoke'::regclass::oid);
DROP TABLE clustered_pg_am_churn_smoke;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
CREATE TABLE clustered_pg_am_filter_query(id bigint);
CREATE INDEX clustered_pg_am_filter_query_idx
	ON clustered_pg_am_filter_query USING clustered_pk_index (id)
		WITH (split_threshold=64, target_fillfactor=90, auto_repack_interval=30.0);
INSERT INTO clustered_pg_am_filter_query(id)
SELECT generate_series(1,40);
SELECT id FROM clustered_pg_am_filter_query WHERE id = 17;
SELECT array_agg(id ORDER BY id) AS am_filter_ids
FROM (SELECT id FROM clustered_pg_am_filter_query WHERE id BETWEEN 10 AND 20) q;
SELECT count(*) AS am_filter_count
FROM clustered_pg_am_filter_query WHERE id BETWEEN 5 AND 10;
DROP TABLE clustered_pg_am_filter_query;

SET enable_mergejoin = on;
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
CREATE TABLE clustered_pg_am_merge_fixture(id bigint);
CREATE TABLE clustered_pg_am_merge_fixture_b(id bigint);
CREATE INDEX clustered_pg_am_merge_fixture_a_idx
	ON clustered_pg_am_merge_fixture USING clustered_pk_index (id)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
CREATE INDEX clustered_pg_am_merge_fixture_b_idx
	ON clustered_pg_am_merge_fixture_b USING clustered_pk_index (id)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
INSERT INTO clustered_pg_am_merge_fixture(id)
SELECT generate_series(1,20);
INSERT INTO clustered_pg_am_merge_fixture_b(id)
SELECT generate_series(10,30);
SELECT count(*) AS am_merge_join_count
FROM clustered_pg_am_merge_fixture l
JOIN clustered_pg_am_merge_fixture_b r USING (id);
SELECT count(*) AS am_merge_join_filter_count
FROM clustered_pg_am_merge_fixture l
JOIN clustered_pg_am_merge_fixture_b r ON l.id = r.id
WHERE l.id BETWEEN 12 AND 16;

-- Exercise merge join mark/restore on repeated matches (duplication-heavy inner side)
CREATE TABLE clustered_pg_am_merge_markrestore(a bigint);
CREATE TABLE clustered_pg_am_merge_markrestore_b(a bigint);
CREATE INDEX clustered_pg_am_merge_markrestore_a_idx
	ON clustered_pg_am_merge_markrestore USING clustered_pk_index (a)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
CREATE INDEX clustered_pg_am_merge_markrestore_b_idx
	ON clustered_pg_am_merge_markrestore_b USING clustered_pk_index (a)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
INSERT INTO clustered_pg_am_merge_markrestore(a)
SELECT generate_series(1,10);
INSERT INTO clustered_pg_am_merge_markrestore_b(a) VALUES
	(1), (1), (2), (2), (2), (4), (6), (6);
SELECT a, count(*) AS matched_cnt
FROM clustered_pg_am_merge_markrestore l
JOIN clustered_pg_am_merge_markrestore_b r USING (a)
GROUP BY a
ORDER BY a;
SELECT count(*) AS am_merge_markrestore_row_count
FROM clustered_pg_am_merge_markrestore l
JOIN clustered_pg_am_merge_markrestore_b r USING (a)
WHERE l.a BETWEEN 2 AND 6;
DROP TABLE clustered_pg_am_merge_markrestore;
DROP TABLE clustered_pg_am_merge_markrestore_b;

DROP TABLE clustered_pg_am_merge_fixture;
DROP TABLE clustered_pg_am_merge_fixture_b;
RESET enable_hashjoin;
RESET enable_nestloop;
RESET enable_mergejoin;
RESET enable_seqscan;
RESET enable_bitmapscan;

SELECT * FROM segment_map_stats('clustered_pg_fixture'::regclass::oid) ORDER BY major_key;

SELECT locator_lt(locator_pack(0,1), locator_pack(1,0)) as op_lt,
       locator_gt(locator_pack(1,0), locator_pack(0,1)) as op_gt,
       locator_eq(locator_pack(1,2), locator_pack(1,2)) as op_eq;

DROP EXTENSION clustered_pg;
