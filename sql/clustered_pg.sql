CREATE EXTENSION clustered_pg;
SELECT public.version();

CREATE TABLE clustered_pg_tableam_smoke(i int) USING clustered_heap;
CREATE INDEX clustered_pg_tableam_smoke_idx ON clustered_pg_tableam_smoke (i);
INSERT INTO clustered_pg_tableam_smoke(i) VALUES (1), (3), (7), (9);
SELECT am.amname AS tableam_name
FROM pg_class c
JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'clustered_pg_tableam_smoke';
SELECT array_agg(i ORDER BY i) AS tableam_values FROM clustered_pg_tableam_smoke;
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

CREATE TABLE clustered_pg_tableam_cluster_smoke(i bigint) USING clustered_heap;
CREATE INDEX clustered_pg_tableam_cluster_smoke_idx
	ON clustered_pg_tableam_cluster_smoke USING clustered_pk_index (i)
		WITH (split_threshold=64, target_fillfactor=90, auto_repack_interval=30.0);
INSERT INTO clustered_pg_tableam_cluster_smoke(i)
SELECT generate_series(1,24);
SELECT count(*) AS tableam_cluster_segment_rows_before_cluster
FROM segment_map_stats('clustered_pg_tableam_cluster_smoke'::regclass::oid);
CLUSTER clustered_pg_tableam_cluster_smoke USING clustered_pg_tableam_cluster_smoke_idx;
SELECT count(*) AS tableam_cluster_segment_rows_after_cluster
FROM segment_map_stats('clustered_pg_tableam_cluster_smoke'::regclass::oid);
DROP TABLE clustered_pg_tableam_cluster_smoke;

CREATE TABLE clustered_pg_tableam_segmented(i bigint) USING clustered_heap;
CREATE INDEX clustered_pg_tableam_segmented_idx
	ON clustered_pg_tableam_segmented USING clustered_pk_index (i)
		WITH (split_threshold=16, target_fillfactor=75, auto_repack_interval=30.0);
INSERT INTO clustered_pg_tableam_segmented(i)
SELECT generate_series(1,12);
SELECT count(*) AS tableam_segment_rows_before_truncate
FROM segment_map_stats('clustered_pg_tableam_segmented'::regclass::oid);
TRUNCATE clustered_pg_tableam_segmented;
SELECT count(*) AS tableam_segment_rows_after_truncate
FROM segment_map_stats('clustered_pg_tableam_segmented'::regclass::oid);
DROP TABLE clustered_pg_tableam_segmented;

CREATE TABLE clustered_pg_tableam_copy_data_smoke(i int) USING clustered_heap;
INSERT INTO clustered_pg_tableam_copy_data_smoke(i)
VALUES (1), (2), (3);
SELECT segment_map_touch(
    'clustered_pg_tableam_copy_data_smoke'::regclass::oid,
    0, 1, 10, 100, 100, 60.0) AS tableam_copy_data_touch_count;
SELECT count(*) AS tableam_copy_data_segment_rows_before_copy
FROM segment_map_stats('clustered_pg_tableam_copy_data_smoke'::regclass::oid);
VACUUM FULL clustered_pg_tableam_copy_data_smoke;
SELECT count(*) AS tableam_copy_data_segment_rows_after_copy
FROM segment_map_stats('clustered_pg_tableam_copy_data_smoke'::regclass::oid);
SELECT count(*) AS tableam_copy_data_rows_after_vacuum_full
FROM clustered_pg_tableam_copy_data_smoke;
DROP TABLE clustered_pg_tableam_copy_data_smoke;

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

CREATE TABLE clustered_pg_gap_allocator(id int);
SELECT locator_to_hex(segment_map_allocate_locator('clustered_pg_gap_allocator'::regclass::oid, 10, 1, 1, 100)) AS gap_alloc_10;
SELECT locator_to_hex(segment_map_allocate_locator('clustered_pg_gap_allocator'::regclass::oid, 30, 1, 1, 100)) AS gap_alloc_30;
SELECT locator_to_hex(segment_map_allocate_locator('clustered_pg_gap_allocator'::regclass::oid, 20, 1, 1, 100)) AS gap_alloc_20;
SELECT COUNT(*) AS gap_allocator_segment_count
FROM segment_map_stats('clustered_pg_gap_allocator'::regclass::oid);
SELECT array_agg(major_key ORDER BY major_key) AS gap_allocator_majors
FROM segment_map_stats('clustered_pg_gap_allocator'::regclass::oid);

-- Simulate allocator collision with an already occupied major key (as if external allocator lag exists).
CREATE TEMP TABLE clustered_pg_gap_collision(id int);
SELECT segment_map_touch('clustered_pg_gap_collision'::regclass::oid, 0, 1, 10, 1, 100, 60.0) AS gap_collision_touch_0;
SELECT segment_map_touch('clustered_pg_gap_collision'::regclass::oid, 2, 30, 40, 1, 100, 60.0) AS gap_collision_touch_2;
INSERT INTO segment_map (relation_oid, major_key, minor_from, minor_to, split_threshold, target_fillfactor, auto_repack_interval, row_count)
SELECT 'clustered_pg_gap_collision'::regclass::oid, 1, 100, 110, 1, 100, 60.0, 0
WHERE NOT EXISTS (
	SELECT 1 FROM segment_map
	WHERE relation_oid = 'clustered_pg_gap_collision'::regclass::oid
		AND major_key = 1
);
SELECT locator_to_hex(segment_map_allocate_locator('clustered_pg_gap_collision'::regclass::oid, 20, 1, 1, 100)) AS gap_collision_alloc_20;
SELECT array_agg(major_key ORDER BY major_key) AS gap_collision_majors
FROM segment_map_stats('clustered_pg_gap_collision'::regclass::oid);

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
SELECT count(*) AS table_segment_map_tids_count_before_cleanup
FROM segment_map_tids
WHERE relation_oid = 'clustered_pk_int8_vacuum_table'::regclass::oid;
SELECT segment_map_tids_gc('clustered_pk_int8_vacuum_table'::regclass) AS segment_map_tids_gc_before_vacuum;
SELECT count(*) AS table_segment_map_tids_count_after_gc
FROM segment_map_tids
WHERE relation_oid = 'clustered_pk_int8_vacuum_table'::regclass::oid;
VACUUM clustered_pk_int8_vacuum_table;
SELECT * FROM segment_map_stats('clustered_pk_int8_vacuum_table'::regclass::oid) ORDER BY major_key;
SELECT count(*) AS table_segment_map_tids_count_after_vacuum
FROM segment_map_tids
WHERE relation_oid = 'clustered_pk_int8_vacuum_table'::regclass::oid;
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

CREATE TABLE clustered_pk_int8_rebuild_fault_table(id bigint);
INSERT INTO clustered_pk_int8_rebuild_fault_table(id)
SELECT generate_series(1,18);
CREATE INDEX clustered_pk_int8_rebuild_fault_table_idx
	ON clustered_pk_int8_rebuild_fault_table USING clustered_pk_index (id)
	WITH (split_threshold=16, target_fillfactor=75, auto_repack_interval=30.0);
SELECT sum(row_count) AS segment_map_rowsum_before_fault,
       count(*) AS segment_map_segment_count_before_fault
FROM segment_map_stats('clustered_pk_int8_rebuild_fault_table'::regclass::oid);
CREATE TEMP TABLE clustered_pg_rebuild_fault_probe(success boolean);
DO $$
DECLARE
	v_rebuild_succeeded boolean := false;
BEGIN
	BEGIN
		PERFORM segment_map_rebuild_from_index('clustered_pk_int8_rebuild_fault_table_idx'::regclass,
											 1, 16, 75, 30.0::double precision, 2);
		v_rebuild_succeeded := true;
	EXCEPTION WHEN OTHERS THEN
		v_rebuild_succeeded := false;
	END;

	INSERT INTO clustered_pg_rebuild_fault_probe VALUES (v_rebuild_succeeded);
END;
$$;
SELECT success AS segment_map_rebuild_fault_injected
FROM clustered_pg_rebuild_fault_probe;
SELECT sum(row_count) AS segment_map_rowsum_after_fault,
       count(*) AS segment_map_segment_count_after_fault
FROM segment_map_stats('clustered_pk_int8_rebuild_fault_table'::regclass::oid);
DROP TABLE clustered_pk_int8_rebuild_fault_table;

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
EXPLAIN (COSTS OFF)
SELECT id FROM clustered_pg_am_filter_query WHERE id = 17;
SELECT id FROM clustered_pg_am_filter_query WHERE id = 17;
SELECT array_agg(id ORDER BY id) AS am_filter_ids
FROM (SELECT id FROM clustered_pg_am_filter_query WHERE id BETWEEN 10 AND 20) q;
SELECT count(*) AS am_filter_count
FROM clustered_pg_am_filter_query WHERE id BETWEEN 5 AND 10;
DROP TABLE clustered_pg_am_filter_query;

SET enable_seqscan = on;
SET enable_bitmapscan = on;
CREATE TABLE clustered_pg_am_costplanner(id bigint);
CREATE INDEX clustered_pg_am_costplanner_idx
	ON clustered_pg_am_costplanner USING clustered_pk_index (id)
		WITH (split_threshold=64, target_fillfactor=90, auto_repack_interval=30.0);
INSERT INTO clustered_pg_am_costplanner(id)
SELECT generate_series(1,10000);

EXPLAIN (COSTS OFF)
SELECT id FROM clustered_pg_am_costplanner WHERE id = 12345;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM clustered_pg_am_costplanner;

DROP TABLE clustered_pg_am_costplanner;
SET enable_seqscan = off;
SET enable_bitmapscan = off;

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

CREATE TABLE clustered_pg_lifecycle_copyupdate_smoke(i bigint) USING clustered_heap;
CREATE INDEX clustered_pg_lifecycle_copyupdate_smoke_idx
	ON clustered_pg_lifecycle_copyupdate_smoke USING clustered_pk_index (i)
		WITH (split_threshold=64, target_fillfactor=75, auto_repack_interval=30.0);
COPY clustered_pg_lifecycle_copyupdate_smoke(i) FROM STDIN;
1
2
3
4
\.
DO $$
DECLARE
	v_segment_rows bigint;
	v_table_rows bigint;
BEGIN
	SELECT count(*) INTO v_table_rows FROM clustered_pg_lifecycle_copyupdate_smoke;
	SELECT coalesce(sum(row_count), 0) INTO v_segment_rows
	FROM segment_map_stats('clustered_pg_lifecycle_copyupdate_smoke'::regclass::oid);
	IF v_segment_rows <> v_table_rows THEN
		RAISE EXCEPTION 'COPY lifecycle invariant violated: segment_map sum %, table rows %',
			v_segment_rows, v_table_rows;
	END IF;
END $$;

INSERT INTO clustered_pg_lifecycle_copyupdate_smoke(i)
VALUES (5), (6), (7);
REINDEX TABLE clustered_pg_lifecycle_copyupdate_smoke;
DO $$
DECLARE
	v_segment_rows bigint;
	v_table_rows bigint;
BEGIN
	SELECT count(*) INTO v_table_rows FROM clustered_pg_lifecycle_copyupdate_smoke;
	SELECT coalesce(sum(row_count), 0) INTO v_segment_rows
	FROM segment_map_stats('clustered_pg_lifecycle_copyupdate_smoke'::regclass::oid);
	IF v_segment_rows <> v_table_rows THEN
		RAISE EXCEPTION 'REINDEX lifecycle invariant violated: segment_map sum %, table rows %',
			v_segment_rows, v_table_rows;
	END IF;
END $$;

ALTER INDEX clustered_pg_lifecycle_copyupdate_smoke_idx
	SET (split_threshold=32, target_fillfactor=80, auto_repack_interval=45.0);
DO $$
DECLARE
	v_segment_rows bigint;
	v_table_rows bigint;
BEGIN
	SELECT count(*) INTO v_table_rows FROM clustered_pg_lifecycle_copyupdate_smoke;
	SELECT coalesce(sum(row_count), 0) INTO v_segment_rows
	FROM segment_map_stats('clustered_pg_lifecycle_copyupdate_smoke'::regclass::oid);
	IF v_segment_rows <> v_table_rows THEN
		RAISE EXCEPTION 'ALTER INDEX SET lifecycle invariant violated: segment_map sum %, table rows %',
			v_segment_rows, v_table_rows;
	END IF;
END $$;

DROP INDEX clustered_pg_lifecycle_copyupdate_smoke_idx;
INSERT INTO clustered_pg_lifecycle_copyupdate_smoke(i) VALUES (8), (9);
CREATE INDEX clustered_pg_lifecycle_copyupdate_smoke_idx
	ON clustered_pg_lifecycle_copyupdate_smoke USING clustered_pk_index (i)
	WITH (split_threshold=16, target_fillfactor=70, auto_repack_interval=20.0);
DO $$
DECLARE
	v_segment_rows bigint;
	v_table_rows bigint;
BEGIN
	SELECT count(*) INTO v_table_rows FROM clustered_pg_lifecycle_copyupdate_smoke;
	SELECT coalesce(sum(row_count), 0) INTO v_segment_rows
	FROM segment_map_stats('clustered_pg_lifecycle_copyupdate_smoke'::regclass::oid);
	IF v_segment_rows <> v_table_rows THEN
		RAISE EXCEPTION 'DROP/RECREATE index lifecycle invariant violated: segment_map sum %, table rows %',
			v_segment_rows, v_table_rows;
	END IF;
END $$;

DROP TABLE clustered_pg_lifecycle_copyupdate_smoke;

DROP EXTENSION clustered_pg;
