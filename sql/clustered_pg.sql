CREATE EXTENSION clustered_pg;
SELECT public.version();
SELECT public.clustered_pg_observability() AS observability_bootstrap;
SELECT (public.clustered_pg_observability() ~ 'clustered_pg=0.1.0') AS observability_probe;
SELECT (public.clustered_pg_observability() ~ 'local_hint_touches=[0-9]+,local_hint_merges=[0-9]+') AS observability_local_hint_schema;
SELECT (public.clustered_pg_observability() ~ 'local_hint_map_resets=[0-9]+') AS observability_local_hint_map_schema;
SELECT (public.clustered_pg_observability() ~ 'local_hint_evictions=[0-9]+') AS observability_local_hint_eviction_schema;
SELECT (public.clustered_pg_observability() ~ 'local_hint_stale_resets=[0-9]+,defensive_state_recovers=0') AS observability_local_hint_stale_schema;
SELECT (public.clustered_pg_observability() ~ 'rescan_keycache_build_attempts=[0-9]+,rescan_keycache_build_successes=[0-9]+,rescan_keycache_disables=[0-9]+') AS observability_rescan_keycache_build_schema;
SELECT (public.clustered_pg_observability() ~ 'rescan_keycache_lookup_hits=[0-9]+,rescan_keycache_lookup_misses=[0-9]+') AS observability_rescan_keycache_lookup_schema;
SELECT (public.clustered_pg_observability() ~ 'exact_local_hint_hits=[0-9]+,exact_local_hint_misses=[0-9]+') AS observability_exact_local_hint_schema;

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
SELECT segment_map_rebuild_from_index(
	'clustered_pg_tableam_cluster_smoke_idx'::regclass,
	1, 64, 90, 30.0::double precision) AS tableam_cluster_rebuild_rows;
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
SELECT segment_map_rebuild_from_index(
	'clustered_pg_tableam_segmented_idx'::regclass,
	1, 16, 75, 30.0::double precision) AS tableam_segmented_rebuild_rows;
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
SELECT segment_map_rebuild_from_index(
	'clustered_pk_int8_table_opts_idx'::regclass,
	1, 16, 75, 30.0::double precision) AS table_opts_rebuild_rows;
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
SELECT segment_map_rebuild_from_index(
	'clustered_pg_am_smoke_idx'::regclass,
	1, 128, 75, 30.0::double precision) AS am_smoke_rebuild_rows;
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
SELECT segment_map_rebuild_from_index(
	'clustered_pg_am_scale_smoke_idx'::regclass,
	1, 256, 70, 30.0::double precision) AS am_scale_rebuild_rows;
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
SELECT segment_map_rebuild_from_index(
	'clustered_pg_am_desc_smoke_idx'::regclass,
	1, 64, 60, 30.0::double precision) AS am_desc_rebuild_rows;
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
SELECT segment_map_rebuild_from_index(
	'clustered_pg_am_churn_smoke_idx'::regclass,
	1, 32, 80, 30.0::double precision) AS am_churn_initial_rebuild_rows;
SELECT count(*) AS am_churn_initial_rows FROM clustered_pg_am_churn_smoke;
SELECT count(*) AS am_churn_initial_segment_count,
       max(major_key) AS am_churn_initial_max_major
FROM segment_map_stats('clustered_pg_am_churn_smoke'::regclass::oid);
DELETE FROM clustered_pg_am_churn_smoke WHERE id % 2 = 0;
INSERT INTO clustered_pg_am_churn_smoke(id)
SELECT generate_series(1,2000);
SELECT segment_map_rebuild_from_index(
	'clustered_pg_am_churn_smoke_idx'::regclass,
	1, 32, 80, 30.0::double precision) AS am_churn_rebuild_rows;
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
EXPLAIN (COSTS OFF)
SELECT * FROM clustered_pg_am_costplanner;
SELECT (public.clustered_pg_observability() ~ 'costestimate=[0-9]+') AS costestimate_tracked;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
EXPLAIN (COSTS OFF)
SELECT * FROM clustered_pg_am_costplanner;
SET enable_indexscan = on;
SET enable_indexonlyscan = on;

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
SELECT segment_map_rebuild_from_index(
	'clustered_pg_lifecycle_copyupdate_smoke_idx'::regclass,
	1, 64, 75, 30.0::double precision) AS lifecycle_copy_rebuild_rows;
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
SELECT segment_map_rebuild_from_index(
	'clustered_pg_lifecycle_copyupdate_smoke_idx'::regclass,
	1, 64, 75, 30.0::double precision) AS lifecycle_reindex_rebuild_rows;
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

-- Guard against SPI/planner interactions for aggregate functions executed in PL/pgSQL DO blocks
-- after bulk-load + reindex lifecycle on clustered index relations.
DO $$
DECLARE
	v_table_rows bigint;
	v_max_value bigint;
	v_ids bigint[];
BEGIN
	SELECT count(*), max(i), array_agg(i ORDER BY i)
	  INTO v_table_rows, v_max_value, v_ids
	FROM clustered_pg_lifecycle_copyupdate_smoke;
	IF v_table_rows <> 7 OR v_max_value <> 7 OR v_ids[1] <> 1 OR v_ids[7] <> 7 THEN
		RAISE EXCEPTION 'SPI aggregate invariant violated: rows %, max %, first %, last %',
			v_table_rows, v_max_value, v_ids[1], v_ids[7];
	END IF;
END $$;

ALTER INDEX clustered_pg_lifecycle_copyupdate_smoke_idx
	SET (split_threshold=32, target_fillfactor=80, auto_repack_interval=45.0);
SELECT segment_map_rebuild_from_index(
	'clustered_pg_lifecycle_copyupdate_smoke_idx'::regclass,
	1, 32, 80, 45.0::double precision) AS lifecycle_alter_rebuild_rows;
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

CREATE TABLE clustered_pg_do_spi_guard(i bigint) USING clustered_heap;
CREATE INDEX clustered_pg_do_spi_guard_idx
	ON clustered_pg_do_spi_guard USING clustered_pk_index (i);
INSERT INTO clustered_pg_do_spi_guard(i)
VALUES (1), (2), (3);
DO $$
DECLARE
	v_rows bigint;
BEGIN
	SELECT count(*) INTO v_rows FROM clustered_pg_do_spi_guard;
	PERFORM 42;
END;
$$;
SELECT count(*) AS do_spi_guard_rows
FROM clustered_pg_do_spi_guard;
DROP TABLE clustered_pg_do_spi_guard;

SET clustered_pg.pkidx_enable_segment_fastpath = on;
SET clustered_pg.pkidx_max_segment_tids = 256;
SET enable_seqscan = off;

CREATE TABLE clustered_pg_fastpath_trunc(i int) USING clustered_heap;
CREATE INDEX clustered_pg_fastpath_trunc_idx
	ON clustered_pg_fastpath_trunc USING clustered_pk_index (i)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
INSERT INTO clustered_pg_fastpath_trunc(i)
SELECT 1 FROM generate_series(1, 400);
INSERT INTO clustered_pg_fastpath_trunc(i) VALUES (2), (3), (4);
SELECT segment_map_rebuild_from_index(
	'clustered_pg_fastpath_trunc_idx'::regclass,
	1, 32, 85, 30.0::double precision) AS fastpath_trunc_rebuild_rows;
SELECT count(*) AS fastpath_trunc_count
FROM clustered_pg_fastpath_trunc
WHERE i = 1;
SELECT (public.clustered_pg_observability() ~ 'segment_lookup_truncated=[1-9][0-9]*') AS fastpath_trunc_observed;
DROP TABLE clustered_pg_fastpath_trunc;

RESET enable_seqscan;
RESET clustered_pg.pkidx_max_segment_tids;
RESET clustered_pg.pkidx_enable_segment_fastpath;

SET clustered_pg.pkidx_enable_segment_fastpath = on;
SET enable_seqscan = off;

CREATE TABLE clustered_pg_fastpath_recheck_guard(i int) USING clustered_heap;
CREATE INDEX clustered_pg_fastpath_recheck_guard_idx
	ON clustered_pg_fastpath_recheck_guard USING clustered_pk_index (i)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
INSERT INTO clustered_pg_fastpath_recheck_guard(i) VALUES (1), (2);
SELECT segment_map_rebuild_from_index(
	'clustered_pg_fastpath_recheck_guard_idx'::regclass,
	1, 32, 85, 30.0::double precision) AS fastpath_recheck_rebuild_rows;
WITH rel AS (
	SELECT
		'clustered_pg_fastpath_recheck_guard'::regclass::oid AS rel_oid,
		(SELECT ctid FROM clustered_pg_fastpath_recheck_guard WHERE i = 2 LIMIT 1) AS wrong_tid
)
INSERT INTO segment_map_tids(relation_oid, major_key, minor_key, tuple_tid)
SELECT rel_oid, 0, 1, wrong_tid FROM rel
ON CONFLICT (relation_oid, tuple_tid)
DO UPDATE SET
	major_key = EXCLUDED.major_key,
	minor_key = EXCLUDED.minor_key,
	updated_at = clock_timestamp();
SELECT count(*) AS fastpath_recheck_guard_count_key1
FROM clustered_pg_fastpath_recheck_guard
WHERE i = 1;
SELECT count(*) AS fastpath_recheck_guard_count_key2
FROM clustered_pg_fastpath_recheck_guard
WHERE i = 2;
DO $$
DECLARE
	v_before_filenode oid;
	v_after_filenode oid;
BEGIN
	SELECT pg_relation_filenode('clustered_pg_fastpath_recheck_guard'::regclass)
	INTO v_before_filenode;
	TRUNCATE clustered_pg_fastpath_recheck_guard;
	SELECT pg_relation_filenode('clustered_pg_fastpath_recheck_guard'::regclass)
	INTO v_after_filenode;
	IF v_before_filenode = v_after_filenode THEN
		RAISE EXCEPTION 'expected relfilenode rotation after TRUNCATE, got same filenode %', v_after_filenode;
	END IF;
END $$;
SELECT count(*) AS fastpath_recheck_guard_post_truncate_key1
FROM clustered_pg_fastpath_recheck_guard
WHERE i = 1;
SELECT (public.clustered_pg_observability() ~ 'local_hint_stale_resets=[1-9][0-9]*') AS local_hint_stale_reset_observed;
DROP TABLE clustered_pg_fastpath_recheck_guard;

CREATE TABLE clustered_pg_post_fastpath_ddl_guard(i int) USING clustered_heap;
CREATE INDEX clustered_pg_post_fastpath_ddl_guard_idx
	ON clustered_pg_post_fastpath_ddl_guard USING clustered_pk_index (i)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
DROP TABLE clustered_pg_post_fastpath_ddl_guard;

CREATE TABLE clustered_pg_local_hint_map_negative_guard(i int) USING clustered_heap;
CREATE INDEX clustered_pg_local_hint_map_negative_guard_idx
	ON clustered_pg_local_hint_map_negative_guard USING clustered_pk_index (i)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
INSERT INTO clustered_pg_local_hint_map_negative_guard(i)
SELECT generate_series(1, 128);
SELECT (public.clustered_pg_observability() ~ 'local_hint_map_resets=0') AS local_hint_map_reset_negative_observed;
DROP TABLE clustered_pg_local_hint_map_negative_guard;

CREATE TABLE clustered_pg_local_hint_map_reset_guard(i int) USING clustered_heap;
CREATE INDEX clustered_pg_local_hint_map_reset_guard_idx
	ON clustered_pg_local_hint_map_reset_guard USING clustered_pk_index (i)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
INSERT INTO clustered_pg_local_hint_map_reset_guard(i)
SELECT generate_series(1, 5000);
SELECT (public.clustered_pg_observability() ~ 'local_hint_map_resets=0') AS local_hint_map_no_global_reset_observed;
SELECT (public.clustered_pg_observability() ~ 'local_hint_evictions=[1-9][0-9]*') AS local_hint_eviction_observed;
DROP TABLE clustered_pg_local_hint_map_reset_guard;

RESET enable_seqscan;
RESET clustered_pg.pkidx_enable_segment_fastpath;

-- ====================================================================
-- Functional regression tests: multi-type index, JOIN UNNEST rescan,
-- delete+vacuum consistency, locator edge cases
-- ====================================================================

-- Test int2 and int4 index support (only int8 tested above)
CREATE TABLE clustered_pg_int2_smoke(id smallint) USING clustered_heap;
CREATE INDEX clustered_pg_int2_smoke_idx
	ON clustered_pg_int2_smoke USING clustered_pk_index (id)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
INSERT INTO clustered_pg_int2_smoke(id) SELECT generate_series(1,20)::smallint;
SELECT segment_map_rebuild_from_index(
	'clustered_pg_int2_smoke_idx'::regclass,
	1, 32, 85, 30.0::double precision) AS int2_rebuild_rows;
SELECT count(*) AS int2_row_count FROM clustered_pg_int2_smoke;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS int2_filter_eq FROM clustered_pg_int2_smoke WHERE id = 10::smallint;
SELECT count(*) AS int2_filter_range FROM clustered_pg_int2_smoke WHERE id BETWEEN 5::smallint AND 15::smallint;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE clustered_pg_int2_smoke;

CREATE TABLE clustered_pg_int4_smoke(id integer) USING clustered_heap;
CREATE INDEX clustered_pg_int4_smoke_idx
	ON clustered_pg_int4_smoke USING clustered_pk_index (id)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
INSERT INTO clustered_pg_int4_smoke(id) SELECT generate_series(1,20);
SELECT segment_map_rebuild_from_index(
	'clustered_pg_int4_smoke_idx'::regclass,
	1, 32, 85, 30.0::double precision) AS int4_rebuild_rows;
SELECT count(*) AS int4_row_count FROM clustered_pg_int4_smoke;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS int4_filter_eq FROM clustered_pg_int4_smoke WHERE id = 10;
SELECT count(*) AS int4_filter_range FROM clustered_pg_int4_smoke WHERE id BETWEEN 5 AND 15;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE clustered_pg_int4_smoke;

-- Test locator edge cases: zero, large values, boundary
SELECT locator_major(locator_pack(0, 0)) AS loc_zero_major,
       locator_minor(locator_pack(0, 0)) AS loc_zero_minor;
SELECT locator_cmp(locator_pack(0, 0), locator_pack(0, 0)) AS loc_cmp_equal_zero;
SELECT locator_cmp(locator_pack(0, 0), locator_pack(0, 1)) AS loc_cmp_zero_vs_one;
SELECT locator_to_hex(locator_pack(9223372036854775807, 9223372036854775807)) AS loc_max_hex;
SELECT locator_major(locator_pack(9223372036854775807, 0)) AS loc_max_major;
SELECT locator_minor(locator_pack(0, 9223372036854775807)) AS loc_max_minor;
SELECT locator_to_hex(locator_next_minor(locator_pack(0, 0), 1)) AS loc_next_from_zero;
SELECT locator_to_hex(locator_advance_major(locator_pack(0, 5), 1)) AS loc_advance_from_zero;

-- Test JOIN UNNEST rescan path (exercises rescan keycache)
CREATE TABLE clustered_pg_join_unnest_base(id bigint) USING clustered_heap;
CREATE INDEX clustered_pg_join_unnest_base_idx
	ON clustered_pg_join_unnest_base USING clustered_pk_index (id)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
INSERT INTO clustered_pg_join_unnest_base(id) SELECT generate_series(1,100);
SELECT segment_map_rebuild_from_index(
	'clustered_pg_join_unnest_base_idx'::regclass,
	1, 32, 85, 30.0::double precision) AS join_unnest_rebuild_rows;
-- Probe with array of keys via JOIN (exercises rescan on inner side)
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SELECT count(*) AS join_unnest_hit_count
FROM clustered_pg_join_unnest_base b
JOIN (SELECT unnest(ARRAY[5,10,15,20,25,30,50,75,99,100]) AS id) k ON b.id = k.id;
-- Probe with keys not in table (should return 0 matches)
SELECT count(*) AS join_unnest_miss_count
FROM clustered_pg_join_unnest_base b
JOIN (SELECT unnest(ARRAY[101,200,300]) AS id) k ON b.id = k.id;
-- Mixed hit/miss
SELECT count(*) AS join_unnest_mixed_count
FROM clustered_pg_join_unnest_base b
JOIN (SELECT unnest(ARRAY[1,50,100,101,200]) AS id) k ON b.id = k.id;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_hashjoin;
RESET enable_mergejoin;
DROP TABLE clustered_pg_join_unnest_base;

-- Test delete + vacuum + re-query consistency
CREATE TABLE clustered_pg_vacuum_consistency(id bigint) USING clustered_heap;
CREATE INDEX clustered_pg_vacuum_consistency_idx
	ON clustered_pg_vacuum_consistency USING clustered_pk_index (id)
		WITH (split_threshold=16, target_fillfactor=75, auto_repack_interval=30.0);
INSERT INTO clustered_pg_vacuum_consistency(id) SELECT generate_series(1,50);
SELECT segment_map_rebuild_from_index(
	'clustered_pg_vacuum_consistency_idx'::regclass,
	1, 16, 75, 30.0::double precision) AS vacuum_consistency_rebuild_rows;
-- Delete a range and verify count
DELETE FROM clustered_pg_vacuum_consistency WHERE id BETWEEN 10 AND 30;
SELECT count(*) AS vacuum_pre_vacuum_count FROM clustered_pg_vacuum_consistency;
-- Vacuum and re-verify
VACUUM clustered_pg_vacuum_consistency;
SELECT count(*) AS vacuum_post_vacuum_count FROM clustered_pg_vacuum_consistency;
-- Verify remaining rows are correct
SELECT array_agg(id ORDER BY id) AS vacuum_remaining_ids
FROM clustered_pg_vacuum_consistency
WHERE id <= 15;
-- GC orphaned TIDs and verify
SELECT segment_map_tids_gc('clustered_pg_vacuum_consistency'::regclass) AS vacuum_gc_count;
-- Re-verify data integrity after gc
SELECT count(*) AS vacuum_post_gc_count FROM clustered_pg_vacuum_consistency;
DROP TABLE clustered_pg_vacuum_consistency;

-- Test segment split boundary: insert exactly at capacity edge
CREATE TABLE clustered_pg_split_edge(id bigint) USING clustered_heap;
CREATE INDEX clustered_pg_split_edge_idx
	ON clustered_pg_split_edge USING clustered_pk_index (id)
		WITH (split_threshold=16, target_fillfactor=100, auto_repack_interval=30.0);
-- Insert exactly split_threshold rows (should fill one segment)
INSERT INTO clustered_pg_split_edge(id) SELECT generate_series(1,16);
SELECT segment_map_rebuild_from_index(
	'clustered_pg_split_edge_idx'::regclass,
	1, 16, 100, 30.0::double precision) AS split_edge_rebuild_at_capacity;
SELECT count(*) AS split_edge_segments_at_capacity
FROM segment_map_stats('clustered_pg_split_edge'::regclass::oid);
-- Insert more to trigger split
INSERT INTO clustered_pg_split_edge(id) SELECT generate_series(17,20);
SELECT segment_map_rebuild_from_index(
	'clustered_pg_split_edge_idx'::regclass,
	1, 16, 100, 30.0::double precision) AS split_edge_rebuild_over_capacity;
SELECT count(*) AS split_edge_segments_over_capacity
FROM segment_map_stats('clustered_pg_split_edge'::regclass::oid);
SELECT count(*) AS split_edge_total_rows FROM clustered_pg_split_edge;
DROP TABLE clustered_pg_split_edge;

-- Test empty table operations
CREATE TABLE clustered_pg_empty(id bigint) USING clustered_heap;
CREATE INDEX clustered_pg_empty_idx
	ON clustered_pg_empty USING clustered_pk_index (id)
		WITH (split_threshold=32, target_fillfactor=85, auto_repack_interval=30.0);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS empty_count FROM clustered_pg_empty;
SELECT count(*) AS empty_filter_count FROM clustered_pg_empty WHERE id = 1;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE clustered_pg_empty;

-- ================================================================
-- Directed placement: verify rows with same key land on same block
-- ================================================================
CREATE TABLE clustered_pg_directed(id int) USING clustered_heap;
CREATE INDEX clustered_pg_directed_idx
    ON clustered_pg_directed USING clustered_pk_index (id)
    WITH (split_threshold = 128, target_fillfactor = 85);

-- Insert 200 rows: 20 distinct keys, 10 rows each.
-- With directed placement, all 10 rows for the same key should land
-- on the same block (or very few blocks).
INSERT INTO clustered_pg_directed(id)
SELECT key_id
FROM generate_series(1, 20) AS key_id,
     generate_series(1, 10) AS rep;

-- For each key, count distinct blocks.  Perfect clustering = 1 block per key.
-- Allow up to 2 (page could fill up for large tuples).
SELECT
    CASE WHEN every(blk_count <= 2)
         THEN 'directed_placement_ok'
         ELSE 'directed_placement_FAIL'
    END AS directed_placement_result
FROM (
    SELECT id, count(DISTINCT (ctid::text::point)[0]::int) AS blk_count
    FROM clustered_pg_directed
    GROUP BY id
) sub;

-- Verify monotonic block ordering: keys inserted in order should have
-- non-decreasing minimum block numbers.
SELECT
    CASE WHEN bool_and(min_blk >= lag_blk OR lag_blk IS NULL)
         THEN 'block_order_ok'
         ELSE 'block_order_FAIL'
    END AS block_order_result
FROM (
    SELECT id,
           min((ctid::text::point)[0]::int) AS min_blk,
           lag(min((ctid::text::point)[0]::int)) OVER (ORDER BY id) AS lag_blk
    FROM clustered_pg_directed
    GROUP BY id
) sub;

DROP TABLE clustered_pg_directed;

-- ================================================================
-- COPY path directed placement (multi_insert override)
-- ================================================================
CREATE TABLE clustered_pg_copy_dp(id int, payload text) USING clustered_heap;
CREATE INDEX clustered_pg_copy_dp_idx
    ON clustered_pg_copy_dp USING clustered_pk_index (id);

-- Use INSERT ... SELECT which goes through multi_insert for large batches
-- 30 keys x 30 rows each = 900 rows, ~500 byte payload -> multi-block
INSERT INTO clustered_pg_copy_dp(id, payload)
SELECT ((g % 30) + 1), repeat('x', 500)
FROM generate_series(1, 900) g;

-- With multi_insert directed placement, same-key rows should cluster
SELECT
    CASE WHEN avg(blk_count) <= 4.0
         THEN 'copy_directed_ok'
         ELSE 'copy_directed_FAIL'
    END AS copy_directed_result
FROM (
    SELECT id, count(DISTINCT (ctid::text::point)[0]::int) AS blk_count
    FROM clustered_pg_copy_dp
    GROUP BY id
) sub;

DROP TABLE clustered_pg_copy_dp;

DROP EXTENSION clustered_pg;
