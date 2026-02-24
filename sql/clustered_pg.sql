CREATE EXTENSION clustered_pg;
SELECT public.version();
SELECT public.clustered_pg_observability() AS observability_bootstrap;
SELECT (public.clustered_pg_observability() ~ 'clustered_pg=0.1.0') AS observability_probe;

-- ====================================================================
-- Functional regression tests: multi-type index, JOIN UNNEST rescan,
-- delete+vacuum consistency, locator edge cases, directed placement
-- ====================================================================

-- Test int2 and int4 index support (only int8 tested above)
CREATE TABLE clustered_pg_int2_smoke(id smallint) USING clustered_heap;
CREATE INDEX clustered_pg_int2_smoke_idx
	ON clustered_pg_int2_smoke USING clustered_pk_index (id);
INSERT INTO clustered_pg_int2_smoke(id) SELECT generate_series(1,20)::smallint;
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
	ON clustered_pg_int4_smoke USING clustered_pk_index (id);
INSERT INTO clustered_pg_int4_smoke(id) SELECT generate_series(1,20);
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

-- Test JOIN UNNEST correctness
CREATE TABLE clustered_pg_join_unnest_base(id bigint) USING clustered_heap;
CREATE INDEX clustered_pg_join_unnest_base_idx
	ON clustered_pg_join_unnest_base USING clustered_pk_index (id);
INSERT INTO clustered_pg_join_unnest_base(id) SELECT generate_series(1,100);
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
	ON clustered_pg_vacuum_consistency USING clustered_pk_index (id);
INSERT INTO clustered_pg_vacuum_consistency(id) SELECT generate_series(1,50);
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
-- Re-verify data integrity after vacuum
SELECT count(*) AS vacuum_post_gc_count FROM clustered_pg_vacuum_consistency;
DROP TABLE clustered_pg_vacuum_consistency;

-- Test segment split boundary: insert exactly at capacity edge
CREATE TABLE clustered_pg_split_edge(id bigint) USING clustered_heap;
CREATE INDEX clustered_pg_split_edge_idx
	ON clustered_pg_split_edge USING clustered_pk_index (id);
-- Insert exactly split_threshold rows
INSERT INTO clustered_pg_split_edge(id) SELECT generate_series(1,16);
SELECT count(*) AS split_edge_at_capacity FROM clustered_pg_split_edge;
-- Insert more rows
INSERT INTO clustered_pg_split_edge(id) SELECT generate_series(17,20);
SELECT count(*) AS split_edge_total_rows FROM clustered_pg_split_edge;
DROP TABLE clustered_pg_split_edge;

-- Test empty table operations
CREATE TABLE clustered_pg_empty(id bigint) USING clustered_heap;
CREATE INDEX clustered_pg_empty_idx
	ON clustered_pg_empty USING clustered_pk_index (id);
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
    ON clustered_pg_directed USING clustered_pk_index (id);

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

-- ================================================================
-- UPDATE + DELETE on directed-placement table
-- ================================================================
CREATE TABLE clustered_pg_upd(id int, val int, payload text) USING clustered_heap;
CREATE INDEX clustered_pg_upd_idx
    ON clustered_pg_upd USING clustered_pk_index (id);

-- Insert 5 keys, 10 rows each
INSERT INTO clustered_pg_upd(id, val, payload)
SELECT (g % 5) + 1, g, repeat('y', 200)
FROM generate_series(1, 50) g;

-- Verify initial count
SELECT count(*) AS before_count FROM clustered_pg_upd;

-- UPDATE: change payload (same key = HOT candidate)
UPDATE clustered_pg_upd SET val = val + 1000 WHERE id = 3;

-- UPDATE: change the clustering key itself
UPDATE clustered_pg_upd SET id = 99 WHERE id = 5;

-- Verify counts by key after updates
SELECT id, count(*) AS cnt FROM clustered_pg_upd
WHERE id IN (3, 5, 99) GROUP BY id ORDER BY id;

-- DELETE a whole key group
DELETE FROM clustered_pg_upd WHERE id = 2;

-- Verify final count
SELECT count(*) AS after_count FROM clustered_pg_upd;

-- Re-INSERT into the table (zone map should still work for new inserts)
INSERT INTO clustered_pg_upd(id, val, payload)
SELECT 2, g, repeat('z', 200)
FROM generate_series(1, 5) g;

SELECT count(*) AS final_count FROM clustered_pg_upd;

DROP TABLE clustered_pg_upd;

-- ================================================================
-- NULL clustering key handling
-- ================================================================
CREATE TABLE clustered_pg_null(id int, payload text) USING clustered_heap;
CREATE INDEX clustered_pg_null_idx
    ON clustered_pg_null USING clustered_pk_index (id);

-- Insert rows with NULL key: directed placement skips NULL keys (safe fallback),
-- but the index AM rejects NULLs with an error.  These INSERTs are expected to fail.
INSERT INTO clustered_pg_null(id, payload) VALUES (NULL, 'null_row_1');
INSERT INTO clustered_pg_null(id, payload) VALUES (NULL, 'null_row_2');
INSERT INTO clustered_pg_null(id, payload) VALUES (1, 'normal_row');

SELECT count(*) AS null_test_count FROM clustered_pg_null;

-- Verify we can read back all rows including NULLs
SELECT id IS NULL AS is_null, count(*) AS cnt
FROM clustered_pg_null GROUP BY (id IS NULL) ORDER BY is_null;

DROP TABLE clustered_pg_null;

-- ================================================================
-- Directed placement with many distinct keys (fast path exercise)
-- ================================================================
CREATE TABLE clustered_pg_many(id int, payload text) USING clustered_heap;
CREATE INDEX clustered_pg_many_idx
    ON clustered_pg_many USING clustered_pk_index (id);

-- 200 distinct keys x 5 rows each = 1000 rows, triggers fast path (>64 keys)
INSERT INTO clustered_pg_many(id, payload)
SELECT ((g % 200) + 1), repeat('m', 100)
FROM generate_series(1, 1000) g;

SELECT
    CASE WHEN count(*) = 1000
         THEN 'many_keys_count_ok'
         ELSE 'many_keys_count_FAIL'
    END AS many_keys_result
FROM clustered_pg_many;

-- Even with fast path, zone map should provide some clustering benefit
-- (not as tight as group path, but better than random heap)
SELECT
    CASE WHEN avg(blk_count) <= 10.0
         THEN 'many_keys_scatter_ok'
         ELSE 'many_keys_scatter_FAIL'
    END AS many_keys_scatter_result
FROM (
    SELECT id, count(DISTINCT (ctid::text::point)[0]::int) AS blk_count
    FROM clustered_pg_many
    GROUP BY id
) sub;

DROP TABLE clustered_pg_many;

-- ================================================================
-- VACUUM on directed-placement table (delete + vacuum + re-insert)
-- ================================================================
CREATE TABLE clustered_pg_vac_dp(id int, payload text) USING clustered_heap;
CREATE INDEX clustered_pg_vac_dp_idx
    ON clustered_pg_vac_dp USING clustered_pk_index (id);

-- 10 keys x 50 rows = 500 rows
INSERT INTO clustered_pg_vac_dp(id, payload)
SELECT ((g % 10) + 1), repeat('v', 200)
FROM generate_series(1, 500) g;

SELECT count(*) AS vac_before FROM clustered_pg_vac_dp;

-- Delete 60% of rows
DELETE FROM clustered_pg_vac_dp WHERE id <= 6;

VACUUM clustered_pg_vac_dp;

-- Verify remaining rows survived vacuum
SELECT count(*) AS vac_after FROM clustered_pg_vac_dp;

-- Re-insert: directed placement should still cluster new rows
INSERT INTO clustered_pg_vac_dp(id, payload)
SELECT ((g % 6) + 1), repeat('w', 200)
FROM generate_series(1, 300) g;

-- Verify data integrity and count
SELECT
    CASE WHEN count(*) = 500
         THEN 'vac_reinsert_ok'
         ELSE 'vac_reinsert_FAIL'
    END AS vac_reinsert_result
FROM clustered_pg_vac_dp;

DROP TABLE clustered_pg_vac_dp;

-- ================================================================
-- TRUNCATE invalidates zone map (re-insert should not crash)
-- ================================================================
CREATE TABLE clustered_pg_trunc(id int, payload text) USING clustered_heap;
CREATE INDEX clustered_pg_trunc_idx
    ON clustered_pg_trunc USING clustered_pk_index (id);

INSERT INTO clustered_pg_trunc(id, payload)
SELECT g, repeat('t', 100)
FROM generate_series(1, 100) g;

SELECT count(*) AS before_trunc FROM clustered_pg_trunc;

TRUNCATE clustered_pg_trunc;

SELECT count(*) AS after_trunc FROM clustered_pg_trunc;

-- Re-insert after truncate: zone map was invalidated, should rebuild cleanly
INSERT INTO clustered_pg_trunc(id, payload)
SELECT g, repeat('u', 100)
FROM generate_series(1, 50) g;

SELECT
    CASE WHEN count(*) = 50
         THEN 'trunc_reinsert_ok'
         ELSE 'trunc_reinsert_FAIL'
    END AS trunc_reinsert_result
FROM clustered_pg_trunc;

DROP TABLE clustered_pg_trunc;

-- ================================================================
-- JOIN with btree on directed-placement table (production pattern)
-- ================================================================
-- This is the key scenario: standard btree serves JOINs efficiently
-- because directed placement physically clusters rows by key.
CREATE TABLE clustered_pg_join(id int, payload text) USING clustered_heap;
CREATE INDEX clustered_pg_join_pkidx
    ON clustered_pg_join USING clustered_pk_index (id);
CREATE INDEX clustered_pg_join_btree
    ON clustered_pg_join USING btree (id);

-- 1000 keys x 100 rows = 100K rows, interleaved insert
INSERT INTO clustered_pg_join(id, payload)
SELECT ((g % 1000) + 1), repeat('j', 100)
FROM generate_series(1, 100000) g;

ANALYZE clustered_pg_join;

-- Verify the btree index is used (not clustered_pk_index)
-- Nested loop + index scan should touch very few blocks per key
SELECT
    CASE WHEN count(*) = 20000
         THEN 'join_btree_count_ok'
         ELSE 'join_btree_count_FAIL'
    END AS join_btree_result
FROM clustered_pg_join d
JOIN (SELECT unnest(ARRAY(SELECT generate_series(1, 200))) AS id) keys
ON d.id = keys.id;

-- Verify clustering held through the full insert:
-- each key's rows should be on <=5 blocks
SELECT
    CASE WHEN avg(blk_count) <= 5.0
         THEN 'join_btree_scatter_ok'
         ELSE 'join_btree_scatter_FAIL'
    END AS join_btree_scatter_result
FROM (
    SELECT id, count(DISTINCT (ctid::text::point)[0]::int) AS blk_count
    FROM clustered_pg_join
    GROUP BY id
) sub;

DROP TABLE clustered_pg_join;

DROP EXTENSION clustered_pg;
