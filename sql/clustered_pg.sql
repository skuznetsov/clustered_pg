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

-- ================================================================
-- sorted_heap Table AM: Phase 1 tests
-- ================================================================

-- Test SH-1: Create table with sorted_heap AM
CREATE TABLE sh_basic(id bigint, val text) USING sorted_heap;

-- Test SH-2: Single INSERT + SELECT
INSERT INTO sh_basic(id, val) VALUES (1, 'hello');
SELECT count(*) AS sh_single_count FROM sh_basic;

-- Test SH-3: Bulk INSERT
INSERT INTO sh_basic(id, val)
SELECT g, 'row_' || g FROM generate_series(2, 100) g;
SELECT count(*) AS sh_multi_count FROM sh_basic;

-- Test SH-4: Data roundtrip (correct values returned)
SELECT id, val FROM sh_basic WHERE id = 50;

-- Test SH-5: DELETE
DELETE FROM sh_basic WHERE id BETWEEN 20 AND 30;
SELECT count(*) AS sh_after_delete FROM sh_basic;

-- Test SH-6: UPDATE
UPDATE sh_basic SET val = 'updated' WHERE id = 1;
SELECT val AS sh_updated_val FROM sh_basic WHERE id = 1;

-- Test SH-7: VACUUM
VACUUM sh_basic;
SELECT count(*) AS sh_after_vacuum FROM sh_basic;

-- Test SH-8: Index creation and index scan
CREATE INDEX sh_basic_idx ON sh_basic USING btree (id);
SET enable_seqscan = off;
SELECT count(*) AS sh_idx_count FROM sh_basic WHERE id = 50;
RESET enable_seqscan;

-- Test SH-9: TRUNCATE + re-insert
TRUNCATE sh_basic;
SELECT count(*) AS sh_after_trunc FROM sh_basic;
INSERT INTO sh_basic(id, val) VALUES (1, 'after_truncate');
SELECT count(*) AS sh_reinsert FROM sh_basic;

DROP TABLE sh_basic;

-- Test SH-10: Empty table
CREATE TABLE sh_empty(id bigint) USING sorted_heap;
SELECT count(*) AS sh_empty FROM sh_empty;
DROP TABLE sh_empty;

-- Test SH-11: Bulk multi-insert path (large batch)
CREATE TABLE sh_bulk(id int, payload text) USING sorted_heap;
INSERT INTO sh_bulk(id, payload)
SELECT g, repeat('x', 200) FROM generate_series(1, 1000) g;
SELECT count(*) AS sh_bulk_count FROM sh_bulk;
SELECT count(*) AS sh_bulk_range
FROM sh_bulk WHERE id BETWEEN 500 AND 510;
DROP TABLE sh_bulk;

-- Test SH-12: ANALYZE
CREATE TABLE sh_analyze(id bigint, val text) USING sorted_heap;
INSERT INTO sh_analyze(id, val)
SELECT g, repeat('a', 100) FROM generate_series(1, 500) g;
ANALYZE sh_analyze;
SELECT count(*) AS sh_post_analyze FROM sh_analyze;
DROP TABLE sh_analyze;

-- Test SH-13: NULL values
CREATE TABLE sh_null(id bigint, val text) USING sorted_heap;
INSERT INTO sh_null(id, val) VALUES (NULL, 'null_id');
INSERT INTO sh_null(id, val) VALUES (1, NULL);
INSERT INTO sh_null(id, val) VALUES (NULL, NULL);
SELECT count(*) AS sh_null_count FROM sh_null;
DROP TABLE sh_null;

-- Test SH-14: Co-existence with clustered_heap
CREATE TABLE ch_coexist(id int, payload text) USING clustered_heap;
CREATE INDEX ch_coexist_pkidx ON ch_coexist USING clustered_pk_index (id);
CREATE INDEX ch_coexist_idx ON ch_coexist USING btree (id);
CREATE TABLE sh_coexist(id int, payload text) USING sorted_heap;
INSERT INTO ch_coexist(id, payload) SELECT g, 'ch_' || g FROM generate_series(1, 10) g;
INSERT INTO sh_coexist(id, payload) SELECT g, 'sh_' || g FROM generate_series(1, 10) g;
SELECT
    (SELECT count(*) FROM ch_coexist) AS ch_count,
    (SELECT count(*) FROM sh_coexist) AS sh_count;
DROP TABLE ch_coexist;
DROP TABLE sh_coexist;

-- Test SH-15: COPY path (exercises multi_insert)
CREATE TABLE sh_copy(id int, val text) USING sorted_heap;
COPY sh_copy FROM stdin;
1	alpha
2	beta
3	gamma
4	delta
5	epsilon
\.
SELECT count(*) AS sh_copy_count FROM sh_copy;
DROP TABLE sh_copy;

-- ================================================================
-- sorted_heap Table AM: Phase 2 tests (PK-sorted COPY)
-- ================================================================

-- Test SH2-1: COPY with int PK — verify physical sort order
CREATE TABLE sh2_pk_int(id int PRIMARY KEY, val text) USING sorted_heap;
-- Generate data in reverse order, COPY into sorted_heap
CREATE TEMP TABLE sh2_src1 AS
    SELECT id, 'v' || id AS val FROM generate_series(1, 500) id ORDER BY id DESC;
COPY sh2_src1 TO '/tmp/sh2_pk_int.csv' CSV;
COPY sh2_pk_int FROM '/tmp/sh2_pk_int.csv' CSV;
-- Verify zero inversions in physical order vs PK order
SELECT
    CASE WHEN count(*) = 0
         THEN 'pk_int_sorted_ok'
         ELSE 'pk_int_sorted_FAIL'
    END AS sh2_pk_int_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh2_pk_int
) sub
WHERE inv;
SELECT count(*) AS sh2_pk_int_count FROM sh2_pk_int;
DROP TABLE sh2_pk_int;
DROP TABLE sh2_src1;

-- Test SH2-2: COPY with composite PK (text, int)
CREATE TABLE sh2_composite(cat text, id int, val text, PRIMARY KEY(cat, id)) USING sorted_heap;
CREATE TEMP TABLE sh2_src2 AS
    SELECT chr(65 + (g % 5)) AS cat, g AS id, 'v' || g AS val
    FROM generate_series(1, 200) g
    ORDER BY random();
COPY sh2_src2 TO '/tmp/sh2_composite.csv' CSV;
COPY sh2_composite FROM '/tmp/sh2_composite.csv' CSV;
-- Verify sort: cat ASC, then id ASC within cat
SELECT
    CASE WHEN count(*) = 0
         THEN 'composite_sorted_ok'
         ELSE 'composite_sorted_FAIL'
    END AS sh2_composite_result
FROM (
    SELECT (cat < lag(cat) OVER (ORDER BY ctid))
        OR (cat = lag(cat) OVER (ORDER BY ctid)
            AND id < lag(id) OVER (ORDER BY ctid)) AS inv
    FROM sh2_composite
) sub
WHERE inv;
SELECT count(*) AS sh2_composite_count FROM sh2_composite;
DROP TABLE sh2_composite;
DROP TABLE sh2_src2;

-- Test SH2-3: COPY without PK (no crash, works as heap)
CREATE TABLE sh2_nopk(id int, val text) USING sorted_heap;
CREATE TEMP TABLE sh2_src3 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 100) g ORDER BY random();
COPY sh2_src3 TO '/tmp/sh2_nopk.csv' CSV;
COPY sh2_nopk FROM '/tmp/sh2_nopk.csv' CSV;
SELECT count(*) AS sh2_nopk_count FROM sh2_nopk;
DROP TABLE sh2_nopk;
DROP TABLE sh2_src3;

-- Test SH2-4: PK created after table — relcache callback triggers re-detection
CREATE TABLE sh2_latepk(id int, val text) USING sorted_heap;
-- COPY without PK (unsorted)
CREATE TEMP TABLE sh2_src4 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 50) g ORDER BY random();
COPY sh2_src4 TO '/tmp/sh2_latepk.csv' CSV;
COPY sh2_latepk FROM '/tmp/sh2_latepk.csv' CSV;
-- Now add PK
ALTER TABLE sh2_latepk ADD PRIMARY KEY (id);
-- COPY more data — this batch should be sorted
TRUNCATE sh2_src4;
INSERT INTO sh2_src4 SELECT g, 'w' || g FROM generate_series(51, 100) g ORDER BY random();
COPY sh2_src4 TO '/tmp/sh2_latepk2.csv' CSV;
COPY sh2_latepk FROM '/tmp/sh2_latepk2.csv' CSV;
-- Verify second batch is sorted (filter by id > 50 to check only new rows)
SELECT
    CASE WHEN count(*) = 0
         THEN 'latepk_sorted_ok'
         ELSE 'latepk_sorted_FAIL'
    END AS sh2_latepk_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh2_latepk
    WHERE id > 50
) sub
WHERE inv;
SELECT count(*) AS sh2_latepk_count FROM sh2_latepk;
DROP TABLE sh2_latepk;
DROP TABLE sh2_src4;

-- Test SH2-5: COPY with text PK (collation-aware sort)
CREATE TABLE sh2_textpk(name text PRIMARY KEY, val int) USING sorted_heap;
CREATE TEMP TABLE sh2_src5 AS
    SELECT 'item_' || lpad(g::text, 4, '0') AS name, g AS val
    FROM generate_series(1, 200) g
    ORDER BY random();
COPY sh2_src5 TO '/tmp/sh2_textpk.csv' CSV;
COPY sh2_textpk FROM '/tmp/sh2_textpk.csv' CSV;
SELECT
    CASE WHEN count(*) = 0
         THEN 'textpk_sorted_ok'
         ELSE 'textpk_sorted_FAIL'
    END AS sh2_textpk_result
FROM (
    SELECT name < lag(name) OVER (ORDER BY ctid) AS inv
    FROM sh2_textpk
) sub
WHERE inv;
SELECT count(*) AS sh2_textpk_count FROM sh2_textpk;
DROP TABLE sh2_textpk;
DROP TABLE sh2_src5;

-- Test SH2-6: COPY with NULLs in non-PK columns (no crash)
CREATE TABLE sh2_nulls(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh2_src6(id int, val text);
INSERT INTO sh2_src6 VALUES (5, NULL), (3, 'three'), (1, NULL), (4, 'four'), (2, NULL);
COPY sh2_src6 TO '/tmp/sh2_nulls.csv' CSV;
COPY sh2_nulls FROM '/tmp/sh2_nulls.csv' CSV;
-- Still sorted by PK
SELECT
    CASE WHEN count(*) = 0
         THEN 'nulls_sorted_ok'
         ELSE 'nulls_sorted_FAIL'
    END AS sh2_nulls_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh2_nulls
) sub
WHERE inv;
SELECT count(*) AS sh2_nulls_count FROM sh2_nulls;
DROP TABLE sh2_nulls;
DROP TABLE sh2_src6;

-- Test SH2-7: INSERT...SELECT (tuple_insert path) — works, no crash
CREATE TABLE sh2_insert_sel(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh2_insert_sel SELECT g, 'v' || g FROM generate_series(1, 100) g;
SELECT count(*) AS sh2_insert_sel_count FROM sh2_insert_sel;
DROP TABLE sh2_insert_sel;

-- Test SH2-8: Single inserts still work after Phase 2 changes
CREATE TABLE sh2_singles(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh2_singles VALUES (5, 'e');
INSERT INTO sh2_singles VALUES (3, 'c');
INSERT INTO sh2_singles VALUES (1, 'a');
INSERT INTO sh2_singles VALUES (4, 'd');
INSERT INTO sh2_singles VALUES (2, 'b');
SELECT count(*) AS sh2_singles_count FROM sh2_singles;
-- Verify data roundtrip
SELECT id, val FROM sh2_singles ORDER BY id;
DROP TABLE sh2_singles;

-- Test SH2-9: COPY + VACUUM + more COPY (PK cache survives)
CREATE TABLE sh2_vac(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh2_src9 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 200) g ORDER BY random();
COPY sh2_src9 TO '/tmp/sh2_vac.csv' CSV;
COPY sh2_vac FROM '/tmp/sh2_vac.csv' CSV;
DELETE FROM sh2_vac WHERE id <= 100;
VACUUM sh2_vac;
-- Second COPY after vacuum
TRUNCATE sh2_src9;
INSERT INTO sh2_src9 SELECT g, 'w' || g FROM generate_series(201, 400) g ORDER BY random();
COPY sh2_src9 TO '/tmp/sh2_vac2.csv' CSV;
COPY sh2_vac FROM '/tmp/sh2_vac2.csv' CSV;
SELECT
    CASE WHEN count(*) = 0
         THEN 'vac_sorted_ok'
         ELSE 'vac_sorted_FAIL'
    END AS sh2_vac_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh2_vac
    WHERE id > 200
) sub
WHERE inv;
SELECT count(*) AS sh2_vac_count FROM sh2_vac;
DROP TABLE sh2_vac;
DROP TABLE sh2_src9;

-- ================================================================
-- sorted_heap Table AM: Phase 3 tests (Zone Maps)
-- ================================================================

-- Test SH3-1: COPY with int PK — zone map created
CREATE TABLE sh3_zonemap(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src1 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 500) g ORDER BY random();
COPY sh3_src1 TO '/tmp/sh3_zonemap.csv' CSV;
COPY sh3_zonemap FROM '/tmp/sh3_zonemap.csv' CSV;
-- Verify zone map was populated
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_zonemap'::regclass) LIKE 'version=% nentries=% pk_typid=23%'
         THEN 'zonemap_created_ok'
         ELSE 'zonemap_created_FAIL'
    END AS sh3_zonemap_created;
SELECT count(*) AS sh3_zonemap_count FROM sh3_zonemap;
DROP TABLE sh3_zonemap;
DROP TABLE sh3_src1;

-- Test SH3-2: Text PK — zone map not used (graceful degradation)
CREATE TABLE sh3_textpk(name text PRIMARY KEY, val int) USING sorted_heap;
CREATE TEMP TABLE sh3_src2 AS
    SELECT 'item_' || lpad(g::text, 4, '0') AS name, g AS val
    FROM generate_series(1, 100) g ORDER BY random();
COPY sh3_src2 TO '/tmp/sh3_textpk.csv' CSV;
COPY sh3_textpk FROM '/tmp/sh3_textpk.csv' CSV;
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_textpk'::regclass) LIKE '%nentries=0%'
         THEN 'zonemap_textpk_skip_ok'
         ELSE 'zonemap_textpk_skip_FAIL'
    END AS sh3_textpk_result;
SELECT count(*) AS sh3_textpk_count FROM sh3_textpk;
DROP TABLE sh3_textpk;
DROP TABLE sh3_src2;

-- Test SH3-3: No PK — zone map not used, data accessible
CREATE TABLE sh3_nopk(id int, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src3 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 100) g ORDER BY random();
COPY sh3_src3 TO '/tmp/sh3_nopk.csv' CSV;
COPY sh3_nopk FROM '/tmp/sh3_nopk.csv' CSV;
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_nopk'::regclass) LIKE '%nentries=0%'
         THEN 'zonemap_nopk_skip_ok'
         ELSE 'zonemap_nopk_skip_FAIL'
    END AS sh3_nopk_result;
SELECT count(*) AS sh3_nopk_count FROM sh3_nopk;
DROP TABLE sh3_nopk;
DROP TABLE sh3_src3;

-- Test SH3-4: TRUNCATE resets zone map
CREATE TABLE sh3_trunc(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src4 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 100) g ORDER BY random();
COPY sh3_src4 TO '/tmp/sh3_trunc.csv' CSV;
COPY sh3_trunc FROM '/tmp/sh3_trunc.csv' CSV;
-- Verify zone map has entries
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_trunc'::regclass) LIKE '%nentries=0%'
         THEN 'pre_trunc_FAIL'
         ELSE 'pre_trunc_has_entries'
    END AS sh3_trunc_before;
TRUNCATE sh3_trunc;
-- Verify zone map is reset after truncate
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_trunc'::regclass) LIKE '%nentries=0%'
         THEN 'zonemap_trunc_ok'
         ELSE 'zonemap_trunc_FAIL'
    END AS sh3_trunc_after;
DROP TABLE sh3_trunc;
DROP TABLE sh3_src4;

-- Test SH3-5: Zone map entries have correct min/max ranges
CREATE TABLE sh3_ranges(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src5 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 100) g ORDER BY random();
COPY sh3_src5 TO '/tmp/sh3_ranges.csv' CSV;
COPY sh3_ranges FROM '/tmp/sh3_ranges.csv' CSV;
-- Zone map should contain entries; first entry's min should be >= 1
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_ranges'::regclass) LIKE 'version=% nentries=% pk_typid=23 flags=0 [1:%'
         THEN 'zonemap_ranges_ok'
         ELSE 'zonemap_ranges_FAIL'
    END AS sh3_ranges_result;
-- All data accessible
SELECT count(*) AS sh3_ranges_count FROM sh3_ranges;
DROP TABLE sh3_ranges;
DROP TABLE sh3_src5;

-- Test SH3-6: COPY + DELETE + VACUUM — zone map survives, data accessible
CREATE TABLE sh3_vacuum(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src6 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 500) g ORDER BY random();
COPY sh3_src6 TO '/tmp/sh3_vacuum.csv' CSV;
COPY sh3_vacuum FROM '/tmp/sh3_vacuum.csv' CSV;
DELETE FROM sh3_vacuum WHERE id BETWEEN 100 AND 200;
VACUUM sh3_vacuum;
-- Zone map still has entries (conservative — may be wider than actual data)
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_vacuum'::regclass) LIKE 'version=% nentries=%'
         THEN 'zonemap_vacuum_ok'
         ELSE 'zonemap_vacuum_FAIL'
    END AS sh3_vacuum_result;
SELECT count(*) AS sh3_vacuum_count FROM sh3_vacuum;
-- Verify data accessible after vacuum
SELECT count(*) AS sh3_vacuum_range FROM sh3_vacuum WHERE id BETWEEN 50 AND 150;
DROP TABLE sh3_vacuum;
DROP TABLE sh3_src6;

-- Test SH3-7: Existing Phase 2 sort still works with zone map
CREATE TABLE sh3_sort(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src7 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 500) g ORDER BY id DESC;
COPY sh3_src7 TO '/tmp/sh3_sort.csv' CSV;
COPY sh3_sort FROM '/tmp/sh3_sort.csv' CSV;
-- Physical sort order still correct
SELECT
    CASE WHEN count(*) = 0
         THEN 'zonemap_sort_ok'
         ELSE 'zonemap_sort_FAIL'
    END AS sh3_sort_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh3_sort
) sub
WHERE inv;
-- Zone map populated
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_sort'::regclass) LIKE 'version=% nentries=% pk_typid=23%'
         THEN 'zonemap_sort_stats_ok'
         ELSE 'zonemap_sort_stats_FAIL'
    END AS sh3_sort_stats;
DROP TABLE sh3_sort;
DROP TABLE sh3_src7;

-- ================================================================
-- sorted_heap Table AM: Phase 4 tests (Compaction)
-- ================================================================

-- Test SH4-1: Multiple COPY batches → compact → global sort
CREATE TABLE sh4_compact(id int PRIMARY KEY, val text) USING sorted_heap;
-- Batch 1: ids 201-400 (will be after batch 2 in PK order)
CREATE TEMP TABLE sh4_src1 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(201, 400) g ORDER BY random();
COPY sh4_src1 TO '/tmp/sh4_batch1.csv' CSV;
COPY sh4_compact FROM '/tmp/sh4_batch1.csv' CSV;
-- Batch 2: ids 1-200 (physically after batch 1 but lower PK)
CREATE TEMP TABLE sh4_src2 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 200) g ORDER BY random();
COPY sh4_src2 TO '/tmp/sh4_batch2.csv' CSV;
COPY sh4_compact FROM '/tmp/sh4_batch2.csv' CSV;

SELECT count(*) AS sh4_pre_compact_count FROM sh4_compact;

-- Compact: rewrites in global PK order
SELECT sorted_heap_compact('sh4_compact'::regclass);

-- Verify global sort — zero inversions
SELECT
    CASE WHEN count(*) = 0
         THEN 'compact_sorted_ok'
         ELSE 'compact_sorted_FAIL'
    END AS sh4_compact_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh4_compact
) sub
WHERE inv;

SELECT count(*) AS sh4_post_compact_count FROM sh4_compact;

DROP TABLE sh4_compact;
DROP TABLE sh4_src1;
DROP TABLE sh4_src2;

-- Test SH4-2: Zone map accuracy after compaction
CREATE TABLE sh4_zonemap(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh4_zm_src AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 500) g ORDER BY random();
COPY sh4_zm_src TO '/tmp/sh4_zonemap.csv' CSV;
COPY sh4_zonemap FROM '/tmp/sh4_zonemap.csv' CSV;
SELECT sorted_heap_compact('sh4_zonemap'::regclass);
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh4_zonemap'::regclass) LIKE 'version=% nentries=% pk_typid=23%'
         THEN 'compact_zonemap_ok'
         ELSE 'compact_zonemap_FAIL'
    END AS sh4_zonemap_result;
SELECT count(*) AS sh4_zonemap_count FROM sh4_zonemap;
DROP TABLE sh4_zonemap;
DROP TABLE sh4_zm_src;

-- Test SH4-3: Compact table without PK — should error
CREATE TABLE sh4_nopk(id int, val text) USING sorted_heap;
INSERT INTO sh4_nopk SELECT g, 'v' || g FROM generate_series(1, 10) g;
SELECT sorted_heap_compact('sh4_nopk'::regclass);
DROP TABLE sh4_nopk;

-- Test SH4-4: Compact after DELETE + VACUUM
CREATE TABLE sh4_vacuum(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh4_vac_src AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 500) g ORDER BY random();
COPY sh4_vac_src TO '/tmp/sh4_vacuum.csv' CSV;
COPY sh4_vacuum FROM '/tmp/sh4_vacuum.csv' CSV;
DELETE FROM sh4_vacuum WHERE id BETWEEN 100 AND 300;
VACUUM sh4_vacuum;
SELECT sorted_heap_compact('sh4_vacuum'::regclass);
SELECT
    CASE WHEN count(*) = 0
         THEN 'compact_vacuum_sorted_ok'
         ELSE 'compact_vacuum_sorted_FAIL'
    END AS sh4_vacuum_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh4_vacuum
) sub
WHERE inv;
SELECT count(*) AS sh4_vacuum_count FROM sh4_vacuum;
DROP TABLE sh4_vacuum;
DROP TABLE sh4_vac_src;

-- Test SH4-5: Standalone zonemap rebuild
CREATE TABLE sh4_rebuild(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh4_rebuild SELECT g, 'v' || g FROM generate_series(1, 100) g;
SELECT sorted_heap_rebuild_zonemap('sh4_rebuild'::regclass);
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh4_rebuild'::regclass) LIKE 'version=% nentries=% pk_typid=23%'
         THEN 'rebuild_zonemap_ok'
         ELSE 'rebuild_zonemap_FAIL'
    END AS sh4_rebuild_result;
DROP TABLE sh4_rebuild;

-- Test SH4-6: Compact with bigint PK
CREATE TABLE sh4_bigint(id bigint PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh4_big_src AS
    SELECT g::bigint AS id, 'v' || g AS val FROM generate_series(1, 300) g ORDER BY random();
COPY sh4_big_src TO '/tmp/sh4_bigint.csv' CSV;
COPY sh4_bigint FROM '/tmp/sh4_bigint.csv' CSV;
CREATE TEMP TABLE sh4_big_src2 AS
    SELECT (g + 300)::bigint AS id, 'w' || g AS val FROM generate_series(1, 300) g ORDER BY random();
COPY sh4_big_src2 TO '/tmp/sh4_bigint2.csv' CSV;
COPY sh4_bigint FROM '/tmp/sh4_bigint2.csv' CSV;
SELECT sorted_heap_compact('sh4_bigint'::regclass);
SELECT
    CASE WHEN count(*) = 0
         THEN 'compact_bigint_sorted_ok'
         ELSE 'compact_bigint_sorted_FAIL'
    END AS sh4_bigint_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh4_bigint
) sub
WHERE inv;
SELECT count(*) AS sh4_bigint_count FROM sh4_bigint;
DROP TABLE sh4_bigint;
DROP TABLE sh4_big_src;
DROP TABLE sh4_big_src2;

-- ================================================================
-- sorted_heap Table AM: Phase 5 tests (Scan Pruning via Zone Maps)
-- ================================================================

-- Helper: check if EXPLAIN plan contains a pattern
CREATE FUNCTION sh5_plan_contains(query text, pattern text) RETURNS boolean AS $$
DECLARE
    r record;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query
    LOOP
        IF r."QUERY PLAN" LIKE '%' || pattern || '%' THEN
            RETURN true;
        END IF;
    END LOOP;
    RETURN false;
END;
$$ LANGUAGE plpgsql;

-- Setup: load data via COPY, enough for many pages
CREATE TABLE sh5_scan(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh5_src AS
    SELECT g AS id, repeat('x', 100) AS val FROM generate_series(1, 2000) g ORDER BY random();
COPY sh5_src TO '/tmp/sh5_scan.csv' CSV;
COPY sh5_scan FROM '/tmp/sh5_scan.csv' CSV;

-- Disable index scans to test seq scan vs custom scan
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Test SH5-1: Before compact — no pruning (zone map not valid)
SELECT sh5_plan_contains(
    'SELECT * FROM sh5_scan WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh5_before_compact;

-- Compact + analyze
SELECT sorted_heap_compact('sh5_scan'::regclass);
ANALYZE sh5_scan;

-- Test SH5-2: After compact — custom scan used for range query
SELECT sh5_plan_contains(
    'SELECT * FROM sh5_scan WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh5_after_compact;

-- Test SH5-3: Range query — correct results
SELECT count(*) AS sh5_range_count FROM sh5_scan WHERE id BETWEEN 100 AND 200;

-- Test SH5-4: Point query — correct result
SELECT count(*) AS sh5_point_count FROM sh5_scan WHERE id = 500;

-- Test SH5-5: Full scan (no WHERE) — all rows
SELECT count(*) AS sh5_full_count FROM sh5_scan;

-- Test SH5-6: INSERT invalidates zone map, falls back to seq scan
INSERT INTO sh5_scan VALUES (2001, 'extra');
SELECT sh5_plan_contains(
    'SELECT * FROM sh5_scan WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh5_after_insert;
SELECT count(*) AS sh5_after_insert_range FROM sh5_scan WHERE id BETWEEN 100 AND 200;

-- Test SH5-7: Re-compact restores pruning
SELECT sorted_heap_compact('sh5_scan'::regclass);
ANALYZE sh5_scan;
SELECT sh5_plan_contains(
    'SELECT * FROM sh5_scan WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh5_recompact;
SELECT count(*) AS sh5_recompact_range FROM sh5_scan WHERE id BETWEEN 100 AND 200;

RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE sh5_scan;
DROP TABLE sh5_src;
DROP FUNCTION sh5_plan_contains(text, text);

-- ================================================================
-- sorted_heap Table AM: Phase 6 tests (Production Hardening)
-- ================================================================

-- Helper: check if EXPLAIN plan contains a pattern
CREATE FUNCTION sh6_plan_contains(query text, pattern text) RETURNS boolean AS $$
DECLARE
    r record;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query
    LOOP
        IF r."QUERY PLAN" LIKE '%' || pattern || '%' THEN
            RETURN true;
        END IF;
    END LOOP;
    RETURN false;
END;
$$ LANGUAGE plpgsql;

-- Setup
CREATE TABLE sh6_guc(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh6_src AS
    SELECT g AS id, repeat('x', 80) AS val FROM generate_series(1, 1000) g ORDER BY random();
COPY sh6_src TO '/tmp/sh6_guc.csv' CSV;
COPY sh6_guc FROM '/tmp/sh6_guc.csv' CSV;
SELECT sorted_heap_compact('sh6_guc'::regclass);
ANALYZE sh6_guc;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Test SH6-1: GUC off → no SortedHeapScan
SET sorted_heap.enable_scan_pruning = off;
SELECT sh6_plan_contains(
    'SELECT * FROM sh6_guc WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh6_guc_off;

-- Test SH6-2: GUC on → SortedHeapScan
SET sorted_heap.enable_scan_pruning = on;
SELECT sh6_plan_contains(
    'SELECT * FROM sh6_guc WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh6_guc_on;

RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE sh6_guc;
DROP TABLE sh6_src;

-- Test SH6-3: TIMESTAMP PK + compact + range query
CREATE TABLE sh6_ts(ts timestamp PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh6_ts
    SELECT '2024-01-01'::timestamp + (g || ' seconds')::interval, 'v' || g
    FROM generate_series(1, 1000) g;
SELECT sorted_heap_compact('sh6_ts'::regclass);
ANALYZE sh6_ts;
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh6_ts'::regclass) LIKE 'version=% nentries=%'
         THEN 'ts_zonemap_ok'
         ELSE 'ts_zonemap_FAIL'
    END AS sh6_ts_result;
SELECT count(*) AS sh6_ts_range
FROM sh6_ts
WHERE ts BETWEEN '2024-01-01 00:05:00'::timestamp AND '2024-01-01 00:10:00'::timestamp;
DROP TABLE sh6_ts;

-- Test SH6-4: DATE PK + compact + point query
CREATE TABLE sh6_date(d date PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh6_date
    SELECT '2024-01-01'::date + g, 'v' || g
    FROM generate_series(1, 500) g;
SELECT sorted_heap_compact('sh6_date'::regclass);
ANALYZE sh6_date;
SELECT count(*) AS sh6_date_point
FROM sh6_date WHERE d = '2024-03-01'::date;
DROP TABLE sh6_date;

-- Test SH6-5: INSERT within zone map range → pruning still works
CREATE TABLE sh6_insert(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh6_ins_src AS
    SELECT g AS id, repeat('x', 80) AS val FROM generate_series(1, 500) g ORDER BY random();
COPY sh6_ins_src TO '/tmp/sh6_insert.csv' CSV;
COPY sh6_insert FROM '/tmp/sh6_insert.csv' CSV;
SELECT sorted_heap_compact('sh6_insert'::regclass);
ANALYZE sh6_insert;

SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Insert into covered page — pruning should survive
INSERT INTO sh6_insert VALUES (250, 'new') ON CONFLICT (id) DO UPDATE SET val = 'new';
SELECT sh6_plan_contains(
    'SELECT * FROM sh6_insert WHERE id = 100',
    'SortedHeapScan') AS sh6_insert_covered;

RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE sh6_insert;
DROP TABLE sh6_ins_src;

-- Test SH6-6: INSERT outside zone map range → pruning disabled
-- (Use a fresh table, insert beyond existing pages)
CREATE TABLE sh6_outside(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh6_outside SELECT g, repeat('x', 80) FROM generate_series(1, 100) g;
SELECT sorted_heap_compact('sh6_outside'::regclass);
ANALYZE sh6_outside;

SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Insert row that will go to a new page (beyond zone map coverage)
INSERT INTO sh6_outside SELECT g, repeat('x', 80) FROM generate_series(101, 1000) g;
SELECT sh6_plan_contains(
    'SELECT * FROM sh6_outside WHERE id = 50',
    'SortedHeapScan') AS sh6_insert_outside;

-- Test SH6-7: Re-compact restores pruning
SELECT sorted_heap_compact('sh6_outside'::regclass);
ANALYZE sh6_outside;
SELECT sh6_plan_contains(
    'SELECT * FROM sh6_outside WHERE id = 50',
    'SortedHeapScan') AS sh6_recompact;

RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE sh6_outside;

-- Test SH6-9: EXPLAIN ANALYZE shows counters (textual check)
CREATE TABLE sh6_explain(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh6_explain SELECT g, repeat('x', 80) FROM generate_series(1, 500) g;
SELECT sorted_heap_compact('sh6_explain'::regclass);
ANALYZE sh6_explain;

SET enable_indexscan = off;
SET enable_bitmapscan = off;

CREATE FUNCTION sh6_explain_has_counters() RETURNS boolean AS $$
DECLARE
    r record;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM sh6_explain WHERE id BETWEEN 100 AND 200'
    LOOP
        IF r."QUERY PLAN" LIKE '%Scanned Blocks%' THEN
            RETURN true;
        END IF;
    END LOOP;
    RETURN false;
END;
$$ LANGUAGE plpgsql;
SELECT sh6_explain_has_counters() AS sh6_explain_counters;

RESET enable_indexscan;
RESET enable_bitmapscan;
DROP FUNCTION sh6_explain_has_counters();
DROP TABLE sh6_explain;

-- Test SH6-10: sorted_heap_scan_stats() with reset
SELECT sorted_heap_reset_stats();
-- Create a small table, compact it, run a pruned query to generate stats
CREATE TABLE sh6_stats(id int PRIMARY KEY, v text) USING sorted_heap;
INSERT INTO sh6_stats SELECT i, 'x' FROM generate_series(1, 1000) i;
SELECT sorted_heap_compact('sh6_stats'::regclass);
ANALYZE sh6_stats;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM sh6_stats WHERE id = 50;
RESET enable_indexscan;
RESET enable_bitmapscan;
SELECT
    CASE WHEN sorted_heap_scan_stats() LIKE 'scans=1 blocks_scanned=% blocks_pruned=%'
         THEN 'scan_stats_ok'
         ELSE 'scan_stats_FAIL: ' || sorted_heap_scan_stats()
    END AS sh6_stats_result;
DROP TABLE sh6_stats;

-- ====================================================================
-- SH7: Online compact (sorted_heap_compact_online)
-- ====================================================================

-- Test SH7-1: Basic online compact produces correct row count
CREATE TABLE sh7_basic(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh7_basic SELECT g, repeat('x', 80) FROM generate_series(1, 500) g;
-- Scramble order: delete and re-insert some rows
DELETE FROM sh7_basic WHERE id % 3 = 0;
INSERT INTO sh7_basic SELECT g, repeat('y', 80) FROM generate_series(501, 700) g;
SELECT count(*) AS sh7_before_count FROM sh7_basic;
CALL sorted_heap_compact_online('sh7_basic'::regclass);
SELECT count(*) AS sh7_after_count FROM sh7_basic;

-- Test SH7-2: Data is sorted after online compact
SELECT (bool_and(id >= lag_id)) AS sh7_sorted
FROM (
    SELECT id, lag(id, 1, 0) OVER (ORDER BY ctid) AS lag_id
    FROM sh7_basic
) sub;

-- Test SH7-3: Zone map populated after online compact
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh7_basic'::regclass) LIKE 'version=% nentries=% pk_typid=23 flags=2%'
         THEN 'zonemap_online_ok'
         ELSE 'zonemap_online_FAIL: ' || sorted_heap_zonemap_stats('sh7_basic'::regclass)
    END AS sh7_zonemap_result;

-- Test SH7-4: Scan pruning works after online compact
ANALYZE sh7_basic;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT sh6_plan_contains(
    'SELECT * FROM sh7_basic WHERE id = 50',
    'SortedHeapScan') AS sh7_pruning_works;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- Test SH7-5: Online compact produces same result as regular compact
CREATE TABLE sh7_compare(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh7_compare SELECT g, repeat('z', 40) FROM generate_series(1, 300) g;
DELETE FROM sh7_compare WHERE id % 5 = 0;
INSERT INTO sh7_compare SELECT g, repeat('w', 40) FROM generate_series(301, 400) g;
CALL sorted_heap_compact_online('sh7_compare'::regclass);
SELECT count(*) AS sh7_compare_online_count FROM sh7_compare;
-- Re-compact with regular compact to verify equivalence
SELECT sorted_heap_compact('sh7_compare'::regclass);
SELECT count(*) AS sh7_compare_regular_count FROM sh7_compare;

DROP TABLE sh7_basic;
DROP TABLE sh7_compare;

DROP FUNCTION sh6_plan_contains(text, text);

DROP EXTENSION clustered_pg;
