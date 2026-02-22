CREATE EXTENSION clustered_pg;
SELECT public.version();
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
SELECT segment_map_count_repack_due('clustered_pk_int8_vacuum_table'::regclass::oid, 3600.0) AS due_repack_before_vacuum;
DELETE FROM clustered_pk_int8_vacuum_table WHERE id BETWEEN 1 AND 4;
VACUUM clustered_pk_int8_vacuum_table;
SELECT * FROM segment_map_stats('clustered_pk_int8_vacuum_table'::regclass::oid) ORDER BY major_key;
SELECT segment_map_count_repack_due('clustered_pk_int8_vacuum_table'::regclass::oid, 3600.0) AS due_repack_after_vacuum;
DROP TABLE clustered_pk_int8_vacuum_table;

CREATE TABLE clustered_pk_int8_rebuild_table(id bigint);
INSERT INTO clustered_pk_int8_rebuild_table(id)
SELECT generate_series(1,18);
CREATE INDEX clustered_pk_int8_rebuild_table_idx
	ON clustered_pk_int8_rebuild_table USING clustered_pk_index (id)
		WITH (split_threshold=16, target_fillfactor=75, auto_repack_interval=30.0);
DELETE FROM clustered_pk_int8_rebuild_table WHERE id BETWEEN 1 AND 4;
SELECT segment_map_count_repack_due('clustered_pk_int8_rebuild_table'::regclass::oid, 3600.0) AS due_repack_before_manual_rebuild;
SELECT segment_map_rebuild_from_index('clustered_pk_int8_rebuild_table_idx'::regclass, 1, 16, 75, 30.0) AS rebuilt_rows;
SELECT * FROM segment_map_stats('clustered_pk_int8_rebuild_table'::regclass::oid) ORDER BY major_key;
DROP TABLE clustered_pk_int8_rebuild_table;

SELECT * FROM segment_map_stats('clustered_pg_fixture'::regclass::oid) ORDER BY major_key;

SELECT locator_lt(locator_pack(0,1), locator_pack(1,0)) as op_lt,
       locator_gt(locator_pack(1,0), locator_pack(0,1)) as op_gt,
       locator_eq(locator_pack(1,2), locator_pack(1,2)) as op_eq;

DROP EXTENSION clustered_pg;
