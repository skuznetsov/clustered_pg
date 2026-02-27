-- clustered_pg extension SQL

\echo Use "CREATE EXTENSION clustered_pg" to load this file.

CREATE DOMAIN @extschema@.clustered_locator AS bytea
	CHECK (octet_length(VALUE) = 16);

CREATE FUNCTION @extschema@.version()
RETURNS text
AS '$libdir/clustered_pg', 'clustered_pg_version'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.observability()
RETURNS text
AS '$libdir/clustered_pg', 'clustered_pg_observability'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.clustered_pg_observability()
RETURNS text
AS '$libdir/clustered_pg', 'clustered_pg_observability'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.tableam_handler(internal)
RETURNS table_am_handler
AS '$libdir/clustered_pg', 'clustered_pg_tableam_handler'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.pk_index_handler(internal)
RETURNS index_am_handler
AS '$libdir/clustered_pg', 'clustered_pg_pkidx_handler'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.locator_pack(bigint, bigint)
RETURNS @extschema@.clustered_locator
AS '$libdir/clustered_pg', 'clustered_pg_locator_pack'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_pack_int8(bigint)
RETURNS @extschema@.clustered_locator
AS '$libdir/clustered_pg', 'clustered_pg_locator_pack_int8'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_major(@extschema@.clustered_locator)
RETURNS bigint
AS '$libdir/clustered_pg', 'clustered_pg_locator_major'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_minor(@extschema@.clustered_locator)
RETURNS bigint
AS '$libdir/clustered_pg', 'clustered_pg_locator_minor'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_to_hex(@extschema@.clustered_locator)
RETURNS text
AS '$libdir/clustered_pg', 'clustered_pg_locator_to_hex'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_cmp(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS int
AS '$libdir/clustered_pg', 'clustered_pg_locator_cmp'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_lt(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) < 0 $$;

CREATE FUNCTION @extschema@.locator_le(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) <= 0 $$;

CREATE FUNCTION @extschema@.locator_eq(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) = 0 $$;

CREATE FUNCTION @extschema@.locator_ge(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) >= 0 $$;

CREATE FUNCTION @extschema@.locator_gt(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) > 0 $$;

CREATE FUNCTION @extschema@.locator_ne(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) <> 0 $$;

CREATE OPERATOR @extschema@.< (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_lt
);

CREATE OPERATOR @extschema@.<= (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_le
);

CREATE OPERATOR @extschema@.>= (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_ge
);

CREATE OPERATOR @extschema@.> (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_gt
);

CREATE OPERATOR @extschema@.= (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_eq,
	NEGATOR = OPERATOR(@extschema@.<>)
);

CREATE OPERATOR @extschema@.<> (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_ne
);

CREATE OPERATOR CLASS @extschema@.clustered_locator_ops
DEFAULT FOR TYPE @extschema@.clustered_locator USING btree AS
	OPERATOR        1  <  (@extschema@.clustered_locator, @extschema@.clustered_locator),
	OPERATOR        2  <= (@extschema@.clustered_locator, @extschema@.clustered_locator),
	OPERATOR        3  =  (@extschema@.clustered_locator, @extschema@.clustered_locator),
	OPERATOR        4  >= (@extschema@.clustered_locator, @extschema@.clustered_locator),
	OPERATOR        5  >  (@extschema@.clustered_locator, @extschema@.clustered_locator),
	FUNCTION        1  @extschema@.locator_cmp(@extschema@.clustered_locator, @extschema@.clustered_locator);

CREATE FUNCTION @extschema@.locator_advance_major(@extschema@.clustered_locator, bigint)
RETURNS @extschema@.clustered_locator
AS '$libdir/clustered_pg', 'clustered_pg_locator_advance_major'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_next_minor(@extschema@.clustered_locator, bigint)
RETURNS @extschema@.clustered_locator
AS '$libdir/clustered_pg', 'clustered_pg_locator_next_minor'
LANGUAGE C STRICT IMMUTABLE;

CREATE ACCESS METHOD clustered_heap TYPE TABLE HANDLER @extschema@.tableam_handler;
CREATE ACCESS METHOD clustered_pk_index TYPE INDEX HANDLER @extschema@.pk_index_handler;

CREATE OPERATOR FAMILY @extschema@.clustered_pk_int_ops
USING clustered_pk_index;

CREATE OPERATOR CLASS @extschema@.clustered_pk_int2_ops
DEFAULT FOR TYPE int2 USING clustered_pk_index
FAMILY @extschema@.clustered_pk_int_ops AS
	OPERATOR        1  <  (int2, int2),
	OPERATOR        2  <= (int2, int2),
	OPERATOR        3  =  (int2, int2),
	OPERATOR        4  >= (int2, int2),
	OPERATOR        5  >  (int2, int2),
	FUNCTION        1  btint2cmp(int2, int2);

CREATE OPERATOR CLASS @extschema@.clustered_pk_int4_ops
DEFAULT FOR TYPE int4 USING clustered_pk_index
FAMILY @extschema@.clustered_pk_int_ops AS
	OPERATOR        1  <  (int4, int4),
	OPERATOR        2  <= (int4, int4),
	OPERATOR        3  =  (int4, int4),
	OPERATOR        4  >= (int4, int4),
	OPERATOR        5  >  (int4, int4),
	FUNCTION        1  btint4cmp(int4, int4);

CREATE OPERATOR CLASS @extschema@.clustered_pk_int8_ops
DEFAULT FOR TYPE int8 USING clustered_pk_index
FAMILY @extschema@.clustered_pk_int_ops AS
	OPERATOR        1  <  (int8, int8),
	OPERATOR        2  <= (int8, int8),
	OPERATOR        3  =  (int8, int8),
	OPERATOR        4  >= (int8, int8),
	OPERATOR        5  >  (int8, int8),
	FUNCTION        1  btint8cmp(int8, int8);

ALTER OPERATOR FAMILY @extschema@.clustered_pk_int_ops
USING clustered_pk_index ADD
	OPERATOR        1  <  (int2, int4),
	OPERATOR        2  <= (int2, int4),
	OPERATOR        3  =  (int2, int4),
	OPERATOR        4  >= (int2, int4),
	OPERATOR        5  >  (int2, int4),
	FUNCTION        1  (int2, int4) btint24cmp(int2, int4),
	OPERATOR        1  <  (int4, int2),
	OPERATOR        2  <= (int4, int2),
	OPERATOR        3  =  (int4, int2),
	OPERATOR        4  >= (int4, int2),
	OPERATOR        5  >  (int4, int2),
	FUNCTION        1  (int4, int2) btint42cmp(int4, int2),
	OPERATOR        1  <  (int2, int8),
	OPERATOR        2  <= (int2, int8),
	OPERATOR        3  =  (int2, int8),
	OPERATOR        4  >= (int2, int8),
	OPERATOR        5  >  (int2, int8),
	FUNCTION        1  (int2, int8) btint28cmp(int2, int8),
	OPERATOR        1  <  (int8, int2),
	OPERATOR        2  <= (int8, int2),
	OPERATOR        3  =  (int8, int2),
	OPERATOR        4  >= (int8, int2),
	OPERATOR        5  >  (int8, int2),
	FUNCTION        1  (int8, int2) btint82cmp(int8, int2),
	OPERATOR        1  <  (int4, int8),
	OPERATOR        2  <= (int4, int8),
	OPERATOR        3  =  (int4, int8),
	OPERATOR        4  >= (int4, int8),
	OPERATOR        5  >  (int4, int8),
	FUNCTION        1  (int4, int8) btint48cmp(int4, int8),
	OPERATOR        1  <  (int8, int4),
	OPERATOR        2  <= (int8, int4),
	OPERATOR        3  =  (int8, int4),
	OPERATOR        4  >= (int8, int4),
	OPERATOR        5  >  (int8, int4),
	FUNCTION        1  (int8, int4) btint84cmp(int8, int4);


COMMENT ON ACCESS METHOD clustered_heap IS 'Clustered table access method with directed placement via zone map.';
COMMENT ON ACCESS METHOD clustered_pk_index IS 'Clustered index AM for key discovery (scan callbacks disabled; use btree for queries).';

CREATE FUNCTION @extschema@.sorted_heap_handler(internal)
RETURNS table_am_handler
AS '$libdir/clustered_pg', 'sorted_heap_tableam_handler'
LANGUAGE C STRICT;

CREATE ACCESS METHOD sorted_heap TYPE TABLE
	HANDLER @extschema@.sorted_heap_handler;

COMMENT ON ACCESS METHOD sorted_heap IS 'Sorted heap table access method with LSM-style tiered storage.';

CREATE FUNCTION @extschema@.sorted_heap_zonemap_stats(regclass)
RETURNS text
AS '$libdir/clustered_pg', 'sorted_heap_zonemap_stats'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.sorted_heap_compact(regclass)
RETURNS void
AS '$libdir/clustered_pg', 'sorted_heap_compact'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.sorted_heap_rebuild_zonemap(regclass)
RETURNS void
AS '$libdir/clustered_pg', 'sorted_heap_rebuild_zonemap_sql'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.sorted_heap_scan_stats()
RETURNS text
AS '$libdir/clustered_pg', 'sorted_heap_scan_stats'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.sorted_heap_reset_stats()
RETURNS void
AS '$libdir/clustered_pg', 'sorted_heap_reset_stats'
LANGUAGE C STRICT;

COMMENT ON EXTENSION clustered_pg IS 'Physically clustered storage via directed placement in table AM.';
