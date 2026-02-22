-- clustered_pg extension SQL

\echo Use "CREATE EXTENSION clustered_pg" to load this file.

CREATE DOMAIN @extschema@.clustered_locator AS bytea
	CHECK (octet_length(VALUE) = 16);

CREATE FUNCTION @extschema@.version()
RETURNS text
AS '$libdir/clustered_pg', 'clustered_pg_version'
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

CREATE OPERATOR CLASS @extschema@.clustered_pk_int2_ops
DEFAULT FOR TYPE int2 USING clustered_pk_index AS
	OPERATOR        1  <  (int2, int2),
	OPERATOR        2  <= (int2, int2),
	OPERATOR        3  =  (int2, int2),
	OPERATOR        4  >= (int2, int2),
	OPERATOR        5  >  (int2, int2),
	FUNCTION        1  btint2cmp(int2, int2);

CREATE OPERATOR CLASS @extschema@.clustered_pk_int4_ops
DEFAULT FOR TYPE int4 USING clustered_pk_index AS
	OPERATOR        1  <  (int4, int4),
	OPERATOR        2  <= (int4, int4),
	OPERATOR        3  =  (int4, int4),
	OPERATOR        4  >= (int4, int4),
	OPERATOR        5  >  (int4, int4),
	FUNCTION        1  btint4cmp(int4, int4);

CREATE OPERATOR CLASS @extschema@.clustered_pk_int8_ops
DEFAULT FOR TYPE int8 USING clustered_pk_index AS
	OPERATOR        1  <  (int8, int8),
	OPERATOR        2  <= (int8, int8),
	OPERATOR        3  =  (int8, int8),
	OPERATOR        4  >= (int8, int8),
	OPERATOR        5  >  (int8, int8),
	FUNCTION        1  btint8cmp(int8, int8);

CREATE TABLE @extschema@.segment_map
(
	relation_oid oid NOT NULL,
	major_key bigint NOT NULL,
	minor_from bigint NOT NULL,
	minor_to bigint NOT NULL,
	split_threshold int NOT NULL DEFAULT 128,
	target_fillfactor int NOT NULL DEFAULT 85,
	auto_repack_interval double precision NOT NULL DEFAULT 60.0,
	row_count bigint NOT NULL DEFAULT 0,
	split_generation bigint NOT NULL DEFAULT 0,
	last_split_at timestamptz,
	updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
	CONSTRAINT segment_map_pk PRIMARY KEY (relation_oid, major_key),
	CONSTRAINT segment_map_valid_range CHECK (minor_to >= minor_from)
);

CREATE INDEX segment_map_relation_idx
	ON @extschema@.segment_map (relation_oid);

CREATE INDEX segment_map_relation_range_idx
	ON @extschema@.segment_map (relation_oid, minor_from, minor_to);

CREATE FUNCTION @extschema@.segment_map_touch(
	p_relation_oid oid,
	p_major_key bigint,
	p_minor_from bigint,
	p_minor_to bigint,
	p_split_threshold int DEFAULT 128,
	p_target_fillfactor int DEFAULT 85,
	p_auto_repack_interval double precision DEFAULT 60.0
) RETURNS bigint
LANGUAGE sql
AS $$
	INSERT INTO @extschema@.segment_map (
		relation_oid, major_key, minor_from, minor_to,
		split_threshold, target_fillfactor, auto_repack_interval, updated_at
	)
	VALUES (
		p_relation_oid, p_major_key, p_minor_from, p_minor_to,
		p_split_threshold, p_target_fillfactor, p_auto_repack_interval, clock_timestamp()
	)
	ON CONFLICT (relation_oid, major_key)
	DO UPDATE
	SET minor_from = LEAST(@extschema@.segment_map.minor_from, EXCLUDED.minor_from),
		minor_to = GREATEST(@extschema@.segment_map.minor_to, EXCLUDED.minor_to),
		split_threshold = EXCLUDED.split_threshold,
		target_fillfactor = EXCLUDED.target_fillfactor,
		auto_repack_interval = EXCLUDED.auto_repack_interval,
		updated_at = clock_timestamp()
	RETURNING 1;
$$;

CREATE FUNCTION @extschema@.segment_map_allocate_locator(
	p_relation_oid oid,
	p_minor bigint,
	p_row_count_delta bigint DEFAULT 1,
	p_split_threshold int DEFAULT 128,
	p_target_fillfactor int DEFAULT 85,
	p_auto_repack_interval double precision DEFAULT 60.0
) RETURNS @extschema@.clustered_locator
LANGUAGE plpgsql
AS $$
DECLARE
	rel_oid oid := p_relation_oid;
	v_major bigint;
	v_split_threshold int;
	v_target_fillfactor int;
	v_auto_repack_interval double precision;
	v_effective_split_threshold int;
	v_effective_row_capacity int;
	v_last_major_key bigint;
	v_last_minor_to bigint;
	v_last_row_count bigint;
	v_last_split_threshold int;
	v_last_target_fillfactor int;
	v_head_major_key bigint;
	v_head_minor_from bigint;
	v_head_minor_to bigint;
	v_head_row_count bigint;
	v_head_split_threshold int;
	v_head_target_fillfactor int;
	v_container_major_key bigint;
	v_container_minor_from bigint;
	v_container_minor_to bigint;
	v_container_row_count bigint;
	v_container_split_threshold int;
	v_container_target_fillfactor int;
	v_prev_container_major_key bigint;
	v_next_major_key bigint;
	v_prev_container_found bool := false;
BEGIN
	IF p_minor IS NULL THEN
		RAISE EXCEPTION 'p_minor cannot be NULL';
	END IF;
	IF p_row_count_delta IS NULL OR p_row_count_delta < 1 THEN
		RAISE EXCEPTION 'p_row_count_delta must be > 0';
	END IF;
	IF p_target_fillfactor IS NOT NULL AND (p_target_fillfactor < 1 OR p_target_fillfactor > 100) THEN
		RAISE EXCEPTION 'p_target_fillfactor must be between 1 and 100';
	END IF;

	PERFORM pg_advisory_xact_lock(hashint8(rel_oid::bigint));

	v_split_threshold := COALESCE(p_split_threshold, 128);
	v_target_fillfactor := COALESCE(p_target_fillfactor, 85);
	v_auto_repack_interval := COALESCE(p_auto_repack_interval, 60.0);

	-- 1) Reuse an existing containing segment when it still has capacity.
	SELECT major_key, minor_from, minor_to, row_count, split_threshold, target_fillfactor
	INTO v_container_major_key, v_container_minor_from, v_container_minor_to,
		v_container_row_count, v_container_split_threshold, v_container_target_fillfactor
	FROM @extschema@.segment_map
	WHERE relation_oid = rel_oid
		AND p_minor BETWEEN minor_from AND minor_to
	ORDER BY major_key
	LIMIT 1;

	IF FOUND THEN
		v_effective_split_threshold := COALESCE(p_split_threshold, v_container_split_threshold);
		v_effective_row_capacity := GREATEST(
			1,
			(v_effective_split_threshold * LEAST(100, COALESCE(p_target_fillfactor, v_container_target_fillfactor, v_target_fillfactor))) / 100
		);
		IF v_container_row_count + p_row_count_delta <= v_effective_row_capacity THEN
			UPDATE @extschema@.segment_map
			SET minor_from = LEAST(minor_from, p_minor),
				minor_to = GREATEST(minor_to, p_minor),
				row_count = row_count + p_row_count_delta,
				split_threshold = COALESCE(p_split_threshold, split_threshold),
				target_fillfactor = COALESCE(p_target_fillfactor, target_fillfactor),
				auto_repack_interval = COALESCE(p_auto_repack_interval, auto_repack_interval),
				updated_at = clock_timestamp()
			WHERE relation_oid = rel_oid AND major_key = v_container_major_key;
			RETURN @extschema@.locator_pack(v_container_major_key, p_minor);
		END IF;
	END IF;

	-- 2) Choose major using boundaries and split policy for append cases.
	SELECT major_key, minor_to, row_count, split_threshold, target_fillfactor
	INTO v_last_major_key, v_last_minor_to, v_last_row_count, v_last_split_threshold, v_last_target_fillfactor
	FROM @extschema@.segment_map
	WHERE relation_oid = rel_oid
	ORDER BY major_key DESC
	LIMIT 1;

	IF NOT FOUND THEN
		v_major := 0;
	ELSE
		SELECT major_key, minor_from, minor_to, row_count, split_threshold, target_fillfactor
		INTO v_head_major_key, v_head_minor_from, v_head_minor_to, v_head_row_count, v_head_split_threshold, v_head_target_fillfactor
		FROM @extschema@.segment_map
		WHERE relation_oid = rel_oid
		ORDER BY major_key ASC
		LIMIT 1;

		IF p_minor < v_head_minor_from THEN
			v_effective_split_threshold := COALESCE(v_head_split_threshold, v_split_threshold);
			v_effective_row_capacity := GREATEST(
				1,
				(v_effective_split_threshold * LEAST(100, COALESCE(p_target_fillfactor, v_head_target_fillfactor, v_target_fillfactor))) / 100
			);
			IF v_head_row_count + p_row_count_delta <= v_effective_row_capacity THEN
				UPDATE @extschema@.segment_map
				SET minor_from = LEAST(minor_from, p_minor),
					row_count = row_count + p_row_count_delta,
					split_threshold = COALESCE(p_split_threshold, split_threshold),
					target_fillfactor = COALESCE(p_target_fillfactor, target_fillfactor),
					auto_repack_interval = COALESCE(p_auto_repack_interval, auto_repack_interval),
					updated_at = clock_timestamp()
				WHERE relation_oid = rel_oid AND major_key = v_head_major_key;
				RETURN @extschema@.locator_pack(v_head_major_key, p_minor);
			END IF;
			v_major := v_head_major_key - 1;
		ELSIF p_minor > v_last_minor_to THEN
			v_effective_split_threshold := COALESCE(v_last_split_threshold, v_split_threshold);
			v_effective_row_capacity := GREATEST(
				1,
				(v_effective_split_threshold * LEAST(100, COALESCE(p_target_fillfactor, v_last_target_fillfactor, v_target_fillfactor))) / 100
			);
			IF v_last_row_count + p_row_count_delta > v_effective_row_capacity THEN
				v_major := v_last_major_key + 1;
			ELSE
				v_major := v_last_major_key;
			END IF;
		ELSE
			-- backfill or gap: allocate segment after the last preceding segment.
			SELECT major_key
			INTO v_prev_container_major_key
			FROM @extschema@.segment_map
			WHERE relation_oid = rel_oid
				AND minor_to < p_minor
			ORDER BY major_key DESC
			LIMIT 1;
			v_prev_container_found := FOUND;
			SELECT major_key
			INTO v_next_major_key
			FROM @extschema@.segment_map
			WHERE relation_oid = rel_oid
				AND minor_from > p_minor
			ORDER BY minor_from, major_key
			LIMIT 1;

			IF v_prev_container_found THEN
				v_major := v_prev_container_major_key + 1;
			ELSE
				v_major := v_last_major_key - 1;
			END IF;

			-- Avoid collapsing back into an existing neighboring segment.
			IF v_container_major_key IS NOT NULL AND v_major = v_container_major_key THEN
				v_major := v_container_major_key + 1;
			END IF;
			IF v_next_major_key IS NOT NULL AND v_major = v_next_major_key THEN
				v_major := v_next_major_key + 1;
			END IF;
		END IF;
	END IF;

	INSERT INTO @extschema@.segment_map (
		relation_oid, major_key, minor_from, minor_to,
		split_threshold, target_fillfactor, auto_repack_interval,
		row_count, updated_at
	)
	VALUES (
		rel_oid, v_major, p_minor, p_minor,
		v_split_threshold, v_target_fillfactor, v_auto_repack_interval,
		p_row_count_delta, clock_timestamp()
	)
	ON CONFLICT (relation_oid, major_key)
	DO UPDATE
	SET minor_from = LEAST(@extschema@.segment_map.minor_from, EXCLUDED.minor_from),
		minor_to = GREATEST(@extschema@.segment_map.minor_to, EXCLUDED.minor_to),
		row_count = @extschema@.segment_map.row_count + EXCLUDED.row_count,
		split_threshold = EXCLUDED.split_threshold,
		target_fillfactor = EXCLUDED.target_fillfactor,
		auto_repack_interval = EXCLUDED.auto_repack_interval,
		updated_at = clock_timestamp();

	RETURN @extschema@.locator_pack(v_major, p_minor);
END;
$$;

CREATE FUNCTION @extschema@.segment_map_allocate_locator_regclass(
	p_relation regclass,
	p_minor bigint,
	p_row_count_delta bigint DEFAULT 1,
	p_split_threshold int DEFAULT 128,
	p_target_fillfactor int DEFAULT 85,
	p_auto_repack_interval double precision DEFAULT 60.0
) RETURNS @extschema@.clustered_locator
LANGUAGE SQL
AS $$
	SELECT @extschema@.segment_map_allocate_locator(
		p_relation::oid,
		p_minor,
		p_row_count_delta,
		p_split_threshold,
		p_target_fillfactor,
		p_auto_repack_interval
	);
$$;

CREATE FUNCTION @extschema@.segment_map_next_locator(
	p_relation regclass,
	p_minor bigint,
	p_row_count_delta bigint DEFAULT 1,
	p_split_threshold int DEFAULT 128,
	p_target_fillfactor int DEFAULT 85,
	p_auto_repack_interval double precision DEFAULT 60.0
) RETURNS @extschema@.clustered_locator
LANGUAGE SQL
AS $$
	SELECT @extschema@.segment_map_allocate_locator_regclass(
		p_relation,
		p_minor,
		p_row_count_delta,
		p_split_threshold,
		p_target_fillfactor,
		p_auto_repack_interval
	);
$$;

CREATE FUNCTION @extschema@.segment_map_rebuild_from_index(
	p_index_relation regclass,
	p_row_count_delta bigint DEFAULT 1,
	p_split_threshold int DEFAULT NULL,
	p_target_fillfactor int DEFAULT NULL,
	p_auto_repack_interval double precision DEFAULT NULL
) RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
	v_index_oid oid := p_index_relation::oid;
	v_relation_oid oid;
	v_index_attr_count int;
	v_index_attnum int;
	v_key_attr name;
	v_key_type oid;
	v_key_oid bigint;
	v_schema_name name;
	v_relation_name name;
	v_index_am_name name;
	v_previous_generation bigint;
	v_repacked_count bigint := 0;
	v_effective_split_threshold int;
	v_effective_target_fillfactor int;
	v_effective_auto_repack_interval double precision;
	v_row record;
	v_sql text;
BEGIN
	IF p_row_count_delta IS NULL OR p_row_count_delta < 1 THEN
		RAISE EXCEPTION 'p_row_count_delta must be greater than 0';
	END IF;
	IF p_split_threshold IS NOT NULL AND p_split_threshold < 1 THEN
		RAISE EXCEPTION 'p_split_threshold must be greater than 0';
	END IF;
	IF p_target_fillfactor IS NOT NULL AND (p_target_fillfactor < 1 OR p_target_fillfactor > 100) THEN
		RAISE EXCEPTION 'p_target_fillfactor must be between 1 and 100';
	END IF;
	IF p_auto_repack_interval IS NOT NULL AND p_auto_repack_interval < 0.0 THEN
		RAISE EXCEPTION 'p_auto_repack_interval must be greater than or equal to 0';
	END IF;

	SELECT i.indrelid, i.indnatts
	INTO v_relation_oid, v_index_attr_count
	FROM pg_index i
	WHERE i.indexrelid = v_index_oid
		AND i.indisvalid
		AND i.indisready;

	IF NOT FOUND THEN
		RAISE EXCEPTION 'segment_map_rebuild_from_index expects an index relation OID';
	END IF;

	IF v_index_attr_count != 1 THEN
		RAISE EXCEPTION 'segment_map_rebuild_from_index supports single-column indexes only';
	END IF;

	SELECT a.amname
	INTO v_index_am_name
	FROM pg_class c
	JOIN pg_am a ON a.oid = c.relam
	WHERE c.oid = v_index_oid;

	IF v_index_am_name IS DISTINCT FROM 'clustered_pk_index' THEN
		RAISE EXCEPTION 'segment_map_rebuild_from_index requires clustered_pk_index access method';
	END IF;

	SELECT k.attnum
	INTO v_index_attnum
	FROM pg_index i
	CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, attnum_order)
	WHERE i.indexrelid = v_index_oid
	  AND k.attnum_order = 1;

	SELECT a.attname, a.atttypid
	INTO v_key_attr, v_key_type
	FROM pg_attribute a
	WHERE a.attrelid = v_relation_oid
	  AND a.attnum = v_index_attnum;

	IF NOT FOUND THEN
		RAISE EXCEPTION 'invalid index attribute reference for index %', p_index_relation;
	END IF;

	IF v_key_type NOT IN ('int2'::regtype::oid, 'int4'::regtype::oid, 'int8'::regtype::oid) THEN
		RAISE EXCEPTION 'segment_map_rebuild_from_index currently supports only int2/int4/int8 key types';
	END IF;

	SELECT n.nspname, c.relname
	INTO v_schema_name, v_relation_name
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.oid = v_relation_oid;

	IF v_schema_name IS NULL OR v_relation_name IS NULL THEN
		RAISE EXCEPTION 'relation metadata not found for index %', p_index_relation;
	END IF;

	/*
	 * Preserve previous maintenance metadata when explicit overrides are omitted.
	 */
	v_previous_generation := COALESCE(
		(SELECT max(split_generation) FROM @extschema@.segment_map WHERE relation_oid = v_relation_oid),
		0
	) + 1;

	v_effective_split_threshold := COALESCE(
		p_split_threshold,
		(SELECT split_threshold FROM @extschema@.segment_map WHERE relation_oid = v_relation_oid ORDER BY major_key LIMIT 1),
		128
	);
	v_effective_target_fillfactor := COALESCE(
		p_target_fillfactor,
		(SELECT target_fillfactor FROM @extschema@.segment_map WHERE relation_oid = v_relation_oid ORDER BY major_key LIMIT 1),
		85
	);
	v_effective_auto_repack_interval := COALESCE(
		p_auto_repack_interval,
		(SELECT auto_repack_interval FROM @extschema@.segment_map WHERE relation_oid = v_relation_oid ORDER BY major_key LIMIT 1),
		60.0
	);

	PERFORM pg_advisory_xact_lock(hashint8(v_relation_oid::bigint));
	EXECUTE format('LOCK TABLE %I.%I IN SHARE UPDATE EXCLUSIVE MODE', v_schema_name, v_relation_name);

	DELETE FROM @extschema@.segment_map
	WHERE relation_oid = v_relation_oid;

	v_sql := format(
		'SELECT %I::bigint AS major_value FROM %I.%I ORDER BY %I::bigint',
		v_key_attr,
		v_schema_name,
		v_relation_name,
		v_key_attr
	);
	FOR v_row IN EXECUTE v_sql
	LOOP
		v_key_oid := v_row.major_value;
		PERFORM @extschema@.segment_map_allocate_locator(
			v_relation_oid,
			v_key_oid,
			p_row_count_delta,
			v_effective_split_threshold,
			v_effective_target_fillfactor,
			v_effective_auto_repack_interval
		);
		v_repacked_count := v_repacked_count + 1;
	END LOOP;

	UPDATE @extschema@.segment_map
	SET split_generation = v_previous_generation,
		last_split_at = clock_timestamp(),
		updated_at = clock_timestamp(),
		split_threshold = v_effective_split_threshold,
		target_fillfactor = v_effective_target_fillfactor,
		auto_repack_interval = v_effective_auto_repack_interval
	WHERE relation_oid = v_relation_oid;

	RETURN v_repacked_count;
END;
$$;

CREATE FUNCTION @extschema@.segment_map_stats(p_relation_oid oid)
RETURNS TABLE(
	major_key bigint,
	minor_from bigint,
	minor_to bigint,
	row_count bigint,
	split_generation bigint
)
LANGUAGE sql
AS $$
	SELECT major_key, minor_from, minor_to, row_count, split_generation
	FROM @extschema@.segment_map
	WHERE relation_oid = p_relation_oid
	ORDER BY major_key;
$$;

CREATE FUNCTION @extschema@.segment_map_count_repack_due(
	p_relation_oid oid,
	p_auto_repack_interval double precision DEFAULT 60.0
) RETURNS bigint
LANGUAGE sql
AS $$
	SELECT count(*)::bigint
	FROM @extschema@.segment_map
	WHERE relation_oid = p_relation_oid
		AND row_count >= GREATEST(
			1,
			(split_threshold * LEAST(100, GREATEST(1, target_fillfactor))) / 100
		)
		AND (
			last_split_at IS NULL
			OR last_split_at <= clock_timestamp() - (
				interval '1 second' * GREATEST(0.0, p_auto_repack_interval::numeric)
			)
		);
$$;

COMMENT ON ACCESS METHOD clustered_heap IS 'Prototype clustered table access method (delegates to heap semantics in v0.1.0).';
COMMENT ON ACCESS METHOD clustered_pk_index IS 'Prototype clustered logical index AM with active index-insert and scan callbacks for constrained predicate evaluation.';
COMMENT ON EXTENSION clustered_pg IS 'Experimental clustered storage and index access method scaffold.';
