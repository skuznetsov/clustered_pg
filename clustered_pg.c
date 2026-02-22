#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/reloptions.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "commands/vacuum.h"
#include "catalog/index.h"
#include "nodes/execnodes.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "nodes/tidbitmap.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "storage/itemptr.h"
#include <inttypes.h>
#include <string.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(clustered_pg_version);
PG_FUNCTION_INFO_V1(clustered_pg_tableam_handler);
PG_FUNCTION_INFO_V1(clustered_pg_pkidx_handler);
PG_FUNCTION_INFO_V1(clustered_pg_locator_pack);
PG_FUNCTION_INFO_V1(clustered_pg_locator_major);
PG_FUNCTION_INFO_V1(clustered_pg_locator_minor);
PG_FUNCTION_INFO_V1(clustered_pg_locator_to_hex);
PG_FUNCTION_INFO_V1(clustered_pg_locator_pack_int8);
PG_FUNCTION_INFO_V1(clustered_pg_locator_cmp);
PG_FUNCTION_INFO_V1(clustered_pg_locator_advance_major);
PG_FUNCTION_INFO_V1(clustered_pg_locator_next_minor);

typedef struct ClusteredPgIndexOptions
{
	int32		vl_len_;
	int		split_threshold;
	int		target_fillfactor;
	double		auto_repack_interval;
} ClusteredPgIndexOptions;

typedef struct ClusteredPgPkidxIndexOptionsCache
{
	uint32		magic;
	int		split_threshold;
	int		target_fillfactor;
	double		auto_repack_interval;
} ClusteredPgPkidxIndexOptionsCache;

#define CLUSTERED_PG_PKIDX_OPTIONS_MAGIC 0x634F5047

typedef struct ClusteredLocator
{
	uint64		major_key;
	uint64		minor_key;
} ClusteredLocator;

static bool			clustered_pg_clustered_heapam_initialized = false;
static TableAmRoutine clustered_pg_clustered_heapam_routine;

typedef struct ClusteredPgPkidxBuildState
{
	Relation	heapRelation;
	IndexInfo  *indexInfo;
	int64		index_tuples;
} ClusteredPgPkidxBuildState;

typedef struct ClusteredPgPkidxScanState
{
	TableScanDesc	table_scan;
	ScanKeyData		*table_scan_keys;
	TupleTableSlot *table_scan_slot;
	int				key_count;
	bool			owns_heap_relation;
	bool			mark_valid;
	bool			mark_at_start;
	bool			restore_pending;
	ItemPointerData	mark_tid;
} ClusteredPgPkidxScanState;

static SPIPlanPtr clustered_pg_pkidx_allocate_locator_plan = NULL;
static SPIPlanPtr clustered_pg_pkidx_count_repack_due_plan = NULL;
static SPIPlanPtr clustered_pg_pkidx_rebuild_segment_map_plan = NULL;

static bool clustered_pg_pkidx_insert(Relation indexRelation, Datum *values,
									 bool *isnull, ItemPointer heap_tid,
									 Relation heapRelation,
									 IndexUniqueCheck checkUnique,
									 bool indexUnchanged, IndexInfo *indexInfo);
static Relation clustered_pg_pkidx_get_heap_relation(IndexScanDesc scan,
													ClusteredPgPkidxScanState *state);
static bool clustered_pg_pkidx_restore_marked_tuple(IndexScanDesc scan,
												   ClusteredPgPkidxScanState *state,
												   ScanDirection direction);
static void clustered_pg_pkidx_reset_mark(ClusteredPgPkidxScanState *state);
static void clustered_pg_pkidx_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
									ScanKey orderbys, int norderbys);
static void clustered_pg_pkidx_rescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys,
									ScanKey orderbys, int norderbys,
									bool preserve_mark);
static void clustered_pg_clustered_heap_relation_set_new_filelocator(Relation rel,
																	const RelFileLocator *rlocator,
																	char persistence,
																	TransactionId *freezeXid,
																	MultiXactId *minmulti);
static void clustered_pg_clustered_heap_relation_nontransactional_truncate(Relation rel);
static void clustered_pg_clustered_heap_clear_segment_map(Oid relationOid);
static void clustered_pg_clustered_heap_init_tableam_routine(void);

static const char *
clustered_pg_qualified_extension_name(const char *name)
{
	Oid			extOid;
	Oid			nsOid;
	const char *nsName;
	static char result[512];

	extOid = get_extension_oid("clustered_pg", true);
	if (!OidIsValid(extOid))
		return quote_identifier(name);

	nsOid = get_extension_schema(extOid);
	nsName = get_namespace_name(nsOid);
	if (nsName == NULL)
		return quote_identifier(name);

	snprintf(result, sizeof(result), "%s.%s",
			 quote_identifier(nsName), quote_identifier(name));
	return result;
}

static void
clustered_pg_clustered_heap_clear_segment_map(Oid relationOid)
{
	char		sql[256];
	Datum		args[1];
	Oid			argtypes[1];
	int			rc;

	if (!OidIsValid(relationOid))
		return;

	args[0] = ObjectIdGetDatum(relationOid);
	argtypes[0] = OIDOID;

	snprintf(sql, sizeof(sql),
			 "DELETE FROM %s WHERE relation_oid = $1::oid",
			 clustered_pg_qualified_extension_name("segment_map"));

	rc = SPI_connect();
	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("SPI_connect() failed while cleaning clustered_pg segment map")));

	PG_TRY();
	{
		rc = SPI_execute_with_args(sql, 1, argtypes, args, NULL, false, 0);
		if (rc != SPI_OK_DELETE)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("clustered_pg segment_map cleanup failed"),
					 errdetail("SPI status code %d", rc)));
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();
}

static void
clustered_pg_clustered_heap_relation_set_new_filelocator(Relation rel,
														const RelFileLocator *rlocator,
														char persistence,
														TransactionId *freezeXid,
														MultiXactId *minmulti)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	if (heap == NULL || heap->relation_set_new_filelocator == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	heap->relation_set_new_filelocator(rel, rlocator, persistence, freezeXid, minmulti);

	clustered_pg_clustered_heap_clear_segment_map(RelationGetRelid(rel));
}

static void
clustered_pg_clustered_heap_relation_nontransactional_truncate(Relation rel)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	if (heap == NULL || heap->relation_nontransactional_truncate == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	heap->relation_nontransactional_truncate(rel);
	clustered_pg_clustered_heap_clear_segment_map(RelationGetRelid(rel));
}

static void
clustered_pg_clustered_heap_init_tableam_routine(void)
{
	const TableAmRoutine *heap;

	if (clustered_pg_clustered_heapam_initialized)
		return;

	heap = GetHeapamTableAmRoutine();
	if (heap == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	clustered_pg_clustered_heapam_routine = *heap;
	clustered_pg_clustered_heapam_routine.type = T_TableAmRoutine;
	clustered_pg_clustered_heapam_routine.relation_set_new_filelocator =
		clustered_pg_clustered_heap_relation_set_new_filelocator;
	clustered_pg_clustered_heapam_routine.relation_nontransactional_truncate =
		clustered_pg_clustered_heap_relation_nontransactional_truncate;
	clustered_pg_clustered_heapam_initialized = true;
}

static SPIPlanPtr
clustered_pg_pkidx_allocate_locator_plan_init(void)
{
	Oid			argtypes[6];
	char		query[256];

	if (clustered_pg_pkidx_allocate_locator_plan != NULL)
		return clustered_pg_pkidx_allocate_locator_plan;

	argtypes[0] = OIDOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT8OID;
	argtypes[3] = INT4OID;
	argtypes[4] = INT4OID;
	argtypes[5] = FLOAT8OID;

	snprintf(query, sizeof(query),
			 "SELECT %s($1::oid, $2::bigint, $3::bigint, $4::integer, $5::integer, $6::double precision)",
			 clustered_pg_qualified_extension_name("segment_map_allocate_locator"));

	clustered_pg_pkidx_allocate_locator_plan = SPI_prepare(query, 6, argtypes);
	if (clustered_pg_pkidx_allocate_locator_plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to prepare clustered_pg_sql plan for segment_map_allocate_locator"),
				 errhint("Inspect clustered_pg extension schema and function visibility.")));
	SPI_keepplan(clustered_pg_pkidx_allocate_locator_plan);

	return clustered_pg_pkidx_allocate_locator_plan;
}

static SPIPlanPtr
clustered_pg_pkidx_count_repack_due_plan_init(void)
{
	Oid			argtypes[2];
	char		query[256];

	if (clustered_pg_pkidx_count_repack_due_plan != NULL)
		return clustered_pg_pkidx_count_repack_due_plan;

	argtypes[0] = OIDOID;
	argtypes[1] = FLOAT8OID;

	snprintf(query, sizeof(query),
			 "SELECT %s($1::oid, $2::double precision)",
			 clustered_pg_qualified_extension_name("segment_map_count_repack_due"));

	clustered_pg_pkidx_count_repack_due_plan = SPI_prepare(query, 2, argtypes);
	if (clustered_pg_pkidx_count_repack_due_plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to prepare clustered_pg_sql plan for segment_map_count_repack_due"),
				 errhint("Inspect clustered_pg extension schema and function visibility.")));
	SPI_keepplan(clustered_pg_pkidx_count_repack_due_plan);

	return clustered_pg_pkidx_count_repack_due_plan;
}

static SPIPlanPtr
clustered_pg_pkidx_rebuild_segment_map_plan_init(void)
{
	Oid			argtypes[5];
	char		query[256];

	if (clustered_pg_pkidx_rebuild_segment_map_plan != NULL)
		return clustered_pg_pkidx_rebuild_segment_map_plan;

	argtypes[0] = REGCLASSOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT4OID;
	argtypes[3] = INT4OID;
	argtypes[4] = FLOAT8OID;

	snprintf(query, sizeof(query),
			 "SELECT %s($1::regclass, $2::bigint, $3::integer, $4::integer, $5::double precision)",
			 clustered_pg_qualified_extension_name("segment_map_rebuild_from_index"));

	clustered_pg_pkidx_rebuild_segment_map_plan = SPI_prepare(query, 5, argtypes);
	if (clustered_pg_pkidx_rebuild_segment_map_plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to prepare clustered_pg_sql plan for segment_map_rebuild_from_index"),
				 errhint("Inspect clustered_pg extension schema and function visibility.")));
	SPI_keepplan(clustered_pg_pkidx_rebuild_segment_map_plan);

	return clustered_pg_pkidx_rebuild_segment_map_plan;
}

static void
clustered_pg_pkidx_execute_segment_map_maintenance(Relation indexRelation,
												  const char *sql)
{
	int			rc;
	Oid			relationOid = InvalidOid;
	Datum		args[1];
	Oid			argtypes[1];

	if (indexRelation == NULL || indexRelation->rd_index == NULL)
		return;
	if (sql == NULL || sql[0] == '\0')
		return;

	relationOid = indexRelation->rd_index->indrelid;
	if (!OidIsValid(relationOid))
		return;

	args[0] = ObjectIdGetDatum(relationOid);
	argtypes[0] = OIDOID;

	rc = SPI_connect();
	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("SPI_connect() failed in clustered_pk_index maintenance callback")));

	PG_TRY();
	{
		rc = SPI_execute_with_args(sql,
								   1,
								   argtypes,
								   args,
								   NULL,
								   false,
								   0);
		if (rc != SPI_OK_DELETE && rc != SPI_OK_UPDATE)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("clustered_pk_index segment_map maintenance failed"),
					 errdetail("SPI query failed with status %d", rc)));
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();
}

static void
clustered_pg_pkidx_purge_segment_map(Relation indexRelation)
{
	char		sql[256];

	snprintf(sql, sizeof(sql),
			 "DELETE FROM %s WHERE relation_oid = $1::oid",
			 clustered_pg_qualified_extension_name("segment_map"));

	clustered_pg_pkidx_execute_segment_map_maintenance(indexRelation, sql);
}

static void
clustered_pg_pkidx_touch_repack(Relation indexRelation)
{
	char		sql[512];

	snprintf(sql, sizeof(sql),
			 "UPDATE %s "
		"SET split_generation = split_generation + 1, "
		"last_split_at = clock_timestamp(), "
		"updated_at = clock_timestamp() "
		"WHERE relation_oid = $1::oid",
			 clustered_pg_qualified_extension_name("segment_map"));

	clustered_pg_pkidx_execute_segment_map_maintenance(indexRelation, sql);
}

static int64
clustered_pg_pkidx_count_repack_due(Relation indexRelation,
								   double auto_repack_interval)
{
	Datum		args[2];
	Oid			relationOid = InvalidOid;
	bool		isnull = false;
	SPIPlanPtr	plan;
	int			rc;
	int64		result = 0;

	if (indexRelation == NULL || indexRelation->rd_index == NULL)
		return 0;

	relationOid = indexRelation->rd_index->indrelid;
	if (!OidIsValid(relationOid))
		return 0;

	args[0] = ObjectIdGetDatum(relationOid);
	args[1] = Float8GetDatum(auto_repack_interval);

	rc = SPI_connect();
	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("SPI_connect() failed in clustered_pk_index maintenance callback")));
	plan = clustered_pg_pkidx_count_repack_due_plan_init();
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to access SPI plan for segment_map_count_repack_due")));

	PG_TRY();
	{
		rc = SPI_execute_plan(plan, args, NULL, false, 0);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment map helper query failed"),
					 errdetail("SPI_execute_plan returned %d", rc)));

		if (SPI_processed != 1)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment map helper query returned unexpected row count"),
					 errdetail("Expected 1 row, got %" PRIu64, (uint64) SPI_processed)));

		result = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc,
											1,
											&isnull));
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment map helper query returned NULL")));
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();
	return result;
}

static void
clustered_pg_pkidx_rebuild_segment_map(Relation indexRelation,
									  int split_threshold,
									  int target_fillfactor,
									  double auto_repack_interval)
{
	Datum		args[5];
	SPIPlanPtr	plan;
	int			rc;
	int64		repacked_rows;
	bool		isnull = false;

	if (indexRelation == NULL || indexRelation->rd_index == NULL)
		return;

	args[0] = ObjectIdGetDatum(RelationGetRelid(indexRelation));
	args[1] = Int64GetDatum(1);
	args[2] = Int32GetDatum(split_threshold);
	args[3] = Int32GetDatum(target_fillfactor);
	args[4] = Float8GetDatum(auto_repack_interval);

	rc = SPI_connect();
	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("SPI_connect() failed in clustered_pk_index maintenance callback")));
	plan = clustered_pg_pkidx_rebuild_segment_map_plan_init();
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to access SPI plan for segment_map_rebuild_from_index")));

	PG_TRY();
	{
		rc = SPI_execute_plan(plan, args, NULL, false, 0);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment map helper query failed"),
					 errdetail("SPI_execute_plan returned %d", rc)));

		if (SPI_processed != 1)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment map helper query returned unexpected row count"),
					 errdetail("Expected 1 row, got %" PRIu64, (uint64) SPI_processed)));

		repacked_rows = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												   SPI_tuptable->tupdesc,
												   1,
												   &isnull));
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment map helper query returned NULL")));
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();

	(void) repacked_rows;
}

/*
 * Segment-mapping contour (v0.2 draft):
 *
 * - major_key: logical segment identifier for ordered locality.
 * - segment meta (future): hi_bound / low_bound, block span, density, rebalance hints.
 * - insert flow (future): select target major based on key order then pack(major, minor).
 * - split flow (future): split full segment -> allocate next major_key, rewrite affected range.
 *
 * This file intentionally keeps these as comments until storage layout is committed
 * to avoid forcing catalog/binary compatibility too early.
 */

static bool
clustered_pg_pkidx_int_key_to_int64(Datum value, Oid valueType, int64 *minor_key)
{
	switch (valueType)
	{
		case INT2OID:
			*minor_key = (int64) DatumGetInt16(value);
			return true;
		case INT4OID:
			*minor_key = (int64) DatumGetInt32(value);
			return true;
		case INT8OID:
			*minor_key = DatumGetInt64(value);
			return true;
		default:
			return false;
	}
}

static bool
clustered_pg_pkidx_extract_minor_key(Relation indexRelation, Datum *values,
									bool *isnull, int64 *minor_key)
{
	TupleDesc	tupdesc;

	if (values == NULL || isnull == NULL || minor_key == NULL)
		return false;
	if (indexRelation == NULL)
		return false;

	tupdesc = RelationGetDescr(indexRelation);
	if (tupdesc == NULL || tupdesc->natts == 0)
		return false;

	if (isnull[0])
		return false;

	return clustered_pg_pkidx_int_key_to_int64(values[0],
											  TupleDescAttr(tupdesc, 0)->atttypid,
											  minor_key);
}

static IndexBulkDeleteResult *
clustered_pg_pkidx_init_bulkdelete_stats(IndexVacuumInfo *info,
										IndexBulkDeleteResult *stats)
{
	IndexBulkDeleteResult *result = stats;

	if (result == NULL)
		result = palloc0_object(IndexBulkDeleteResult);

	if (info != NULL)
	{
		result->estimated_count = info->estimated_count;
		if (info->estimated_count)
			result->num_index_tuples = info->num_heap_tuples;
	}

	return result;
}

static void
clustered_pg_pkidx_allocate_locator(Relation heapRelation, int64 minor_key,
								   int split_threshold, int target_fillfactor,
								   double auto_repack_interval)
{
	int			rc;
	Datum		args[6];
	SPIPlanPtr	plan;

	if (heapRelation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_pk_index requires a valid heap relation for insertion"),
				 errhint("Create and populate indexes on a real heap table relation.")));

	args[0] = ObjectIdGetDatum(RelationGetRelid(heapRelation));
	args[1] = Int64GetDatum(minor_key);
	args[2] = Int64GetDatum(1);
	args[3] = Int32GetDatum(split_threshold);
	args[4] = Int32GetDatum(target_fillfactor);
	args[5] = Float8GetDatum(auto_repack_interval);

	rc = SPI_connect();
	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("SPI_connect() failed in clustered_pk_index insert path")));
	plan = clustered_pg_pkidx_allocate_locator_plan_init();
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to access SPI plan for segment_map_allocate_locator")));

	PG_TRY();
	{
		rc = SPI_execute_plan(plan, args, NULL, false, 0);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment_map_allocate_locator call failed"),
					 errdetail("SPI_execute_plan returned %d", rc)));
		if (SPI_processed != 1)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment_map_allocate_locator returned unexpected row count"),
					 errdetail("Expected 1 row, got %" PRIu64, (uint64) SPI_processed)));
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();
}

static void
clustered_pg_pkidx_build_callback(Relation indexRelation, ItemPointer heap_tid,
                                 Datum *values, bool *isnull, bool tupleIsAlive,
                                 void *state)
{
	ClusteredPgPkidxBuildState *buildstate = (ClusteredPgPkidxBuildState *) state;
	int64		minor_key = 0;

	if (buildstate == NULL || indexRelation == NULL || buildstate->indexInfo == NULL)
		return;
	if (!tupleIsAlive)
		return;
	if (!clustered_pg_pkidx_extract_minor_key(indexRelation, values, isnull, &minor_key))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("clustered_pg build path does not support this index key"),
				 errhint("clustered_pk_index supports exactly one key attribute of types int2, int4 or int8.")));
	buildstate->index_tuples++;
}


static relopt_parse_elt clustered_pg_pkidx_relopt_tab[3];
static relopt_kind clustered_pg_pkidx_relopt_kind;

static relopt_kind
clustered_pg_index_relopt_kind_init(void)
{
	if (clustered_pg_pkidx_relopt_kind == InvalidOid)
		clustered_pg_pkidx_relopt_kind = add_reloption_kind();

	return clustered_pg_pkidx_relopt_kind;
}

static void
clustered_pg_pkidx_init_reloptions(void)
{
	int i = 0;

	clustered_pg_index_relopt_kind_init();

	add_int_reloption(clustered_pg_pkidx_relopt_kind,
					"split_threshold",
					"Estimated split trigger (tuples per segment) for clustered index AM",
					128, 16, 8192,
					AccessExclusiveLock);
	clustered_pg_pkidx_relopt_tab[i].optname = "split_threshold";
	clustered_pg_pkidx_relopt_tab[i].opttype = RELOPT_TYPE_INT;
	clustered_pg_pkidx_relopt_tab[i].offset = offsetof(ClusteredPgIndexOptions, split_threshold);
	i++;

	add_int_reloption(clustered_pg_pkidx_relopt_kind,
				"target_fillfactor",
				"Target tuple density for initial split behavior",
				85, 20, 100,
				AccessExclusiveLock);
	clustered_pg_pkidx_relopt_tab[i].optname = "target_fillfactor";
	clustered_pg_pkidx_relopt_tab[i].opttype = RELOPT_TYPE_INT;
	clustered_pg_pkidx_relopt_tab[i].offset = offsetof(ClusteredPgIndexOptions, target_fillfactor);
	i++;

	add_real_reloption(clustered_pg_pkidx_relopt_kind,
				"auto_repack_interval",
				"Repack cadence hint for cluster maintenance loop",
				60.0, 1.0, 3600.0,
				AccessExclusiveLock);
	clustered_pg_pkidx_relopt_tab[i].optname = "auto_repack_interval";
	clustered_pg_pkidx_relopt_tab[i].opttype = RELOPT_TYPE_REAL;
	clustered_pg_pkidx_relopt_tab[i].offset = offsetof(ClusteredPgIndexOptions, auto_repack_interval);
}

static void
clustered_pg_pkidx_get_index_options(Relation indexRelation,
									int *split_threshold,
									int *target_fillfactor,
									double *auto_repack_interval)
{
	ClusteredPgPkidxIndexOptionsCache *cache = NULL;
	ClusteredPgIndexOptions defaults = {
		.split_threshold = 128,
		.target_fillfactor = 85,
		.auto_repack_interval = 60.0,
	};
	ClusteredPgIndexOptions *parsed;

	if (split_threshold != NULL)
		*split_threshold = defaults.split_threshold;
	if (target_fillfactor != NULL)
		*target_fillfactor = defaults.target_fillfactor;
	if (auto_repack_interval != NULL)
		*auto_repack_interval = defaults.auto_repack_interval;

	if (indexRelation == NULL || indexRelation->rd_options == NULL)
		return;

	if (indexRelation->rd_amcache != NULL)
	{
		cache = (ClusteredPgPkidxIndexOptionsCache *) indexRelation->rd_amcache;
		if (cache->magic == CLUSTERED_PG_PKIDX_OPTIONS_MAGIC)
		{
			if (split_threshold != NULL)
				*split_threshold = cache->split_threshold;
			if (target_fillfactor != NULL)
				*target_fillfactor = cache->target_fillfactor;
			if (auto_repack_interval != NULL)
				*auto_repack_interval = cache->auto_repack_interval;
			return;
		}
	}

	if (VARSIZE_ANY(indexRelation->rd_options) >= sizeof(ClusteredPgIndexOptions))
	{
		parsed = (ClusteredPgIndexOptions *) indexRelation->rd_options;
	}
	else
	{
		Datum reloptions;

		clustered_pg_index_relopt_kind_init();

		reloptions = PointerGetDatum(indexRelation->rd_options);
		parsed = (ClusteredPgIndexOptions *) build_reloptions(reloptions,
															 false,
															 clustered_pg_pkidx_relopt_kind,
															 sizeof(ClusteredPgIndexOptions),
															 clustered_pg_pkidx_relopt_tab,
															 3);
	}

	if (parsed != NULL && split_threshold != NULL)
		*split_threshold = parsed->split_threshold;
	if (parsed != NULL && target_fillfactor != NULL)
		*target_fillfactor = parsed->target_fillfactor;
	if (parsed != NULL && auto_repack_interval != NULL)
		*auto_repack_interval = parsed->auto_repack_interval;

	if (parsed != NULL && indexRelation->rd_amcache == NULL &&
		indexRelation->rd_indexcxt != NULL)
	{
		cache = MemoryContextAlloc(indexRelation->rd_indexcxt,
								  sizeof(ClusteredPgPkidxIndexOptionsCache));
		cache->magic = CLUSTERED_PG_PKIDX_OPTIONS_MAGIC;
		cache->split_threshold = parsed->split_threshold;
		cache->target_fillfactor = parsed->target_fillfactor;
		cache->auto_repack_interval = parsed->auto_repack_interval;
		indexRelation->rd_amcache = cache;
	}

	if (VARSIZE_ANY(indexRelation->rd_options) < sizeof(ClusteredPgIndexOptions) && parsed != NULL)
		pfree(parsed);
}

static void
clustered_pg_pack_u64_be(uint8_t *dst, uint64 src)
{
	int			i;

	for (i = 0; i < 8; i++)
		dst[i] = (uint8_t) (src >> (56 - (i * 8)));
}

static uint64
clustered_pg_unpack_u64_be(const uint8_t *src)
{
	uint64		result = 0;
	int			i;

	for (i = 0; i < 8; i++)
		result = (result << 8) | (uint64) src[i];

	return result;
}

static int
clustered_pg_pkidx_gettreeheight(Relation rel)
{
	(void) rel;
	return 1;
}

static bool
clustered_pg_pkidx_canreturn(Relation indexRelation, int attno)
{
	(void)indexRelation;
	(void)attno;
	return false;
}

static Relation
clustered_pg_pkidx_get_heap_relation(IndexScanDesc scan,
									ClusteredPgPkidxScanState *state)
{
	Relation	heapRelation;

	if (scan == NULL)
		return NULL;
	if (scan->heapRelation != NULL)
		return scan->heapRelation;

	if (scan->indexRelation == NULL || scan->indexRelation->rd_index == NULL)
		return NULL;

	heapRelation = table_open(scan->indexRelation->rd_index->indrelid, NoLock);
	scan->heapRelation = heapRelation;

	if (state != NULL)
		state->owns_heap_relation = true;

	return heapRelation;
}

static bool
clustered_pg_pkidx_restore_marked_tuple(IndexScanDesc scan,
										ClusteredPgPkidxScanState *state,
										ScanDirection direction)
{
	if (scan == NULL || scan->opaque == NULL || state == NULL)
		return false;

	if (state->table_scan == NULL)
		return false;
	if (state->table_scan_slot == NULL)
		return false;

	if (!state->mark_valid)
		return false;

	if (state->mark_at_start)
	{
		if (table_scan_getnextslot(state->table_scan, direction,
								  state->table_scan_slot))
		{
			ItemPointerCopy(&state->table_scan_slot->tts_tid, &scan->xs_heaptid);
			scan->xs_recheck = false;
			return true;
		}

		return false;
	}

	if (!ItemPointerIsValid(&state->mark_tid))
		return false;

	while (table_scan_getnextslot(state->table_scan, direction,
								 state->table_scan_slot))
	{
		if (ItemPointerEquals(&state->table_scan_slot->tts_tid,
							  &state->mark_tid))
		{
			ItemPointerCopy(&state->table_scan_slot->tts_tid, &scan->xs_heaptid);
			scan->xs_recheck = false;
			return true;
		}
	}

	return false;
}

static void
clustered_pg_pkidx_reset_mark(ClusteredPgPkidxScanState *state)
{
	if (state == NULL)
		return;

	state->mark_valid = false;
	state->mark_at_start = false;
	state->restore_pending = false;
	ItemPointerSetInvalid(&state->mark_tid);
}

static bool
clustered_pg_pkidx_gettuple(IndexScanDesc scan, ScanDirection direction)
{
	ClusteredPgPkidxScanState *state;
	Relation	heapRelation;

	if (scan == NULL || scan->indexRelation == NULL)
		return false;

	state = (ClusteredPgPkidxScanState *) scan->opaque;
	if (state == NULL)
		ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("clustered_pk_index scan state is not initialized")));

	if (state->table_scan == NULL)
	{
		heapRelation = clustered_pg_pkidx_get_heap_relation(scan, state);
		if (heapRelation == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("clustered_pk_index cannot resolve heap relation for index scan")));

		clustered_pg_pkidx_rescan_internal(scan, scan->keyData, scan->numberOfKeys,
										  scan->orderByData, scan->numberOfOrderBys,
										  true);
	}
	else if (state->restore_pending)
	{
		if (clustered_pg_pkidx_restore_marked_tuple(scan, state, direction))
		{
			state->restore_pending = false;
			return true;
		}

		state->restore_pending = false;
		return false;
	}

	if (table_scan_getnextslot(state->table_scan, direction, state->table_scan_slot))
	{
		ItemPointerCopy(&state->table_scan_slot->tts_tid, &scan->xs_heaptid);
		scan->xs_recheck = false;
		return true;
	}

	return false;
}

static int64
clustered_pg_pkidx_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	ClusteredPgPkidxScanState *state;
	int64		rows = 0;
	Relation	heapRelation;

	if (scan == NULL || scan->indexRelation == NULL || tbm == NULL)
		return 0;

	state = (ClusteredPgPkidxScanState *) scan->opaque;
	if (state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("clustered_pk_index scan state is not initialized")));

	if (state->table_scan == NULL)
	{
		heapRelation = clustered_pg_pkidx_get_heap_relation(scan, state);
		if (heapRelation == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("clustered_pk_index cannot resolve heap relation for bitmap index scan")));

		clustered_pg_pkidx_rescan_internal(scan, scan->keyData, scan->numberOfKeys,
										  scan->orderByData, scan->numberOfOrderBys,
										  true);
	}

	while (table_scan_getnextslot(state->table_scan, ForwardScanDirection,
								 state->table_scan_slot))
	{
		tbm_add_tuples(tbm, &state->table_scan_slot->tts_tid, 1, false);
		rows++;
	}

	return rows;
}

static void
clustered_pg_pkidx_markpos(IndexScanDesc scan)
{
	ClusteredPgPkidxScanState *state;

	if (scan == NULL)
		return;
	state = (ClusteredPgPkidxScanState *) scan->opaque;
	if (state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("clustered_pk_index scan state is not initialized")));

	state->mark_valid = true;
	state->mark_at_start = !ItemPointerIsValid(&scan->xs_heaptid);
	if (!state->mark_at_start)
		ItemPointerCopy(&scan->xs_heaptid, &state->mark_tid);
}

static void
clustered_pg_pkidx_restrpos(IndexScanDesc scan)
{
	ClusteredPgPkidxScanState *state;

	if (scan == NULL || scan->indexRelation == NULL)
		return;

	state = (ClusteredPgPkidxScanState *) scan->opaque;
	if (state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("clustered_pk_index scan state is not initialized")));

	if (!state->mark_valid)
		return;

	if (state->table_scan != NULL)
	{
		table_rescan(state->table_scan,
					 state->key_count > 0 ? state->table_scan_keys : NULL);
	}
	else
	{
		clustered_pg_pkidx_rescan_internal(scan, scan->keyData, scan->numberOfKeys,
										  scan->orderByData, scan->numberOfOrderBys,
										  true);
	}

	scan->xs_recheck = false;
	state->restore_pending = true;
}

static void
clustered_pg_pkidx_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
						ScanKey orderbys, int norderbys)
{
	clustered_pg_pkidx_rescan_internal(scan, keys, nkeys, orderbys, norderbys, false);
}

static void
clustered_pg_pkidx_rescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys,
								  ScanKey orderbys, int norderbys,
								  bool preserve_mark)
{
	ClusteredPgPkidxScanState *state;
	int			i;
	int16		key_attr_count;
	Relation	heapRelation;

	(void) orderbys;
	(void) norderbys;

	if (scan == NULL)
		return;

	state = (ClusteredPgPkidxScanState *) scan->opaque;
	if (state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("clustered_pk_index scan state is not initialized")));

	if (!preserve_mark)
		clustered_pg_pkidx_reset_mark(state);

	heapRelation = clustered_pg_pkidx_get_heap_relation(scan, state);
	if (heapRelation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_pk_index requires a valid heap relation")));

	scan->numberOfKeys = nkeys;
	scan->numberOfOrderBys = norderbys;
	if (scan->keyData == NULL && scan->numberOfKeys > 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("clustered_pk_index key storage is not initialized")));

	if (scan->xs_snapshot == InvalidSnapshot)
		scan->xs_snapshot = GetTransactionSnapshot();

	if (scan->numberOfKeys > 0 && keys != NULL && keys != scan->keyData)
		memcpy(scan->keyData, keys, sizeof(ScanKeyData) * scan->numberOfKeys);

	if (scan->indexRelation == NULL || scan->indexRelation->rd_index == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_pk_index requires a valid index relation")));
	key_attr_count = IndexRelationGetNumberOfKeyAttributes(scan->indexRelation);

	if (scan->numberOfKeys > key_attr_count)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("clustered_pk_index supports up to %d key columns", key_attr_count),
				 errhint("Use only index key columns from the index definition.")));

	if (state->key_count != scan->numberOfKeys)
	{
		if (state->table_scan_keys != NULL)
		{
			pfree(state->table_scan_keys);
			state->table_scan_keys = NULL;
		}
		state->key_count = scan->numberOfKeys;
		if (state->key_count > 0)
			state->table_scan_keys = (ScanKeyData *) palloc0_array(ScanKeyData, state->key_count);
	}

	for (i = 0; i < scan->numberOfKeys; i++)
	{
		AttrNumber	index_attno = scan->keyData[i].sk_attno;
		AttrNumber	heap_attno;

		if (index_attno < 1 || index_attno > key_attr_count)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("clustered_pk_index uses explicit index keys only")));
		}

		heap_attno = scan->indexRelation->rd_index->indkey.values[index_attno - 1];
		if (heap_attno <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("clustered_pk_index does not support expression keys")));

		state->table_scan_keys[i] = scan->keyData[i];
		state->table_scan_keys[i].sk_attno = heap_attno;
	}

	if (state->table_scan != NULL)
		table_rescan(state->table_scan,
					 state->key_count > 0 ? state->table_scan_keys : NULL);
	else
	{
		if (state->table_scan_slot == NULL)
			state->table_scan_slot = table_slot_create(heapRelation, NULL);
		state->table_scan = table_beginscan(heapRelation, scan->xs_snapshot,
										   state->key_count,
										   state->key_count > 0 ? state->table_scan_keys : NULL);
	}

	if (state->table_scan_slot == NULL)
		state->table_scan_slot = table_slot_create(heapRelation, NULL);

	if (state->table_scan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("clustered_pk_index failed to initialize table scan")));
	scan->xs_recheck = false;
	state->restore_pending = false;
}

static IndexScanDesc
clustered_pg_pkidx_beginscan(Relation indexRelation, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(indexRelation, nkeys, norderbys);
	ClusteredPgPkidxScanState *state;

	state = (ClusteredPgPkidxScanState *) palloc0(sizeof(ClusteredPgPkidxScanState));
	state->key_count = nkeys;
	scan->opaque = state;
	return scan;
}

static void
clustered_pg_pkidx_endscan(IndexScanDesc scan)
{
	if (scan != NULL)
	{
		ClusteredPgPkidxScanState *state = (ClusteredPgPkidxScanState *) scan->opaque;

		if (state != NULL)
		{
			if (state->table_scan_slot != NULL)
				ExecDropSingleTupleTableSlot(state->table_scan_slot);
			if (state->table_scan_keys != NULL)
				pfree(state->table_scan_keys);
			if (state->table_scan != NULL)
				table_endscan(state->table_scan);
			if (scan->heapRelation != NULL && state->owns_heap_relation)
				table_close(scan->heapRelation, NoLock);
			scan->heapRelation = NULL;
			pfree(state);
			scan->opaque = NULL;
		}
		index_endscan(scan);
	}
}

Datum
clustered_pg_locator_pack(PG_FUNCTION_ARGS)
{
	int64		major = PG_GETARG_INT64(0);
	int64		minor = PG_GETARG_INT64(1);
	bytea	   *locator;
	uint8	   *payload;

	locator = palloc(VARHDRSZ + (int) sizeof(ClusteredLocator));
	SET_VARSIZE(locator, VARHDRSZ + (int) sizeof(ClusteredLocator));
	payload = (uint8 *) VARDATA(locator);

	clustered_pg_pack_u64_be(payload, (uint64) major);
	clustered_pg_pack_u64_be(payload + 8, (uint64) minor);

	PG_RETURN_BYTEA_P(locator);
}

Datum
clustered_pg_locator_pack_int8(PG_FUNCTION_ARGS)
{
	int64		pk = PG_GETARG_INT64(0);
	bytea	   *locator;
	uint8	   *payload;

	locator = palloc(VARHDRSZ + (int) sizeof(ClusteredLocator));
	SET_VARSIZE(locator, VARHDRSZ + (int) sizeof(ClusteredLocator));
	payload = (uint8 *) VARDATA(locator);

	clustered_pg_pack_u64_be(payload, 0);
	clustered_pg_pack_u64_be(payload + 8, (uint64) pk);

	PG_RETURN_BYTEA_P(locator);
}

static void
clustered_pg_validate_locator_len(bytea *locator)
{
	if (VARSIZE_ANY_EXHDR(locator) != (int) sizeof(ClusteredLocator))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("clustered locator must be exactly %zu bytes",
						sizeof(ClusteredLocator))));
}

Datum
clustered_pg_locator_major(PG_FUNCTION_ARGS)
{
	bytea	   *locator = PG_GETARG_BYTEA_P(0);
	const uint8 *payload = (const uint8 *) VARDATA_ANY(locator);

	clustered_pg_validate_locator_len(locator);
	PG_RETURN_INT64((int64) clustered_pg_unpack_u64_be(payload));
}

Datum
clustered_pg_locator_minor(PG_FUNCTION_ARGS)
{
	bytea	   *locator = PG_GETARG_BYTEA_P(0);
	const uint8 *payload = (const uint8 *) VARDATA_ANY(locator);

	clustered_pg_validate_locator_len(locator);
	PG_RETURN_INT64((int64) clustered_pg_unpack_u64_be(payload + 8));
}

Datum
clustered_pg_locator_to_hex(PG_FUNCTION_ARGS)
{
	bytea	   *locator = PG_GETARG_BYTEA_P(0);
	uint64		major_key;
	uint64		minor_key;
	const uint8 *payload = (const uint8 *) VARDATA_ANY(locator);
	char	   *text_repr;

	clustered_pg_validate_locator_len(locator);

	major_key = clustered_pg_unpack_u64_be(payload);
	minor_key = clustered_pg_unpack_u64_be(payload + 8);

	text_repr = psprintf("%016" PRIX64 ":%016" PRIX64,
						 major_key, minor_key);

	PG_RETURN_TEXT_P(cstring_to_text(text_repr));
}

Datum
clustered_pg_locator_cmp(PG_FUNCTION_ARGS)
{
	bytea	   *a = PG_GETARG_BYTEA_P(0);
	bytea	   *b = PG_GETARG_BYTEA_P(1);
	const uint8 *pa = (const uint8 *) VARDATA_ANY(a);
	const uint8 *pb = (const uint8 *) VARDATA_ANY(b);
	int64		a_major, a_minor;
	int64		b_major, b_minor;

	clustered_pg_validate_locator_len(a);
	clustered_pg_validate_locator_len(b);

	a_major = (int64) clustered_pg_unpack_u64_be(pa);
	a_minor = (int64) clustered_pg_unpack_u64_be(pa + 8);
	b_major = (int64) clustered_pg_unpack_u64_be(pb);
	b_minor = (int64) clustered_pg_unpack_u64_be(pb + 8);

	if (a_major < b_major)
		PG_RETURN_INT32(-1);
	if (a_major > b_major)
		PG_RETURN_INT32(1);
	if (a_minor < b_minor)
		PG_RETURN_INT32(-1);
	if (a_minor > b_minor)
		PG_RETURN_INT32(1);

	PG_RETURN_INT32(0);
}

Datum
clustered_pg_locator_advance_major(PG_FUNCTION_ARGS)
{
	bytea	   *locator = PG_GETARG_BYTEA_P(0);
	int64		delta = PG_GETARG_INT64(1);
	const uint8 *payload = (const uint8 *) VARDATA_ANY(locator);
	uint64		major;
	uint64		minor;
	bytea	   *moved;
	uint8	   *out;

	clustered_pg_validate_locator_len(locator);

	major = clustered_pg_unpack_u64_be(payload);
	minor = clustered_pg_unpack_u64_be(payload + 8);
	major = (uint64) ((int64) major + delta);

	moved = palloc(VARHDRSZ + (int) sizeof(ClusteredLocator));
	SET_VARSIZE(moved, VARHDRSZ + (int) sizeof(ClusteredLocator));
	out = (uint8 *) VARDATA(moved);
	clustered_pg_pack_u64_be(out, major);
	clustered_pg_pack_u64_be(out + 8, minor);

	PG_RETURN_BYTEA_P(moved);
}

Datum
clustered_pg_locator_next_minor(PG_FUNCTION_ARGS)
{
	bytea	   *locator = PG_GETARG_BYTEA_P(0);
	int64		delta = PG_GETARG_INT64(1);
	const uint8 *payload = (const uint8 *) VARDATA_ANY(locator);
	uint64		major;
	int64		minor;
	int64		next_minor;
	bytea	   *moved;
	uint8	   *out;

	clustered_pg_validate_locator_len(locator);

	major = clustered_pg_unpack_u64_be(payload);
	minor = (int64) clustered_pg_unpack_u64_be(payload + 8);

	next_minor = minor + delta;
	if ((delta > 0 && next_minor < minor) ||
		(delta < 0 && next_minor > minor))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("minor locator overflow")));

	moved = palloc(VARHDRSZ + (int) sizeof(ClusteredLocator));
	SET_VARSIZE(moved, VARHDRSZ + (int) sizeof(ClusteredLocator));
	out = (uint8 *) VARDATA(moved);
	clustered_pg_pack_u64_be(out, major);
	clustered_pg_pack_u64_be(out + 8, (uint64) next_minor);

	PG_RETURN_BYTEA_P(moved);
}

/*
 * Simple extension identity.
 */
Datum
clustered_pg_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text("clustered_pg 0.1.0"));
}

/*
 * Table AM handler: bootstrap phase delegates to heapam implementation.
 *
 * Current behavior keeps heap semantics, but exposes a dedicated clustered table
 * AM entry point so future locator-aware hooks can be layered in safely.
 */
Datum
clustered_pg_tableam_handler(PG_FUNCTION_ARGS)
{
	clustered_pg_clustered_heap_init_tableam_routine();
	PG_RETURN_POINTER(&clustered_pg_clustered_heapam_routine);
}

/*
 * Index AM handler: minimal skeleton AM used as a safe extension point for
 * incremental development of a clustered index method.
 */

static IndexBuildResult *
clustered_pg_pkidx_build(Relation heapRelation, Relation indexRelation,
						IndexInfo *indexInfo)
{
	ClusteredPgPkidxBuildState buildstate;
	IndexBuildResult *result;
	int			split_threshold = 128;
	int			target_fillfactor = 85;
	double		auto_repack_interval = 60.0;

	if (heapRelation == NULL || indexRelation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_pk_index ambuild requires valid relations"),
				 errhint("Call CREATE INDEX on a valid relation.")));

	buildstate.heapRelation = heapRelation;
	buildstate.indexInfo = indexInfo;
	buildstate.index_tuples = 0;

	if (indexInfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_pk_index ambuild requires index metadata"),
				 errhint("Call CREATE INDEX with a valid catalog state.")));

	if (indexInfo->ii_NumIndexAttrs != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("clustered_pk_index ambuild supports exactly one key attribute"),
				 errhint("Create a single-column index for the first iteration.")));

	clustered_pg_pkidx_get_index_options(indexRelation,
										&split_threshold,
										&target_fillfactor,
										&auto_repack_interval);

	clustered_pg_pkidx_purge_segment_map(indexRelation);

	result = palloc0_object(IndexBuildResult);

	result->heap_tuples = table_index_build_scan(heapRelation,
													indexRelation,
													indexInfo,
												(indexInfo == NULL || !indexInfo->ii_Concurrent),
												false,
													clustered_pg_pkidx_build_callback,
													(void *) &buildstate,
													NULL);
	result->index_tuples = (double) buildstate.index_tuples;
	clustered_pg_pkidx_rebuild_segment_map(indexRelation,
										  split_threshold,
										  target_fillfactor,
										  auto_repack_interval);

	return result;
}

static void
clustered_pg_pkidx_buildempty(Relation indexRelation)
{
	clustered_pg_pkidx_purge_segment_map(indexRelation);
}

static bool
clustered_pg_pkidx_insert(Relation indexRelation, Datum *values, bool *isnull,
					ItemPointer heap_tid, Relation heapRelation,
					IndexUniqueCheck checkUnique, bool indexUnchanged,
					IndexInfo *indexInfo)
{
	int64		minor_key;
	int			split_threshold = 128;
	int			target_fillfactor = 85;
	double		auto_repack_interval = 60.0;

	(void) heap_tid;
	(void)checkUnique;
	(void)indexUnchanged;

	if (indexInfo == NULL || indexInfo->ii_NumIndexAttrs != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("clustered_pk_index supports exactly one key attribute"),
				 errhint("Create a single-column index for the first iteration.")));

	if (!clustered_pg_pkidx_extract_minor_key(indexRelation, values, isnull, &minor_key))
			ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("clustered_pk_index currently supports only int2/int4/int8 index key types"),
						 errdetail("Index key is NULL, missing, or has unsupported type.")));

	clustered_pg_pkidx_get_index_options(indexRelation,
										&split_threshold,
										&target_fillfactor,
										&auto_repack_interval);

	clustered_pg_pkidx_allocate_locator(heapRelation,
									   minor_key,
									   split_threshold,
									   target_fillfactor,
									   auto_repack_interval);

	return true;
}

static bytea *
clustered_pg_pkidx_options(Datum reloptions, bool validate)
{
	clustered_pg_index_relopt_kind_init();
	return (bytea *) build_reloptions(reloptions, validate,
									  clustered_pg_pkidx_relopt_kind,
									  sizeof(ClusteredPgIndexOptions),
									  clustered_pg_pkidx_relopt_tab,
									  3);
}

static IndexBulkDeleteResult *
clustered_pg_pkidx_bulkdelete(IndexVacuumInfo *info,
						IndexBulkDeleteResult *stats,
						IndexBulkDeleteCallback callback,
						void *callback_state)
{
	(void)callback;
	(void)callback_state;
	return clustered_pg_pkidx_init_bulkdelete_stats(info, stats);
}

static IndexBulkDeleteResult *
clustered_pg_pkidx_vacuumcleanup(IndexVacuumInfo *info,
							IndexBulkDeleteResult *stats)
{
	int			split_threshold = 128;
	int			target_fillfactor = 85;
	double		auto_repack_interval = 60.0;
	int64		due_segments = 0;

	if (info != NULL && !info->analyze_only && info->index != NULL &&
		info->index->rd_index != NULL)
	{
		clustered_pg_pkidx_get_index_options(info->index,
											&split_threshold,
											&target_fillfactor,
											&auto_repack_interval);

		PG_TRY();
		{
			due_segments = clustered_pg_pkidx_count_repack_due(info->index,
															  auto_repack_interval);
			if (due_segments > 0)
				clustered_pg_pkidx_rebuild_segment_map(info->index,
													   split_threshold,
													   target_fillfactor,
													   auto_repack_interval);
			else
				clustered_pg_pkidx_touch_repack(info->index);
		}
		PG_CATCH();
		{
			ErrorData  *edata = CopyErrorData();

			FlushErrorState();
			ereport(WARNING,
					(errcode(ERRCODE_WARNING),
					 errmsg("clustered_pk_index maintenance callback failed"),
					 errdetail("%s",
							  edata->message != NULL ? edata->message : "unknown error"),
					 errhint("Vacuum maintenance metadata update was skipped for relation %u.",
							 RelationGetRelid(info->index))));
			FreeErrorData(edata);
		}
		PG_END_TRY();
	}

	return clustered_pg_pkidx_init_bulkdelete_stats(info, stats);
}

static void
clustered_pg_pkidx_costestimate(struct PlannerInfo *root, struct IndexPath *path,
							double loop_count, Cost *startup_cost,
							Cost *total_cost, Selectivity *selectivity,
							double *correlation, double *pages)
{
	(void)root;
	(void)path;
	(void)loop_count;
	*startup_cost = 100.0;
	*total_cost = 200.0;
	*selectivity = 1.0;
	*correlation = 0.0;
	*pages = 1.0;
}

static bool
clustered_pg_pkidx_validate(Oid opclassoid)
{
	(void)opclassoid;
	return true;
}

Datum
clustered_pg_pkidx_handler(PG_FUNCTION_ARGS)
{
	static const IndexAmRoutine amroutine = {
		.type = T_IndexAmRoutine,
		.amstrategies = 5,
		.amsupport = 1,
		.amcanorder = false,
		.amcanorderbyop = false,
		.amcanhash = false,
		.amconsistentequality = true,
		.amconsistentordering = true,
		.amcanbackward = true,
		.amcanunique = false,
		.amcanmulticol = false,
		.amoptionalkey = false,
		.amsearcharray = false,
		.amsearchnulls = false,
		.amstorage = false,
		.amclusterable = false,
		.ampredlocks = false,
		.amcanparallel = false,
		.amcanbuildparallel = false,
		.amcaninclude = false,
		.amusemaintenanceworkmem = false,
		.amsummarizing = false,
		.amparallelvacuumoptions = VACUUM_OPTION_NO_PARALLEL,
		.amkeytype = InvalidOid,

		.ambuild = clustered_pg_pkidx_build,
		.ambuildempty = clustered_pg_pkidx_buildempty,
		.aminsert = clustered_pg_pkidx_insert,
		.ambulkdelete = clustered_pg_pkidx_bulkdelete,
		.amvacuumcleanup = clustered_pg_pkidx_vacuumcleanup,
		.amcanreturn = clustered_pg_pkidx_canreturn,
		.amgettreeheight = clustered_pg_pkidx_gettreeheight,
		.amcostestimate = clustered_pg_pkidx_costestimate,
		.amoptions = clustered_pg_pkidx_options,
		.amvalidate = clustered_pg_pkidx_validate,
		.ambeginscan = clustered_pg_pkidx_beginscan,
		.amrescan = clustered_pg_pkidx_rescan,
		.amgettuple = clustered_pg_pkidx_gettuple,
		.amgetbitmap = clustered_pg_pkidx_getbitmap,
		.amendscan = clustered_pg_pkidx_endscan,
		.ammarkpos = clustered_pg_pkidx_markpos,
		.amrestrpos = clustered_pg_pkidx_restrpos,
	};
	IndexAmRoutine *result;

	result = (IndexAmRoutine *) palloc(sizeof(IndexAmRoutine));
	*result = amroutine;
	PG_RETURN_POINTER(result);
}

void
_PG_init(void)
{
	clustered_pg_pkidx_init_reloptions();
}
