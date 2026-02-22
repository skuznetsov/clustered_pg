#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/reloptions.h"
#include "access/skey.h"
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
#include "utils/selfuncs.h"
#include <inttypes.h>
#include <string.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(clustered_pg_version);
PG_FUNCTION_INFO_V1(clustered_pg_observability);
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
#define CLUSTERED_PG_EXTENSION_VERSION "0.1.0"
#define CLUSTERED_PG_OBS_API_VERSION 1
#define CLUSTERED_PG_DEFAULT_SPLIT_THRESHOLD 128
#define CLUSTERED_PG_DEFAULT_TARGET_FILLFACTOR 85
#define CLUSTERED_PG_DEFAULT_AUTO_REPACK_INTERVAL 60.0

typedef struct ClusteredPgStats
{
	uint64		observability_calls;
	uint64		costestimate_calls;
	uint64		segment_rowcount_estimate_calls;
	uint64		segment_rowcount_estimate_errors;
	uint64		segment_map_lookup_calls;
	uint64		segment_map_lookup_failures;
	uint64		insert_calls;
	uint64		insert_errors;
	uint64		scan_rescan_calls;
	uint64		scan_getcalls;
	uint64		vacuumcleanup_calls;
	uint64		maintenance_rebuild_calls;
	uint64		maintenance_touch_calls;
	uint64		maintenance_vacuumcleanup_errors;
} ClusteredPgStats;

static ClusteredPgStats clustered_pg_stats = {0};

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
	int				table_scan_key_count;
	bool			use_segment_tids;
	ItemPointerData	*segment_tids;
	int				segment_tid_count;
	int				segment_tid_pos;
	ScanDirection	segment_tid_direction;
	bool			scan_ready;
	bool			owns_heap_relation;
	bool			mark_valid;
	bool			mark_at_start;
	bool			restore_pending;
	ItemPointerData	mark_tid;
} ClusteredPgPkidxScanState;

static SPIPlanPtr clustered_pg_pkidx_allocate_locator_plan = NULL;
static SPIPlanPtr clustered_pg_pkidx_count_repack_due_plan = NULL;
static SPIPlanPtr clustered_pg_pkidx_rebuild_segment_map_plan = NULL;
static SPIPlanPtr clustered_pg_pkidx_segment_tid_touch_plan = NULL;
static SPIPlanPtr clustered_pg_pkidx_segment_tid_lookup_plan = NULL;
static SPIPlanPtr clustered_pg_pkidx_segment_rowcount_plan = NULL;

static const char *clustered_pg_format_relation_label(Oid relationOid,
													 char *buffer,
													 int bufferSize);

static bytea *clustered_pg_pkidx_allocate_locator(Relation heapRelation, int64 minor_key,
												int split_threshold, int target_fillfactor,
												double auto_repack_interval);
static void clustered_pg_pack_u64_be(uint8_t *dst, uint64 src);
static uint64 clustered_pg_unpack_u64_be(const uint8_t *src);
static void clustered_pg_pkidx_collect_segment_tids(Relation indexRelation, int64 minor_key,
												  ClusteredPgPkidxScanState *state,
												  ScanDirection direction);
static void clustered_pg_pkidx_free_segment_tids(ClusteredPgPkidxScanState *state);
static void clustered_pg_validate_locator_len(bytea *locator);
static bool clustered_pg_pkidx_next_segment_tid(ClusteredPgPkidxScanState *state,
											  ScanDirection direction, ItemPointer tid);
static void clustered_pg_pkidx_touch_segment_tids(Relation heapRelation, int64 major_key,
												int64 minor_key, ItemPointer heap_tid);
static void clustered_pg_pkidx_gc_segment_tids(Relation indexRelation);
static bool clustered_pg_pkidx_ensure_table_scan(IndexScanDesc scan,
												ClusteredPgPkidxScanState *state);
static SPIPlanPtr clustered_pg_pkidx_segment_rowcount_plan_init(void);
static int64 clustered_pg_pkidx_estimate_segment_rows(Oid relationOid);
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
static void clustered_pg_pkidx_reset_segment_tids(ClusteredPgPkidxScanState *state);
static void clustered_pg_clustered_heap_relation_set_new_filelocator(Relation rel,
														const RelFileLocator *rlocator,
														char persistence,
														TransactionId *freezeXid,
														MultiXactId *minmulti);
static void clustered_pg_clustered_heap_relation_nontransactional_truncate(Relation rel);
static double clustered_pg_clustered_heap_index_build_range_scan(
	Relation tableRelation,
	Relation indexRelation,
	IndexInfo *indexInfo,
	bool allow_sync,
	bool anyvisible,
	bool progress,
	BlockNumber start_blockno,
	BlockNumber numblocks,
	IndexBuildCallback callback,
	void *callback_state,
	TableScanDesc scan);
static void clustered_pg_clustered_heap_index_validate_scan(
	Relation tableRelation,
	Relation indexRelation,
	IndexInfo *indexInfo,
	Snapshot snapshot,
	ValidateIndexState *state);
static void clustered_pg_clustered_heap_relation_copy_data(Relation rel,
														const RelFileLocator *newrlocator);
static void clustered_pg_clustered_heap_relation_copy_for_cluster(Relation OldTable,
															 Relation NewTable,
															 Relation OldIndex,
															 bool use_sort,
															 TransactionId OldestXmin,
															 TransactionId *xid_cutoff,
															 MultiXactId *multi_cutoff,
															 double *num_tuples,
															 double *tups_vacuumed,
															 double *tups_recently_dead);
static void clustered_pg_clustered_heap_clear_segment_map(Oid relationOid);
static void clustered_pg_clustered_heap_init_tableam_routine(void);

static const char *
clustered_pg_format_relation_label(Oid relationOid, char *buffer, int bufferSize)
{
	const char *relName;

	if (buffer == NULL || bufferSize <= 0)
		return NULL;

	if (!OidIsValid(relationOid))
	{
		snprintf(buffer, bufferSize, "oid=0");
		return buffer;
	}

	relName = get_rel_name(relationOid);
	if (relName != NULL)
		snprintf(buffer, bufferSize, "%u (%s)", relationOid, relName);
	else
		snprintf(buffer, bufferSize, "oid=%u", relationOid);

	return buffer;
}

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
	char		sql_tids[256];
	char		sql_has_metadata[320];
	Datum		args[1];
	Oid			argtypes[1];
	const char *lock_sql = "SELECT pg_advisory_xact_lock($1::bigint)";
	int			rc;
	bool		has_metadata = false;
	bool		isnull = false;

	if (!OidIsValid(relationOid))
		return;

	args[0] = ObjectIdGetDatum(relationOid);
	argtypes[0] = OIDOID;

	snprintf(sql, sizeof(sql),
			 "DELETE FROM %s WHERE relation_oid = $1::oid",
			 clustered_pg_qualified_extension_name("segment_map"));
	snprintf(sql_tids, sizeof(sql_tids),
			 "DELETE FROM %s WHERE relation_oid = $1::oid",
			 clustered_pg_qualified_extension_name("segment_map_tids"));
	snprintf(sql_has_metadata, sizeof(sql_has_metadata),
			 "SELECT EXISTS (SELECT 1 FROM %s WHERE relation_oid = $1::oid) "
			 "OR EXISTS (SELECT 1 FROM %s WHERE relation_oid = $1::oid)",
			 clustered_pg_qualified_extension_name("segment_map"),
			 clustered_pg_qualified_extension_name("segment_map_tids"));

	rc = SPI_connect();
	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("SPI_connect() failed while cleaning clustered_pg segment map")));

	PG_TRY();
	{
		rc = SPI_execute_with_args(lock_sql,
								   1,
								   argtypes,
								   args,
								   NULL,
								   false,
								   0);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("clustered_pg segment_map cleanup lock acquisition failed"),
					 errdetail("SPI status code %d", rc)));

		rc = SPI_execute_with_args(sql_has_metadata,
								   1,
								   argtypes,
								   args,
								   NULL,
								   false,
								   0);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("clustered_pg segment_map metadata lookup failed"),
					 errdetail("SPI status code %d", rc)));
		if (SPI_processed != 1)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("clustered_pg segment_map metadata lookup returned unexpected row count"),
					 errdetail("Expected 1 row, got %" PRIu64, (uint64) SPI_processed)));

		has_metadata = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc,
												 1,
												 &isnull));
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("clustered_pg segment_map metadata lookup returned NULL")));

		if (!has_metadata)
			goto skip_cleanup;

		rc = SPI_execute_with_args(sql, 1, argtypes, args, NULL, false, 0);
		if (rc != SPI_OK_DELETE)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("clustered_pg segment_map cleanup failed"),
					 errdetail("SPI status code %d", rc)));

		rc = SPI_execute_with_args(sql_tids, 1, argtypes, args, NULL, false, 0);
		if (rc != SPI_OK_DELETE)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("clustered_pg segment_map_tids cleanup failed"),
					 errdetail("SPI status code %d", rc)));

skip_cleanup:
		;
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

static double
clustered_pg_clustered_heap_index_build_range_scan(Relation tableRelation,
									 Relation indexRelation,
									 IndexInfo *indexInfo,
									 bool allow_sync,
									 bool anyvisible,
									 bool progress,
									 BlockNumber start_blockno,
									 BlockNumber numblocks,
									 IndexBuildCallback callback,
									 void *callback_state,
									 TableScanDesc scan)
{
	const TableAmRoutine *heap;
	const TableAmRoutine *old_tableam = tableRelation ? tableRelation->rd_tableam : NULL;
	double result;

	if (tableRelation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_heap index_build_range_scan requires a valid relation")));

	if (indexRelation == NULL || indexInfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_heap index_build_range_scan requires valid index relation and index info")));

	heap = GetHeapamTableAmRoutine();
	if (heap == NULL || heap->index_build_range_scan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method build callback is unavailable")));

	tableRelation->rd_tableam = heap;
	PG_TRY();
	{
		result = heap->index_build_range_scan(tableRelation,
									 indexRelation,
									 indexInfo,
									 allow_sync,
									 anyvisible,
									 progress,
									 start_blockno,
									 numblocks,
									 callback,
									 callback_state,
									 scan);
	}
	PG_CATCH();
	{
		tableRelation->rd_tableam = old_tableam;
		PG_RE_THROW();
	}
	PG_END_TRY();

	tableRelation->rd_tableam = old_tableam;
	return result;
}

static void
clustered_pg_clustered_heap_index_validate_scan(Relation tableRelation,
								Relation indexRelation,
								IndexInfo *indexInfo,
								Snapshot snapshot,
								ValidateIndexState *state)
{
	const TableAmRoutine *heap;
	const TableAmRoutine *old_tableam = tableRelation ? tableRelation->rd_tableam : NULL;

	if (tableRelation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_heap index_validate_scan requires a valid relation")));

	if (indexRelation == NULL || indexInfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_heap index_validate_scan requires valid index relation and index info")));

	heap = GetHeapamTableAmRoutine();
	if (heap == NULL || heap->index_validate_scan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method validate callback is unavailable")));

	tableRelation->rd_tableam = heap;
	PG_TRY();
	{
		heap->index_validate_scan(tableRelation,
							 indexRelation,
							 indexInfo,
							 snapshot,
							 state);
	}
	PG_CATCH();
	{
		tableRelation->rd_tableam = old_tableam;
		PG_RE_THROW();
	}
	PG_END_TRY();

	tableRelation->rd_tableam = old_tableam;
}

static void
clustered_pg_clustered_heap_relation_copy_data(Relation rel,
								   const RelFileLocator *newrlocator)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	if (heap == NULL || heap->relation_copy_data == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	heap->relation_copy_data(rel, newrlocator);
	clustered_pg_clustered_heap_clear_segment_map(RelationGetRelid(rel));
}

static void
clustered_pg_clustered_heap_relation_copy_for_cluster(Relation OldTable,
													Relation NewTable,
													Relation OldIndex,
													bool use_sort,
													TransactionId OldestXmin,
													TransactionId *xid_cutoff,
													MultiXactId *multi_cutoff,
													double *num_tuples,
													double *tups_vacuumed,
													double *tups_recently_dead)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	if (heap == NULL || heap->relation_copy_for_cluster == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	heap->relation_copy_for_cluster(OldTable,
									NewTable,
									OldIndex,
									use_sort,
									OldestXmin,
									xid_cutoff,
									multi_cutoff,
									num_tuples,
									tups_vacuumed,
									tups_recently_dead);
	clustered_pg_clustered_heap_clear_segment_map(RelationGetRelid(OldTable));
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
	clustered_pg_clustered_heapam_routine.index_build_range_scan =
		clustered_pg_clustered_heap_index_build_range_scan;
	clustered_pg_clustered_heapam_routine.index_validate_scan =
		clustered_pg_clustered_heap_index_validate_scan;
	clustered_pg_clustered_heapam_routine.relation_copy_data =
		clustered_pg_clustered_heap_relation_copy_data;
	clustered_pg_clustered_heapam_routine.relation_copy_for_cluster =
		clustered_pg_clustered_heap_relation_copy_for_cluster;
	clustered_pg_clustered_heapam_initialized = true;
}

static SPIPlanPtr
clustered_pg_pkidx_allocate_locator_plan_init(void)
{
	Oid			argtypes[6];
	char        query[1024];

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
	char        query[1024];

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
	char        query[1024];

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

static SPIPlanPtr
clustered_pg_pkidx_segment_tid_touch_plan_init(void)
{
	Oid			argtypes[4];
	char        query[1024];

	if (clustered_pg_pkidx_segment_tid_touch_plan != NULL)
		return clustered_pg_pkidx_segment_tid_touch_plan;

	argtypes[0] = OIDOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT8OID;
	argtypes[3] = TIDOID;

	snprintf(query, sizeof(query),
			 "INSERT INTO %s (relation_oid, major_key, minor_key, tuple_tid, updated_at) "
			 "VALUES ($1::oid, $2::bigint, $3::bigint, $4::tid, clock_timestamp()) "
			 "ON CONFLICT (relation_oid, tuple_tid) "
			 "DO UPDATE SET major_key = EXCLUDED.major_key, "
			 "minor_key = EXCLUDED.minor_key, "
			 "updated_at = clock_timestamp()",
			 clustered_pg_qualified_extension_name("segment_map_tids"));

	clustered_pg_pkidx_segment_tid_touch_plan = SPI_prepare(query, 4, argtypes);
	if (clustered_pg_pkidx_segment_tid_touch_plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to prepare clustered_pg_sql plan for segment_map_tids touch"),
				 errhint("Inspect clustered_pg extension schema and function visibility.")));
	SPI_keepplan(clustered_pg_pkidx_segment_tid_touch_plan);

	return clustered_pg_pkidx_segment_tid_touch_plan;
}

static SPIPlanPtr
clustered_pg_pkidx_segment_tid_lookup_plan_init(void)
{
	Oid			argtypes[2];
	char        query[1024];

	if (clustered_pg_pkidx_segment_tid_lookup_plan != NULL)
		return clustered_pg_pkidx_segment_tid_lookup_plan;

	argtypes[0] = OIDOID;
	argtypes[1] = INT8OID;

	snprintf(query, sizeof(query),
			 "SELECT tuple_tid, major_key FROM %s "
			 "WHERE relation_oid = $1::oid AND minor_key = $2::bigint "
			 "ORDER BY major_key, tuple_tid",
			 clustered_pg_qualified_extension_name("segment_map_tids"));

	clustered_pg_pkidx_segment_tid_lookup_plan = SPI_prepare(query, 2, argtypes);
	if (clustered_pg_pkidx_segment_tid_lookup_plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to prepare clustered_pg_sql plan for segment_map_tids lookup"),
				 errhint("Inspect clustered_pg extension schema and function visibility.")));
	SPI_keepplan(clustered_pg_pkidx_segment_tid_lookup_plan);

	return clustered_pg_pkidx_segment_tid_lookup_plan;
}

static SPIPlanPtr
clustered_pg_pkidx_segment_rowcount_plan_init(void)
{
	Oid			argtypes[1];
	char        query[1024];

	if (clustered_pg_pkidx_segment_rowcount_plan != NULL)
		return clustered_pg_pkidx_segment_rowcount_plan;

	argtypes[0] = OIDOID;

	snprintf(query, sizeof(query),
			 "SELECT COALESCE(SUM(row_count), 0)::bigint "
			 "FROM %s WHERE relation_oid = $1::oid",
			 clustered_pg_qualified_extension_name("segment_map"));

	clustered_pg_pkidx_segment_rowcount_plan = SPI_prepare(query, 1, argtypes);
	if (clustered_pg_pkidx_segment_rowcount_plan == NULL)
		return NULL;
	SPI_keepplan(clustered_pg_pkidx_segment_rowcount_plan);

	return clustered_pg_pkidx_segment_rowcount_plan;
}

static int64
clustered_pg_pkidx_estimate_segment_rows(Oid relationOid)
{
	Datum		args[1];
	int			rc;
	bool		isnull = false;
	int64		result = -1;
	SPIPlanPtr	plan;
	bool		spi_connected = false;

	if (!OidIsValid(relationOid))
		return -1;
	clustered_pg_stats.segment_rowcount_estimate_calls++;

	args[0] = ObjectIdGetDatum(relationOid);

	rc = SPI_connect();
	if (rc != SPI_OK_CONNECT)
		return -1;
	spi_connected = true;

	plan = clustered_pg_pkidx_segment_rowcount_plan_init();
	if (plan == NULL)
	{
		SPI_finish();
		return -1;
	}

	PG_TRY();
	{
		rc = SPI_execute_plan(plan, args, NULL, false, 0);
		if (rc != SPI_OK_SELECT || SPI_processed != 1)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("clustered_pg segment_map row-count estimate failed"),
					 errdetail("SPI status code %d, processed rows %" PRIu64,
							   rc, (uint64) SPI_processed)));

		result = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc,
											1,
											&isnull));
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("clustered_pg segment_map row-count estimate returned NULL")));
	}
	PG_CATCH();
	{
		clustered_pg_stats.segment_rowcount_estimate_errors++;
		result = -1;
	}
	PG_END_TRY();

	if (spi_connected)
		SPI_finish();
	return result;
}

static void
clustered_pg_pkidx_execute_segment_map_maintenance(Relation indexRelation,
												  const char *sql)
{
	int			rc;
	Oid			relationOid = InvalidOid;
	Datum		args[1];
	Oid			argtypes[1];
	const char *lock_sql = "SELECT pg_advisory_xact_lock($1::bigint)";

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
		rc = SPI_execute_with_args(
			lock_sql,
			1,
			argtypes,
			args,
			NULL,
			false,
			0);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("clustered_pg segment_map maintenance lock acquisition failed"),
					 errdetail("SPI status code %d", rc)));

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
	char		sql_tids[256];

	snprintf(sql, sizeof(sql),
			 "DELETE FROM %s WHERE relation_oid = $1::oid",
			 clustered_pg_qualified_extension_name("segment_map"));
	snprintf(sql_tids, sizeof(sql_tids),
			 "DELETE FROM %s WHERE relation_oid = $1::oid",
			 clustered_pg_qualified_extension_name("segment_map_tids"));

	clustered_pg_pkidx_execute_segment_map_maintenance(indexRelation, sql);
	clustered_pg_pkidx_execute_segment_map_maintenance(indexRelation, sql_tids);
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

static void
clustered_pg_pkidx_gc_segment_tids(Relation indexRelation)
{
	Datum		args[1];
	const char *function_name;
	Oid			relationOid = InvalidOid;
	Oid			argtypes[1];
	char		query[1024];
	bool		isnull = false;
	int			rc;
	int64		deleted_tids = 0;

	if (indexRelation == NULL || indexRelation->rd_index == NULL)
		return;

	relationOid = indexRelation->rd_index->indrelid;
	if (!OidIsValid(relationOid))
		return;

	args[0] = ObjectIdGetDatum(relationOid);
	argtypes[0] = OIDOID;
	function_name = clustered_pg_qualified_extension_name("segment_map_tids_gc");
	snprintf(query, sizeof(query),
			 "SELECT %s($1::regclass)",
			 function_name != NULL ? function_name : "segment_map_tids_gc");

	rc = SPI_connect();
	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("SPI_connect() failed while running segment_map_tids gc")));

	PG_TRY();
	{
		rc = SPI_execute_with_args(query,
								   1,
								   argtypes,
								   args,
								   NULL,
								   false,
								   0);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment_map_tids gc failed"),
					 errdetail("SPI_execute_with_args returned %d", rc)));

		if (SPI_processed != 1)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment_map_tids gc returned unexpected row count"),
					 errdetail("Expected 1 row, got %" PRIu64, (uint64) SPI_processed)));

		deleted_tids = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc,
												 1,
												 &isnull));
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment_map_tids gc returned NULL")));
		(void) deleted_tids;
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();
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

static bool
clustered_pg_pkidx_extract_minor_key_from_scan_key(Relation indexRelation, ScanKey key,
												  int64 *minor_key)
{
	TupleDesc	tupdesc;
	AttrNumber	index_attno;
	Oid			atttype;
	Datum		value;

	if (indexRelation == NULL || key == NULL || minor_key == NULL)
		return false;
	if (key->sk_flags & SK_ISNULL)
		return false;
	if (key->sk_flags & (SK_SEARCHARRAY | SK_SEARCHNULL | SK_SEARCHNOTNULL |
						SK_ROW_HEADER | SK_ROW_MEMBER | SK_ROW_END))
		return false;

	if (key->sk_strategy != BTEqualStrategyNumber)
		return false;

	tupdesc = RelationGetDescr(indexRelation);
	if (tupdesc == NULL)
		return false;

	index_attno = key->sk_attno;
	if (index_attno < 1 || index_attno > tupdesc->natts)
		return false;

	atttype = TupleDescAttr(tupdesc, index_attno - 1)->atttypid;

	switch (atttype)
	{
		case INT2OID:
			value = key->sk_argument;
			*minor_key = (int64) DatumGetInt16(value);
			return true;
		case INT4OID:
			value = key->sk_argument;
			*minor_key = (int64) DatumGetInt32(value);
			return true;
		case INT8OID:
			value = key->sk_argument;
			*minor_key = DatumGetInt64(value);
			return true;
		default:
			return false;
	}
}

static void
clustered_pg_pkidx_lookup_locator_values(bytea *locator, int64 *major_key, int64 *minor_key)
{
	const uint8 *payload;

	if (locator == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("clustered_pg locator is NULL")));
	if (major_key == NULL || minor_key == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("clustered_pg locator output pointer is NULL")));

	clustered_pg_validate_locator_len(locator);

	payload = (const uint8 *) VARDATA(locator);
	*major_key = (int64) clustered_pg_unpack_u64_be(payload);
	*minor_key = (int64) clustered_pg_unpack_u64_be(payload + 8);
}

static void
clustered_pg_pkidx_free_segment_tids(ClusteredPgPkidxScanState *state)
{
	if (state == NULL)
		return;

	if (state->segment_tids != NULL)
	{
		pfree(state->segment_tids);
		state->segment_tids = NULL;
	}

	state->segment_tid_count = 0;
	state->segment_tid_pos = 0;
	state->use_segment_tids = false;
}

static void
clustered_pg_pkidx_reset_segment_tids(ClusteredPgPkidxScanState *state)
{
	if (state == NULL)
		return;

	clustered_pg_pkidx_free_segment_tids(state);
	state->segment_tid_direction = ForwardScanDirection;
	state->segment_tid_pos = 0;
}

static void
clustered_pg_pkidx_collect_segment_tids(Relation indexRelation, int64 minor_key,
									   ClusteredPgPkidxScanState *state,
									   ScanDirection direction)
{
	Oid			relationOid = InvalidOid;
	Datum		args[2];
	SPIPlanPtr	plan;
	int			rc;
	bool		isnull;
	int			i;

	if (state == NULL)
		return;
	clustered_pg_stats.segment_map_lookup_calls++;

	clustered_pg_pkidx_free_segment_tids(state);

	if (indexRelation == NULL || indexRelation->rd_index == NULL)
		return;

	relationOid = indexRelation->rd_index->indrelid;
	if (!OidIsValid(relationOid))
		return;

	args[0] = ObjectIdGetDatum(relationOid);
	args[1] = Int64GetDatum(minor_key);

	rc = SPI_connect();
	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("SPI_connect() failed in clustered_pk_index scan helper")));

	plan = clustered_pg_pkidx_segment_tid_lookup_plan_init();
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to access SPI plan for segment_map_tids lookup")));

	PG_TRY();
	{
		rc = SPI_execute_plan(plan, args, NULL, false, 0);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment_map_tids lookup failed"),
					 errdetail("SPI_execute_plan returned %d", rc)));

		if (SPI_processed > 0)
		{
			int			tid_count = (int) SPI_processed;

			state->segment_tids = (ItemPointerData *) palloc0_array(ItemPointerData,
																  tid_count);
			for (i = 0; i < tid_count; i++)
			{
				Datum		tidDatum;
				ItemPointerData *sourceTid;

				tidDatum = SPI_getbinval(SPI_tuptable->vals[i],
										 SPI_tuptable->tupdesc,
										 1,
										 &isnull);
				if (isnull)
					continue;

				sourceTid = (ItemPointerData *) DatumGetPointer(tidDatum);
				if (sourceTid == NULL)
					continue;

				ItemPointerCopy(sourceTid,
							   &state->segment_tids[state->segment_tid_count]);
				state->segment_tid_count++;
			}

			if (state->segment_tid_count == 0)
			{
				pfree(state->segment_tids);
				state->segment_tids = NULL;
			}
		}
	}
	PG_CATCH();
	{
		clustered_pg_stats.segment_map_lookup_failures++;
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();

	state->segment_tid_direction = direction;
	state->segment_tid_pos = (direction == ForwardScanDirection) ? 0 : state->segment_tid_count - 1;
	state->use_segment_tids = (state->segment_tid_count > 0);
}

static bool
clustered_pg_pkidx_next_segment_tid(ClusteredPgPkidxScanState *state,
								   ScanDirection direction, ItemPointer tid)
{
	if (state == NULL || tid == NULL)
		return false;

	if (state->segment_tids == NULL)
		return false;

	if (!state->use_segment_tids)
		return false;
	if (state->segment_tid_count <= 0)
		return false;
	if (direction != ForwardScanDirection && direction != BackwardScanDirection)
		return false;

	if (state->segment_tid_direction != direction)
	{
		state->segment_tid_direction = direction;
		state->segment_tid_pos = (direction == ForwardScanDirection) ?
			0 : state->segment_tid_count - 1;
	}

	if (direction == ForwardScanDirection)
	{
		if (state->segment_tid_pos >= state->segment_tid_count)
			return false;
		ItemPointerCopy(&state->segment_tids[state->segment_tid_pos], tid);
		state->segment_tid_pos++;
		return true;
	}

	if (state->segment_tid_pos < 0)
		return false;
	ItemPointerCopy(&state->segment_tids[state->segment_tid_pos], tid);
	state->segment_tid_pos--;
	return true;
}

static void
clustered_pg_pkidx_touch_segment_tids(Relation heapRelation, int64 major_key,
									  int64 minor_key, ItemPointer heap_tid)
{
	Datum		args[4];
	SPIPlanPtr	plan;
	Oid			relationOid = InvalidOid;
	ItemPointerData safeHeapTid;
	int			rc;

	if (heapRelation == NULL || !OidIsValid(RelationGetRelid(heapRelation)) || heap_tid == NULL)
		return;
	if (ItemPointerIsValid(heap_tid) == false)
		return;
	memcpy(&safeHeapTid, heap_tid, sizeof(ItemPointerData));
	relationOid = RelationGetRelid(heapRelation);

	args[0] = ObjectIdGetDatum(relationOid);
	args[1] = Int64GetDatum(major_key);
	args[2] = Int64GetDatum(minor_key);
	args[3] = PointerGetDatum(&safeHeapTid);

	rc = SPI_connect();
	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("SPI_connect() failed while updating segment_map_tids")));

	plan = clustered_pg_pkidx_segment_tid_touch_plan_init();
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to access SPI plan for segment_map_tids touch")));

	PG_TRY();
	{
		rc = SPI_execute_plan(plan, args, NULL, false, 0);
		if (rc != SPI_OK_INSERT && rc != SPI_OK_UPDATE)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment_map_tids touch failed"),
					 errdetail("SPI_execute_plan returned %d", rc)));
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();
}

static bool
clustered_pg_pkidx_ensure_table_scan(IndexScanDesc scan, ClusteredPgPkidxScanState *state)
{
	Relation	heapRelation;

	if (scan == NULL || state == NULL)
		return false;

	heapRelation = clustered_pg_pkidx_get_heap_relation(scan, state);
	if (heapRelation == NULL)
		return false;

	if (scan->xs_snapshot == InvalidSnapshot)
		scan->xs_snapshot = GetTransactionSnapshot();

	if (state->table_scan_slot == NULL)
		state->table_scan_slot = table_slot_create(heapRelation, NULL);

	if (state->table_scan != NULL && state->table_scan_key_count != state->key_count)
	{
		table_endscan(state->table_scan);
		state->table_scan = NULL;
	}

	if (state->table_scan == NULL)
	{
		state->table_scan = table_beginscan(heapRelation,
										   scan->xs_snapshot,
										   state->key_count,
										   state->key_count > 0 ? state->table_scan_keys : NULL);
		state->table_scan_key_count = state->key_count;
	}
	else
		table_rescan(state->table_scan,
					 state->key_count > 0 ? state->table_scan_keys : NULL);

	if (state->table_scan == NULL)
		return false;

	return true;
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

static bytea *
clustered_pg_pkidx_allocate_locator(Relation heapRelation, int64 minor_key,
								   int split_threshold, int target_fillfactor,
								   double auto_repack_interval)
{
	int			rc;
	Datum		args[6];
	SPIPlanPtr	plan;
	bool		isnull = false;
	Datum		locatorDatum;
	bytea	   *locator = NULL;

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

		locatorDatum = SPI_getbinval(SPI_tuptable->vals[0],
									 SPI_tuptable->tupdesc,
									 1,
									 &isnull);
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("segment_map_allocate_locator returned NULL")));

		locator = DatumGetByteaPCopy(locatorDatum);
		if (locator == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("clustered_pg locator allocation returned NULL")));

		if (VARSIZE_ANY_EXHDR(locator) != (Size) sizeof(ClusteredLocator))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("segment_map_allocate_locator returned invalid locator payload"),
					 errdetail("Expected %zu bytes, got %zu bytes.",
							   (size_t) sizeof(ClusteredLocator),
							   (size_t) VARSIZE_ANY_EXHDR(locator))));
	}
	PG_CATCH();
	{
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();
	return locator;
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
					CLUSTERED_PG_DEFAULT_SPLIT_THRESHOLD, 16, 8192,
					AccessExclusiveLock);
	clustered_pg_pkidx_relopt_tab[i].optname = "split_threshold";
	clustered_pg_pkidx_relopt_tab[i].opttype = RELOPT_TYPE_INT;
	clustered_pg_pkidx_relopt_tab[i].offset = offsetof(ClusteredPgIndexOptions, split_threshold);
	i++;

	add_int_reloption(clustered_pg_pkidx_relopt_kind,
				"target_fillfactor",
				"Target tuple density for initial split behavior",
				CLUSTERED_PG_DEFAULT_TARGET_FILLFACTOR, 20, 100,
				AccessExclusiveLock);
	clustered_pg_pkidx_relopt_tab[i].optname = "target_fillfactor";
	clustered_pg_pkidx_relopt_tab[i].opttype = RELOPT_TYPE_INT;
	clustered_pg_pkidx_relopt_tab[i].offset = offsetof(ClusteredPgIndexOptions, target_fillfactor);
	i++;

	add_real_reloption(clustered_pg_pkidx_relopt_kind,
				"auto_repack_interval",
				"Repack cadence hint for cluster maintenance loop",
				CLUSTERED_PG_DEFAULT_AUTO_REPACK_INTERVAL, 1.0, 3600.0,
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
		.split_threshold = CLUSTERED_PG_DEFAULT_SPLIT_THRESHOLD,
		.target_fillfactor = CLUSTERED_PG_DEFAULT_TARGET_FILLFACTOR,
		.auto_repack_interval = CLUSTERED_PG_DEFAULT_AUTO_REPACK_INTERVAL,
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
	int			i;

	if (scan == NULL || scan->opaque == NULL || state == NULL)
		return false;

	if (state->use_segment_tids)
	{
		if (!state->mark_valid || state->segment_tid_count <= 0)
			return false;
		if (state->segment_tids == NULL)
			return false;

		if (state->mark_at_start)
		{
			state->segment_tid_pos = (direction == ForwardScanDirection) ? 0 : state->segment_tid_count - 1;
			state->segment_tid_direction = direction;
			if (clustered_pg_pkidx_next_segment_tid(state, direction, &scan->xs_heaptid))
			{
				scan->xs_recheck = false;
				return true;
			}
			return false;
		}

		if (!ItemPointerIsValid(&state->mark_tid))
			return false;

		for (i = 0; i < state->segment_tid_count; i++)
		{
			if (ItemPointerEquals(&state->segment_tids[i], &state->mark_tid))
			{
				ItemPointerCopy(&state->segment_tids[i], &scan->xs_heaptid);
				state->segment_tid_direction = direction;
				if (direction == ForwardScanDirection)
					state->segment_tid_pos = i + 1;
				else
					state->segment_tid_pos = i - 1;
				scan->xs_recheck = false;
				return true;
			}
		}

		return false;
	}

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

	if (scan == NULL || scan->indexRelation == NULL)
		return false;

	clustered_pg_stats.scan_getcalls++;

	state = (ClusteredPgPkidxScanState *) scan->opaque;
	if (state == NULL)
		ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("clustered_pk_index scan state is not initialized")));

	if (!state->scan_ready)
	{
		ScanKey	rescan_keys = scan->keyData;
		int		rescan_nkeys = scan->numberOfKeys;

		clustered_pg_pkidx_rescan_internal(scan, rescan_keys, rescan_nkeys,
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

	if (state->use_segment_tids)
		return clustered_pg_pkidx_next_segment_tid(state, direction, &scan->xs_heaptid);

	if (state->table_scan == NULL || state->table_scan_slot == NULL)
	{
		if (!clustered_pg_pkidx_ensure_table_scan(scan, state))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("clustered_pk_index failed to initialize table scan for index tuple lookup")));
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

	if (scan == NULL || scan->indexRelation == NULL || tbm == NULL)
		return 0;

	clustered_pg_stats.scan_getcalls++;

	state = (ClusteredPgPkidxScanState *) scan->opaque;
	if (state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("clustered_pk_index scan state is not initialized")));

	if (!state->scan_ready)
	{
		ScanKey	rescan_keys = scan->keyData;
		int		rescan_nkeys = scan->numberOfKeys;

		clustered_pg_pkidx_rescan_internal(scan, rescan_keys, rescan_nkeys,
								  scan->orderByData, scan->numberOfOrderBys,
								  true);
	}

	if (state->use_segment_tids)
	{
		if (state->segment_tids == NULL || state->segment_tid_count <= 0)
			state->use_segment_tids = false;
		else
		{
			int			i;

			for (i = 0; i < state->segment_tid_count; i++)
			{
				tbm_add_tuples(tbm, &state->segment_tids[i], 1, false);
				rows++;
			}
			return rows;
		}
	}

	if (state->table_scan == NULL || state->table_scan_slot == NULL)
	{
		if (!clustered_pg_pkidx_ensure_table_scan(scan, state))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("clustered_pk_index failed to initialize table scan for bitmap index lookup")));
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

	if (state->use_segment_tids)
	{
		clustered_pg_pkidx_reset_segment_tids(state);

		if (!clustered_pg_pkidx_ensure_table_scan(scan, state))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("clustered_pk_index restore_marked_tuple requires table scan"),
					 errhint("segment_map_tids restore path is unavailable.")));

		scan->xs_recheck = false;
		state->restore_pending = true;
		return;
	}

	if (state->table_scan != NULL)
	{
		if (!clustered_pg_pkidx_ensure_table_scan(scan, state))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("clustered_pk_index restore_marked_tuple requires table scan refresh")));
	}
	else
	{
		ScanKey rescan_keys = scan->keyData;
		int		rescan_nkeys = scan->numberOfKeys;

		if (state->table_scan_keys != NULL && state->key_count > 0)
		{
			rescan_keys = state->table_scan_keys;
			rescan_nkeys = state->key_count;
		}

		clustered_pg_pkidx_rescan_internal(scan, rescan_keys, rescan_nkeys,
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
	int64		target_minor_key = 0;
	bool		use_segment_lookup = false;
	ScanKeyData *source_keys = NULL;
	bool		source_keys_are_state = false;

	(void) orderbys;
	(void) norderbys;

	if (scan == NULL)
		return;
	clustered_pg_stats.scan_rescan_calls++;

	state = (ClusteredPgPkidxScanState *) scan->opaque;
	if (state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("clustered_pk_index scan state is not initialized")));

	if (!preserve_mark)
		clustered_pg_pkidx_reset_mark(state);

	scan->numberOfKeys = nkeys;
	scan->numberOfOrderBys = norderbys;
	if (scan->numberOfKeys > 0)
	{
		if (keys != NULL)
			source_keys = keys;
		else if (state->table_scan_keys != NULL &&
				 state->key_count == scan->numberOfKeys)
		{
			source_keys = state->table_scan_keys;
			source_keys_are_state = true;
		}
	}

	if (state->table_scan != NULL && state->table_scan_key_count != scan->numberOfKeys)
	{
		table_endscan(state->table_scan);
		state->table_scan = NULL;
		state->table_scan_key_count = 0;
	}

	if (scan->xs_snapshot == InvalidSnapshot)
		scan->xs_snapshot = GetTransactionSnapshot();

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

	if (scan->numberOfKeys > 0 && source_keys == NULL)
	{
		/* No stable scan key source available: fallback to heap-only scan */
		if (state->table_scan_keys != NULL)
			pfree(state->table_scan_keys);
		state->table_scan_keys = NULL;
		state->key_count = 0;
		scan->numberOfKeys = 0;
	}
	else if (scan->numberOfKeys == 0 && state->table_scan_keys != NULL)
	{
		pfree(state->table_scan_keys);
		state->table_scan_keys = NULL;
	}
	else if (scan->numberOfKeys > 0)
	{
		if (state->key_count != scan->numberOfKeys)
		{
			if (state->table_scan_keys != NULL)
				pfree(state->table_scan_keys);
			state->table_scan_keys = (ScanKeyData *) palloc0_array(ScanKeyData, scan->numberOfKeys);
		}
		else if (state->table_scan_keys == NULL)
			state->table_scan_keys = (ScanKeyData *) palloc0_array(ScanKeyData, scan->numberOfKeys);
		state->key_count = scan->numberOfKeys;

		for (i = 0; i < scan->numberOfKeys; i++)
		{
			AttrNumber	index_attno = source_keys[i].sk_attno;
			AttrNumber	heap_attno;

			state->table_scan_keys[i] = source_keys[i];

			if (source_keys_are_state)
				continue;

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

			state->table_scan_keys[i].sk_attno = heap_attno;
		}
	}

	state->scan_ready = false;
	state->segment_tid_direction = ForwardScanDirection;
	state->segment_tid_pos = 0;
	state->segment_tid_count = 0;
	clustered_pg_pkidx_free_segment_tids(state);

	use_segment_lookup = (scan->numberOfKeys == 1 &&
						 state->table_scan_keys != NULL &&
						 clustered_pg_pkidx_extract_minor_key_from_scan_key(scan->indexRelation,
																		  &state->table_scan_keys[0],
																		  &target_minor_key));
	if (use_segment_lookup)
	{
		clustered_pg_pkidx_collect_segment_tids(scan->indexRelation,
											   target_minor_key,
											   state,
											   ForwardScanDirection);
		if (state->use_segment_tids)
		{
			state->scan_ready = true;
			state->restore_pending = false;
			return;
		}
	}

	heapRelation = clustered_pg_pkidx_get_heap_relation(scan, state);
	if (heapRelation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_pk_index requires a valid heap relation")));

	if (state->table_scan == NULL)
	{
		if (state->table_scan_slot == NULL)
			state->table_scan_slot = table_slot_create(heapRelation, NULL);
		state->table_scan = table_beginscan(heapRelation, scan->xs_snapshot,
										   state->key_count,
										   state->key_count > 0 ? state->table_scan_keys : NULL);
		state->table_scan_key_count = state->key_count;
	}
	else
		table_rescan(state->table_scan,
					 state->key_count > 0 ? state->table_scan_keys : NULL);

	if (state->table_scan_slot == NULL)
		state->table_scan_slot = table_slot_create(heapRelation, NULL);

	if (state->table_scan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("clustered_pk_index failed to initialize table scan")));
	scan->xs_recheck = false;
	state->restore_pending = false;
	state->scan_ready = true;
}

static IndexScanDesc
clustered_pg_pkidx_beginscan(Relation indexRelation, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(indexRelation, nkeys, norderbys);
	ClusteredPgPkidxScanState *state;

	state = (ClusteredPgPkidxScanState *) palloc0(sizeof(ClusteredPgPkidxScanState));
	state->key_count = nkeys;
	state->table_scan_key_count = nkeys;
	scan->opaque = state;
	return scan;
}

static void __attribute__((noinline))
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
			state->table_scan_key_count = 0;
			pfree(state);
			scan->opaque = NULL;
		}
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
	PG_RETURN_TEXT_P(cstring_to_text("clustered_pg " CLUSTERED_PG_EXTENSION_VERSION));
}

Datum
clustered_pg_observability(PG_FUNCTION_ARGS)
{
	char		text_buf[768];

	clustered_pg_stats.observability_calls++;

	snprintf(text_buf, sizeof(text_buf),
			 "clustered_pg=%s api=%d defaults={split_threshold=%d,target_fillfactor=%d,auto_repack_interval=%.2f} "
			 "counters={observability=%" PRIu64 ",costestimate=%" PRIu64
			 ",segment_rowcount_estimates=%" PRIu64 ",segment_rowcount_errors=%" PRIu64
			 ",segment_lookups=%" PRIu64 ",segment_lookup_errors=%" PRIu64
			 ",index_inserts=%" PRIu64 ",insert_errors=%" PRIu64
			 ",scan_rescans=%" PRIu64 ",scan_getcalls=%" PRIu64
			 ",vacuumcleanup=%" PRIu64 ",rebuilds=%" PRIu64 ",touches=%" PRIu64
			 ",maintenance_errors=%" PRIu64 "}",
			 CLUSTERED_PG_EXTENSION_VERSION,
			 CLUSTERED_PG_OBS_API_VERSION,
			 CLUSTERED_PG_DEFAULT_SPLIT_THRESHOLD,
			 CLUSTERED_PG_DEFAULT_TARGET_FILLFACTOR,
			 CLUSTERED_PG_DEFAULT_AUTO_REPACK_INTERVAL,
			 clustered_pg_stats.observability_calls,
			 clustered_pg_stats.costestimate_calls,
			 clustered_pg_stats.segment_rowcount_estimate_calls,
			 clustered_pg_stats.segment_rowcount_estimate_errors,
			 clustered_pg_stats.segment_map_lookup_calls,
			 clustered_pg_stats.segment_map_lookup_failures,
			 clustered_pg_stats.insert_calls,
			 clustered_pg_stats.insert_errors,
			 clustered_pg_stats.scan_rescan_calls,
			 clustered_pg_stats.scan_getcalls,
			 clustered_pg_stats.vacuumcleanup_calls,
			 clustered_pg_stats.maintenance_rebuild_calls,
			 clustered_pg_stats.maintenance_touch_calls,
			 clustered_pg_stats.maintenance_vacuumcleanup_errors);

	PG_RETURN_TEXT_P(cstring_to_text(text_buf));
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
	int			split_threshold = CLUSTERED_PG_DEFAULT_SPLIT_THRESHOLD;
	int			target_fillfactor = CLUSTERED_PG_DEFAULT_TARGET_FILLFACTOR;
	double		auto_repack_interval = CLUSTERED_PG_DEFAULT_AUTO_REPACK_INTERVAL;
	bool		skip_rebuild_maintenance = false;

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

	skip_rebuild_maintenance = ReindexIsProcessingIndex(RelationGetRelid(indexRelation));

	if (!skip_rebuild_maintenance)
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

	if (!skip_rebuild_maintenance)
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
	int64		major_key;
	int64		locator_minor_key;
	bytea	   *locator = NULL;
	int			split_threshold = CLUSTERED_PG_DEFAULT_SPLIT_THRESHOLD;
	int			target_fillfactor = CLUSTERED_PG_DEFAULT_TARGET_FILLFACTOR;
	double		auto_repack_interval = CLUSTERED_PG_DEFAULT_AUTO_REPACK_INTERVAL;

	(void) heap_tid;
	(void)checkUnique;
	(void)indexUnchanged;
	clustered_pg_stats.insert_calls++;

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

	PG_TRY();
	{
		locator = clustered_pg_pkidx_allocate_locator(heapRelation,
													 minor_key,
													 split_threshold,
													 target_fillfactor,
													 auto_repack_interval);
		if (locator == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("clustered_pg locator allocation returned NULL")));

		clustered_pg_pkidx_lookup_locator_values(locator, &major_key, &locator_minor_key);
		clustered_pg_pkidx_touch_segment_tids(heapRelation,
											 major_key,
											 locator_minor_key,
											 heap_tid);
	}
	PG_CATCH();
	{
		clustered_pg_stats.insert_errors++;
		if (locator != NULL)
			pfree(locator);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (locator != NULL)
	{
		pfree(locator);
		locator = NULL;
	}

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
	char		relation_label[128];
	bool		skip_maintenance = false;
	int			split_threshold = CLUSTERED_PG_DEFAULT_SPLIT_THRESHOLD;
	int			target_fillfactor = CLUSTERED_PG_DEFAULT_TARGET_FILLFACTOR;
	double		auto_repack_interval = CLUSTERED_PG_DEFAULT_AUTO_REPACK_INTERVAL;
	int64		due_segments = 0;

	if (info != NULL && !info->analyze_only && info->index != NULL &&
		info->index->rd_index != NULL)
	{
		clustered_pg_stats.vacuumcleanup_calls++;
		skip_maintenance = ReindexIsProcessingIndex(RelationGetRelid(info->index));
		clustered_pg_format_relation_label(RelationGetRelid(info->index),
										  relation_label,
										  sizeof(relation_label));

		clustered_pg_pkidx_get_index_options(info->index,
											&split_threshold,
											&target_fillfactor,
											&auto_repack_interval);

		PG_TRY();
		{
			if (!skip_maintenance)
			{
				due_segments = clustered_pg_pkidx_count_repack_due(info->index,
																  auto_repack_interval);
				if (due_segments > 0)
				{
					clustered_pg_stats.maintenance_rebuild_calls++;
					clustered_pg_pkidx_rebuild_segment_map(info->index,
														   split_threshold,
														   target_fillfactor,
														   auto_repack_interval);
				}
				else
				{
					clustered_pg_stats.maintenance_touch_calls++;
					clustered_pg_pkidx_touch_repack(info->index);
				}
			}
		}
		PG_CATCH();
		{
			ErrorData  *edata = CopyErrorData();

			FlushErrorState();
			clustered_pg_stats.maintenance_vacuumcleanup_errors++;
			ereport(WARNING,
					(errcode(ERRCODE_WARNING),
					 errmsg("clustered_pk_index maintenance callback failed"),
					 errdetail("operation=vacuum_maintenance relation=%s error=%s",
							   relation_label,
							   edata->message != NULL ? edata->message : "unknown error")));
			FreeErrorData(edata);
		}
		PG_END_TRY();

		PG_TRY();
		{
			/*
			 * Keep segment_map_tids from drifting after VACUUM-removed rows.  This
			 * keeps index scan path cardinality bounded for repeated update/delete
			 * workloads.
			 */
			clustered_pg_pkidx_gc_segment_tids(info->index);
		}
		PG_CATCH();
		{
			ErrorData  *edata = CopyErrorData();

			FlushErrorState();
			clustered_pg_stats.maintenance_vacuumcleanup_errors++;
			ereport(WARNING,
					(errcode(ERRCODE_WARNING),
					 errmsg("clustered_pk_index tid-gc callback failed"),
					 errdetail("operation=segment_map_tids_gc relation=%s error=%s",
							   relation_label,
							   edata->message != NULL ? edata->message : "unknown error")));
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
	GenericCosts costs = {0};
	int64		segment_rows = -1;
	double		est_pages;
	double		est_selectivity;
	IndexOptInfo *index = NULL;
	double		relation_rows = 0.0;
	double		hard_selectivity_floor = 0.0;

	clustered_pg_stats.costestimate_calls++;

	if (root == NULL || path == NULL || path->indexinfo == NULL)
	{
		*startup_cost = 0.0;
		*total_cost = 0.0;
		*selectivity = 1.0;
		*correlation = 0.0;
		*pages = 1.0;
		return;
	}

	index = path->indexinfo;
	if (index->rel == NULL)
	{
		*startup_cost = 0.0;
		*total_cost = 0.0;
		*selectivity = 1.0;
		*correlation = 0.0;
		*pages = 1.0;
		return;
	}

	if (index->rel != NULL)
	{
		relation_rows = Max(index->rel->tuples, 0.0);
		/*
		 * SPI usage inside planner callbacks is currently disabled to avoid
		 * recursive SPI re-entry crashes observed in PL/pgSQL/DO execution paths.
		 * TODO: restore optional async-safe lookup when a robust out-of-band cache
		 * becomes available.
		 */
		segment_rows = -1;
		if (segment_rows < 0)
			segment_rows = -1;
		if (segment_rows > 0 && segment_rows > relation_rows)
			relation_rows = (double) segment_rows;
	}

	genericcostestimate(root, path, loop_count, &costs);

	est_pages = costs.numIndexPages;
	if (est_pages < 1.0)
		est_pages = 1.0;

	est_selectivity = costs.indexSelectivity;
	if (est_selectivity < 0.0)
		est_selectivity = 0.0;
	else if (est_selectivity > 1.0)
		est_selectivity = 1.0;

	if (segment_rows > 0 && relation_rows > 0.0 && path->indexclauses != NIL &&
		est_selectivity < 0.20)
	{
		/*
		 * Segment map can become a stronger row-count signal for highly
		 * selective index predicates by expressing an optimistic floor based
		 * on live segment metadata.
		 */
		hard_selectivity_floor = 1.0 / relation_rows;
		if (hard_selectivity_floor > est_selectivity)
			est_selectivity = hard_selectivity_floor;
	}

	if (path->indexclauses != NULL)
	{
		/*
		 * Favor correlated execution when query predicates anchor to the
		 * leading index key: this mirrors the intended clustered access
		 * pattern used by the extension.
		 */
		*correlation = 0.2 + (1.0 - est_selectivity) * 0.7;
		if (*correlation > 0.95)
			*correlation = 0.95;
	}
	else
	{
		*correlation = 0.0;
	}

	*startup_cost = costs.indexStartupCost;
	*total_cost = costs.indexTotalCost;
	*selectivity = est_selectivity;
	*pages = est_pages;
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
		.amclusterable = true,
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
