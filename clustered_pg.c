#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/reloptions.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
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
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "storage/itemptr.h"
#include "utils/selfuncs.h"
#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>
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
#define CLUSTERED_PG_MAX_SEGMENT_TIDS 131072
#define CLUSTERED_PG_LOCAL_HINT_MAX_KEYS 65536
#define CLUSTERED_PG_LOCAL_HINT_MAX_TIDS_PER_KEY 512
#define CLUSTERED_PG_LOCAL_HINT_EVICT_SCAN_BUDGET 256
#define CLUSTERED_PG_RESCAN_KEYCACHE_TRIGGER 2
#define CLUSTERED_PG_RESCAN_KEYCACHE_MIN_DISTINCT_KEYS 4
#define CLUSTERED_PG_RESCAN_KEYCACHE_MAX_TIDS 262144
#define CLUSTERED_PG_RESCAN_ADAPTIVE_MIN_RESCANS 8
#define CLUSTERED_PG_RESCAN_ADAPTIVE_MIN_DISTINCT_KEYS 2
#define CLUSTERED_PG_RESCAN_ADAPTIVE_MAX_DISTINCT_KEYS 4
#define CLUSTERED_PG_RESCAN_ADAPTIVE_DISTINCT_RESCAN_PCT 90

typedef struct ClusteredPgStats
{
	uint64		observability_calls;
	uint64		costestimate_calls;
	uint64		segment_map_lookup_calls;
	uint64		segment_map_lookup_failures;
	uint64		segment_map_lookup_truncated;
	uint64		scan_fastpath_fallbacks;
	uint64		insert_calls;
	uint64		insert_errors;
	uint64		local_hint_touches;
	uint64		local_hint_merges;
	uint64		local_hint_map_resets;
	uint64		local_hint_evictions;
	uint64		local_hint_stale_resets;
	uint64		rescan_keycache_build_attempts;
	uint64		rescan_keycache_build_successes;
	uint64		rescan_keycache_disables;
	uint64		rescan_keycache_lookup_hits;
	uint64		rescan_keycache_lookup_misses;
	uint64		exact_local_hint_hits;
	uint64		exact_local_hint_misses;
	uint64		rescan_adaptive_sparse_decisions;
	uint64		rescan_adaptive_sparse_bypasses;
	uint64		defensive_state_recovers;
	uint64		scan_rescan_calls;
	uint64		scan_getcalls;
	uint64		vacuumcleanup_calls;
	uint64		maintenance_rebuild_calls;
	uint64		maintenance_touch_calls;
	uint64		maintenance_vacuumcleanup_errors;
} ClusteredPgStats;

static ClusteredPgStats clustered_pg_stats = {0};
static int			clustered_pg_pkidx_max_segment_tids = CLUSTERED_PG_MAX_SEGMENT_TIDS;
static int			clustered_pg_pkidx_segment_prefetch_span = 0;
static int			clustered_pg_pkidx_local_hint_max_keys = CLUSTERED_PG_LOCAL_HINT_MAX_KEYS;
static int			clustered_pg_pkidx_exact_hint_publish_max_keys = 64;
static int			clustered_pg_pkidx_rescan_keycache_trigger = CLUSTERED_PG_RESCAN_KEYCACHE_TRIGGER;
static int			clustered_pg_pkidx_rescan_keycache_min_distinct_keys = CLUSTERED_PG_RESCAN_KEYCACHE_MIN_DISTINCT_KEYS;
static int			clustered_pg_pkidx_rescan_keycache_max_tids = CLUSTERED_PG_RESCAN_KEYCACHE_MAX_TIDS;
static bool			clustered_pg_pkidx_enable_adaptive_sparse_select = false;
static int			clustered_pg_pkidx_adaptive_sparse_min_rescans = CLUSTERED_PG_RESCAN_ADAPTIVE_MIN_RESCANS;
static int			clustered_pg_pkidx_adaptive_sparse_min_distinct_keys = CLUSTERED_PG_RESCAN_ADAPTIVE_MIN_DISTINCT_KEYS;
static int			clustered_pg_pkidx_adaptive_sparse_max_distinct_keys = CLUSTERED_PG_RESCAN_ADAPTIVE_MAX_DISTINCT_KEYS;
static int			clustered_pg_pkidx_adaptive_sparse_distinct_rescan_pct = CLUSTERED_PG_RESCAN_ADAPTIVE_DISTINCT_RESCAN_PCT;
static bool			clustered_pg_pkidx_assume_unique_keys = false;

typedef struct ClusteredLocator
{
	uint64		major_key;
	uint64		minor_key;
} ClusteredLocator;

static bool			clustered_pg_clustered_heapam_initialized = false;
static TableAmRoutine clustered_pg_clustered_heapam_routine;
static bool			clustered_pg_pkidx_enable_segment_fastpath = false;

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
	Relation		private_heap_relation;
	int				key_count;
	int				table_scan_key_count;
	bool			use_segment_tids;
	ItemPointerData	*segment_tids;
	bool			segment_tids_borrowed;
	int				segment_tid_count;
	int				segment_tid_pos;
	uint64			segment_tid_min_key;
	uint64			segment_tid_max_key;
	ScanDirection	segment_tid_direction;
	bool			segment_tids_exact;
	bool			scan_ready;
	bool			mark_valid;
	bool			mark_at_start;
	bool			restore_pending;
	int				rescan_keycache_rescans;
	int				rescan_keycache_distinct_keys;
	bool			rescan_keycache_last_valid;
	int64			rescan_keycache_last_minor_key;
	bool			rescan_keycache_built;
	bool			rescan_keycache_disabled;
	MemoryContext	rescan_keycache_cxt;
	HTAB		   *rescan_keycache_map;
	ItemPointerData	mark_tid;
} ClusteredPgPkidxScanState;

typedef struct ClusteredPgPkidxRescanKeycacheKey
{
	int64		minor_key;
} ClusteredPgPkidxRescanKeycacheKey;

typedef struct ClusteredPgPkidxRescanKeycacheEntry
{
	ClusteredPgPkidxRescanKeycacheKey key;
	int			tid_count;
	int			tid_capacity;
	ItemPointerData *tids;
	uint64		tid_min_key;
	uint64		tid_max_key;
	bool		tid_range_valid;
} ClusteredPgPkidxRescanKeycacheEntry;

typedef struct ClusteredPgPkidxLocalHintKey
{
	Oid			relation_oid;
	int64		minor_key;
} ClusteredPgPkidxLocalHintKey;

typedef struct ClusteredPgPkidxLocalHintEntry
{
	ClusteredPgPkidxLocalHintKey key;
	RelFileNumber	relation_relfilenumber;
	int			tid_count;
	int			tid_capacity;
	ItemPointerData *tids;
	bool		exact;
} ClusteredPgPkidxLocalHintEntry;

/*
 * Zone map for directed placement: maps minor_key -> BlockNumber so that
 * tuple_insert can direct rows with the same clustering key to the same
 * heap block, achieving physical clustering at insertion time.
 */
typedef struct ClusteredPgZoneMapBlockKey
{
	int64		minor_key;
} ClusteredPgZoneMapBlockKey;

typedef struct ClusteredPgZoneMapBlockEntry
{
	ClusteredPgZoneMapBlockKey key;
	BlockNumber	block;
} ClusteredPgZoneMapBlockEntry;

typedef struct ClusteredPgZoneMapRelInfo
{
	Oid			relid;			/* hash key */
	AttrNumber	key_attnum;		/* heap attribute number of clustering key */
	Oid			key_typid;		/* INT2OID, INT4OID, or INT8OID */
	HTAB	   *block_map;		/* minor_key -> BlockNumber */
	bool		initialized;	/* true once clustering index found */
} ClusteredPgZoneMapRelInfo;

static HTAB	   *clustered_pg_zone_map_rels = NULL;
static Oid		clustered_pg_pkidx_am_oid_cache = InvalidOid;

/* Sort helper for multi_insert key grouping */
typedef struct ClusteredPgMultiInsertKeySlot
{
	int64		key;
	int			idx;
	bool		valid;
} ClusteredPgMultiInsertKeySlot;

/* Saved original heap callbacks for delegation */
static void (*clustered_pg_heap_tuple_insert_orig)(Relation rel,
												   TupleTableSlot *slot,
												   CommandId cid,
												   int options,
												   struct BulkInsertStateData *bistate) = NULL;
static void (*clustered_pg_heap_multi_insert_orig)(Relation rel,
												   TupleTableSlot **slots,
												   int nslots,
												   CommandId cid,
												   int options,
												   struct BulkInsertStateData *bistate) = NULL;

static SPIPlanPtr clustered_pg_pkidx_count_repack_due_plan = NULL;
static SPIPlanPtr clustered_pg_pkidx_rebuild_segment_map_plan = NULL;
static SPIPlanPtr clustered_pg_pkidx_segment_tid_lookup_plan = NULL;
static SPIPlanPtr clustered_pg_pkidx_segment_tid_range_lookup_plan = NULL;
static HTAB	   *clustered_pg_pkidx_local_hint_map = NULL;

static const char *clustered_pg_format_relation_label(Oid relationOid,
													 char *buffer,
													 int bufferSize);
static bool clustered_pg_pkidx_int_key_to_int64(Datum value, Oid valueType,
												int64 *minor_key);
static ClusteredPgZoneMapRelInfo *clustered_pg_zone_map_get_relinfo(Relation rel);
static void clustered_pg_zone_map_invalidate(Oid relid);
static void clustered_pg_clustered_heap_tuple_insert(Relation rel,
													TupleTableSlot *slot,
													CommandId cid, int options,
													struct BulkInsertStateData *bistate);
static void clustered_pg_clustered_heap_multi_insert(Relation rel,
													TupleTableSlot **slots,
													int nslots,
													CommandId cid, int options,
													struct BulkInsertStateData *bistate);
static int clustered_pg_multi_insert_key_cmp(const void *a, const void *b);

static void clustered_pg_pack_u64_be(uint8_t *dst, uint64 src);
static uint64 clustered_pg_unpack_u64_be(const uint8_t *src);
static void clustered_pg_pkidx_collect_segment_tids(Relation indexRelation,
												  RelFileNumber relationRelfilenumber,
												  int64 minor_key,
												  ClusteredPgPkidxScanState *state,
												  ScanDirection direction);
static void clustered_pg_pkidx_free_segment_tids(ClusteredPgPkidxScanState *state);
static void clustered_pg_pkidx_touch_local_hint_tid(Oid relationOid,
												   RelFileNumber relationRelfilenumber,
												   int64 minor_key,
												   ItemPointer heap_tid);
static void clustered_pg_pkidx_promote_local_hint_exact_if_single(Oid relationOid,
															  RelFileNumber relationRelfilenumber,
															  int64 minor_key);
static bool clustered_pg_pkidx_append_local_hint_tids(Oid relationOid,
												 RelFileNumber relationRelfilenumber,
												 int64 minor_key,
												 ClusteredPgPkidxScanState *state);
static bool clustered_pg_pkidx_local_hint_is_exact(Oid relationOid,
												 RelFileNumber relationRelfilenumber,
												 int64 minor_key);
static void clustered_pg_pkidx_publish_rescan_keycache_to_local_hints(Relation heapRelation,
															 ClusteredPgPkidxScanState *state,
															 const int64 *minor_keys,
															 int minor_key_count);
static void clustered_pg_pkidx_reset_local_hint_map(void);
static void clustered_pg_pkidx_reset_local_hint_relation(Oid relationOid);
static bool clustered_pg_pkidx_evict_one_local_hint_entry(HTAB *map);
static void clustered_pg_pkidx_remove_stale_local_hint_entry(HTAB *map,
															 ClusteredPgPkidxLocalHintEntry *entry);
#ifdef USE_ASSERT_CHECKING
static bool clustered_pg_pkidx_tids_sorted_unique(const ItemPointerData *tids,
												 int count);
#endif
static int clustered_pg_next_capacity(int current, int initial, int hard_cap);
static void clustered_pg_validate_locator_len(bytea *locator);
static bool clustered_pg_pkidx_next_segment_tid(ClusteredPgPkidxScanState *state,
											  ScanDirection direction, ItemPointer tid);
static bool clustered_pg_pkidx_tid_in_segment_tids(const ClusteredPgPkidxScanState *state,
												 ItemPointer tid);
static void clustered_pg_pkidx_gc_segment_tids(Relation indexRelation);
static bool clustered_pg_pkidx_ensure_table_scan(IndexScanDesc scan,
												ClusteredPgPkidxScanState *state);
static bool clustered_pg_pkidx_insert(Relation indexRelation, Datum *values,
									 bool *isnull, ItemPointer heap_tid,
									 Relation heapRelation,
									 IndexUniqueCheck checkUnique,
									 bool indexUnchanged, IndexInfo *indexInfo);
static CompareType clustered_pg_pkidx_translate_strategy(StrategyNumber strategy,
													 Oid opfamily);
static StrategyNumber clustered_pg_pkidx_translate_cmptype(CompareType cmptype,
													 Oid opfamily);
static Relation clustered_pg_pkidx_get_heap_relation(IndexScanDesc scan,
													ClusteredPgPkidxScanState *state);
static bool clustered_pg_pkidx_restore_marked_tuple(IndexScanDesc scan,
												   ClusteredPgPkidxScanState *state,
												   ScanDirection direction);
static void clustered_pg_pkidx_reset_mark(ClusteredPgPkidxScanState *state);
static void clustered_pg_pkidx_reset_rescan_keycache(ClusteredPgPkidxScanState *state);
static bool clustered_pg_pkidx_should_adaptive_sparse_bypass(const ClusteredPgPkidxScanState *state);
static bool clustered_pg_pkidx_match_unique_key_tids(Relation heapRelation,
												 Snapshot snapshot,
												 AttrNumber heap_attno,
												 Oid atttype,
												 int64 target_minor_key,
												 ClusteredPgPkidxScanState *state);
static void clustered_pg_pkidx_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
									ScanKey orderbys, int norderbys);
static void clustered_pg_pkidx_rescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys,
									ScanKey orderbys, int norderbys,
									bool preserve_mark);
static void clustered_pg_pkidx_reset_segment_tids(ClusteredPgPkidxScanState *state);
static bool clustered_pg_pkidx_build_rescan_keycache(Relation heapRelation,
											  Snapshot snapshot,
											  AttrNumber heap_attno,
											  Oid atttype,
											  ClusteredPgPkidxScanState *state);
static bool clustered_pg_pkidx_load_rescan_keycache_tids(ClusteredPgPkidxScanState *state,
												  int64 minor_key,
												  ScanDirection direction);
static bool clustered_pg_pkidx_extract_array_minor_keys_from_scan_key_type(ScanKey key,
																Oid atttype,
																int64 **minor_keys,
																int *minor_key_count);
static bool clustered_pg_pkidx_load_rescan_keycache_tids_for_keys(ClusteredPgPkidxScanState *state,
														 const int64 *minor_keys,
														 int minor_key_count,
														 ScanDirection direction);
static bool clustered_pg_pkidx_load_exact_local_hint_tids(Oid relationOid,
													 RelFileNumber relationRelfilenumber,
													 ClusteredPgPkidxScanState *state,
													 int64 minor_key,
													 ScanDirection direction);
static bool clustered_pg_pkidx_load_exact_local_hint_tids_for_keys(Oid relationOid,
														 RelFileNumber relationRelfilenumber,
														 ClusteredPgPkidxScanState *state,
														 const int64 *minor_keys,
														 int minor_key_count,
														 ScanDirection direction);
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
	static char result_ring[8][512];
	static int	result_ring_pos = 0;
	int			slot;

	extOid = get_extension_oid("clustered_pg", true);
	if (!OidIsValid(extOid))
		return quote_identifier(name);

	nsOid = get_extension_schema(extOid);
	nsName = get_namespace_name(nsOid);
	if (nsName == NULL)
		return quote_identifier(name);

	slot = result_ring_pos;
	result_ring_pos = (result_ring_pos + 1) % lengthof(result_ring);
	snprintf(result_ring[slot], sizeof(result_ring[slot]), "%s.%s",
			 quote_identifier(nsName), quote_identifier(name));
	return result_ring[slot];
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

	clustered_pg_pkidx_reset_local_hint_relation(relationOid);

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
	clustered_pg_zone_map_invalidate(RelationGetRelid(rel));
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
	clustered_pg_zone_map_invalidate(RelationGetRelid(rel));
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
	clustered_pg_zone_map_invalidate(RelationGetRelid(rel));
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
	clustered_pg_zone_map_invalidate(RelationGetRelid(OldTable));
}

/* Maximum distinct keys tracked per relation before resetting zone map */
#define CLUSTERED_PG_ZONE_MAP_MAX_KEYS 1048576

/* ----------------------------------------------------------------
 * Zone map: directed placement for physical clustering at INSERT time.
 *
 * On first tuple_insert for a relation, discover the clustered_pk_index
 * (if any), extract the key column number, and build an in-memory
 * minor_key -> BlockNumber map.  For each subsequent insert, look up
 * the key in the zone map and hint the heap via RelationSetTargetBlock.
 * ----------------------------------------------------------------
 */

/*
 * Invalidate zone map for a relation.  Called from lifecycle hooks
 * (truncate, new filelocator, copy_data, copy_for_cluster) to prevent
 * stale block references after physical storage changes.
 */
static void
clustered_pg_zone_map_invalidate(Oid relid)
{
	ClusteredPgZoneMapRelInfo *info;

	if (clustered_pg_zone_map_rels == NULL)
		return;

	info = hash_search(clustered_pg_zone_map_rels, &relid, HASH_FIND, NULL);
	if (info != NULL)
	{
		if (info->block_map != NULL)
			hash_destroy(info->block_map);
		info->block_map = NULL;
		info->initialized = false;
		hash_search(clustered_pg_zone_map_rels, &relid, HASH_REMOVE, NULL);
	}
}

static Oid
clustered_pg_get_pkidx_am_oid(void)
{
	if (!OidIsValid(clustered_pg_pkidx_am_oid_cache))
		clustered_pg_pkidx_am_oid_cache = get_am_oid("clustered_pk_index", true);
	return clustered_pg_pkidx_am_oid_cache;
}

static ClusteredPgZoneMapRelInfo *
clustered_pg_zone_map_get_relinfo(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	ClusteredPgZoneMapRelInfo *info;
	bool		found;

	/* Create top-level hash on first call */
	if (clustered_pg_zone_map_rels == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(ClusteredPgZoneMapRelInfo);
		clustered_pg_zone_map_rels = hash_create("clustered_pg zone map rels",
												 16, &ctl,
												 HASH_ELEM | HASH_BLOBS);
	}

	info = hash_search(clustered_pg_zone_map_rels, &relid, HASH_ENTER, &found);
	if (!found)
	{
		info->relid = relid;
		info->key_attnum = InvalidAttrNumber;
		info->key_typid = InvalidOid;
		info->block_map = NULL;
		info->initialized = false;
	}

	if (!info->initialized)
	{
		Oid			pkidx_am = clustered_pg_get_pkidx_am_oid();
		List	   *indexlist;
		ListCell   *lc;

		if (!OidIsValid(pkidx_am))
			return info;

		indexlist = RelationGetIndexList(rel);
		foreach(lc, indexlist)
		{
			Oid			indexoid = lfirst_oid(lc);
			Relation	indexrel = index_open(indexoid, AccessShareLock);

			if (indexrel->rd_rel->relam == pkidx_am &&
				indexrel->rd_index->indnatts >= 1)
			{
				AttrNumber	heap_attnum = indexrel->rd_index->indkey.values[0];
				TupleDesc	idxdesc = RelationGetDescr(indexrel);

				if (heap_attnum > 0 && idxdesc->natts > 0)
				{
					HASHCTL		ctl;

					info->key_attnum = heap_attnum;
					info->key_typid = TupleDescAttr(idxdesc, 0)->atttypid;

					memset(&ctl, 0, sizeof(ctl));
					ctl.keysize = sizeof(ClusteredPgZoneMapBlockKey);
					ctl.entrysize = sizeof(ClusteredPgZoneMapBlockEntry);
					info->block_map = hash_create("clustered_pg zone block map",
												  256, &ctl,
												  HASH_ELEM | HASH_BLOBS);
					info->initialized = true;
				}
				index_close(indexrel, AccessShareLock);
				break;
			}
			index_close(indexrel, AccessShareLock);
		}
		list_free(indexlist);
	}

	return info;
}

static void
clustered_pg_clustered_heap_tuple_insert(Relation rel, TupleTableSlot *slot,
										 CommandId cid, int options,
										 struct BulkInsertStateData *bistate)
{
	ClusteredPgZoneMapRelInfo *relinfo;
	int64		minor_key = 0;
	bool		key_valid = false;

	relinfo = clustered_pg_zone_map_get_relinfo(rel);

	if (relinfo != NULL && relinfo->initialized)
	{
		Datum	val;
		bool	isnull;

		val = slot_getattr(slot, relinfo->key_attnum, &isnull);
		if (!isnull &&
			clustered_pg_pkidx_int_key_to_int64(val, relinfo->key_typid,
												&minor_key))
		{
			ClusteredPgZoneMapBlockKey mapkey;
			ClusteredPgZoneMapBlockEntry *entry;

			mapkey.minor_key = minor_key;
			entry = hash_search(relinfo->block_map, &mapkey, HASH_FIND, NULL);
			if (entry != NULL)
				RelationSetTargetBlock(rel, entry->block);

			key_valid = true;
		}
	}

	/* Delegate to standard heap insert */
	clustered_pg_heap_tuple_insert_orig(rel, slot, cid, options, bistate);

	/* Record actual placement in zone map */
	if (key_valid && relinfo != NULL && relinfo->block_map != NULL)
	{
		BlockNumber		actual_block = ItemPointerGetBlockNumber(&slot->tts_tid);
		ClusteredPgZoneMapBlockKey mapkey;
		ClusteredPgZoneMapBlockEntry *entry;
		bool			found;

		/* Overflow guard: reset if too many distinct keys tracked */
		if (hash_get_num_entries(relinfo->block_map) >=
			CLUSTERED_PG_ZONE_MAP_MAX_KEYS)
		{
			hash_destroy(relinfo->block_map);
			{
				HASHCTL		ctl;

				memset(&ctl, 0, sizeof(ctl));
				ctl.keysize = sizeof(ClusteredPgZoneMapBlockKey);
				ctl.entrysize = sizeof(ClusteredPgZoneMapBlockEntry);
				relinfo->block_map = hash_create("clustered_pg zone block map",
												 256, &ctl,
												 HASH_ELEM | HASH_BLOBS);
			}
		}

		mapkey.minor_key = minor_key;
		entry = hash_search(relinfo->block_map, &mapkey, HASH_ENTER, &found);
		entry->block = actual_block;
	}
}

static int
clustered_pg_multi_insert_key_cmp(const void *a, const void *b)
{
	const ClusteredPgMultiInsertKeySlot *ka = (const ClusteredPgMultiInsertKeySlot *) a;
	const ClusteredPgMultiInsertKeySlot *kb = (const ClusteredPgMultiInsertKeySlot *) b;

	/* Invalid keys sort to end */
	if (!ka->valid && !kb->valid) return 0;
	if (!ka->valid) return 1;
	if (!kb->valid) return -1;

	if (ka->key < kb->key) return -1;
	if (ka->key > kb->key) return 1;
	return 0;
}

/*
 * Threshold: if a multi_insert batch has more than this many distinct keys,
 * skip sort+group (too expensive) and fall back to lightweight placement
 * that just sets target for the first slot and records all placements.
 */
#define CLUSTERED_PG_MULTI_INSERT_GROUP_THRESHOLD 64

static void
clustered_pg_clustered_heap_multi_insert(Relation rel, TupleTableSlot **slots,
										 int nslots, CommandId cid, int options,
										 struct BulkInsertStateData *bistate)
{
	ClusteredPgZoneMapRelInfo *relinfo;
	ClusteredPgMultiInsertKeySlot *ks;
	TupleTableSlot **sorted_slots;
	int			pos;
	int			i;
	int			distinct_keys;
	int64		prev_key;
	bool		prev_valid;

	relinfo = clustered_pg_zone_map_get_relinfo(rel);

	/* No directed placement possible: delegate directly */
	if (relinfo == NULL || !relinfo->initialized || nslots <= 0)
	{
		clustered_pg_heap_multi_insert_orig(rel, slots, nslots,
											cid, options, bistate);
		return;
	}

	/* Extract clustering key from every slot and count distinct keys */
	ks = palloc(nslots * sizeof(ClusteredPgMultiInsertKeySlot));
	distinct_keys = 0;
	prev_key = 0;
	prev_valid = false;

	for (i = 0; i < nslots; i++)
	{
		Datum	val;
		bool	isnull;

		ks[i].idx = i;
		val = slot_getattr(slots[i], relinfo->key_attnum, &isnull);
		if (!isnull &&
			clustered_pg_pkidx_int_key_to_int64(val, relinfo->key_typid,
												&ks[i].key))
			ks[i].valid = true;
		else
		{
			ks[i].key = 0;
			ks[i].valid = false;
		}

		/* Approximate distinct key count (exact would need a hash) */
		if (ks[i].valid && (!prev_valid || ks[i].key != prev_key))
		{
			distinct_keys++;
			prev_key = ks[i].key;
			prev_valid = true;
		}
	}

	/*
	 * Fast path: if too many distinct keys in this batch, skip sort+group.
	 * Just hint with the first valid key and insert in one call.
	 * The zone map still records placements for future batches.
	 */
	if (distinct_keys > CLUSTERED_PG_MULTI_INSERT_GROUP_THRESHOLD)
	{
		/* Set target block for first valid key */
		for (i = 0; i < nslots; i++)
		{
			if (ks[i].valid)
			{
				ClusteredPgZoneMapBlockKey mapkey;
				ClusteredPgZoneMapBlockEntry *entry;

				mapkey.minor_key = ks[i].key;
				entry = hash_search(relinfo->block_map, &mapkey,
									HASH_FIND, NULL);
				if (entry != NULL)
					RelationSetTargetBlock(rel, entry->block);
				break;
			}
		}

		clustered_pg_heap_multi_insert_orig(rel, slots, nslots,
											cid, options, bistate);

		/*
		 * Record placements efficiently: sort ks by key (lightweight
		 * 12-byte elements), then record only one representative slot
		 * per distinct key.  This reduces hash_search calls from nslots
		 * to distinct_keys.
		 */
		if (relinfo->block_map != NULL)
		{
			qsort(ks, nslots, sizeof(ClusteredPgMultiInsertKeySlot),
				  clustered_pg_multi_insert_key_cmp);

			for (i = 0; i < nslots; )
			{
				int64	key = ks[i].key;
				bool	valid = ks[i].valid;
				int		last_idx = ks[i].idx;

				while (i < nslots &&
					   ks[i].valid == valid &&
					   (!valid || ks[i].key == key))
				{
					last_idx = ks[i].idx;
					i++;
				}

				if (valid)
				{
					BlockNumber blk;
					ClusteredPgZoneMapBlockKey mk;
					ClusteredPgZoneMapBlockEntry *e;
					bool	found;

					blk = ItemPointerGetBlockNumber(&slots[last_idx]->tts_tid);
					mk.minor_key = key;
					e = hash_search(relinfo->block_map, &mk,
									HASH_ENTER, &found);
					e->block = blk;
				}
			}
		}

		pfree(ks);
		return;
	}

	/* Sort by key so same-key slots are adjacent */
	qsort(ks, nslots, sizeof(ClusteredPgMultiInsertKeySlot),
		  clustered_pg_multi_insert_key_cmp);

	/* Build reordered slot pointer array */
	sorted_slots = palloc(nslots * sizeof(TupleTableSlot *));
	for (i = 0; i < nslots; i++)
		sorted_slots[i] = slots[ks[i].idx];

	/* Process one key group at a time */
	pos = 0;
	while (pos < nslots)
	{
		int		group_start = pos;
		int64	group_key = ks[pos].key;
		bool	group_valid = ks[pos].valid;
		int		group_size;

		while (pos < nslots &&
			   ks[pos].valid == group_valid &&
			   (!group_valid || ks[pos].key == group_key))
			pos++;

		group_size = pos - group_start;

		if (group_valid)
		{
			ClusteredPgZoneMapBlockKey mapkey;
			ClusteredPgZoneMapBlockEntry *entry;

			mapkey.minor_key = group_key;
			entry = hash_search(relinfo->block_map, &mapkey,
								HASH_FIND, NULL);

			/* Release bistate buffer pin so target block takes effect */
			if (bistate != NULL)
				ReleaseBulkInsertStatePin(bistate);

			if (entry != NULL)
				RelationSetTargetBlock(rel, entry->block);
		}

		clustered_pg_heap_multi_insert_orig(rel, sorted_slots + group_start,
											group_size, cid, options, bistate);

		/* Record last-used block for this key in zone map */
		if (group_valid && relinfo->block_map != NULL)
		{
			BlockNumber last_block;

			last_block = ItemPointerGetBlockNumber(
				&sorted_slots[group_start + group_size - 1]->tts_tid);

			if (BlockNumberIsValid(last_block))
			{
				ClusteredPgZoneMapBlockKey mk;
				ClusteredPgZoneMapBlockEntry *e;
				bool	found;

				mk.minor_key = group_key;
				e = hash_search(relinfo->block_map, &mk,
								HASH_ENTER, &found);
				e->block = last_block;
			}
		}
	}

	pfree(sorted_slots);
	pfree(ks);
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

	/* Directed placement: override insert paths to steer rows by key */
	clustered_pg_heap_tuple_insert_orig = heap->tuple_insert;
	clustered_pg_clustered_heapam_routine.tuple_insert =
		clustered_pg_clustered_heap_tuple_insert;

	clustered_pg_heap_multi_insert_orig = heap->multi_insert;
	clustered_pg_clustered_heapam_routine.multi_insert =
		clustered_pg_clustered_heap_multi_insert;

	clustered_pg_clustered_heapam_initialized = true;
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
clustered_pg_pkidx_segment_tid_lookup_plan_init(void)
{
	Oid			argtypes[3];
	char        query[1024];

	if (clustered_pg_pkidx_segment_tid_lookup_plan != NULL)
		return clustered_pg_pkidx_segment_tid_lookup_plan;

	argtypes[0] = OIDOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT4OID;

	snprintf(query, sizeof(query),
			 "SELECT tuple_tid FROM %s "
			 "WHERE relation_oid = $1::oid AND minor_key = $2::bigint "
			 "ORDER BY tuple_tid "
			 "LIMIT $3::integer",
			 clustered_pg_qualified_extension_name("segment_map_tids"));

	clustered_pg_pkidx_segment_tid_lookup_plan = SPI_prepare(query, 3, argtypes);
	if (clustered_pg_pkidx_segment_tid_lookup_plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to prepare clustered_pg_sql plan for segment_map_tids lookup"),
				 errhint("Inspect clustered_pg extension schema and function visibility.")));
	SPI_keepplan(clustered_pg_pkidx_segment_tid_lookup_plan);

	return clustered_pg_pkidx_segment_tid_lookup_plan;
}

static SPIPlanPtr
clustered_pg_pkidx_segment_tid_range_lookup_plan_init(void)
{
	Oid			argtypes[4];
	char        query[1024];

	if (clustered_pg_pkidx_segment_tid_range_lookup_plan != NULL)
		return clustered_pg_pkidx_segment_tid_range_lookup_plan;

	argtypes[0] = OIDOID;
	argtypes[1] = INT8OID;
	argtypes[2] = INT8OID;
	argtypes[3] = INT4OID;

	snprintf(query, sizeof(query),
			 "SELECT minor_key, tuple_tid FROM %s "
			 "WHERE relation_oid = $1::oid "
			 "AND minor_key >= $2::bigint AND minor_key <= $3::bigint "
			 "ORDER BY minor_key, tuple_tid "
			 "LIMIT $4::integer",
			 clustered_pg_qualified_extension_name("segment_map_tids"));

	clustered_pg_pkidx_segment_tid_range_lookup_plan = SPI_prepare(query, 4, argtypes);
	if (clustered_pg_pkidx_segment_tid_range_lookup_plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to prepare clustered_pg_sql plan for segment_map_tids range lookup"),
				 errhint("Inspect clustered_pg extension schema and function visibility.")));
	SPI_keepplan(clustered_pg_pkidx_segment_tid_range_lookup_plan);

	return clustered_pg_pkidx_segment_tid_range_lookup_plan;
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

	PG_TRY();
	{
		plan = clustered_pg_pkidx_count_repack_due_plan_init();
		if (plan == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Unable to access SPI plan for segment_map_count_repack_due")));

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

	PG_TRY();
	{
		plan = clustered_pg_pkidx_rebuild_segment_map_plan_init();
		if (plan == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Unable to access SPI plan for segment_map_rebuild_from_index")));

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
clustered_pg_pkidx_extract_minor_key_from_scan_key_type(ScanKey key, Oid atttype,
												 int64 *minor_key)
{
	if (key == NULL || minor_key == NULL)
		return false;
	if (key->sk_flags & SK_ISNULL)
		return false;
	if (key->sk_flags & (SK_SEARCHARRAY | SK_SEARCHNULL | SK_SEARCHNOTNULL |
						SK_ROW_HEADER | SK_ROW_MEMBER | SK_ROW_END))
		return false;
	if (key->sk_strategy != BTEqualStrategyNumber)
		return false;

	return clustered_pg_pkidx_int_key_to_int64(key->sk_argument, atttype, minor_key);
}

static int
clustered_pg_int64_qsort_cmp(const void *lhs, const void *rhs)
{
	int64		a = *((const int64 *) lhs);
	int64		b = *((const int64 *) rhs);

	if (a < b)
		return -1;
	if (a > b)
		return 1;
	return 0;
}

static bool
clustered_pg_pkidx_extract_array_minor_keys_from_scan_key_type(ScanKey key, Oid atttype,
																int64 **minor_keys,
																int *minor_key_count)
{
	ArrayType  *arr;
	Oid			elemtype;
	int16		typlen = 0;
	bool		typbyval = true;
	char		typalign = 'i';
	Datum	   *elem_datums = NULL;
	bool	   *elem_nulls = NULL;
	int			elem_count = 0;
	int64	   *keys = NULL;
	int			valid_count = 0;
	int			i;

	if (minor_keys == NULL || minor_key_count == NULL)
		return false;
	*minor_keys = NULL;
	*minor_key_count = 0;

	if (key == NULL)
		return false;
	if (key->sk_flags & SK_ISNULL)
		return false;
	if (!(key->sk_flags & SK_SEARCHARRAY))
		return false;
	if (key->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL |
						SK_ROW_HEADER | SK_ROW_MEMBER | SK_ROW_END))
		return false;
	if (key->sk_strategy != BTEqualStrategyNumber)
		return false;

	switch (atttype)
	{
		case INT2OID:
			typlen = sizeof(int16);
			typbyval = true;
			typalign = 's';
			break;
		case INT4OID:
			typlen = sizeof(int32);
			typbyval = true;
			typalign = 'i';
			break;
		case INT8OID:
			typlen = sizeof(int64);
			typbyval = FLOAT8PASSBYVAL;
			typalign = 'd';
			break;
		default:
			return false;
	}

	arr = DatumGetArrayTypeP(key->sk_argument);
	elemtype = ARR_ELEMTYPE(arr);
	if (elemtype != atttype)
		return false;

	if (ARR_NDIM(arr) == 0)
		return true;

	deconstruct_array(arr, elemtype, typlen, typbyval, typalign,
					  &elem_datums, &elem_nulls, &elem_count);
	if (elem_count <= 0)
	{
		if (elem_datums != NULL)
			pfree(elem_datums);
		if (elem_nulls != NULL)
			pfree(elem_nulls);
		return true;
	}

	keys = (int64 *) palloc_array(int64, elem_count);
	for (i = 0; i < elem_count; i++)
	{
		int64		minor_key = 0;

		if (elem_nulls != NULL && elem_nulls[i])
			continue;
		if (!clustered_pg_pkidx_int_key_to_int64(elem_datums[i], atttype, &minor_key))
			continue;
		keys[valid_count++] = minor_key;
	}

	if (elem_datums != NULL)
		pfree(elem_datums);
	if (elem_nulls != NULL)
		pfree(elem_nulls);

	if (valid_count <= 0)
	{
		pfree(keys);
		return true;
	}

	qsort(keys, valid_count, sizeof(int64), clustered_pg_int64_qsort_cmp);
	if (valid_count > 1)
	{
		int			unique_count = 1;
		int64		last = keys[0];

		for (i = 1; i < valid_count; i++)
		{
			if (keys[i] == last)
				continue;
			keys[unique_count++] = keys[i];
			last = keys[i];
		}
		valid_count = unique_count;
	}

	*minor_keys = keys;
	*minor_key_count = valid_count;
	return true;
}

static inline void
clustered_pg_pkidx_release_segment_tids_buffer(ClusteredPgPkidxScanState *state)
{
	if (state == NULL)
		return;

	if (state->segment_tids != NULL && !state->segment_tids_borrowed)
		pfree(state->segment_tids);

	state->segment_tids = NULL;
	state->segment_tids_borrowed = false;
}

static void
clustered_pg_pkidx_free_segment_tids(ClusteredPgPkidxScanState *state)
{
	if (state == NULL)
		return;

	clustered_pg_pkidx_release_segment_tids_buffer(state);

	state->segment_tid_count = 0;
	state->segment_tid_pos = 0;
	state->segment_tid_min_key = 0;
	state->segment_tid_max_key = 0;
	state->use_segment_tids = false;
	state->segment_tids_exact = false;
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

static int
clustered_pg_next_capacity(int current, int initial, int hard_cap)
{
	int next;
	int alloc_cap;

	if (initial < 1)
		initial = 1;
	if (hard_cap < 1)
		hard_cap = 1;

	/*
	 * Ensure hard_cap stays within a safe allocation bound so that
	 * callers doing sizeof(ItemPointerData) * capacity cannot overflow.
	 */
	alloc_cap = INT_MAX / (int) sizeof(ItemPointerData);
	if (hard_cap > alloc_cap)
		hard_cap = alloc_cap;

	if (current <= 0)
		next = initial;
	else if (current > hard_cap / 2)
		next = hard_cap;
	else
		next = current * 2;

	if (next > hard_cap)
		next = hard_cap;

	return next;
}

static inline uint64
clustered_pg_itemptr_sortkey(const ItemPointerData *tid)
{
	return (((uint64) ItemPointerGetBlockNumber(tid)) << 16) |
		(uint64) ItemPointerGetOffsetNumber(tid);
}

static int
clustered_pg_itemptr_qsort_cmp(const void *lhs, const void *rhs)
{
	const ItemPointerData *a = (const ItemPointerData *) lhs;
	const ItemPointerData *b = (const ItemPointerData *) rhs;
	uint64		ak = clustered_pg_itemptr_sortkey(a);
	uint64		bk = clustered_pg_itemptr_sortkey(b);

	if (ak < bk)
		return -1;
	if (ak > bk)
		return 1;
	return 0;
}

#ifdef USE_ASSERT_CHECKING
static bool
clustered_pg_pkidx_tids_sorted_unique(const ItemPointerData *tids, int count)
{
	int			i;
	uint64		prev_key;

	if (count < 0)
		return false;
	if (count == 0)
		return true;
	if (tids == NULL)
		return false;
	if (count == 1)
		return true;

	prev_key = clustered_pg_itemptr_sortkey(&tids[0]);
	for (i = 1; i < count; i++)
	{
		uint64		cur_key = clustered_pg_itemptr_sortkey(&tids[i]);

		if (cur_key <= prev_key)
			return false;
		prev_key = cur_key;
	}

	return true;
}
#endif

static HTAB *
clustered_pg_pkidx_get_local_hint_map(void)
{
	HASHCTL	ctl;

	if (clustered_pg_pkidx_local_hint_map != NULL)
		return clustered_pg_pkidx_local_hint_map;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ClusteredPgPkidxLocalHintKey);
	ctl.entrysize = sizeof(ClusteredPgPkidxLocalHintEntry);
	ctl.hcxt = TopMemoryContext;

	clustered_pg_pkidx_local_hint_map =
		hash_create("clustered_pg local tid hints",
					Max(1, clustered_pg_pkidx_local_hint_max_keys),
					&ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	return clustered_pg_pkidx_local_hint_map;
}

static void
clustered_pg_pkidx_reset_local_hint_map(void)
{
	HTAB	   *map = clustered_pg_pkidx_local_hint_map;
	HASH_SEQ_STATUS seq;
	ClusteredPgPkidxLocalHintEntry *entry;

	if (map == NULL)
		return;

	hash_seq_init(&seq, map);
	while ((entry = (ClusteredPgPkidxLocalHintEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (entry->tids != NULL)
		{
			pfree(entry->tids);
			entry->tids = NULL;
		}
		entry->tid_count = 0;
		entry->tid_capacity = 0;
		entry->exact = false;
	}

	hash_destroy(map);
	clustered_pg_pkidx_local_hint_map = NULL;
	clustered_pg_stats.local_hint_map_resets++;
}

static void
clustered_pg_pkidx_reset_local_hint_relation(Oid relationOid)
{
	HTAB	   *map = clustered_pg_pkidx_local_hint_map;
	HASH_SEQ_STATUS seq;
	ClusteredPgPkidxLocalHintEntry *entry;
	ClusteredPgPkidxLocalHintKey *keys = NULL;
	int			key_count = 0;
	int			key_capacity = 0;
	int			i;

	if (map == NULL || !OidIsValid(relationOid))
		return;

	hash_seq_init(&seq, map);
	while ((entry = (ClusteredPgPkidxLocalHintEntry *) hash_seq_search(&seq)) != NULL)
	{
		ClusteredPgPkidxLocalHintKey *new_keys;
		int			new_capacity;
		int			max_keys;

		if (entry->key.relation_oid != relationOid)
			continue;

		if (entry->tids != NULL)
		{
			pfree(entry->tids);
			entry->tids = NULL;
		}
		entry->tid_count = 0;
		entry->tid_capacity = 0;
		entry->exact = false;

		if (key_count >= key_capacity)
		{
			max_keys = Max(1, clustered_pg_pkidx_local_hint_max_keys);
			new_capacity = clustered_pg_next_capacity(key_capacity, 16, max_keys);
			if (new_capacity <= key_capacity)
			{
				hash_seq_term(&seq);
				clustered_pg_pkidx_reset_local_hint_map();
				return;
			}
			if (keys == NULL)
				new_keys = (ClusteredPgPkidxLocalHintKey *)
					palloc_array(ClusteredPgPkidxLocalHintKey, new_capacity);
			else
				new_keys = (ClusteredPgPkidxLocalHintKey *)
					repalloc(keys, sizeof(ClusteredPgPkidxLocalHintKey) * new_capacity);

			keys = new_keys;
			key_capacity = new_capacity;
		}

		keys[key_count++] = entry->key;
	}

	for (i = 0; i < key_count; i++)
	{
		bool		found = false;

		(void) hash_search(map, &keys[i], HASH_REMOVE, &found);
	}

	if (keys != NULL)
		pfree(keys);

	if (key_count > 0)
		clustered_pg_stats.local_hint_stale_resets += (uint64) key_count;

	if (hash_get_num_entries(map) == 0)
	{
		hash_destroy(map);
		clustered_pg_pkidx_local_hint_map = NULL;
		clustered_pg_stats.local_hint_map_resets++;
	}
}

static void
clustered_pg_pkidx_free_local_hint_entry_payload(ClusteredPgPkidxLocalHintEntry *entry)
{
	if (entry == NULL)
		return;

	if (entry->tids != NULL)
	{
		pfree(entry->tids);
		entry->tids = NULL;
	}
	entry->tid_count = 0;
	entry->tid_capacity = 0;
	entry->exact = false;
}

static void
clustered_pg_pkidx_remove_stale_local_hint_entry(HTAB *map,
												  ClusteredPgPkidxLocalHintEntry *entry)
{
	ClusteredPgPkidxLocalHintKey key;
	bool		found = false;

	if (map == NULL || entry == NULL)
		return;

	key = entry->key;
	clustered_pg_pkidx_free_local_hint_entry_payload(entry);
	(void) hash_search(map, &key, HASH_REMOVE, &found);
	if (!found)
		return;

	clustered_pg_stats.local_hint_stale_resets++;
	if (map == clustered_pg_pkidx_local_hint_map &&
		hash_get_num_entries(map) == 0)
	{
		hash_destroy(map);
		clustered_pg_pkidx_local_hint_map = NULL;
		clustered_pg_stats.local_hint_map_resets++;
	}
}

static bool
clustered_pg_pkidx_evict_one_local_hint_entry(HTAB *map)
{
	HASH_SEQ_STATUS seq;
	ClusteredPgPkidxLocalHintEntry *entry;
	ClusteredPgPkidxLocalHintEntry *fallback_entry = NULL;
	ClusteredPgPkidxLocalHintKey key;
	bool		found = false;
	bool		terminated_early = false;
	int			scanned = 0;
	int			scan_budget = CLUSTERED_PG_LOCAL_HINT_EVICT_SCAN_BUDGET;

	if (map == NULL || hash_get_num_entries(map) <= 0)
		return false;

	hash_seq_init(&seq, map);
	while ((entry = (ClusteredPgPkidxLocalHintEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (fallback_entry == NULL)
			fallback_entry = entry;
		if (!entry->exact)
		{
			terminated_early = true;
			break;
		}

		scanned++;
		if (scanned >= scan_budget)
		{
			entry = fallback_entry;
			terminated_early = true;
			break;
		}
	}
	if (entry == NULL)
		entry = fallback_entry;
	if (entry == NULL)
		return false;
	key = entry->key;
	clustered_pg_pkidx_free_local_hint_entry_payload(entry);
	if (terminated_early)
		hash_seq_term(&seq);

	(void) hash_search(map, &key, HASH_REMOVE, &found);
	if (found)
		clustered_pg_stats.local_hint_evictions++;

	return found;
}

static void
clustered_pg_pkidx_touch_local_hint_tid(Oid relationOid,
									 RelFileNumber relationRelfilenumber,
									 int64 minor_key,
									 ItemPointer heap_tid)
{
	HTAB	   *map;
	ClusteredPgPkidxLocalHintKey key;
	ClusteredPgPkidxLocalHintEntry *entry;
	bool		found = false;
	int			max_tids;

	if (!OidIsValid(relationOid))
		return;
	if (relationRelfilenumber == InvalidRelFileNumber)
		return;
	if (heap_tid == NULL || !ItemPointerIsValid(heap_tid))
		return;

	map = clustered_pg_pkidx_get_local_hint_map();
	if (map == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.relation_oid = relationOid;
	key.minor_key = minor_key;

	entry = (ClusteredPgPkidxLocalHintEntry *) hash_search(map, &key, HASH_FIND, &found);
	if (!found || entry == NULL)
	{
		long num_entries = hash_get_num_entries(map);

		if (num_entries >= Max(1, clustered_pg_pkidx_local_hint_max_keys))
		{
			if (!clustered_pg_pkidx_evict_one_local_hint_entry(map))
			{
				clustered_pg_pkidx_reset_local_hint_map();
				map = clustered_pg_pkidx_get_local_hint_map();
				if (map == NULL)
					return;
			}
		}

		entry = (ClusteredPgPkidxLocalHintEntry *) hash_search(map, &key, HASH_ENTER, &found);
		if (entry == NULL)
			return;
		entry->relation_relfilenumber = relationRelfilenumber;
		entry->tid_count = 0;
		entry->tid_capacity = 0;
		entry->tids = NULL;
		entry->exact = false;
	}
	else if (entry->relation_relfilenumber != relationRelfilenumber)
	{
		entry->relation_relfilenumber = relationRelfilenumber;
		clustered_pg_pkidx_free_local_hint_entry_payload(entry);
		clustered_pg_stats.local_hint_stale_resets++;
	}

	entry->exact = false;

	max_tids = clustered_pg_pkidx_max_segment_tids;
	if (max_tids > CLUSTERED_PG_LOCAL_HINT_MAX_TIDS_PER_KEY)
		max_tids = CLUSTERED_PG_LOCAL_HINT_MAX_TIDS_PER_KEY;
	if (max_tids < 1)
		max_tids = 1;
	if (entry->tid_count >= max_tids)
		return;

	if (entry->tid_capacity <= entry->tid_count)
	{
		int			new_capacity = clustered_pg_next_capacity(entry->tid_capacity, 8, max_tids);
		MemoryContext oldcxt;

		if (new_capacity <= entry->tid_capacity)
			return;

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		if (entry->tids == NULL)
			entry->tids = (ItemPointerData *) palloc_array(ItemPointerData, new_capacity);
		else
			entry->tids = (ItemPointerData *) repalloc(entry->tids,
													   sizeof(ItemPointerData) * new_capacity);
		MemoryContextSwitchTo(oldcxt);
		entry->tid_capacity = new_capacity;
	}

	ItemPointerCopy(heap_tid, &entry->tids[entry->tid_count]);
	entry->tid_count++;
	clustered_pg_stats.local_hint_touches++;
}

static void
clustered_pg_pkidx_promote_local_hint_exact_if_single(Oid relationOid,
													   RelFileNumber relationRelfilenumber,
													   int64 minor_key)
{
	HTAB	   *map;
	ClusteredPgPkidxLocalHintKey key;
	ClusteredPgPkidxLocalHintEntry *entry;
	bool		found = false;

	if (!OidIsValid(relationOid))
		return;
	if (relationRelfilenumber == InvalidRelFileNumber)
		return;

	map = clustered_pg_pkidx_local_hint_map;
	if (map == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.relation_oid = relationOid;
	key.minor_key = minor_key;
	entry = (ClusteredPgPkidxLocalHintEntry *) hash_search(map, &key, HASH_FIND, &found);
	if (!found || entry == NULL)
		return;

	if (entry->relation_relfilenumber != relationRelfilenumber)
	{
		clustered_pg_pkidx_remove_stale_local_hint_entry(map, entry);
		return;
	}

	entry->exact = (entry->tid_count == 1 && entry->tids != NULL);
}

static bool
clustered_pg_pkidx_local_hint_is_exact(Oid relationOid,
									   RelFileNumber relationRelfilenumber,
									   int64 minor_key)
{
	HTAB	   *map;
	ClusteredPgPkidxLocalHintKey key;
	ClusteredPgPkidxLocalHintEntry *entry;
	bool		found = false;

	if (!OidIsValid(relationOid))
		return false;
	if (relationRelfilenumber == InvalidRelFileNumber)
		return false;

	map = clustered_pg_pkidx_local_hint_map;
	if (map == NULL)
		return false;

	memset(&key, 0, sizeof(key));
	key.relation_oid = relationOid;
	key.minor_key = minor_key;
	entry = (ClusteredPgPkidxLocalHintEntry *) hash_search(map, &key, HASH_FIND, &found);
	if (!found || entry == NULL)
		return false;

	if (entry->relation_relfilenumber != relationRelfilenumber)
	{
		clustered_pg_pkidx_remove_stale_local_hint_entry(map, entry);
		return false;
	}

	return (entry->exact && entry->tid_count > 0 && entry->tids != NULL);
}

static void
clustered_pg_pkidx_publish_rescan_keycache_to_local_hints(Relation heapRelation,
														   ClusteredPgPkidxScanState *state,
														   const int64 *minor_keys,
														   int minor_key_count)
{
	HTAB	   *map;
	Oid			relationOid;
	RelFileNumber relationRelfilenumber;
	int			max_tids;
	int			publish_cap;
	int			i;

	if (heapRelation == NULL || state == NULL)
		return;
	if (state->rescan_keycache_map == NULL)
		return;
	if (minor_key_count <= 0 || minor_keys == NULL)
		return;
	publish_cap = clustered_pg_pkidx_exact_hint_publish_max_keys;
	if (publish_cap < 1)
		publish_cap = 1;
	if (minor_key_count > publish_cap)
		minor_key_count = publish_cap;

	relationOid = RelationGetRelid(heapRelation);
	relationRelfilenumber = heapRelation->rd_locator.relNumber;
	if (!OidIsValid(relationOid) || relationRelfilenumber == InvalidRelFileNumber)
		return;

	max_tids = clustered_pg_pkidx_max_segment_tids;
	if (max_tids > CLUSTERED_PG_LOCAL_HINT_MAX_TIDS_PER_KEY)
		max_tids = CLUSTERED_PG_LOCAL_HINT_MAX_TIDS_PER_KEY;
	if (max_tids < 1)
		max_tids = 1;

	map = clustered_pg_pkidx_get_local_hint_map();
	if (map == NULL)
		return;

	for (i = 0; i < minor_key_count; i++)
	{
		ClusteredPgPkidxRescanKeycacheKey kkey;
		ClusteredPgPkidxRescanKeycacheEntry *kentry;
		bool		kfound = false;
		ClusteredPgPkidxLocalHintKey key;
		ClusteredPgPkidxLocalHintEntry *entry;
		bool		found = false;
		MemoryContext oldcxt;

		kkey.minor_key = minor_keys[i];
		kentry = (ClusteredPgPkidxRescanKeycacheEntry *)
			hash_search(state->rescan_keycache_map, &kkey, HASH_FIND, &kfound);
		if (!kfound || kentry == NULL)
			continue;
		if (kentry->tid_count <= 0 || kentry->tids == NULL)
			continue;
		if (kentry->tid_count > max_tids)
			continue;

		memset(&key, 0, sizeof(key));
		key.relation_oid = relationOid;
		key.minor_key = kentry->key.minor_key;

		entry = (ClusteredPgPkidxLocalHintEntry *) hash_search(map, &key, HASH_FIND, &found);
		if (!found || entry == NULL)
		{
			long num_entries = hash_get_num_entries(map);

			if (num_entries >= Max(1, clustered_pg_pkidx_local_hint_max_keys))
			{
				if (!clustered_pg_pkidx_evict_one_local_hint_entry(map))
				{
					clustered_pg_pkidx_reset_local_hint_map();
					map = clustered_pg_pkidx_get_local_hint_map();
					if (map == NULL)
						return;
				}
			}

			entry = (ClusteredPgPkidxLocalHintEntry *) hash_search(map, &key, HASH_ENTER, &found);
			if (entry == NULL)
				continue;
			entry->relation_relfilenumber = relationRelfilenumber;
			entry->tid_count = 0;
			entry->tid_capacity = 0;
			entry->tids = NULL;
			entry->exact = false;
		}
		else if (entry->relation_relfilenumber != relationRelfilenumber)
		{
			entry->relation_relfilenumber = relationRelfilenumber;
			clustered_pg_pkidx_free_local_hint_entry_payload(entry);
			clustered_pg_stats.local_hint_stale_resets++;
		}

		if (entry->exact &&
			entry->tid_count == kentry->tid_count &&
			entry->tid_count > 0 &&
			entry->tids != NULL &&
			memcmp(entry->tids,
				   kentry->tids,
				   sizeof(ItemPointerData) * (size_t) kentry->tid_count) == 0)
			continue;

		if (entry->tid_capacity < kentry->tid_count)
		{
			oldcxt = MemoryContextSwitchTo(TopMemoryContext);
			if (entry->tids == NULL)
				entry->tids = (ItemPointerData *) palloc_array(ItemPointerData, kentry->tid_count);
			else
				entry->tids = (ItemPointerData *) repalloc(entry->tids,
														 sizeof(ItemPointerData) * kentry->tid_count);
			MemoryContextSwitchTo(oldcxt);
			entry->tid_capacity = kentry->tid_count;
		}

		memcpy(entry->tids, kentry->tids, sizeof(ItemPointerData) * kentry->tid_count);
		entry->tid_count = kentry->tid_count;
		entry->exact = true;
	}
}

static bool
clustered_pg_pkidx_append_local_hint_tids(Oid relationOid,
									   RelFileNumber relationRelfilenumber,
									   int64 minor_key,
									   ClusteredPgPkidxScanState *state)
{
	HTAB	   *map;
	ClusteredPgPkidxLocalHintKey key;
	ClusteredPgPkidxLocalHintEntry *entry;
	bool		found = false;
	ItemPointerData *local_tids = NULL;
	int			local_count;
	int			local_unique_count;
	int			max_tids;
	int			old_count;
	int			candidate_capacity;
	ItemPointerData *merged_tids = NULL;
	int			merged_count = 0;
	int			i;
	int			j;
	uint64		last_key = 0;
	bool		has_last_key = false;
	bool		truncated = false;
	bool		changed = false;

	if (state == NULL || !OidIsValid(relationOid))
		return false;
	if (relationRelfilenumber == InvalidRelFileNumber)
		return false;
	if (clustered_pg_pkidx_max_segment_tids < 1)
		return false;

	map = clustered_pg_pkidx_local_hint_map;
	if (map == NULL)
		return false;

	memset(&key, 0, sizeof(key));
	key.relation_oid = relationOid;
	key.minor_key = minor_key;
	entry = (ClusteredPgPkidxLocalHintEntry *) hash_search(map, &key, HASH_FIND, &found);
	if (!found || entry == NULL || entry->tid_count <= 0 || entry->tids == NULL)
		return false;
	if (entry->relation_relfilenumber != relationRelfilenumber)
	{
		clustered_pg_pkidx_remove_stale_local_hint_entry(map, entry);
		return false;
	}

	local_count = entry->tid_count;
	if (local_count <= 0)
		return false;

	max_tids = clustered_pg_pkidx_max_segment_tids;
	if (max_tids < 1)
		return false;

	old_count = state->segment_tid_count;
	if (old_count < 0)
		old_count = 0;
	if (old_count > 0 && state->segment_tids == NULL)
	{
		/*
		 * Defensive recovery for inconsistent transient state: avoid null
		 * dereference in release builds and continue from local hints only.
		 */
		old_count = 0;
		state->segment_tid_count = 0;
		clustered_pg_stats.defensive_state_recovers++;
	}
#ifdef USE_ASSERT_CHECKING
	Assert(clustered_pg_pkidx_tids_sorted_unique(state->segment_tids, old_count));
#endif

	if (entry->exact && old_count == 0)
	{
		int			final_count = entry->tid_count;

		if (final_count > max_tids)
		{
			final_count = max_tids;
			truncated = true;
		}
		if (truncated)
			clustered_pg_stats.segment_map_lookup_truncated++;
		if (final_count <= 0)
			return false;

		if (state->segment_tids != NULL)
			clustered_pg_pkidx_release_segment_tids_buffer(state);
		state->segment_tids = entry->tids;
		state->segment_tids_borrowed = true;
		state->segment_tid_count = final_count;
		return true;
	}

	local_tids = (ItemPointerData *) palloc_array(ItemPointerData, local_count);
	for (i = 0; i < local_count; i++)
		ItemPointerCopy(&entry->tids[i], &local_tids[i]);

	qsort(local_tids,
		  local_count,
		  sizeof(ItemPointerData),
		  clustered_pg_itemptr_qsort_cmp);

	local_unique_count = 1;
	last_key = clustered_pg_itemptr_sortkey(&local_tids[0]);
	for (i = 1; i < local_count; i++)
	{
		uint64		cur_key = clustered_pg_itemptr_sortkey(&local_tids[i]);

		if (cur_key == last_key)
			continue;
		local_tids[local_unique_count] = local_tids[i];
		local_unique_count++;
		last_key = cur_key;
	}
#ifdef USE_ASSERT_CHECKING
	Assert(clustered_pg_pkidx_tids_sorted_unique(local_tids, local_unique_count));
#endif

	if (local_unique_count <= 0)
	{
		pfree(local_tids);
		return false;
	}

	if (old_count == 0)
	{
		int			final_count = local_unique_count;

		if (final_count > max_tids)
		{
			final_count = max_tids;
			truncated = true;
		}
		if (truncated)
			clustered_pg_stats.segment_map_lookup_truncated++;
		if (final_count <= 0)
		{
			pfree(local_tids);
			return false;
		}

		if (state->segment_tids != NULL)
			clustered_pg_pkidx_release_segment_tids_buffer(state);
		state->segment_tids = local_tids;
		state->segment_tids_borrowed = false;
		state->segment_tid_count = final_count;
		clustered_pg_stats.local_hint_merges++;
#ifdef USE_ASSERT_CHECKING
		Assert(clustered_pg_pkidx_tids_sorted_unique(state->segment_tids,
												 state->segment_tid_count));
#endif
		return true;
	}

	candidate_capacity = old_count + local_unique_count;
	if (candidate_capacity > max_tids)
	{
		candidate_capacity = max_tids;
		truncated = true;
	}
	if (candidate_capacity <= 0)
	{
		clustered_pg_stats.segment_map_lookup_truncated++;
		pfree(local_tids);
		return false;
	}

	merged_tids = (ItemPointerData *) palloc_array(ItemPointerData, candidate_capacity);

	i = 0;
	j = 0;
	while (merged_count < candidate_capacity && (i < old_count || j < local_unique_count))
	{
		uint64		left_key = 0;
		uint64		right_key = 0;
		const ItemPointerData *selected;
		bool		selected_from_local = false;

		if (i < old_count)
			left_key = clustered_pg_itemptr_sortkey(&state->segment_tids[i]);
		if (j < local_unique_count)
			right_key = clustered_pg_itemptr_sortkey(&local_tids[j]);

		if (i < old_count && j < local_unique_count)
		{
			if (left_key <= right_key)
			{
				selected = &state->segment_tids[i++];
				if (left_key == right_key)
					j++;
			}
			else
			{
				selected = &local_tids[j++];
				selected_from_local = true;
			}
		}
		else if (i < old_count)
		{
			selected = &state->segment_tids[i++];
		}
		else
		{
			selected = &local_tids[j++];
			selected_from_local = true;
		}

		if (!has_last_key)
		{
			last_key = clustered_pg_itemptr_sortkey(selected);
			has_last_key = true;
			ItemPointerCopy(selected, &merged_tids[merged_count++]);
			if (selected_from_local)
				changed = true;
		}
		else
		{
			uint64		cur_key = clustered_pg_itemptr_sortkey(selected);

			if (cur_key != last_key)
			{
				ItemPointerCopy(selected, &merged_tids[merged_count++]);
				last_key = cur_key;
				if (selected_from_local)
					changed = true;
			}
		}

	}

	if (i < old_count || j < local_unique_count)
		truncated = true;

	if (truncated)
		clustered_pg_stats.segment_map_lookup_truncated++;

	if (merged_count <= 0)
	{
		pfree(merged_tids);
		pfree(local_tids);
		return false;
	}

	if (merged_count != old_count)
		changed = true;

	if (changed)
	{
		if (state->segment_tids != NULL)
			clustered_pg_pkidx_release_segment_tids_buffer(state);
		state->segment_tids = merged_tids;
		state->segment_tids_borrowed = false;
		state->segment_tid_count = merged_count;
		clustered_pg_stats.local_hint_merges++;
#ifdef USE_ASSERT_CHECKING
		Assert(clustered_pg_pkidx_tids_sorted_unique(state->segment_tids,
												 state->segment_tid_count));
#endif
	}
	else
		pfree(merged_tids);

	pfree(local_tids);
	return changed;
}

static bool
clustered_pg_pkidx_tid_in_segment_tids(const ClusteredPgPkidxScanState *state,
									 ItemPointer tid)
{
	uint64		tid_key;
	int			left;
	int			right;

	if (state == NULL || tid == NULL)
		return false;
	if (state->segment_tids == NULL || state->segment_tid_count <= 0)
		return false;

	tid_key = clustered_pg_itemptr_sortkey(tid);

	if (tid_key < state->segment_tid_min_key)
		return false;

	if (tid_key > state->segment_tid_max_key)
		return false;

	/*
	 * For very small hint sets, linear scan is typically cheaper than binary
	 * search due to lower branch/setup overhead.
	 */
	if (state->segment_tid_count <= 8)
	{
		int i;

		for (i = 0; i < state->segment_tid_count; i++)
		{
			if (tid_key == clustered_pg_itemptr_sortkey(&state->segment_tids[i]))
				return true;
		}
		return false;
	}

	left = 0;
	right = state->segment_tid_count - 1;
	while (left <= right)
	{
		int			mid;
		uint64		mid_key;

		mid = left + ((right - left) / 2);
		mid_key = clustered_pg_itemptr_sortkey(&state->segment_tids[mid]);

		if (tid_key == mid_key)
			return true;

		if (tid_key > mid_key)
			left = mid + 1;
		else
			right = mid - 1;
	}

	return false;
}

static bool
clustered_pg_pkidx_prefetch_local_hint_range(Oid relationOid,
											 RelFileNumber relationRelfilenumber,
											 int64 minor_key_lo,
											 int64 minor_key_hi,
											 int row_limit)
{
	Datum		args[4];
	SPIPlanPtr	plan;
	int			rc;
	int			i;
	bool		loaded = false;
	Oid			minorTypeId;
	Oid			tidTypeId;

	if (!OidIsValid(relationOid))
		return false;
	if (relationRelfilenumber == InvalidRelFileNumber)
		return false;
	if (row_limit < 1)
		return false;
	if (minor_key_hi < minor_key_lo)
		return false;

	args[0] = ObjectIdGetDatum(relationOid);
	args[1] = Int64GetDatum(minor_key_lo);
	args[2] = Int64GetDatum(minor_key_hi);
	args[3] = Int32GetDatum(row_limit);

	clustered_pg_stats.segment_map_lookup_calls++;

	rc = SPI_connect();
	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("SPI_connect() failed in clustered_pk_index range prefetch helper")));

	plan = clustered_pg_pkidx_segment_tid_range_lookup_plan_init();
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to access SPI plan for segment_map_tids range lookup")));

	PG_TRY();
	{
		rc = SPI_execute_plan(plan, args, NULL, false, 0);
		if (rc != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("segment_map_tids range lookup failed"),
					 errdetail("SPI_execute_plan returned %d", rc)));

		if (SPI_processed > 0)
		{
			minorTypeId = SPI_gettypeid(SPI_tuptable->tupdesc, 1);
			tidTypeId = SPI_gettypeid(SPI_tuptable->tupdesc, 2);
			if (minorTypeId != INT8OID || tidTypeId != TIDOID)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("segment_map_tids range lookup returned unexpected types"),
						 errdetail("Expected (%u,%u), got (%u,%u)",
								   INT8OID, TIDOID, minorTypeId, tidTypeId)));

			for (i = 0; i < (int) SPI_processed; i++)
			{
				bool		isnull_minor = false;
				bool		isnull_tid = false;
				Datum		minorDatum;
				Datum		tidDatum;
				int64		minor_key;
				ItemPointerData sourceTidData;
				ItemPointerData *sourceTid = &sourceTidData;

				minorDatum = SPI_getbinval(SPI_tuptable->vals[i],
										   SPI_tuptable->tupdesc,
										   1,
										   &isnull_minor);
				tidDatum = SPI_getbinval(SPI_tuptable->vals[i],
										 SPI_tuptable->tupdesc,
										 2,
										 &isnull_tid);
				if (isnull_minor || isnull_tid)
					continue;

				minor_key = DatumGetInt64(minorDatum);
				memcpy(sourceTid, DatumGetPointer(tidDatum), sizeof(ItemPointerData));
				if (!ItemPointerIsValid(sourceTid))
					continue;

				clustered_pg_pkidx_touch_local_hint_tid(relationOid,
													 relationRelfilenumber,
													 minor_key,
													 sourceTid);
				loaded = true;
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
	return loaded;
}

static void
clustered_pg_pkidx_collect_segment_tids(Relation indexRelation,
									   RelFileNumber relationRelfilenumber,
									   int64 minor_key,
									   ClusteredPgPkidxScanState *state,
									   ScanDirection direction)
{
	Oid			relationOid = InvalidOid;
	Datum		args[3];
	SPIPlanPtr	plan;
	int			rc;
	bool		isnull;
	int			i;
	int			tid_count = 0;
	uint64		raw_tid_count = 0;
	int			fetch_limit = clustered_pg_pkidx_max_segment_tids;
	Oid			columnTypeId;
	bool		local_hint_exact = false;
	MemoryContext	resultcxt;
	MemoryContext	oldcxt;

	if (state == NULL)
		return;

	clustered_pg_pkidx_free_segment_tids(state);
	resultcxt = CurrentMemoryContext;

	if (indexRelation == NULL || indexRelation->rd_index == NULL)
		return;

	relationOid = indexRelation->rd_index->indrelid;
	if (!OidIsValid(relationOid))
		return;
	if (relationRelfilenumber == InvalidRelFileNumber)
		return;

	local_hint_exact = clustered_pg_pkidx_local_hint_is_exact(relationOid,
														 relationRelfilenumber,
														 minor_key);
	if (clustered_pg_pkidx_append_local_hint_tids(relationOid,
												 relationRelfilenumber,
												 minor_key,
												 state))
	{
		state->segment_tids_exact = local_hint_exact;
		if (state->segment_tid_count > 0 && state->segment_tids != NULL)
		{
			state->segment_tid_min_key =
				clustered_pg_itemptr_sortkey(&state->segment_tids[0]);
			state->segment_tid_max_key =
				clustered_pg_itemptr_sortkey(&state->segment_tids[state->segment_tid_count - 1]);
		}
		else
		{
			state->segment_tid_min_key = 0;
			state->segment_tid_max_key = 0;
		}

		state->segment_tid_direction = direction;
		state->segment_tid_pos = (direction == ForwardScanDirection) ? 0 : state->segment_tid_count - 1;
		state->use_segment_tids = (state->segment_tid_count > 0);
		if (state->use_segment_tids)
			return;
	}

	if (clustered_pg_pkidx_segment_prefetch_span > 1)
	{
		int64		range_hi;
		int64		span_delta = (int64) clustered_pg_pkidx_segment_prefetch_span - 1;
		int			row_limit = clustered_pg_pkidx_segment_prefetch_span * 8;

		if (span_delta < 0)
			span_delta = 0;
		if (minor_key > PG_INT64_MAX - span_delta)
			range_hi = PG_INT64_MAX;
		else
			range_hi = minor_key + span_delta;

		if (row_limit < clustered_pg_pkidx_segment_prefetch_span)
			row_limit = clustered_pg_pkidx_segment_prefetch_span;
		if (row_limit > 65536)
			row_limit = 65536;

		if (clustered_pg_pkidx_prefetch_local_hint_range(relationOid,
														relationRelfilenumber,
														minor_key,
														range_hi,
														row_limit) &&
			clustered_pg_pkidx_append_local_hint_tids(relationOid,
													 relationRelfilenumber,
													 minor_key,
													 state))
		{
			if (state->segment_tid_count > 0 && state->segment_tids != NULL)
			{
				state->segment_tid_min_key =
					clustered_pg_itemptr_sortkey(&state->segment_tids[0]);
				state->segment_tid_max_key =
					clustered_pg_itemptr_sortkey(&state->segment_tids[state->segment_tid_count - 1]);
			}
			else
			{
				state->segment_tid_min_key = 0;
				state->segment_tid_max_key = 0;
			}

			state->segment_tid_direction = direction;
			state->segment_tid_pos = (direction == ForwardScanDirection) ? 0 : state->segment_tid_count - 1;
			state->use_segment_tids = (state->segment_tid_count > 0);
			if (state->use_segment_tids)
				return;
		}
	}

	if (fetch_limit < 1)
		fetch_limit = 1;
	else if (fetch_limit < PG_INT32_MAX)
		fetch_limit = fetch_limit + 1;

	args[0] = ObjectIdGetDatum(relationOid);
	args[1] = Int64GetDatum(minor_key);
	args[2] = Int32GetDatum(fetch_limit);

	clustered_pg_stats.segment_map_lookup_calls++;
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
			columnTypeId = SPI_gettypeid(SPI_tuptable->tupdesc, 1);
			if (columnTypeId != TIDOID)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("segment_map_tids lookup returned unexpected type"),
						 errdetail("Expected %u for tuple_tid, got %u", TIDOID, columnTypeId)));
			raw_tid_count = (uint64) SPI_processed;
			if (raw_tid_count > (uint64) clustered_pg_pkidx_max_segment_tids)
			{
				clustered_pg_stats.segment_map_lookup_truncated++;
				tid_count = clustered_pg_pkidx_max_segment_tids;
			}
			else
				tid_count = (int) raw_tid_count;

			oldcxt = MemoryContextSwitchTo(resultcxt);
			state->segment_tids = (ItemPointerData *) palloc_array(ItemPointerData,
																tid_count);
			state->segment_tids_borrowed = false;
			MemoryContextSwitchTo(oldcxt);
			for (i = 0; i < tid_count; i++)
			{
				Datum		tidDatum;
				ItemPointerData sourceTidData;
				ItemPointerData *sourceTid = &sourceTidData;

				tidDatum = SPI_getbinval(SPI_tuptable->vals[i],
										 SPI_tuptable->tupdesc,
										 1,
										 &isnull);
				if (isnull)
					continue;

				memcpy(sourceTid, DatumGetPointer(tidDatum), sizeof(ItemPointerData));
				if (!ItemPointerIsValid(sourceTid))
					continue;

				ItemPointerCopy(sourceTid,
							   &state->segment_tids[state->segment_tid_count]);
				state->segment_tid_count++;
			}

			if (state->segment_tid_count == 0)
			{
				clustered_pg_pkidx_release_segment_tids_buffer(state);
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

	(void) clustered_pg_pkidx_append_local_hint_tids(relationOid,
												 relationRelfilenumber,
												 minor_key,
												 state);
#ifdef USE_ASSERT_CHECKING
	Assert(clustered_pg_pkidx_tids_sorted_unique(state->segment_tids,
												 state->segment_tid_count));
#endif

	if (state->segment_tid_count > 0 && state->segment_tids != NULL)
	{
		state->segment_tid_min_key =
			clustered_pg_itemptr_sortkey(&state->segment_tids[0]);
		state->segment_tid_max_key =
			clustered_pg_itemptr_sortkey(&state->segment_tids[state->segment_tid_count - 1]);
	}
	else
	{
		state->segment_tid_min_key = 0;
		state->segment_tid_max_key = 0;
	}

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
	Oid			heapOid;

	if (scan == NULL || state == NULL)
		return NULL;
	if (state->private_heap_relation != NULL)
		return state->private_heap_relation;

	if (scan->indexRelation == NULL || scan->indexRelation->rd_index == NULL)
		return NULL;
	heapOid = scan->indexRelation->rd_index->indrelid;
	if (!OidIsValid(heapOid))
		return NULL;

	/*
	 * Keep scan ownership local to this AM implementation to avoid relying on
	 * executor-provided heapRelation lifetime across rescan/mark-restore paths.
	 */
	state->private_heap_relation = table_open(heapOid, AccessShareLock);

	return state->private_heap_relation;
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

static void
clustered_pg_pkidx_reset_rescan_keycache(ClusteredPgPkidxScanState *state)
{
	if (state == NULL)
		return;

	if (state->rescan_keycache_cxt != NULL)
	{
		MemoryContextDelete(state->rescan_keycache_cxt);
		state->rescan_keycache_cxt = NULL;
	}

	state->rescan_keycache_map = NULL;
	state->rescan_keycache_built = false;
	state->rescan_keycache_disabled = false;
	state->rescan_keycache_rescans = 0;
	state->rescan_keycache_distinct_keys = 0;
	state->rescan_keycache_last_valid = false;
	state->rescan_keycache_last_minor_key = 0;
}

static bool
clustered_pg_pkidx_should_adaptive_sparse_bypass(const ClusteredPgPkidxScanState *state)
{
	int			rescans;
	int			distinct_keys;
	int64		lhs;
	int64		rhs;

	if (state == NULL || !clustered_pg_pkidx_enable_adaptive_sparse_select)
		return false;

	rescans = state->rescan_keycache_rescans;
	distinct_keys = state->rescan_keycache_distinct_keys;
	if (rescans < clustered_pg_pkidx_adaptive_sparse_min_rescans)
		return false;
	if (distinct_keys < clustered_pg_pkidx_adaptive_sparse_min_distinct_keys)
		return false;
	if (distinct_keys > clustered_pg_pkidx_adaptive_sparse_max_distinct_keys)
		return false;
	if (rescans <= 0 || distinct_keys <= 0)
		return false;

	lhs = (int64) distinct_keys * 100;
	rhs = (int64) clustered_pg_pkidx_adaptive_sparse_distinct_rescan_pct * (int64) rescans;
	return lhs >= rhs;
}

static bool
clustered_pg_pkidx_build_rescan_keycache(Relation heapRelation,
										 Snapshot snapshot,
										 AttrNumber heap_attno,
										 Oid atttype,
										 ClusteredPgPkidxScanState *state)
{
	HASHCTL		ctl;
	TableScanDesc scanDesc = NULL;
	TupleTableSlot *slot = NULL;
	MemoryContext keycache_cxt = NULL;
	HTAB	   *map = NULL;
	uint64		total_tids = 0;
	uint64		max_cache_tids;
	int			max_entry_tids;

	if (state == NULL || heapRelation == NULL)
		return false;
	if (state->rescan_keycache_disabled)
		return false;
	if (state->rescan_keycache_built && state->rescan_keycache_map != NULL)
		return true;
	if (snapshot == InvalidSnapshot)
		return false;
	max_cache_tids = (uint64) clustered_pg_pkidx_rescan_keycache_max_tids;
	max_entry_tids = (max_cache_tids > (uint64) INT_MAX) ? INT_MAX : (int) max_cache_tids;

	if (heap_attno < 1 || heap_attno > RelationGetDescr(heapRelation)->natts)
		return false;

	switch (atttype)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
			break;
		default:
			return false;
	}

	clustered_pg_stats.rescan_keycache_build_attempts++;

	keycache_cxt = AllocSetContextCreate(CurrentMemoryContext,
										 "clustered_pg rescan keycache",
										 ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ClusteredPgPkidxRescanKeycacheKey);
	ctl.entrysize = sizeof(ClusteredPgPkidxRescanKeycacheEntry);
	ctl.hcxt = keycache_cxt;

	map = hash_create("clustered_pg pkidx rescan keycache",
					  1024,
					  &ctl,
					  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	if (map == NULL)
	{
		MemoryContextDelete(keycache_cxt);
		return false;
	}

	scanDesc = table_beginscan(heapRelation, snapshot, 0, NULL);
	slot = table_slot_create(heapRelation, NULL);

	while (table_scan_getnextslot(scanDesc, ForwardScanDirection, slot))
	{
		Datum		value;
		bool		isnull = false;
		int64		minor_key = 0;
		ClusteredPgPkidxRescanKeycacheKey key;
		ClusteredPgPkidxRescanKeycacheEntry *entry;
		bool		found = false;

		value = slot_getattr(slot, heap_attno, &isnull);
		if (isnull)
			continue;
		if (!clustered_pg_pkidx_int_key_to_int64(value, atttype, &minor_key))
			continue;

		if (total_tids >= max_cache_tids)
		{
			state->rescan_keycache_disabled = true;
			clustered_pg_stats.rescan_keycache_disables++;
			ExecDropSingleTupleTableSlot(slot);
			table_endscan(scanDesc);
			MemoryContextDelete(keycache_cxt);
			state->rescan_keycache_cxt = NULL;
			state->rescan_keycache_map = NULL;
			state->rescan_keycache_built = false;
			return false;
		}

		key.minor_key = minor_key;
		entry = (ClusteredPgPkidxRescanKeycacheEntry *)
			hash_search(map, &key, HASH_ENTER, &found);
		if (entry == NULL)
		{
			ExecDropSingleTupleTableSlot(slot);
			table_endscan(scanDesc);
			MemoryContextDelete(keycache_cxt);
			state->rescan_keycache_cxt = NULL;
			state->rescan_keycache_map = NULL;
			state->rescan_keycache_built = false;
			return false;
		}

		if (!found)
		{
			entry->tid_count = 0;
			entry->tid_capacity = 0;
			entry->tids = NULL;
			entry->tid_min_key = 0;
			entry->tid_max_key = 0;
			entry->tid_range_valid = false;
		}

		if (entry->tid_count >= entry->tid_capacity)
		{
			int			new_capacity = clustered_pg_next_capacity(entry->tid_capacity,
															 8,
															 max_entry_tids);
			MemoryContext oldcxt;

			if (new_capacity <= entry->tid_capacity)
				continue;

			oldcxt = MemoryContextSwitchTo(keycache_cxt);
			if (entry->tids == NULL)
				entry->tids = (ItemPointerData *) palloc_array(ItemPointerData, new_capacity);
			else
				entry->tids = (ItemPointerData *) repalloc(entry->tids,
														 sizeof(ItemPointerData) * new_capacity);
			MemoryContextSwitchTo(oldcxt);
			entry->tid_capacity = new_capacity;
		}

		ItemPointerCopy(&slot->tts_tid, &entry->tids[entry->tid_count]);
		entry->tid_count++;
		{
			uint64		tid_key = clustered_pg_itemptr_sortkey(&slot->tts_tid);

			if (!entry->tid_range_valid)
			{
				entry->tid_min_key = tid_key;
				entry->tid_max_key = tid_key;
				entry->tid_range_valid = true;
			}
			else
			{
				if (tid_key < entry->tid_min_key)
					entry->tid_min_key = tid_key;
				if (tid_key > entry->tid_max_key)
					entry->tid_max_key = tid_key;
			}
		}
		total_tids++;
	}

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scanDesc);

	state->rescan_keycache_cxt = keycache_cxt;
	state->rescan_keycache_map = map;
	state->rescan_keycache_built = true;
	clustered_pg_stats.rescan_keycache_build_successes++;
	return true;
}

static bool
clustered_pg_pkidx_load_rescan_keycache_tids(ClusteredPgPkidxScanState *state,
											 int64 minor_key,
											 ScanDirection direction)
{
	ClusteredPgPkidxRescanKeycacheKey key;
	ClusteredPgPkidxRescanKeycacheEntry *entry;
	bool		found = false;

	if (state == NULL || state->rescan_keycache_map == NULL)
		return false;

	clustered_pg_pkidx_free_segment_tids(state);

	key.minor_key = minor_key;
	entry = (ClusteredPgPkidxRescanKeycacheEntry *)
		hash_search(state->rescan_keycache_map, &key, HASH_FIND, &found);

	state->segment_tid_direction = direction;
	state->segment_tid_pos = 0;
	state->segment_tid_min_key = 0;
	state->segment_tid_max_key = 0;
	state->segment_tids_exact = true;

	if (!found || entry == NULL || entry->tid_count <= 0 || entry->tids == NULL)
	{
		state->use_segment_tids = false;
		state->segment_tid_count = 0;
		clustered_pg_stats.rescan_keycache_lookup_misses++;
		return true;
	}

	state->segment_tids = entry->tids;
	state->segment_tids_borrowed = true;
	state->segment_tid_count = entry->tid_count;
	if (entry->tid_range_valid)
	{
		state->segment_tid_min_key = entry->tid_min_key;
		state->segment_tid_max_key = entry->tid_max_key;
	}
	else
	{
		state->segment_tid_min_key = 0;
		state->segment_tid_max_key = 0;
	}

	if (direction == BackwardScanDirection)
		state->segment_tid_pos = state->segment_tid_count - 1;

	state->use_segment_tids = true;
	clustered_pg_stats.rescan_keycache_lookup_hits++;
	return true;
}

static bool
clustered_pg_pkidx_load_rescan_keycache_tids_for_keys(ClusteredPgPkidxScanState *state,
													   const int64 *minor_keys,
													   int minor_key_count,
													   ScanDirection direction)
{
	ItemPointerData *combined_tids = NULL;
	int			combined_count = 0;
	int			combined_capacity = 0;
	int			max_tids;
	bool		truncated = false;
	int			i;

	if (state == NULL || state->rescan_keycache_map == NULL)
		return false;
	if (minor_key_count > 0 && minor_keys == NULL)
		return false;

	clustered_pg_pkidx_free_segment_tids(state);

	state->segment_tid_direction = direction;
	state->segment_tid_pos = 0;
	state->segment_tid_min_key = 0;
	state->segment_tid_max_key = 0;
	state->segment_tids_exact = true;

	if (minor_key_count <= 0)
	{
		state->use_segment_tids = false;
		state->segment_tid_count = 0;
		return true;
	}

	max_tids = clustered_pg_pkidx_max_segment_tids;
	if (max_tids < 1)
		max_tids = 1;

	for (i = 0; i < minor_key_count; i++)
	{
		ClusteredPgPkidxRescanKeycacheKey key;
		ClusteredPgPkidxRescanKeycacheEntry *entry;
		bool		found = false;
		int			j;

		key.minor_key = minor_keys[i];
		entry = (ClusteredPgPkidxRescanKeycacheEntry *)
			hash_search(state->rescan_keycache_map, &key, HASH_FIND, &found);
		if (!found || entry == NULL || entry->tid_count <= 0 || entry->tids == NULL)
			continue;

		if (combined_tids == NULL)
		{
			combined_capacity = Min(max_tids, Max(entry->tid_count, 8));
			combined_tids = (ItemPointerData *) palloc_array(ItemPointerData, combined_capacity);
		}

		for (j = 0; j < entry->tid_count; j++)
		{
			if (combined_count >= max_tids)
			{
				truncated = true;
				break;
			}

			if (combined_count >= combined_capacity)
			{
				int			new_capacity = clustered_pg_next_capacity(combined_capacity,
															 8,
															 max_tids);
				if (new_capacity <= combined_capacity)
				{
					truncated = true;
					break;
				}

				combined_tids = (ItemPointerData *) repalloc(combined_tids,
														 sizeof(ItemPointerData) * new_capacity);
				combined_capacity = new_capacity;
			}

			ItemPointerCopy(&entry->tids[j], &combined_tids[combined_count]);
			combined_count++;
		}

		if (truncated)
			break;
	}

	if (truncated)
		clustered_pg_stats.segment_map_lookup_truncated++;

	if (combined_count <= 0 || combined_tids == NULL)
	{
		if (combined_tids != NULL)
			pfree(combined_tids);
		state->use_segment_tids = false;
		state->segment_tid_count = 0;
		clustered_pg_stats.rescan_keycache_lookup_misses++;
		return true;
	}

	{
		uint64		min_key = clustered_pg_itemptr_sortkey(&combined_tids[0]);
		uint64		max_key = min_key;

		for (i = 1; i < combined_count; i++)
		{
			uint64		cur_key = clustered_pg_itemptr_sortkey(&combined_tids[i]);

			if (cur_key < min_key)
				min_key = cur_key;
			if (cur_key > max_key)
				max_key = cur_key;
		}
		state->segment_tid_min_key = min_key;
		state->segment_tid_max_key = max_key;
	}

	state->segment_tids = combined_tids;
	state->segment_tids_borrowed = false;
	state->segment_tid_count = combined_count;
	if (direction == BackwardScanDirection)
		state->segment_tid_pos = state->segment_tid_count - 1;

	state->use_segment_tids = true;
	clustered_pg_stats.rescan_keycache_lookup_hits++;
	return true;
}

static bool
clustered_pg_pkidx_load_exact_local_hint_tids(Oid relationOid,
											   RelFileNumber relationRelfilenumber,
											   ClusteredPgPkidxScanState *state,
											   int64 minor_key,
											   ScanDirection direction)
{
	HTAB	   *map;
	ClusteredPgPkidxLocalHintKey key;
	ClusteredPgPkidxLocalHintEntry *entry;
	bool		found = false;

	if (state == NULL || !OidIsValid(relationOid))
	{
		clustered_pg_stats.exact_local_hint_misses++;
		return false;
	}
	if (relationRelfilenumber == InvalidRelFileNumber)
	{
		clustered_pg_stats.exact_local_hint_misses++;
		return false;
	}

	map = clustered_pg_pkidx_local_hint_map;
	if (map == NULL)
	{
		clustered_pg_stats.exact_local_hint_misses++;
		return false;
	}

	memset(&key, 0, sizeof(key));
	key.relation_oid = relationOid;
	key.minor_key = minor_key;
	entry = (ClusteredPgPkidxLocalHintEntry *) hash_search(map, &key, HASH_FIND, &found);
	if (!found || entry == NULL || !entry->exact)
	{
		clustered_pg_stats.exact_local_hint_misses++;
		return false;
	}
	if (entry->relation_relfilenumber != relationRelfilenumber)
	{
		clustered_pg_pkidx_remove_stale_local_hint_entry(map, entry);
		clustered_pg_stats.exact_local_hint_misses++;
		return false;
	}
	if (entry->tid_count <= 0 || entry->tids == NULL)
	{
		clustered_pg_stats.exact_local_hint_misses++;
		return false;
	}

	clustered_pg_pkidx_free_segment_tids(state);
	state->segment_tid_direction = direction;
	state->segment_tid_pos = 0;
	state->segment_tids = entry->tids;
	state->segment_tids_borrowed = true;
	state->segment_tid_count = entry->tid_count;
	state->segment_tids_exact = true;
	state->segment_tid_min_key = clustered_pg_itemptr_sortkey(&entry->tids[0]);
	state->segment_tid_max_key =
		clustered_pg_itemptr_sortkey(&entry->tids[entry->tid_count - 1]);
	if (direction == BackwardScanDirection)
		state->segment_tid_pos = state->segment_tid_count - 1;
	state->use_segment_tids = true;
	clustered_pg_stats.exact_local_hint_hits++;
	return true;
}

static bool
clustered_pg_pkidx_load_exact_local_hint_tids_for_keys(Oid relationOid,
													   RelFileNumber relationRelfilenumber,
													   ClusteredPgPkidxScanState *state,
													   const int64 *minor_keys,
													   int minor_key_count,
													   ScanDirection direction)
{
	HTAB	   *map;
	ItemPointerData *combined_tids = NULL;
	int			combined_count = 0;
	int			combined_capacity = 0;
	int			max_tids;
	int			i;

	if (state == NULL || !OidIsValid(relationOid))
	{
		clustered_pg_stats.exact_local_hint_misses++;
		return false;
	}
	if (relationRelfilenumber == InvalidRelFileNumber)
	{
		clustered_pg_stats.exact_local_hint_misses++;
		return false;
	}
	if (minor_key_count <= 0 || minor_keys == NULL)
	{
		clustered_pg_stats.exact_local_hint_misses++;
		return false;
	}

	map = clustered_pg_pkidx_local_hint_map;
	if (map == NULL)
	{
		clustered_pg_stats.exact_local_hint_misses++;
		return false;
	}

	clustered_pg_pkidx_free_segment_tids(state);
	state->segment_tid_direction = direction;
	state->segment_tid_pos = 0;
	state->segment_tid_min_key = 0;
	state->segment_tid_max_key = 0;
	state->segment_tids_exact = true;

	max_tids = clustered_pg_pkidx_max_segment_tids;
	if (max_tids < 1)
		max_tids = 1;

	for (i = 0; i < minor_key_count; i++)
	{
		ClusteredPgPkidxLocalHintKey key;
		ClusteredPgPkidxLocalHintEntry *entry;
		bool		found = false;
		int			j;

		memset(&key, 0, sizeof(key));
		key.relation_oid = relationOid;
		key.minor_key = minor_keys[i];
		entry = (ClusteredPgPkidxLocalHintEntry *) hash_search(map, &key, HASH_FIND, &found);
		if (!found || entry == NULL || !entry->exact)
			goto fail;
		if (entry->relation_relfilenumber != relationRelfilenumber)
		{
			clustered_pg_pkidx_remove_stale_local_hint_entry(map, entry);
			goto fail;
		}
		if (entry->tid_count <= 0 || entry->tids == NULL)
			continue;

		if (combined_tids == NULL)
		{
			combined_capacity = Min(max_tids, Max(entry->tid_count, 8));
			combined_tids = (ItemPointerData *) palloc_array(ItemPointerData, combined_capacity);
		}

		for (j = 0; j < entry->tid_count; j++)
		{
			if (combined_count >= max_tids)
				goto fail;

			if (combined_count >= combined_capacity)
			{
				int			new_capacity = clustered_pg_next_capacity(combined_capacity,
															 8,
															 max_tids);
				if (new_capacity <= combined_capacity)
					goto fail;

				combined_tids = (ItemPointerData *) repalloc(combined_tids,
														 sizeof(ItemPointerData) * new_capacity);
				combined_capacity = new_capacity;
			}

			ItemPointerCopy(&entry->tids[j], &combined_tids[combined_count]);
			combined_count++;
		}
	}

	if (combined_count <= 0 || combined_tids == NULL)
		goto fail;

	{
		uint64		min_key = clustered_pg_itemptr_sortkey(&combined_tids[0]);
		uint64		max_key = min_key;

		for (i = 1; i < combined_count; i++)
		{
			uint64		cur_key = clustered_pg_itemptr_sortkey(&combined_tids[i]);

			if (cur_key < min_key)
				min_key = cur_key;
			if (cur_key > max_key)
				max_key = cur_key;
		}
		state->segment_tid_min_key = min_key;
		state->segment_tid_max_key = max_key;
	}

	state->segment_tids = combined_tids;
	state->segment_tids_borrowed = false;
	state->segment_tid_count = combined_count;
	if (direction == BackwardScanDirection)
		state->segment_tid_pos = state->segment_tid_count - 1;

	state->use_segment_tids = true;
	clustered_pg_stats.exact_local_hint_hits++;
	return true;

fail:
	if (combined_tids != NULL)
		pfree(combined_tids);
	state->use_segment_tids = false;
	state->segment_tid_count = 0;
	state->segment_tids_exact = false;
	clustered_pg_stats.exact_local_hint_misses++;
	return false;
}

static bool
clustered_pg_pkidx_match_unique_key_tids(Relation heapRelation,
										 Snapshot snapshot,
										 AttrNumber heap_attno,
										 Oid atttype,
										 int64 target_minor_key,
										 ClusteredPgPkidxScanState *state)
{
	TupleTableSlot *slot;
	int			i;
	int			match_count = 0;

	if (heapRelation == NULL || state == NULL)
		return false;
	if (snapshot == InvalidSnapshot)
		return false;
	if (state->segment_tids == NULL || state->segment_tid_count <= 0)
		return false;
	if (heap_attno < 1 || heap_attno > RelationGetDescr(heapRelation)->natts)
		return false;

	slot = table_slot_create(heapRelation, NULL);

	for (i = 0; i < state->segment_tid_count; i++)
	{
		bool		isnull = false;
		Datum		value;
		int64		minor_key = 0;

		if (!table_tuple_fetch_row_version(heapRelation,
										   &state->segment_tids[i],
										   snapshot,
										   slot))
			continue;

		value = slot_getattr(slot, heap_attno, &isnull);
		if (!isnull &&
			clustered_pg_pkidx_int_key_to_int64(value, atttype, &minor_key))
		{
			if (minor_key != target_minor_key)
			{
				ExecDropSingleTupleTableSlot(slot);
				return false;
			}

			match_count++;
			if (match_count > 1)
			{
				ExecDropSingleTupleTableSlot(slot);
				return false;
			}
		}

		ExecClearTuple(slot);
	}

	ExecDropSingleTupleTableSlot(slot);
	return (match_count == 1);
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

	if (state->segment_tids_exact && state->segment_tid_count <= 0)
		return false;

	if (state->use_segment_tids)
	{
		if (clustered_pg_pkidx_next_segment_tid(state, direction, &scan->xs_heaptid))
		{
			/*
			 * segment_map_tids is a performance hint that may lag between rebuilds;
			 * keep correctness by requiring executor-level qual recheck for hinted TIDs.
			 */
			scan->xs_recheck = !state->segment_tids_exact;
			return true;
		}

		if (state->segment_tids_exact)
			return false;

		/*
		 * Metadata can be stale between batch rebuilds; once fastpath entries
		 * are exhausted, continue with heap-key scan to preserve correctness.
		 */
		clustered_pg_stats.scan_fastpath_fallbacks++;
		state->use_segment_tids = false;
		if (!clustered_pg_pkidx_ensure_table_scan(scan, state))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("clustered_pk_index failed to initialize fallback table scan")));
	}

	if (state->table_scan == NULL || state->table_scan_slot == NULL)
	{
		if (!clustered_pg_pkidx_ensure_table_scan(scan, state))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("clustered_pk_index failed to initialize table scan for index tuple lookup")));
	}

	while (table_scan_getnextslot(state->table_scan, direction, state->table_scan_slot))
	{
		if (state->segment_tid_count > 0 &&
			clustered_pg_pkidx_tid_in_segment_tids(state,
												  &state->table_scan_slot->tts_tid))
			continue;
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

	if (state->segment_tids_exact && state->segment_tid_count <= 0)
		return rows;

	if (state->use_segment_tids)
	{
		if (state->segment_tids == NULL || state->segment_tid_count <= 0)
		{
			if (state->segment_tids_exact)
				return rows;
			state->use_segment_tids = false;
		}
		else
		{
			int			i;

			for (i = 0; i < state->segment_tid_count; i++)
			{
				/*
				 * Hint-derived TIDs can be stale; request recheck for bitmap tuples
				 * produced from segment_map_tids.
				 */
				tbm_add_tuples(tbm, &state->segment_tids[i], 1,
							  !state->segment_tids_exact);
				rows++;
			}
			if (state->segment_tids_exact)
				return rows;
			clustered_pg_stats.scan_fastpath_fallbacks++;
			state->use_segment_tids = false;
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
		if (state->segment_tid_count > 0 &&
			clustered_pg_pkidx_tid_in_segment_tids(state,
												  &state->table_scan_slot->tts_tid))
			continue;
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
	AttrNumber	key_heap_attno = InvalidAttrNumber;
	Oid			key_heap_atttype = InvalidOid;
	int64		target_minor_key = 0;
	int64	   *target_minor_keys = NULL;
	int			target_minor_key_count = 0;
	bool		single_eq_key = false;
	bool		array_eq_keys = false;
	bool		use_segment_lookup = false;
	bool		segment_lookup_attempted = false;
	bool		adaptive_sparse_bypass = false;
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

	/*
	 * Hot-loop fastpath: when an exact local hint is already available for a
	 * single equality key, skip heavyweight rescan preparation.
	 */
	if (nkeys == 1 && keys != NULL &&
		scan->indexRelation != NULL &&
		scan->indexRelation->rd_index != NULL)
	{
		Relation	early_heap_relation = NULL;
		AttrNumber	early_index_attno;
		AttrNumber	early_heap_attno;
		Oid			early_key_atttype = InvalidOid;
		int64		early_minor_key = 0;
		bool		early_single_eq = false;
		int			early_key_attr_count;

		early_heap_relation = clustered_pg_pkidx_get_heap_relation(scan, state);
		if (early_heap_relation != NULL)
		{
			early_key_attr_count = IndexRelationGetNumberOfKeyAttributes(scan->indexRelation);
			early_index_attno = keys[0].sk_attno;
			if (early_index_attno >= 1 && early_index_attno <= early_key_attr_count)
			{
				early_heap_attno = scan->indexRelation->rd_index->indkey.values[early_index_attno - 1];
				if (early_heap_attno > 0 &&
					early_heap_attno <= RelationGetDescr(early_heap_relation)->natts)
				{
					early_key_atttype =
						TupleDescAttr(RelationGetDescr(early_heap_relation),
									  early_heap_attno - 1)->atttypid;
					early_single_eq =
						clustered_pg_pkidx_extract_minor_key_from_scan_key_type(&keys[0],
																		early_key_atttype,
																		&early_minor_key);
				}
			}

			if (early_single_eq &&
				clustered_pg_pkidx_load_exact_local_hint_tids(scan->indexRelation->rd_index->indrelid,
														 early_heap_relation->rd_locator.relNumber,
														 state,
														 early_minor_key,
														 ForwardScanDirection))
			{
				scan->numberOfKeys = nkeys;
				scan->numberOfOrderBys = norderbys;
				state->scan_ready = true;
				state->restore_pending = false;
				return;
			}
		}
	}

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
		state->key_count = 0;
	}
	else if (scan->numberOfKeys == 0)
		state->key_count = 0;
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

	if (state->table_scan != NULL && state->table_scan_key_count != state->key_count)
	{
		table_endscan(state->table_scan);
		state->table_scan = NULL;
		state->table_scan_key_count = 0;
	}

	heapRelation = clustered_pg_pkidx_get_heap_relation(scan, state);
	if (heapRelation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_pk_index requires a valid heap relation")));

	state->scan_ready = false;
	state->segment_tid_direction = ForwardScanDirection;
	state->segment_tid_pos = 0;
	state->segment_tid_count = 0;
	clustered_pg_pkidx_free_segment_tids(state);

	if (scan->numberOfKeys == 1 && state->table_scan_keys != NULL)
	{
		key_heap_attno = state->table_scan_keys[0].sk_attno;
		if (key_heap_attno >= 1 &&
			key_heap_attno <= RelationGetDescr(heapRelation)->natts)
		{
			key_heap_atttype =
				TupleDescAttr(RelationGetDescr(heapRelation), key_heap_attno - 1)->atttypid;
			single_eq_key =
				clustered_pg_pkidx_extract_minor_key_from_scan_key_type(&state->table_scan_keys[0],
																		key_heap_atttype,
																		&target_minor_key);
			if (!single_eq_key)
			{
				array_eq_keys =
					clustered_pg_pkidx_extract_array_minor_keys_from_scan_key_type(&state->table_scan_keys[0],
																	key_heap_atttype,
																	&target_minor_keys,
																	&target_minor_key_count);
				if (array_eq_keys && target_minor_key_count == 1 && target_minor_keys != NULL)
				{
					target_minor_key = target_minor_keys[0];
					single_eq_key = true;
					array_eq_keys = false;
					pfree(target_minor_keys);
					target_minor_keys = NULL;
					target_minor_key_count = 0;
				}
			}
		}
	}

	if (single_eq_key)
	{
		if (clustered_pg_pkidx_enable_segment_fastpath &&
			clustered_pg_pkidx_load_exact_local_hint_tids(scan->indexRelation->rd_index->indrelid,
													 heapRelation->rd_locator.relNumber,
													 state,
													 target_minor_key,
													 ForwardScanDirection))
		{
			segment_lookup_attempted = true;
			if (target_minor_keys != NULL)
				pfree(target_minor_keys);
			state->scan_ready = true;
			state->restore_pending = false;
			return;
		}

		state->rescan_keycache_rescans++;
		if (!state->rescan_keycache_last_valid ||
			state->rescan_keycache_last_minor_key != target_minor_key)
		{
			state->rescan_keycache_last_valid = true;
			state->rescan_keycache_last_minor_key = target_minor_key;
			state->rescan_keycache_distinct_keys++;
		}
		adaptive_sparse_bypass = clustered_pg_pkidx_should_adaptive_sparse_bypass(state);
		if (adaptive_sparse_bypass)
			clustered_pg_stats.rescan_adaptive_sparse_decisions++;
		if (!adaptive_sparse_bypass &&
			!state->rescan_keycache_disabled &&
			(state->rescan_keycache_built ||
			 (state->rescan_keycache_rescans >= clustered_pg_pkidx_rescan_keycache_trigger &&
			  (state->rescan_keycache_distinct_keys >= clustered_pg_pkidx_rescan_keycache_min_distinct_keys ||
			   clustered_pg_pkidx_rescan_keycache_trigger <= 1))))
		{
			if (clustered_pg_pkidx_build_rescan_keycache(heapRelation,
													   scan->xs_snapshot,
													   key_heap_attno,
													   key_heap_atttype,
													   state) &&
				clustered_pg_pkidx_load_rescan_keycache_tids(state,
													   target_minor_key,
													   ForwardScanDirection))
			{
				int64		publish_key = target_minor_key;

				if (!clustered_pg_pkidx_local_hint_is_exact(scan->indexRelation->rd_index->indrelid,
															heapRelation->rd_locator.relNumber,
															target_minor_key))
				{
					clustered_pg_pkidx_publish_rescan_keycache_to_local_hints(heapRelation,
																	 state,
																	 &publish_key,
																	 1);
				}
				if (target_minor_keys != NULL)
					pfree(target_minor_keys);
				state->scan_ready = true;
				state->restore_pending = false;
				return;
			}
		}
		if (adaptive_sparse_bypass)
			clustered_pg_stats.rescan_adaptive_sparse_bypasses++;
	}
	else if (array_eq_keys)
	{
		state->rescan_keycache_rescans++;
		state->rescan_keycache_last_valid = false;
		state->rescan_keycache_last_minor_key = 0;
		if (target_minor_key_count > state->rescan_keycache_distinct_keys)
			state->rescan_keycache_distinct_keys = target_minor_key_count;

		if (clustered_pg_pkidx_load_exact_local_hint_tids_for_keys(scan->indexRelation->rd_index->indrelid,
															  heapRelation->rd_locator.relNumber,
															  state,
															  target_minor_keys,
															  target_minor_key_count,
															  ForwardScanDirection))
		{
			if (target_minor_keys != NULL)
				pfree(target_minor_keys);
			state->scan_ready = true;
			state->restore_pending = false;
			return;
		}

		if (!state->rescan_keycache_disabled &&
			target_minor_key_count > 0 &&
			(state->rescan_keycache_built ||
			 state->rescan_keycache_rescans >= clustered_pg_pkidx_rescan_keycache_trigger ||
			 target_minor_key_count >= clustered_pg_pkidx_rescan_keycache_min_distinct_keys))
		{
			if (clustered_pg_pkidx_build_rescan_keycache(heapRelation,
													   scan->xs_snapshot,
													   key_heap_attno,
													   key_heap_atttype,
													   state) &&
				clustered_pg_pkidx_load_rescan_keycache_tids_for_keys(state,
														 target_minor_keys,
														 target_minor_key_count,
														 ForwardScanDirection))
			{
				clustered_pg_pkidx_publish_rescan_keycache_to_local_hints(heapRelation,
																 state,
																 target_minor_keys,
																 target_minor_key_count);
				if (target_minor_keys != NULL)
					pfree(target_minor_keys);
				state->scan_ready = true;
				state->restore_pending = false;
				return;
			}
		}
	}
	else
	{
		state->rescan_keycache_rescans = 0;
		state->rescan_keycache_distinct_keys = 0;
		state->rescan_keycache_last_valid = false;
		state->rescan_keycache_last_minor_key = 0;
	}

	use_segment_lookup = (clustered_pg_pkidx_enable_segment_fastpath &&
						 single_eq_key);
	if (use_segment_lookup)
	{
		if (!segment_lookup_attempted)
			clustered_pg_pkidx_collect_segment_tids(scan->indexRelation,
												   heapRelation->rd_locator.relNumber,
												   target_minor_key,
												   state,
												   ForwardScanDirection);
		if (state->use_segment_tids)
		{
			if (scan->indexRelation->rd_index->indisunique)
			{
				if (clustered_pg_pkidx_match_unique_key_tids(heapRelation,
													 scan->xs_snapshot,
													 key_heap_attno,
													 key_heap_atttype,
													 target_minor_key,
													 state))
					state->segment_tids_exact = true;
			}
			else if (clustered_pg_pkidx_assume_unique_keys)
				state->segment_tids_exact = true;

			if (target_minor_keys != NULL)
				pfree(target_minor_keys);
			state->scan_ready = true;
			state->restore_pending = false;
			return;
		}
	}

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
	if (target_minor_keys != NULL)
		pfree(target_minor_keys);
}

static IndexScanDesc
clustered_pg_pkidx_beginscan(Relation indexRelation, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(indexRelation, nkeys, norderbys);
	ClusteredPgPkidxScanState *state;

	state = (ClusteredPgPkidxScanState *) palloc0(sizeof(ClusteredPgPkidxScanState));
	state->private_heap_relation = NULL;
	state->key_count = nkeys;
	state->table_scan_key_count = 0;
	scan->opaque = state;
	return scan;
}

static void __attribute__((noinline))
clustered_pg_pkidx_endscan(IndexScanDesc scan)
{
	ClusteredPgPkidxScanState *state;

	if (scan == NULL)
		return;

	/*
	 * Break possible recursive cleanup paths by detaching opaque state first.
	 */
	state = (ClusteredPgPkidxScanState *) scan->opaque;
	scan->opaque = NULL;
	if (state == NULL)
		return;

	if (state->table_scan != NULL)
	{
		table_endscan(state->table_scan);
		state->table_scan = NULL;
	}
	if (state->table_scan_slot != NULL)
	{
		ExecDropSingleTupleTableSlot(state->table_scan_slot);
		state->table_scan_slot = NULL;
	}
	if (state->table_scan_keys != NULL)
	{
		pfree(state->table_scan_keys);
		state->table_scan_keys = NULL;
	}
	clustered_pg_pkidx_reset_rescan_keycache(state);
	clustered_pg_pkidx_free_segment_tids(state);
	if (state->private_heap_relation != NULL)
	{
		table_close(state->private_heap_relation, AccessShareLock);
		state->private_heap_relation = NULL;
	}

	state->table_scan_key_count = 0;
	state->key_count = 0;
	pfree(state);
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
	char		text_buf[2304];

	clustered_pg_stats.observability_calls++;

	snprintf(text_buf, sizeof(text_buf),
			 "clustered_pg=%s api=%d modes={segment_fastpath=%s,assume_unique_keys=%s,max_segment_tids=%d,segment_prefetch_span=%d,local_hint_max_keys=%d,exact_hint_publish_max_keys=%d,rescan_keycache_trigger=%d,rescan_keycache_min_distinct=%d,rescan_keycache_max_tids=%d,adaptive_sparse_select=%s,adaptive_sparse_min_rescans=%d,adaptive_sparse_min_distinct=%d,adaptive_sparse_max_distinct=%d,adaptive_sparse_distinct_rescan_pct=%d} defaults={split_threshold=%d,target_fillfactor=%d,auto_repack_interval=%.2f} "
			 "counters={observability=%" PRIu64 ",costestimate=%" PRIu64
			 ",segment_lookups=%" PRIu64 ",segment_lookup_errors=%" PRIu64
			 ",segment_lookup_truncated=%" PRIu64
			 ",scan_fastpath_fallbacks=%" PRIu64
			 ",index_inserts=%" PRIu64 ",insert_errors=%" PRIu64
			 ",local_hint_touches=%" PRIu64 ",local_hint_merges=%" PRIu64 ",local_hint_map_resets=%" PRIu64 ",local_hint_evictions=%" PRIu64 ",local_hint_stale_resets=%" PRIu64
			 ",rescan_keycache_build_attempts=%" PRIu64 ",rescan_keycache_build_successes=%" PRIu64 ",rescan_keycache_disables=%" PRIu64
			 ",rescan_keycache_lookup_hits=%" PRIu64 ",rescan_keycache_lookup_misses=%" PRIu64
			 ",exact_local_hint_hits=%" PRIu64 ",exact_local_hint_misses=%" PRIu64
			 ",rescan_adaptive_sparse_decisions=%" PRIu64 ",rescan_adaptive_sparse_bypasses=%" PRIu64
			 ",defensive_state_recovers=%" PRIu64
			 ",scan_rescans=%" PRIu64 ",scan_getcalls=%" PRIu64
			 ",vacuumcleanup=%" PRIu64 ",rebuilds=%" PRIu64 ",touches=%" PRIu64
			 ",maintenance_errors=%" PRIu64 "}",
			 CLUSTERED_PG_EXTENSION_VERSION,
			 CLUSTERED_PG_OBS_API_VERSION,
			 clustered_pg_pkidx_enable_segment_fastpath ? "on" : "off",
			 clustered_pg_pkidx_assume_unique_keys ? "on" : "off",
			 clustered_pg_pkidx_max_segment_tids,
			 clustered_pg_pkidx_segment_prefetch_span,
			 clustered_pg_pkidx_local_hint_max_keys,
			 clustered_pg_pkidx_exact_hint_publish_max_keys,
			 clustered_pg_pkidx_rescan_keycache_trigger,
			 clustered_pg_pkidx_rescan_keycache_min_distinct_keys,
			 clustered_pg_pkidx_rescan_keycache_max_tids,
			 clustered_pg_pkidx_enable_adaptive_sparse_select ? "on" : "off",
			 clustered_pg_pkidx_adaptive_sparse_min_rescans,
			 clustered_pg_pkidx_adaptive_sparse_min_distinct_keys,
			 clustered_pg_pkidx_adaptive_sparse_max_distinct_keys,
			 clustered_pg_pkidx_adaptive_sparse_distinct_rescan_pct,
			 CLUSTERED_PG_DEFAULT_SPLIT_THRESHOLD,
			 CLUSTERED_PG_DEFAULT_TARGET_FILLFACTOR,
			 CLUSTERED_PG_DEFAULT_AUTO_REPACK_INTERVAL,
			 clustered_pg_stats.observability_calls,
			 clustered_pg_stats.costestimate_calls,
			 clustered_pg_stats.segment_map_lookup_calls,
			 clustered_pg_stats.segment_map_lookup_failures,
			 clustered_pg_stats.segment_map_lookup_truncated,
			 clustered_pg_stats.scan_fastpath_fallbacks,
			 clustered_pg_stats.insert_calls,
			 clustered_pg_stats.insert_errors,
			 clustered_pg_stats.local_hint_touches,
			 clustered_pg_stats.local_hint_merges,
			 clustered_pg_stats.local_hint_map_resets,
			 clustered_pg_stats.local_hint_evictions,
			 clustered_pg_stats.local_hint_stale_resets,
			 clustered_pg_stats.rescan_keycache_build_attempts,
			 clustered_pg_stats.rescan_keycache_build_successes,
			 clustered_pg_stats.rescan_keycache_disables,
			 clustered_pg_stats.rescan_keycache_lookup_hits,
			 clustered_pg_stats.rescan_keycache_lookup_misses,
			 clustered_pg_stats.exact_local_hint_hits,
			 clustered_pg_stats.exact_local_hint_misses,
			 clustered_pg_stats.rescan_adaptive_sparse_decisions,
			 clustered_pg_stats.rescan_adaptive_sparse_bypasses,
			 clustered_pg_stats.defensive_state_recovers,
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
	/*
	 * Expose clustered_heap wrapper routine: core tuple semantics still delegate
	 * to heap, while clustered metadata lifecycle hooks stay active.
	 */
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

	if (indexRelation->rd_index != NULL)
		clustered_pg_pkidx_reset_local_hint_relation(indexRelation->rd_index->indrelid);

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
	if (indexRelation != NULL && indexRelation->rd_index != NULL)
		clustered_pg_pkidx_reset_local_hint_relation(indexRelation->rd_index->indrelid);

	clustered_pg_pkidx_purge_segment_map(indexRelation);
}

static bool
clustered_pg_pkidx_insert(Relation indexRelation, Datum *values, bool *isnull,
					ItemPointer heap_tid, Relation heapRelation,
					IndexUniqueCheck checkUnique, bool indexUnchanged,
					IndexInfo *indexInfo)
{
	int64		minor_key;

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

	/*
	 * Stability + freshness:
	 * avoid SPI re-entrancy in aminsert, but keep a backend-local bounded hint
	 * cache so same-backend scans can benefit from newly inserted keys before
	 * the next batch metadata rebuild.
	 */
	if (clustered_pg_pkidx_enable_segment_fastpath &&
		indexRelation != NULL &&
		indexRelation->rd_index != NULL &&
		heapRelation != NULL)
	{
		clustered_pg_pkidx_touch_local_hint_tid(indexRelation->rd_index->indrelid,
											   heapRelation->rd_locator.relNumber,
											   minor_key,
											   heap_tid);
		if (clustered_pg_pkidx_assume_unique_keys)
			clustered_pg_pkidx_promote_local_hint_exact_if_single(indexRelation->rd_index->indrelid,
															 heapRelation->rd_locator.relNumber,
															 minor_key);
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
		clustered_pg_pkidx_reset_local_hint_relation(info->index->rd_index->indrelid);
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
	double		segment_rows = -1.0;
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
		 * Use planner-visible catalog estimates as an out-of-band signal inside
		 * cost callbacks; this avoids SPI re-entry and still provides a stable
		 * non-zero row-count floor for highly selective predicates.
		 */
		if (index->tuples > 0.0)
			segment_rows = index->tuples;
		if (segment_rows > 0.0 && segment_rows > relation_rows)
			relation_rows = segment_rows;
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

	if (!clustered_pg_pkidx_enable_segment_fastpath)
	{
		/*
		 * Safety-first default:
		 * when fastpath is disabled, avoid planner preference for this AM
		 * because scan execution uses heap fallback semantics.
		 */
		*startup_cost = costs.indexStartupCost + 100000.0;
		*total_cost = costs.indexTotalCost + 1000000.0;
		*selectivity = 1.0;
		*correlation = 0.0;
		*pages = est_pages;
		return;
	}

	if (segment_rows > 0.0 && relation_rows > 0.0 && path->indexclauses != NIL &&
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

static CompareType
clustered_pg_pkidx_translate_strategy(StrategyNumber strategy, Oid opfamily)
{
	(void) opfamily;

	switch (strategy)
	{
		case BTLessStrategyNumber:
			return COMPARE_LT;
		case BTLessEqualStrategyNumber:
			return COMPARE_LE;
		case BTEqualStrategyNumber:
			return COMPARE_EQ;
		case BTGreaterEqualStrategyNumber:
			return COMPARE_GE;
		case BTGreaterStrategyNumber:
			return COMPARE_GT;
		default:
			return COMPARE_INVALID;
	}
}

static StrategyNumber
clustered_pg_pkidx_translate_cmptype(CompareType cmptype, Oid opfamily)
{
	(void) opfamily;

	switch (cmptype)
	{
		case COMPARE_LT:
			return BTLessStrategyNumber;
		case COMPARE_LE:
			return BTLessEqualStrategyNumber;
		case COMPARE_EQ:
			return BTEqualStrategyNumber;
		case COMPARE_GE:
			return BTGreaterEqualStrategyNumber;
		case COMPARE_GT:
			return BTGreaterStrategyNumber;
		default:
			return InvalidStrategy;
	}
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
		.amtranslatestrategy = clustered_pg_pkidx_translate_strategy,
		.amtranslatecmptype = clustered_pg_pkidx_translate_cmptype,
	};
	IndexAmRoutine *result;

	result = (IndexAmRoutine *) palloc(sizeof(IndexAmRoutine));
	*result = amroutine;
	PG_RETURN_POINTER(result);
}

void
_PG_init(void)
{
	DefineCustomBoolVariable("clustered_pg.pkidx_enable_segment_fastpath",
							 "Enable segment_map_tids-driven scan fastpath for clustered_pk_index.",
							 "Default is off for production safety while metadata freshness is hardened.",
							 &clustered_pg_pkidx_enable_segment_fastpath,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("clustered_pg.pkidx_assume_unique_keys",
							 "Assume probed keys are unique and allow exact fastpath for non-unique clustered_pk_index scans.",
							 "Use only when application-level uniqueness is guaranteed; unsafe for duplicate-key datasets.",
							 &clustered_pg_pkidx_assume_unique_keys,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("clustered_pg.pkidx_max_segment_tids",
							"Maximum number of segment_map_tids entries loaded into in-memory scan hint array.",
							"Higher values may improve hint coverage but increase per-scan memory usage.",
							&clustered_pg_pkidx_max_segment_tids,
							CLUSTERED_PG_MAX_SEGMENT_TIDS,
							256,
							1048576,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("clustered_pg.pkidx_segment_prefetch_span",
							"Minor-key span for segment_map_tids range prefetch into local hint cache.",
							"0 disables range prefetch; higher values may reduce repeated point lookups for batched probes.",
							&clustered_pg_pkidx_segment_prefetch_span,
							0,
							0,
							4096,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("clustered_pg.pkidx_local_hint_max_keys",
							"Maximum number of per-key local hint entries retained in session cache.",
							"Higher values increase warm-probe retention at the cost of session memory.",
							&clustered_pg_pkidx_local_hint_max_keys,
							CLUSTERED_PG_LOCAL_HINT_MAX_KEYS,
							128,
							262144,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("clustered_pg.pkidx_exact_hint_publish_max_keys",
							"Maximum number of keys published as exact local hints from one rescan keycache build.",
							"Caps per-statement hint publication work to keep tail latency stable.",
							&clustered_pg_pkidx_exact_hint_publish_max_keys,
							64,
							1,
							65536,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("clustered_pg.pkidx_rescan_keycache_trigger",
							"Rescan-count threshold before building per-scan equality keycache.",
							"Lower values warm keycache sooner; higher values reduce warmup overhead on tiny loops.",
							&clustered_pg_pkidx_rescan_keycache_trigger,
							CLUSTERED_PG_RESCAN_KEYCACHE_TRIGGER,
							1,
							65536,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("clustered_pg.pkidx_rescan_keycache_min_distinct_keys",
							"Distinct-key threshold required before keycache warmup on rescan path.",
							"Prevents eager full-cache builds on low-cardinality probe loops.",
							&clustered_pg_pkidx_rescan_keycache_min_distinct_keys,
							CLUSTERED_PG_RESCAN_KEYCACHE_MIN_DISTINCT_KEYS,
							1,
							65536,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("clustered_pg.pkidx_rescan_keycache_max_tids",
							"Maximum total TIDs retained in per-scan keycache.",
							"Bounds memory usage for keycache warmup; keycache auto-disables when limit is reached.",
							&clustered_pg_pkidx_rescan_keycache_max_tids,
							CLUSTERED_PG_RESCAN_KEYCACHE_MAX_TIDS,
							1024,
							1048576,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("clustered_pg.pkidx_enable_adaptive_sparse_select",
							 "Enable adaptive sparse-select bypass for per-scan keycache warmup on low-reuse workloads.",
							 "When enabled, scan path can skip keycache build if rescan/distinct-key profile indicates sparse reuse.",
							 &clustered_pg_pkidx_enable_adaptive_sparse_select,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("clustered_pg.pkidx_adaptive_sparse_min_rescans",
							"Minimum rescan count required before adaptive sparse-select bypass can activate.",
							"Higher values make adaptive bypass more conservative.",
							&clustered_pg_pkidx_adaptive_sparse_min_rescans,
							CLUSTERED_PG_RESCAN_ADAPTIVE_MIN_RESCANS,
							1,
							65536,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("clustered_pg.pkidx_adaptive_sparse_min_distinct_keys",
							"Minimum distinct-key count required before adaptive sparse-select bypass can activate.",
							"Higher values reduce bypass activation for narrow key loops.",
							&clustered_pg_pkidx_adaptive_sparse_min_distinct_keys,
							CLUSTERED_PG_RESCAN_ADAPTIVE_MIN_DISTINCT_KEYS,
							1,
							65536,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("clustered_pg.pkidx_adaptive_sparse_max_distinct_keys",
							"Maximum distinct-key count allowed for adaptive sparse-select bypass activation.",
							"Prevents bypass in high-cardinality probe loops where keycache warmup is typically beneficial.",
							&clustered_pg_pkidx_adaptive_sparse_max_distinct_keys,
							CLUSTERED_PG_RESCAN_ADAPTIVE_MAX_DISTINCT_KEYS,
							1,
							65536,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("clustered_pg.pkidx_adaptive_sparse_distinct_rescan_pct",
							"Distinct-to-rescan ratio percentage threshold for adaptive sparse-select bypass.",
							"Bypass activates when distinct_keys * 100 >= threshold_pct * rescans.",
							&clustered_pg_pkidx_adaptive_sparse_distinct_rescan_pct,
							CLUSTERED_PG_RESCAN_ADAPTIVE_DISTINCT_RESCAN_PCT,
							1,
							100,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	clustered_pg_pkidx_init_reloptions();
}
