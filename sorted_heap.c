/*
 * sorted_heap.c
 *
 * Sorted heap table access method — Phase 2 (PK-sorted bulk insert).
 *
 * Uses standard heap page format.  Block 0 carries a meta page with
 * SortedHeapMetaPageData in special space; data lives on pages >= 1.
 * Single-row inserts delegate to heap (zero overhead).
 * multi_insert (COPY path) sorts each batch by PK before delegating
 * to heap, producing physically sorted runs.
 * Scans, deletes, updates, and vacuum all delegate to heap.
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/stratnum.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_index.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sortsupport.h"
#include "executor/tuptable.h"

#include "sorted_heap.h"

PG_FUNCTION_INFO_V1(sorted_heap_tableam_handler);

/* ----------------------------------------------------------------
 *  Forward declarations
 * ---------------------------------------------------------------- */
static void sorted_heap_init_meta_page(Relation rel);
static SortedHeapRelInfo *sorted_heap_get_relinfo(Relation rel);
static void sorted_heap_relinfo_invalidate(Oid relid);

static void sorted_heap_relation_set_new_filelocator(Relation rel,
													 const RelFileLocator *rlocator,
													 char persistence,
													 TransactionId *freezeXid,
													 MultiXactId *minmulti);
static void sorted_heap_relation_nontransactional_truncate(Relation rel);
static void sorted_heap_relation_copy_data(Relation rel,
										   const RelFileLocator *newrlocator);
static void sorted_heap_relation_copy_for_cluster(Relation OldTable,
												  Relation NewTable,
												  Relation OldIndex,
												  bool use_sort,
												  TransactionId OldestXmin,
												  TransactionId *xid_cutoff,
												  MultiXactId *multi_cutoff,
												  double *num_tuples,
												  double *tups_vacuumed,
												  double *tups_recently_dead);
static void sorted_heap_multi_insert(Relation rel, TupleTableSlot **slots,
									 int nslots, CommandId cid, int options,
									 struct BulkInsertStateData *bistate);
static double sorted_heap_index_build_range_scan(Relation tableRelation,
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
static void sorted_heap_index_validate_scan(Relation tableRelation,
											Relation indexRelation,
											IndexInfo *indexInfo,
											Snapshot snapshot,
											ValidateIndexState *state);

/* ----------------------------------------------------------------
 *  Static state
 * ---------------------------------------------------------------- */
static bool sorted_heap_am_initialized = false;
static TableAmRoutine sorted_heap_am_routine;
static HTAB *sorted_heap_relinfo_hash = NULL;

/* ----------------------------------------------------------------
 *  Handler + initialization
 * ---------------------------------------------------------------- */
static void
sorted_heap_init_routine(void)
{
	const TableAmRoutine *heap;

	if (sorted_heap_am_initialized)
		return;

	heap = GetHeapamTableAmRoutine();
	if (heap == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	sorted_heap_am_routine = *heap;
	sorted_heap_am_routine.type = T_TableAmRoutine;

	/* DDL lifecycle */
	sorted_heap_am_routine.relation_set_new_filelocator =
		sorted_heap_relation_set_new_filelocator;
	sorted_heap_am_routine.relation_nontransactional_truncate =
		sorted_heap_relation_nontransactional_truncate;
	sorted_heap_am_routine.relation_copy_data =
		sorted_heap_relation_copy_data;
	sorted_heap_am_routine.relation_copy_for_cluster =
		sorted_heap_relation_copy_for_cluster;

	/* Bulk insert — sort batch by PK */
	sorted_heap_am_routine.multi_insert = sorted_heap_multi_insert;

	/* Index build — needs rd_tableam swap to delegate to heap */
	sorted_heap_am_routine.index_build_range_scan =
		sorted_heap_index_build_range_scan;
	sorted_heap_am_routine.index_validate_scan =
		sorted_heap_index_validate_scan;

	sorted_heap_am_initialized = true;
}

Datum
sorted_heap_tableam_handler(PG_FUNCTION_ARGS)
{
	sorted_heap_init_routine();
	PG_RETURN_POINTER(&sorted_heap_am_routine);
}

/* ----------------------------------------------------------------
 *  PK detection infrastructure
 *
 *  Per-relation cache of PK columns and sort operators.
 *  Populated lazily on first multi_insert.  Invalidated by
 *  relcache callback when indexes are created/dropped.
 * ---------------------------------------------------------------- */
static void
sorted_heap_ensure_relinfo_hash(void)
{
	HASHCTL ctl;

	if (sorted_heap_relinfo_hash != NULL)
		return;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(SortedHeapRelInfo);
	ctl.hcxt = TopMemoryContext;
	sorted_heap_relinfo_hash = hash_create("sorted_heap relinfo",
										   32, &ctl,
										   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static SortedHeapRelInfo *
sorted_heap_get_relinfo(Relation rel)
{
	Oid				relid = RelationGetRelid(rel);
	SortedHeapRelInfo *info;
	bool			found;

	sorted_heap_ensure_relinfo_hash();

	info = hash_search(sorted_heap_relinfo_hash, &relid, HASH_ENTER, &found);
	if (!found)
	{
		info->pk_probed = false;
		info->pk_index_oid = InvalidOid;
		info->nkeys = 0;
	}

	if (!info->pk_probed)
	{
		Oid		pk_oid;

		/* Ensure index list is loaded */
		if (!rel->rd_indexvalid)
			RelationGetIndexList(rel);

		pk_oid = rel->rd_pkindex;

		if (OidIsValid(pk_oid))
		{
			Relation	idxrel;
			int			nkeys;
			int			i;
			bool		usable = true;

			idxrel = index_open(pk_oid, AccessShareLock);
			nkeys = idxrel->rd_index->indnkeyatts;

			for (i = 0; i < nkeys; i++)
			{
				AttrNumber		attnum;
				int16			opt;
				bool			reverse;
				StrategyNumber	strat;
				Oid				sortop;

				attnum = idxrel->rd_index->indkey.values[i];
				if (attnum == 0)
				{
					/* Expression column — can't sort */
					usable = false;
					break;
				}
				info->attNums[i] = attnum;

				opt = idxrel->rd_indoption[i];
				reverse = (opt & INDOPTION_DESC) != 0;
				info->nullsFirst[i] = (opt & INDOPTION_NULLS_FIRST) != 0;

				strat = reverse ? BTGreaterStrategyNumber
								: BTLessStrategyNumber;
				sortop = get_opfamily_member(idxrel->rd_opfamily[i],
											 idxrel->rd_opcintype[i],
											 idxrel->rd_opcintype[i],
											 strat);
				if (!OidIsValid(sortop))
				{
					usable = false;
					break;
				}
				info->sortOperators[i] = sortop;
				info->sortCollations[i] = idxrel->rd_indcollation[i];
			}

			if (usable)
			{
				info->pk_index_oid = pk_oid;
				info->nkeys = nkeys;
			}
			else
			{
				info->pk_index_oid = InvalidOid;
				info->nkeys = 0;
			}

			index_close(idxrel, AccessShareLock);
		}
		else
		{
			info->pk_index_oid = InvalidOid;
			info->nkeys = 0;
		}

		info->pk_probed = true;
	}

	return info;
}

/*
 * Relcache invalidation callback.
 *
 * When an index is created or dropped, PG fires relcache invalidation
 * for the parent table.  We clear pk_probed so the next multi_insert
 * re-discovers the (possibly new) PK.
 */
void
sorted_heap_relcache_callback(Datum arg, Oid relid)
{
	if (sorted_heap_relinfo_hash == NULL)
		return;

	if (OidIsValid(relid))
	{
		SortedHeapRelInfo *info;

		info = hash_search(sorted_heap_relinfo_hash, &relid,
						   HASH_FIND, NULL);
		if (info != NULL)
			info->pk_probed = false;
	}
	else
	{
		/* Invalidate all entries */
		HASH_SEQ_STATUS status;
		SortedHeapRelInfo *info;

		hash_seq_init(&status, sorted_heap_relinfo_hash);
		while ((info = hash_seq_search(&status)) != NULL)
			info->pk_probed = false;
	}
}

/*
 * Remove entry from cache on DDL (CREATE TABLE, TRUNCATE).
 */
static void
sorted_heap_relinfo_invalidate(Oid relid)
{
	if (sorted_heap_relinfo_hash == NULL)
		return;

	hash_search(sorted_heap_relinfo_hash, &relid, HASH_REMOVE, NULL);
}

/* ----------------------------------------------------------------
 *  Meta page initialization
 * ---------------------------------------------------------------- */
static void
sorted_heap_init_meta_page(Relation rel)
{
	Buffer				metabuf;
	Page				metapage;
	GenericXLogState   *state;
	SortedHeapMetaPageData *meta;

	metabuf = ExtendBufferedRel(BMR_REL(rel), MAIN_FORKNUM, NULL,
								EB_LOCK_FIRST);

	Assert(BufferGetBlockNumber(metabuf) == SORTED_HEAP_META_BLOCK);

	state = GenericXLogStart(rel);
	metapage = GenericXLogRegisterBuffer(state, metabuf,
										 GENERIC_XLOG_FULL_IMAGE);

	PageInit(metapage, BLCKSZ, sizeof(SortedHeapMetaPageData));

	meta = (SortedHeapMetaPageData *) PageGetSpecialPointer(metapage);
	meta->shm_magic = SORTED_HEAP_MAGIC;
	meta->shm_version = SORTED_HEAP_VERSION;
	meta->shm_flags = 0;
	meta->shm_pk_index_oid = InvalidOid;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(metabuf);
}

/* ----------------------------------------------------------------
 *  DDL lifecycle callbacks
 * ---------------------------------------------------------------- */
static void
sorted_heap_relation_set_new_filelocator(Relation rel,
										 const RelFileLocator *rlocator,
										 char persistence,
										 TransactionId *freezeXid,
										 MultiXactId *minmulti)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	heap->relation_set_new_filelocator(rel, rlocator, persistence,
									   freezeXid, minmulti);

	sorted_heap_init_meta_page(rel);
	sorted_heap_relinfo_invalidate(RelationGetRelid(rel));
}

static void
sorted_heap_relation_nontransactional_truncate(Relation rel)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	heap->relation_nontransactional_truncate(rel);

	sorted_heap_init_meta_page(rel);
	sorted_heap_relinfo_invalidate(RelationGetRelid(rel));
}

static void
sorted_heap_relation_copy_data(Relation rel,
							   const RelFileLocator *newrlocator)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	heap->relation_copy_data(rel, newrlocator);
}

static void
sorted_heap_relation_copy_for_cluster(Relation OldTable,
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

	heap->relation_copy_for_cluster(OldTable, NewTable, OldIndex,
									use_sort, OldestXmin,
									xid_cutoff, multi_cutoff,
									num_tuples, tups_vacuumed,
									tups_recently_dead);
}

/* ----------------------------------------------------------------
 *  Sorted multi_insert
 *
 *  If a PK exists, sort the incoming batch of slot pointers by PK
 *  using qsort_arg + SortSupport, then delegate to heap's
 *  multi_insert.  This avoids tuplesort's MinimalTuple slot
 *  incompatibility with COPY's BufferHeapTupleTableSlot.
 * ---------------------------------------------------------------- */

/* Comparison context passed through qsort_arg */
typedef struct SortedHeapCmpCtx
{
	SortedHeapRelInfo *info;
	SortSupportData   *sortKeys;
} SortedHeapCmpCtx;

static int
sorted_heap_cmp_slots(const void *a, const void *b, void *arg)
{
	SortedHeapCmpCtx *ctx = (SortedHeapCmpCtx *) arg;
	TupleTableSlot *sa = *(TupleTableSlot *const *) a;
	TupleTableSlot *sb = *(TupleTableSlot *const *) b;
	int		i;

	for (i = 0; i < ctx->info->nkeys; i++)
	{
		Datum	val1,
				val2;
		bool	null1,
				null2;
		int		cmp;

		val1 = slot_getattr(sa, ctx->info->attNums[i], &null1);
		val2 = slot_getattr(sb, ctx->info->attNums[i], &null2);

		cmp = ApplySortComparator(val1, null1, val2, null2,
								  &ctx->sortKeys[i]);
		if (cmp != 0)
			return cmp;
	}
	return 0;
}

static void
sorted_heap_multi_insert(Relation rel, TupleTableSlot **slots,
						 int nslots, CommandId cid, int options,
						 struct BulkInsertStateData *bistate)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();
	SortedHeapRelInfo *info;

	info = sorted_heap_get_relinfo(rel);

	if (OidIsValid(info->pk_index_oid) && nslots > 1)
	{
		SortSupportData *sortKeys;
		SortedHeapCmpCtx ctx;
		int				i;

		sortKeys = palloc0(sizeof(SortSupportData) * info->nkeys);
		for (i = 0; i < info->nkeys; i++)
		{
			sortKeys[i].ssup_cxt = CurrentMemoryContext;
			sortKeys[i].ssup_collation = info->sortCollations[i];
			sortKeys[i].ssup_nulls_first = info->nullsFirst[i];
			sortKeys[i].ssup_attno = info->attNums[i];
			PrepareSortSupportFromOrderingOp(info->sortOperators[i],
											  &sortKeys[i]);
		}

		ctx.info = info;
		ctx.sortKeys = sortKeys;

		qsort_arg(slots, nslots, sizeof(TupleTableSlot *),
				   sorted_heap_cmp_slots, &ctx);

		pfree(sortKeys);
	}

	heap->multi_insert(rel, slots, nslots, cid, options, bistate);
}

/* ----------------------------------------------------------------
 *  Index build support — rd_tableam swap trick
 *
 *  Heap's index_build_range_scan checks rd_tableam internally and
 *  takes optimized paths for heap.  We temporarily swap to heap AM
 *  so the build succeeds, then restore.
 * ---------------------------------------------------------------- */
static double
sorted_heap_index_build_range_scan(Relation tableRelation,
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
	const TableAmRoutine *old_tableam;
	double		result;

	if (tableRelation == NULL || indexRelation == NULL || indexInfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap index_build_range_scan requires valid arguments")));

	heap = GetHeapamTableAmRoutine();
	old_tableam = tableRelation->rd_tableam;

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
sorted_heap_index_validate_scan(Relation tableRelation,
								Relation indexRelation,
								IndexInfo *indexInfo,
								Snapshot snapshot,
								ValidateIndexState *state)
{
	const TableAmRoutine *heap;
	const TableAmRoutine *old_tableam;

	if (tableRelation == NULL || indexRelation == NULL || indexInfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap index_validate_scan requires valid arguments")));

	heap = GetHeapamTableAmRoutine();
	old_tableam = tableRelation->rd_tableam;

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
