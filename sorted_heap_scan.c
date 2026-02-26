/*
 * sorted_heap_scan.c
 *
 * Custom scan provider for sorted_heap zone map pruning.
 *
 * Hooks into the planner via set_rel_pathlist_hook. When a query has
 * WHERE predicates on the first PK column of a sorted_heap table whose
 * zone map is valid (after COMPACT/REBUILD), we offer a CustomScan path
 * that restricts the heap scan to only matching blocks using
 * heap_setscanlimits().
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/stratnum.h"
#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "catalog/pg_opclass.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "sorted_heap.h"

/* ----------------------------------------------------------------
 *  Bounds extracted from WHERE clause
 * ---------------------------------------------------------------- */
typedef struct SortedHeapScanBounds
{
	bool		has_lo;
	bool		has_hi;
	bool		lo_inclusive;
	bool		hi_inclusive;
	int64		lo;
	int64		hi;
} SortedHeapScanBounds;

/* ----------------------------------------------------------------
 *  Custom scan state
 * ---------------------------------------------------------------- */
typedef struct SortedHeapScanState
{
	CustomScanState css;
	TableScanDesc	heap_scan;
	SortedHeapScanBounds bounds;
	SortedHeapRelInfo *relinfo;
	BlockNumber		total_blocks;
	BlockNumber		scan_start;
	BlockNumber		scan_nblocks;
} SortedHeapScanState;

/* ----------------------------------------------------------------
 *  Forward declarations
 * ---------------------------------------------------------------- */
static void sorted_heap_set_rel_pathlist(PlannerInfo *root,
										 RelOptInfo *rel,
										 Index rti,
										 RangeTblEntry *rte);
static bool sorted_heap_extract_bounds(RelOptInfo *rel,
									   AttrNumber pk_attno,
									   Oid pk_typid,
									   SortedHeapScanBounds *bounds);
static void sorted_heap_compute_block_range(SortedHeapRelInfo *info,
											SortedHeapScanBounds *bounds,
											BlockNumber total_blocks,
											BlockNumber *start_block,
											BlockNumber *nblocks);
static bool sorted_heap_zone_overlaps(SortedHeapZoneMapEntry *e,
									  SortedHeapScanBounds *bounds);

/* CustomPath callback */
static Plan *sorted_heap_plan_custom_path(PlannerInfo *root,
										  RelOptInfo *rel,
										  struct CustomPath *best_path,
										  List *tlist,
										  List *clauses,
										  List *custom_plans);

/* CustomScan callbacks */
static Node *sorted_heap_create_scan_state(CustomScan *cscan);
static void sorted_heap_begin_custom_scan(CustomScanState *node,
										  EState *estate, int eflags);
static TupleTableSlot *sorted_heap_exec_custom_scan(CustomScanState *node);
static void sorted_heap_end_custom_scan(CustomScanState *node);
static void sorted_heap_rescan_custom_scan(CustomScanState *node);
static void sorted_heap_explain_custom_scan(CustomScanState *node,
											List *ancestors,
											ExplainState *es);

/* ----------------------------------------------------------------
 *  Static state
 * ---------------------------------------------------------------- */
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;

static CustomPathMethods sorted_heap_path_methods = {
	.CustomName = "SortedHeapScan",
	.PlanCustomPath = sorted_heap_plan_custom_path,
};

static CustomScanMethods sorted_heap_plan_methods = {
	.CustomName = "SortedHeapScan",
	.CreateCustomScanState = sorted_heap_create_scan_state,
};

static CustomExecMethods sorted_heap_exec_methods = {
	.CustomName = "SortedHeapScan",
	.BeginCustomScan = sorted_heap_begin_custom_scan,
	.ExecCustomScan = sorted_heap_exec_custom_scan,
	.EndCustomScan = sorted_heap_end_custom_scan,
	.ReScanCustomScan = sorted_heap_rescan_custom_scan,
	.ExplainCustomScan = sorted_heap_explain_custom_scan,
};

/* ----------------------------------------------------------------
 *  Initialization — called from _PG_init()
 * ---------------------------------------------------------------- */
void
sorted_heap_scan_init(void)
{
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = sorted_heap_set_rel_pathlist;
	RegisterCustomScanMethods(&sorted_heap_plan_methods);
}

/* ----------------------------------------------------------------
 *  Planner hook: offer SortedHeapScan path when applicable
 * ---------------------------------------------------------------- */
static void
sorted_heap_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
							 Index rti, RangeTblEntry *rte)
{
	Relation			table_rel;
	SortedHeapRelInfo  *info;
	SortedHeapScanBounds bounds;
	BlockNumber			start_block, nblocks, total_blocks;
	CustomPath		   *cpath;
	double				sel;

	/* Chain to previous hook */
	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	/* Only base relations with restrictions */
	if (rel->reloptkind != RELOPT_BASEREL)
		return;
	if (rte->rtekind != RTE_RELATION)
		return;
	if (rel->baserestrictinfo == NIL)
		return;

	/* Check if this is a sorted_heap table */
	table_rel = table_open(rte->relid, NoLock);
	if (table_rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(table_rel, NoLock);
		return;
	}

	/* Load relinfo and check zone map validity */
	info = sorted_heap_get_relinfo(table_rel);
	if (!info->zm_usable || !info->zm_loaded || info->zm_nentries == 0)
	{
		table_close(table_rel, NoLock);
		return;
	}

	if (!info->zm_scan_valid)
	{
		table_close(table_rel, NoLock);
		return;
	}

	/* Extract PK bounds from baserestrictinfo */
	if (!sorted_heap_extract_bounds(rel, info->attNums[0],
									info->zm_pk_typid, &bounds))
	{
		table_close(table_rel, NoLock);
		return;
	}

	/* Compute pruned block range */
	total_blocks = RelationGetNumberOfBlocks(table_rel);
	sorted_heap_compute_block_range(info, &bounds, total_blocks,
									&start_block, &nblocks);
	table_close(table_rel, NoLock);

	/* If no pruning benefit, don't bother */
	if (nblocks >= total_blocks || total_blocks <= 1)
		return;

	/* Create CustomPath */
	cpath = makeNode(CustomPath);
	cpath->path.type = T_CustomPath;
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.param_info = NULL;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = rel->consider_parallel;
	cpath->path.parallel_workers = 0;
	cpath->path.pathkeys = NIL;

	/* Cost: only scan pruned block range */
	sel = (double) nblocks / (double) total_blocks;
	cpath->path.rows = clamp_row_est(rel->rows * sel);
	cpath->path.startup_cost = 0;
	cpath->path.total_cost = seq_page_cost * nblocks +
		cpu_tuple_cost * rel->tuples * sel +
		cpu_operator_cost * rel->tuples * sel;

	cpath->flags = 0;
	cpath->methods = &sorted_heap_path_methods;

	/* Store range and bounds in custom_private as two IntLists
	 * wrapped in a pointer list: list_make2(range_list, bounds_list) */
	{
		List *range_list = NIL;
		List *bounds_list = NIL;

		range_list = lappend_int(range_list, (int32) start_block);
		range_list = lappend_int(range_list, (int32) nblocks);
		range_list = lappend_int(range_list, (int32) total_blocks);

		bounds_list = lappend_int(bounds_list, bounds.has_lo ? 1 : 0);
		bounds_list = lappend_int(bounds_list, bounds.has_hi ? 1 : 0);
		bounds_list = lappend_int(bounds_list, bounds.lo_inclusive ? 1 : 0);
		bounds_list = lappend_int(bounds_list, bounds.hi_inclusive ? 1 : 0);
		/* Store int64 as two int32 values */
		bounds_list = lappend_int(bounds_list,
								 (int32) (bounds.lo >> 32));
		bounds_list = lappend_int(bounds_list,
								 (int32) (bounds.lo & 0xFFFFFFFF));
		bounds_list = lappend_int(bounds_list,
								 (int32) (bounds.hi >> 32));
		bounds_list = lappend_int(bounds_list,
								 (int32) (bounds.hi & 0xFFFFFFFF));

		cpath->custom_private = list_make2(range_list, bounds_list);
	}

	add_path(rel, &cpath->path);
}

/* ----------------------------------------------------------------
 *  Extract PK bounds from baserestrictinfo
 * ---------------------------------------------------------------- */
static bool
sorted_heap_extract_bounds(RelOptInfo *rel, AttrNumber pk_attno,
						   Oid pk_typid, SortedHeapScanBounds *bounds)
{
	ListCell   *lc;
	Oid			opfamily;
	Oid			opcid;

	memset(bounds, 0, sizeof(SortedHeapScanBounds));

	/* Get btree opfamily for this type */
	opcid = GetDefaultOpClass(pk_typid, BTREE_AM_OID);
	if (!OidIsValid(opcid))
		return false;
	opfamily = get_opclass_family(opcid);
	if (!OidIsValid(opfamily))
		return false;

	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		OpExpr	   *opexpr;
		Var		   *var;
		Const	   *cnst;
		int			strategy;
		bool		varonleft;
		int64		val;

		if (!IsA(rinfo->clause, OpExpr))
			continue;

		opexpr = (OpExpr *) rinfo->clause;
		if (list_length(opexpr->args) != 2)
			continue;

		/* Check for Var op Const or Const op Var */
		if (IsA(linitial(opexpr->args), Var) &&
			IsA(lsecond(opexpr->args), Const))
		{
			var = (Var *) linitial(opexpr->args);
			cnst = (Const *) lsecond(opexpr->args);
			varonleft = true;
		}
		else if (IsA(linitial(opexpr->args), Const) &&
				 IsA(lsecond(opexpr->args), Var))
		{
			cnst = (Const *) linitial(opexpr->args);
			var = (Var *) lsecond(opexpr->args);
			varonleft = false;
		}
		else
			continue;

		/* Must be our PK column */
		if (var->varattno != pk_attno)
			continue;
		if (cnst->constisnull)
			continue;

		/* Determine btree strategy */
		strategy = get_op_opfamily_strategy(opexpr->opno, opfamily);
		if (strategy == 0)
			continue;

		/* If var is on right, flip strategy */
		if (!varonleft)
		{
			switch (strategy)
			{
				case BTLessStrategyNumber:
					strategy = BTGreaterStrategyNumber;
					break;
				case BTLessEqualStrategyNumber:
					strategy = BTGreaterEqualStrategyNumber;
					break;
				case BTGreaterStrategyNumber:
					strategy = BTLessStrategyNumber;
					break;
				case BTGreaterEqualStrategyNumber:
					strategy = BTLessEqualStrategyNumber;
					break;
			}
		}

		/* Convert constant to int64 */
		if (!sorted_heap_key_to_int64(cnst->constvalue, pk_typid, &val))
			continue;

		/* Update bounds */
		switch (strategy)
		{
			case BTEqualStrategyNumber:
				bounds->has_lo = true;
				bounds->lo = val;
				bounds->lo_inclusive = true;
				bounds->has_hi = true;
				bounds->hi = val;
				bounds->hi_inclusive = true;
				break;
			case BTLessStrategyNumber:		/* col < val */
				if (!bounds->has_hi || val < bounds->hi ||
					(val == bounds->hi && bounds->hi_inclusive))
				{
					bounds->has_hi = true;
					bounds->hi = val;
					bounds->hi_inclusive = false;
				}
				break;
			case BTLessEqualStrategyNumber:	/* col <= val */
				if (!bounds->has_hi || val < bounds->hi)
				{
					bounds->has_hi = true;
					bounds->hi = val;
					bounds->hi_inclusive = true;
				}
				break;
			case BTGreaterStrategyNumber:	/* col > val */
				if (!bounds->has_lo || val > bounds->lo ||
					(val == bounds->lo && bounds->lo_inclusive))
				{
					bounds->has_lo = true;
					bounds->lo = val;
					bounds->lo_inclusive = false;
				}
				break;
			case BTGreaterEqualStrategyNumber:	/* col >= val */
				if (!bounds->has_lo || val > bounds->lo)
				{
					bounds->has_lo = true;
					bounds->lo = val;
					bounds->lo_inclusive = true;
				}
				break;
			default:
				break;
		}
	}

	return bounds->has_lo || bounds->has_hi;
}

/* ----------------------------------------------------------------
 *  Compute block range from zone map
 * ---------------------------------------------------------------- */
static void
sorted_heap_compute_block_range(SortedHeapRelInfo *info,
								SortedHeapScanBounds *bounds,
								BlockNumber total_blocks,
								BlockNumber *start_block,
								BlockNumber *nblocks)
{
	BlockNumber		first_match = total_blocks;
	BlockNumber		last_match = 0;
	uint16			i;

	for (i = 0; i < info->zm_nentries; i++)
	{
		SortedHeapZoneMapEntry *e = &info->zm_entries[i];

		if (e->zme_min == PG_INT64_MAX)
			continue;			/* empty page */

		if (!sorted_heap_zone_overlaps(e, bounds))
			continue;			/* zone map says no match */

		if ((BlockNumber)(i + 1) < first_match)
			first_match = i + 1;	/* +1 for meta page */
		last_match = i + 1;
	}

	if (first_match >= total_blocks)
	{
		/* No blocks match — minimal scan that finds nothing */
		*start_block = 1;
		*nblocks = 0;
	}
	else
	{
		*start_block = first_match;
		*nblocks = last_match - first_match + 1;
	}
}

/* ----------------------------------------------------------------
 *  Check if a zone map entry overlaps with scan bounds
 * ---------------------------------------------------------------- */
static bool
sorted_heap_zone_overlaps(SortedHeapZoneMapEntry *e,
						  SortedHeapScanBounds *bounds)
{
	if (e->zme_min == PG_INT64_MAX)
		return false;

	/* Check lower bound: skip if entire page is below lo */
	if (bounds->has_lo)
	{
		if (bounds->lo_inclusive)
		{
			if (e->zme_max < bounds->lo)
				return false;
		}
		else
		{
			if (e->zme_max <= bounds->lo)
				return false;
		}
	}

	/* Check upper bound: skip if entire page is above hi */
	if (bounds->has_hi)
	{
		if (bounds->hi_inclusive)
		{
			if (e->zme_min > bounds->hi)
				return false;
		}
		else
		{
			if (e->zme_min >= bounds->hi)
				return false;
		}
	}

	return true;
}

/* ----------------------------------------------------------------
 *  PlanCustomPath: convert CustomPath to CustomScan plan node
 * ---------------------------------------------------------------- */
static Plan *
sorted_heap_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
							 struct CustomPath *best_path,
							 List *tlist, List *clauses,
							 List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.scanrelid = rel->relid;
	cscan->flags = best_path->flags;
	cscan->custom_private = best_path->custom_private;
	cscan->custom_scan_tlist = NIL;
	cscan->custom_plans = NIL;
	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = extract_actual_clauses(clauses, false);
	cscan->methods = &sorted_heap_plan_methods;

	return &cscan->scan.plan;
}

/* ----------------------------------------------------------------
 *  CustomScan state creation
 * ---------------------------------------------------------------- */
static Node *
sorted_heap_create_scan_state(CustomScan *cscan)
{
	SortedHeapScanState *shstate;

	shstate = (SortedHeapScanState *) newNode(sizeof(SortedHeapScanState),
											  T_CustomScanState);
	shstate->css.methods = &sorted_heap_exec_methods;
	shstate->css.slotOps = &TTSOpsBufferHeapTuple;
	return (Node *) &shstate->css;
}

/* ----------------------------------------------------------------
 *  BeginCustomScan
 * ---------------------------------------------------------------- */
static void
sorted_heap_begin_custom_scan(CustomScanState *node, EState *estate,
							  int eflags)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	Relation	rel = node->ss.ss_currentRelation;
	List	   *range_list;
	List	   *bounds_list;

	/* Extract range from custom_private: list_make2(range_list, bounds_list) */
	range_list = (List *) linitial(cscan->custom_private);
	shstate->scan_start = (BlockNumber) linitial_int(range_list);
	shstate->scan_nblocks = (BlockNumber) lsecond_int(range_list);
	shstate->total_blocks = (BlockNumber) lthird_int(range_list);

	/* Extract bounds */
	bounds_list = (List *) lsecond(cscan->custom_private);
	shstate->bounds.has_lo = list_nth_int(bounds_list, 0) != 0;
	shstate->bounds.has_hi = list_nth_int(bounds_list, 1) != 0;
	shstate->bounds.lo_inclusive = list_nth_int(bounds_list, 2) != 0;
	shstate->bounds.hi_inclusive = list_nth_int(bounds_list, 3) != 0;
	shstate->bounds.lo = ((int64) list_nth_int(bounds_list, 4) << 32) |
		((int64) (uint32) list_nth_int(bounds_list, 5));
	shstate->bounds.hi = ((int64) list_nth_int(bounds_list, 6) << 32) |
		((int64) (uint32) list_nth_int(bounds_list, 7));

	/* Load relinfo for per-block zone map checks */
	shstate->relinfo = sorted_heap_get_relinfo(rel);

	/* Open heap scan without syncscan (conflicts with setscanlimits) */
	shstate->heap_scan = table_beginscan(rel, estate->es_snapshot,
										 0, NULL);

	/* Restrict scan to pruned block range */
	if (shstate->scan_nblocks > 0)
		heap_setscanlimits(shstate->heap_scan,
						   shstate->scan_start,
						   shstate->scan_nblocks);
	else
		heap_setscanlimits(shstate->heap_scan, 1, 0);
}

/* ----------------------------------------------------------------
 *  ExecCustomScan — return next matching tuple
 * ---------------------------------------------------------------- */
static TupleTableSlot *
sorted_heap_exec_custom_scan(CustomScanState *node)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	ExprState  *qual = node->ss.ps.qual;

	ResetExprContext(econtext);

	while (table_scan_getnextslot(shstate->heap_scan,
								  ForwardScanDirection, slot))
	{
		BlockNumber blk = ItemPointerGetBlockNumber(&slot->tts_tid);

		/* Per-block zone map check for fine-grained pruning */
		if (blk >= 1 && (blk - 1) < shstate->relinfo->zm_nentries)
		{
			SortedHeapZoneMapEntry *e =
				&shstate->relinfo->zm_entries[blk - 1];

			if (!sorted_heap_zone_overlaps(e, &shstate->bounds))
				continue;
		}

		/* Evaluate quals */
		econtext->ecxt_scantuple = slot;
		if (qual && !ExecQual(qual, econtext))
		{
			ResetExprContext(econtext);
			continue;
		}

		return slot;
	}

	return NULL;
}

/* ----------------------------------------------------------------
 *  EndCustomScan
 * ---------------------------------------------------------------- */
static void
sorted_heap_end_custom_scan(CustomScanState *node)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;

	if (shstate->heap_scan)
	{
		table_endscan(shstate->heap_scan);
		shstate->heap_scan = NULL;
	}
}

/* ----------------------------------------------------------------
 *  ReScanCustomScan
 * ---------------------------------------------------------------- */
static void
sorted_heap_rescan_custom_scan(CustomScanState *node)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;

	if (shstate->heap_scan)
	{
		table_rescan(shstate->heap_scan, NULL);

		/* Re-apply scan limits (rescan resets rs_inited) */
		if (shstate->scan_nblocks > 0)
			heap_setscanlimits(shstate->heap_scan,
							   shstate->scan_start,
							   shstate->scan_nblocks);
		else
			heap_setscanlimits(shstate->heap_scan, 1, 0);
	}
}

/* ----------------------------------------------------------------
 *  ExplainCustomScan
 * ---------------------------------------------------------------- */
static void
sorted_heap_explain_custom_scan(CustomScanState *node, List *ancestors,
								ExplainState *es)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "%u of %u blocks (pruned %u)",
					 shstate->scan_nblocks,
					 shstate->total_blocks,
					 shstate->total_blocks - shstate->scan_nblocks);
	ExplainPropertyText("Zone Map", buf.data, es);
	pfree(buf.data);
}
