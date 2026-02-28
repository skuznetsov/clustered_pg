#ifndef SORTED_HEAP_H
#define SORTED_HEAP_H

#include "postgres.h"
#include "fmgr.h"
#include "access/attnum.h"
#include "access/tableam.h"
#include "port/atomics.h"
#include "storage/block.h"

#define SORTED_HEAP_MAGIC		0x534F5254	/* 'SORT' */
#define SORTED_HEAP_VERSION		5
#define SORTED_HEAP_META_BLOCK	0
#define SORTED_HEAP_MAX_KEYS	INDEX_MAX_KEYS
#define SORTED_HEAP_ZONEMAP_MAX	250		/* v5 on-disk meta page entries */
#define SORTED_HEAP_ZONEMAP_CACHE_MAX 500	/* in-memory cache entries (supports v4 + v5) */

/* Overflow zone map: up to 32 overflow pages, 255 entries each (v5) */
#define SORTED_HEAP_OVERFLOW_MAX_PAGES		32
#define SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE 255
/* Total capacity: 250 + 32*255 = 8,410 entries (~65 MB at 8 KB/page) */

/* v4 backward compatibility constants */
#define SORTED_HEAP_ZONEMAP_MAX_V4				500
#define SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE_V4 509

/* Flag bits for shm_flags */
#define SORTED_HEAP_FLAG_ZONEMAP_STALE	0x0001
#define SHM_FLAG_ZONEMAP_VALID			0x0002	/* zone map safe for scan pruning */

/*
 * Per-page zone map entry: min/max of PK columns as int64.
 * Column 1 always tracked. Column 2 tracked when composite PK is usable.
 * Sentinel: zme_min == PG_INT64_MAX means "no data tracked".
 * For column 2: zme_min2 == PG_INT64_MAX means "column 2 not tracked".
 */
typedef struct SortedHeapZoneMapEntry
{
	int64		zme_min;	/* column 1 min */
	int64		zme_max;	/* column 1 max */
	int64		zme_min2;	/* column 2 min (PG_INT64_MAX = not tracked) */
	int64		zme_max2;	/* column 2 max */
} SortedHeapZoneMapEntry;	/* 32 bytes */

/*
 * Meta page data stored in the special space of page 0.
 * Data pages (>= 1) use standard heap page format with no special space.
 *
 * v5 size: 32 header + 250 * 32 entries + 128 overflow = 8160 bytes.
 * Fits within max special space of 8168 bytes.
 */
typedef struct SortedHeapMetaPageData
{
	uint32		shm_magic;
	uint32		shm_version;
	uint32		shm_flags;
	Oid			shm_pk_index_oid;		/* cached PK index OID */
	uint16		shm_zonemap_nentries;	/* valid zone map entry count (in meta page) */
	uint16		shm_overflow_npages;	/* number of overflow pages */
	Oid			shm_zonemap_pk_typid;	/* type of first PK column */
	Oid			shm_zonemap_pk_typid2;	/* type of second PK column (v5+) */
	uint32		shm_padding;			/* align entries to 8 bytes */
	/* 32 bytes of header above */
	SortedHeapZoneMapEntry shm_zonemap[SORTED_HEAP_ZONEMAP_MAX];
	/* overflow page block numbers (128 bytes) */
	BlockNumber	shm_overflow_blocks[SORTED_HEAP_OVERFLOW_MAX_PAGES];
} SortedHeapMetaPageData;

/*
 * Overflow page data stored in special space of overflow pages.
 * v5: each page holds up to 255 entries (32 bytes each).
 * Total: 8 header + 255 * 32 = 8168 bytes (fits exactly).
 */
typedef struct SortedHeapOverflowPageData
{
	uint32		shmo_magic;			/* SORTED_HEAP_MAGIC */
	uint16		shmo_nentries;		/* entries in this page */
	uint16		shmo_page_index;	/* 0-based index among overflow pages */
	SortedHeapZoneMapEntry shmo_entries[SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE];
} SortedHeapOverflowPageData;

/*
 * Per-relation PK info + zone map cache, backend-local hash table.
 * Populated lazily on first multi_insert call.
 */
typedef struct SortedHeapRelInfo
{
	Oid			relid;								/* hash key */
	bool		pk_probed;							/* true after first lookup */
	Oid			pk_index_oid;						/* PK index OID, or InvalidOid */
	int			nkeys;								/* number of PK columns */
	AttrNumber	attNums[SORTED_HEAP_MAX_KEYS];
	Oid			sortOperators[SORTED_HEAP_MAX_KEYS];
	Oid			sortCollations[SORTED_HEAP_MAX_KEYS];
	bool		nullsFirst[SORTED_HEAP_MAX_KEYS];

	/* Zone map cache */
	bool		zm_usable;			/* first PK col is int2/4/8/timestamp/date */
	bool		zm_loaded;			/* zone map read from meta page */
	bool		zm_scan_valid;		/* zone map valid for scan pruning */
	Oid			zm_pk_typid;		/* type of first PK column */
	bool		zm_col2_usable;		/* second PK col is int2/4/8/timestamp/date */
	Oid			zm_pk_typid2;		/* type of second PK column */
	uint16		zm_nentries;		/* entries in cache (max CACHE_MAX) */
	SortedHeapZoneMapEntry zm_entries[SORTED_HEAP_ZONEMAP_CACHE_MAX];

	/* Overflow zone map (for tables > 500 data pages) */
	SortedHeapZoneMapEntry *zm_overflow;	/* palloc'd, or NULL */
	uint32		zm_overflow_nentries;		/* entries in overflow pages */
	uint32		zm_total_entries;			/* zm_nentries + zm_overflow_nentries */
	uint16		zm_overflow_npages;			/* number of overflow pages */
} SortedHeapRelInfo;

/*
 * Inline helper to access zone map entry by global index.
 * Entries 0..zm_nentries-1 are in the cache array; the rest in overflow.
 * (v4 stores up to 500 in cache, v5 stores up to 250.)
 */
static inline SortedHeapZoneMapEntry *
sorted_heap_get_zm_entry(SortedHeapRelInfo *info, uint32 idx)
{
	if (idx < info->zm_nentries)
		return &info->zm_entries[idx];
	return &info->zm_overflow[idx - info->zm_nentries];
}

extern Datum sorted_heap_tableam_handler(PG_FUNCTION_ARGS);
extern Datum sorted_heap_zonemap_stats(PG_FUNCTION_ARGS);
extern Datum sorted_heap_compact(PG_FUNCTION_ARGS);
extern Datum sorted_heap_rebuild_zonemap_sql(PG_FUNCTION_ARGS);
extern void sorted_heap_relcache_callback(Datum arg, Oid relid);

/* Exported for sorted_heap_scan.c */
extern TableAmRoutine sorted_heap_am_routine;
extern SortedHeapRelInfo *sorted_heap_get_relinfo(Relation rel);
extern bool sorted_heap_key_to_int64(Datum value, Oid typid, int64 *out);
extern void sorted_heap_scan_init(void);
extern Datum sorted_heap_scan_stats(PG_FUNCTION_ARGS);
extern Datum sorted_heap_reset_stats(PG_FUNCTION_ARGS);
extern Datum sorted_heap_compact_trigger(PG_FUNCTION_ARGS);
extern Datum sorted_heap_compact_online(PG_FUNCTION_ARGS);
extern Datum sorted_heap_merge(PG_FUNCTION_ARGS);
extern Datum sorted_heap_merge_online(PG_FUNCTION_ARGS);
extern BlockNumber sorted_heap_detect_sorted_prefix(SortedHeapRelInfo *info);
extern void sorted_heap_zonemap_load(Relation rel, SortedHeapRelInfo *info);
extern void sorted_heap_rebuild_zonemap_internal(Relation rel, Oid pk_typid,
												 AttrNumber pk_attnum,
												 Oid pk_typid2,
												 AttrNumber pk_attnum2);

/* Shared memory stats (cluster-wide when loaded via shared_preload_libraries) */
typedef struct SortedHeapSharedStats
{
	pg_atomic_uint64 total_scans;
	pg_atomic_uint64 blocks_scanned;
	pg_atomic_uint64 blocks_pruned;
} SortedHeapSharedStats;

/* GUC variable */
extern bool sorted_heap_enable_scan_pruning;

#endif							/* SORTED_HEAP_H */
