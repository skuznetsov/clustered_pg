#ifndef SORTED_HEAP_H
#define SORTED_HEAP_H

#include "postgres.h"
#include "fmgr.h"
#include "access/attnum.h"

#define SORTED_HEAP_MAGIC		0x534F5254	/* 'SORT' */
#define SORTED_HEAP_VERSION		3
#define SORTED_HEAP_META_BLOCK	0
#define SORTED_HEAP_MAX_KEYS	INDEX_MAX_KEYS
#define SORTED_HEAP_ZONEMAP_MAX	500		/* max zone map entries in meta page */

/* Flag bits for shm_flags */
#define SORTED_HEAP_FLAG_ZONEMAP_STALE	0x0001

/*
 * Per-page zone map entry: min/max of first PK column as int64.
 * Only maintained for integer PK types (int2, int4, int8).
 * Sentinel: zme_min == PG_INT64_MAX means "no data tracked".
 */
typedef struct SortedHeapZoneMapEntry
{
	int64		zme_min;
	int64		zme_max;
} SortedHeapZoneMapEntry;	/* 16 bytes */

/*
 * Meta page data stored in the special space of page 0.
 * Data pages (>= 1) use standard heap page format with no special space.
 *
 * Total size: 24 header + 500 * 16 entries = 8024 bytes.
 * Fits within max special space of 8168 bytes (BLCKSZ - SizeOfPageHeaderData).
 */
typedef struct SortedHeapMetaPageData
{
	uint32		shm_magic;
	uint32		shm_version;
	uint32		shm_flags;
	Oid			shm_pk_index_oid;		/* cached PK index OID */
	uint16		shm_zonemap_nentries;	/* valid zone map entry count */
	uint16		shm_zonemap_reserved;	/* padding */
	Oid			shm_zonemap_pk_typid;	/* type of first PK column */
	/* 24 bytes of header above */
	SortedHeapZoneMapEntry shm_zonemap[SORTED_HEAP_ZONEMAP_MAX];
} SortedHeapMetaPageData;

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
	bool		zm_usable;			/* first PK col is int2/4/8 */
	bool		zm_loaded;			/* zone map read from meta page */
	Oid			zm_pk_typid;		/* type of first PK column */
	uint16		zm_nentries;		/* number of valid entries */
	SortedHeapZoneMapEntry zm_entries[SORTED_HEAP_ZONEMAP_MAX];
} SortedHeapRelInfo;

extern Datum sorted_heap_tableam_handler(PG_FUNCTION_ARGS);
extern Datum sorted_heap_zonemap_stats(PG_FUNCTION_ARGS);
extern void sorted_heap_relcache_callback(Datum arg, Oid relid);

#endif							/* SORTED_HEAP_H */
