#ifndef SORTED_HEAP_H
#define SORTED_HEAP_H

#include "postgres.h"
#include "fmgr.h"
#include "access/attnum.h"

#define SORTED_HEAP_MAGIC		0x534F5254	/* 'SORT' */
#define SORTED_HEAP_VERSION		2
#define SORTED_HEAP_META_BLOCK	0
#define SORTED_HEAP_MAX_KEYS	INDEX_MAX_KEYS

/*
 * Meta page data stored in the special space of page 0.
 * Data pages (>= 1) use standard heap page format with no special space.
 */
typedef struct SortedHeapMetaPageData
{
	uint32		shm_magic;
	uint32		shm_version;
	uint32		shm_flags;
	Oid			shm_pk_index_oid;	/* cached PK index OID */
} SortedHeapMetaPageData;

/*
 * Per-relation PK info, cached in backend-local hash table.
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
} SortedHeapRelInfo;

extern Datum sorted_heap_tableam_handler(PG_FUNCTION_ARGS);
extern void sorted_heap_relcache_callback(Datum arg, Oid relid);

#endif							/* SORTED_HEAP_H */
