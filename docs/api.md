---
layout: default
title: SQL API
nav_order: 4
---

# SQL API Reference

## Compaction

### `sorted_heap_compact(regclass)`

Rewrites the table in globally sorted PK order and rebuilds the zone map.
Acquires `AccessExclusiveLock`.

```sql
SELECT sorted_heap_compact('events'::regclass);
```

### `sorted_heap_compact_online(regclass)`

Non-blocking compaction using trigger-based change capture. Concurrent
reads and writes continue during the operation.

```sql
CALL sorted_heap_compact_online('events'::regclass);
```

### `sorted_heap_merge(regclass)`

Incremental merge: detects the already-sorted prefix and only re-sorts the
unsorted tail. 50--90% faster than full compact when data is partially sorted.
Acquires `AccessExclusiveLock`.

```sql
SELECT sorted_heap_merge('events'::regclass);
```

### `sorted_heap_merge_online(regclass)`

Non-blocking variant of merge with the same three-phase approach as
`sorted_heap_compact_online`.

```sql
CALL sorted_heap_merge_online('events'::regclass);
```

---

## Zone map

### `sorted_heap_zonemap_stats(regclass)`

Returns a text summary of the zone map: format version, number of entries,
validity flags, and overflow page chain.

```sql
SELECT sorted_heap_zonemap_stats('events'::regclass);
```

Example output:

```
v6 nentries=1946 flags=valid,sorted overflow_pages=7
```

### `sorted_heap_rebuild_zonemap(regclass)`

Forces a full zone map rebuild by scanning all tuples. Useful after
bulk operations that invalidate the zone map.

```sql
SELECT sorted_heap_rebuild_zonemap('events'::regclass);
```

---

## Monitoring

### `sorted_heap_scan_stats()`

Returns scan statistics as a record: total scans, blocks scanned, blocks
pruned, and stats source (shared memory or per-backend).

```sql
SELECT * FROM sorted_heap_scan_stats();
```

```
 total_scans | blocks_scanned | blocks_pruned | source
-------------+----------------+---------------+---------
         142 |           284  |        276012 | shmem
```

### `sorted_heap_reset_stats()`

Resets the scan statistics counters.

```sql
SELECT sorted_heap_reset_stats();
```

---

## Trigger

### `sorted_heap_compact_trigger()`

A trigger function used internally by the online compact/merge procedures
to capture changes during non-blocking operations. Not intended for
direct use.

---

## Configuration (GUCs)

### `sorted_heap.enable_scan_pruning`

| Property | Value |
|----------|-------|
| Type | boolean |
| Default | `on` |
| Context | user (SET) |

Enables or disables zone map scan pruning for sorted_heap tables. When
disabled, queries fall back to sequential scan.

```sql
SET sorted_heap.enable_scan_pruning = off;
```

### `sorted_heap.vacuum_rebuild_zonemap`

| Property | Value |
|----------|-------|
| Type | boolean |
| Default | `on` |
| Context | user (SET) |

When enabled, VACUUM automatically rebuilds an invalid zone map, re-enabling
scan pruning without a manual compact step.

```sql
SET sorted_heap.vacuum_rebuild_zonemap = off;
```
