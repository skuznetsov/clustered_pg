# clustered_pg — Project Status

PostgreSQL extension providing the `sorted_heap` table access method:
physically sorted storage with zone-map-based scan pruning.

## Architecture

```
COPY / multi_insert
    → sort batch by PK
    → delegate to heap
    → update per-page zone map in meta page (block 0)

sorted_heap_compact(regclass)
    → CLUSTER-based full rewrite
    → rebuild zone map
    → set SHM_FLAG_ZONEMAP_VALID

SELECT ... WHERE pk_col <op> const
    → set_rel_pathlist_hook
    → extract PK bounds from baserestrictinfo
    → compute block range from zone map
    → CustomPath (SortedHeapScan) with pruned cost
    → heap_setscanlimits(start, nblocks) — physical I/O skip
    → per-block zone map check in ExecCustomScan
```

## Source Files

| File | Lines | Purpose |
|------|------:|---------|
| `sorted_heap.h` | 86 | Meta page layout, zone map structs, SortedHeapRelInfo |
| `sorted_heap.c` | 1084 | Table AM: sorted multi_insert, zone map persistence, compact |
| `sorted_heap_scan.c` | 702 | Custom scan provider: planner hook, block pruning, EXPLAIN |
| `clustered_pg.c` | 1517 | Extension entry point, legacy clustered index AM |
| `sql/clustered_pg.sql` | 1010 | Regression tests |
| `expected/clustered_pg.out` | 1492 | Expected test output |

## Completed Phases

### Phase 1 — Table AM Skeleton
Basic `sorted_heap` AM that delegates everything to heap.

### Phase 2 — PK Auto-Detection + Sorted Bulk Insert
- Auto-detect PK via `pg_index` catalog scan
- Cache PK info in backend-local hash table (`SortedHeapRelInfo`)
- Sort each `multi_insert` batch by PK columns before delegating to heap
- Result: physically sorted runs within each COPY batch

### Phase 3 — Persistent Zone Maps
- Meta page (block 0) with `SortedHeapMetaPageData` in special space
- Per-page `(min, max)` of first PK column (int2/int4/int8)
- Zone map updated during `multi_insert` via GenericXLog
- Up to 500 entries (8024 bytes, fits in special space)
- `sorted_heap_zonemap_stats()` SQL function for inspection
- Staleness flag (`SHM_FLAG_ZONEMAP_STALE`) for INSERT-after-COPY detection

### Phase 4 — CLUSTER-Based Compaction + Zone Map Rebuild
- `sorted_heap_compact(regclass)` — full table rewrite via `cluster_rel()`
- Rebuilds zone map from scratch after compaction
- `sorted_heap_rebuild_zonemap_sql()` for manual rebuild
- Result: globally sorted table with accurate zone map

### Phase 5 — Scan Pruning via Custom Scan Provider
- `set_rel_pathlist_hook` → `SortedHeapScan` CustomPath
- Extract PK bounds from WHERE clause (=, <, <=, >, >=, BETWEEN)
- Map operator OIDs to btree strategies via `get_op_opfamily_strategy()`
- Compute contiguous block range from zone map overlap
- `heap_setscanlimits()` for physical I/O skip
- Per-block zone map check in ExecCustomScan for non-contiguous pruning
- `SHM_FLAG_ZONEMAP_VALID` flag: cleared on first INSERT after compact,
  set during compact/rebuild — prevents stale pruning
- Uncovered pages (beyond zone map capacity) included in scan unless
  upper bound falls within covered range
- EXPLAIN output: "Zone Map: N of M blocks (pruned P)"

## Benchmark Results (500K rows, ~78MB)

| Query | Heap (SeqScan) | sorted_heap (SortedHeapScan) | Speedup |
|-------|---------------|------------------------------|---------|
| Narrow range (100 rows) | 12.6ms / 8621 bufs | 0.053ms / 3 bufs | 237x |
| Medium range (5K rows) | 12.5ms / 8621 bufs | 0.634ms / 87 bufs | 20x |
| Point query (1 row) | 12.3ms / 8621 bufs | 0.015ms / 1 buf | 820x |
| Wide range (100K rows) | 13.3ms / 8621 bufs | 13.3ms / 8622 bufs | ~1x |
| Full scan | 14.3ms | 14.8ms | ~1x |

## Known Limitations

- Zone map capacity: 16,788 pages (~131 MB). 500 in meta page + up to
  32 overflow pages × 509 entries each.
- Zone map tracks first PK column only. Supported: int2/int4/int8,
  timestamp, timestamptz, date. Composite PK `(a, b)` prunes on `a`;
  standard qual evaluation handles `b`.
- Single-row INSERT into a covered page updates zone map in-place
  (preserving scan pruning). INSERT into an uncovered page invalidates
  scan pruning until next compact.
- `sorted_heap_compact()` acquires AccessExclusiveLock — blocks all
  concurrent reads and writes.
- `heap_setscanlimits()` only supports contiguous block ranges.
  Non-contiguous pruning handled per-block in ExecCustomScan (still reads
  pages, but skips tuple processing).

## Possible Future Work

- Online compact (pg_repack-style) to avoid AccessExclusiveLock
- Multi-column zone map for composite PK pruning
- Zone map support for text, uuid types
- Merge multiple sorted runs without full CLUSTER rewrite
- Parallel custom scan support
- Index-only scan equivalent using zone map
