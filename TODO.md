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

Parallel scan (large tables):
    → add_partial_path with parallel_aware=true
    → Gather wraps SortedHeapScan
    → PG parallel table scan API distributes blocks among workers
    → each worker applies per-block zone map pruning independently
```

## Source Files

| File | Lines | Purpose |
|------|------:|---------|
| `sorted_heap.h` | 162 | Meta page layout, zone map structs (v5), SortedHeapRelInfo |
| `sorted_heap.c` | 2118 | Table AM: sorted multi_insert, zone map persistence, compact, merge |
| `sorted_heap_scan.c` | 1168 | Custom scan provider: planner hook, parallel scan, multi-col pruning |
| `sorted_heap_online.c` | 1044 | Online compact + online merge: trigger, copy, replay, swap |
| `clustered_pg.c` | 1528 | Extension entry point, legacy clustered index AM |
| `sql/clustered_pg.sql` | 1728 | Regression tests (SH1–SH13) |
| `expected/clustered_pg.out` | 2630 | Expected test output |

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

## Benchmark Results (500K rows, ~56 MB, warm cache)

PostgreSQL 18.1, Apple M-series, zone map v5 with multi-column support.

### sorted_heap vs Heap SeqScan (no index)

Primary use case — sorted_heap eliminates the need for a separate index.

| Query | Heap (SeqScan) | sorted_heap (SortedHeapScan) | Speedup |
|-------|---------------|------------------------------|---------|
| Point query (1 row) | 10.1ms / 7143 bufs | 0.017ms / 1 buf | 595x |
| Narrow range (100 rows) | 11.7ms / 7143 bufs | 0.018ms / 2 bufs | 650x |
| Medium range (5K rows) | 11.3ms / 7143 bufs | 0.512ms / 73 bufs | 22x |
| Wide range (100K rows) | 21.5ms / 7143 bufs | 7.3ms / 1430 bufs | 3x |
| Full scan | 14.8ms / 7143 bufs | 15.2ms / 7158 bufs | ~1x |

### sorted_heap vs Heap IndexScan (btree PK)

| Query | Heap (IndexScan) | sorted_heap (SortedHeapScan) | Ratio |
|-------|-----------------|------------------------------|-------|
| Point query (1 row) | 0.009ms / 4 bufs | 0.017ms / 1 buf | 0.5x |
| Narrow range (100 rows) | 0.015ms / 6 bufs | 0.018ms / 2 bufs | ~1x |
| Medium range (5K rows) | 0.477ms / 89 bufs | 0.512ms / 73 bufs | ~1x |
| Wide range (100K rows) | 9.0ms / 1706 bufs | 7.3ms / 1430 bufs | 1.2x |

Zone map granularity is per-page, so point queries are ~2x slower than
btree. For wide ranges sorted_heap wins on buffer hits due to physical
sort order (sequential I/O vs random index lookups).

## Known Limitations

- Zone map capacity: 8,410 pages (~65 MB). 250 in meta page + up to
  32 overflow pages × 255 entries each (v5 format, 32 bytes/entry).
- Zone map tracks first two PK columns (col1 + col2). Supported types:
  int2/int4/int8, timestamp, timestamptz, date, uuid, text/varchar
  (text requires `COLLATE "C"`). UUID/text use lossy first-8-byte mapping
  (conservative pruning for values sharing long prefixes).
- Online compact/merge (`_online` variants) not supported for UUID/text/varchar
  PKs due to lossy int64 hash key. Use regular compact/merge instead.
- Single-row INSERT into a covered page updates zone map in-place
  (preserving scan pruning). INSERT into an uncovered page invalidates
  scan pruning until next compact.
- `sorted_heap_compact()` acquires AccessExclusiveLock — blocks all
  concurrent reads and writes. Use `sorted_heap_compact_online()` for
  non-blocking compaction.
- `heap_setscanlimits()` only supports contiguous block ranges.
  Non-contiguous pruning handled per-block in ExecCustomScan (still reads
  pages, but skips tuple processing).

### Phase 6 — Production Hardening
- GUC `sorted_heap.enable_scan_pruning` (default on)
- Timestamp/date PK support in zone map
- INSERT-after-compact zone map updates
- EXPLAIN ANALYZE counters (Scanned Blocks, Pruned Blocks)
- Shared memory scan statistics (`sorted_heap_scan_stats()`)

### Phase 7 — Online Compact
- `sorted_heap_compact_online(regclass)` — PROCEDURE (use with CALL)
- Trigger-based change capture (pg_repack-style)
- Phase 1: UNLOGGED log table + AFTER ROW trigger (committed mid-call)
- Phase 2: Index scan in PK order → bulk copy to new table
  (ShareUpdateExclusiveLock — concurrent reads and writes allowed)
- Phase 3: Replay log entries, up to 10 convergence passes
- AccessExclusiveLock only for brief final filenode swap
- PK→TID hash table for O(1) replay lookups
- Zone map rebuilt on new table before swap

### Phase 8 — Multi-Column Zone Map
- Zone map v5: 32-byte entries (col1 + col2 min/max per page)
- Composite PK `(a, b)` prunes on both columns (AND semantics)
- v4 backward compatibility: load expands 16→32 byte entries,
  flush writes v4 format, compact/rebuild upgrades to v5
- Supported col2 types: int2/int4/int8, timestamp, timestamptz, date
- Non-trackable col2 degrades gracefully (col1-only pruning)
- Meta page capacity: 250 entries (v5) vs 500 (v4)
- Overflow capacity: 255 entries/page (v5) vs 509 (v4)

### Phase 9 — Parallel Custom Scan
- Parallel-aware SortedHeapScan via PostgreSQL's parallel table scan API
- Planner adds partial path (`add_partial_path`) when `compute_parallel_worker`
  returns > 0 workers; Gather node wraps parallel SortedHeapScan
- DSM callbacks: `EstimateDSM`, `InitializeDSM`, `ReInitializeDSM`,
  `InitializeWorker` — use `table_parallelscan_initialize` / `table_beginscan_parallel`
- Workers coordinate block distribution through PG's native mechanism;
  each worker applies per-block zone map pruning independently
- Fix: `sorted_heap_get_zm_entry()` used hardcoded `SORTED_HEAP_ZONEMAP_CACHE_MAX`
  (500) as cache/overflow boundary — incorrect for v5 format (250 entries).
  Changed to `info->zm_nentries` so overflow lookup adapts to format version.

### Production Hardening (post-Phase 9)
- multi_insert zone map boundary: capped at `SORTED_HEAP_ZONEMAP_MAX` (250)
  to match flush capacity and read path. Previously used `SORTED_HEAP_ZONEMAP_CACHE_MAX`
  (500), causing writes to entries that were never flushed to disk.
- Overflow page bounds check: `overflow_npages` clamped to
  `SORTED_HEAP_OVERFLOW_MAX_PAGES` (32) on both v4 and v5 read paths,
  preventing OOM from corrupted on-disk metadata.
- SH10 test suite: zone map overflow boundary correctness (INSERT into
  overflow range, re-compact verification).

### Phase 10 — Incremental Merge Compaction
- `sorted_heap_merge(regclass)` — two-way merge avoiding full CLUSTER rewrite
- Sorted prefix detection: scan zone map entries for monotonicity
  (`entry[i+1].min >= entry[i].max`); works regardless of `SHM_FLAG_ZONEMAP_VALID`
  since compacted entries remain accurate after subsequent INSERTs
- Stream A: sequential heap scan of sorted prefix (no index needed)
- Stream B: tuplesort of unsorted tail via PG's tuplesort API
- Merge loop: compare PK columns via SortSupport, write winner to new
  relation via `heap->tuple_insert` (bypasses sorted_heap AM sorting)
- Zone map rebuilt on new table via `sorted_heap_rebuild_zonemap_internal`
- Atomic filenode swap via `finish_heap_swap`
- AccessExclusiveLock for entire operation (same as compact)
- Edge cases: empty table → early return; already sorted → early return;
  never compacted (no valid prefix) → full tuplesort fallback;
  zone map doesn't cover trailing pages → uncovered pages go to tail
- SH11 test suite: normal merge with overlapping data, zone map validity,
  scan pruning after merge, re-compact verification, already-sorted,
  never-compacted fallback, empty table

### Phase 11 — Online Merge Compaction
- `sorted_heap_merge_online(regclass)` — PROCEDURE (use with CALL)
- Non-blocking variant of merge using trigger-based change capture
  (same pg_repack-style approach as `sorted_heap_compact_online`)
- Phase 0b: Detect prefix under ShareUpdateExclusiveLock; early exit
  for empty/already-sorted tables (no SPI/log table overhead)
- Phase 1: UNLOGGED log table + AFTER ROW trigger (committed mid-call)
- Phase 2: Prefix seq scan + tail tuplesort merge → new table under
  ShareUpdateExclusiveLock (concurrent reads and writes allowed)
- PK→TID hash populated during merge for O(1) replay lookups
- Phase 2b: Replay loop (up to 10 convergence passes)
- Phase 3: AccessExclusiveLock only for brief final replay + swap
- Re-detect prefix in Phase 2 under lock eliminates TOCTOU race
- Exported `sorted_heap_detect_sorted_prefix` and `sorted_heap_zonemap_load`
  from sorted_heap.c for cross-file use
- SH12 test suite: normal online merge, physical sort verification,
  zone map validity, scan pruning, already-sorted, empty table,
  never-compacted fallback

### Phase 12 — Zone Map Support for UUID and Text Types
- UUID PK: first 8 bytes as big-endian uint64, sign-flipped to int64
  (lossy but order-preserving; UUIDs sharing first 8 bytes collapse)
- TEXT/VARCHAR PK: first 8 bytes zero-padded, same uint64→int64 conversion.
  Requires `COLLATE "C"` (byte order = sort order). Non-C collation
  degrades gracefully (`zm_usable = false`, no zone map, no scan pruning)
- Zone map rebuild probe replaced: `Int32GetDatum(0)` probe caused NULL
  dereference for pointer-based types; now uses direct typid switch
- Online compact/merge blocked for UUID/text/varchar PKs — the int8
  log table and PK→TID hash use lossy int64 representation, causing
  hash collisions and incorrect replay
- Collation check uses `C_COLLATION_OID` from `pg_collation_d.h`
  (avoids ICU header dependency from `pg_locale.h`)
- SH13 test suite: UUID zone map + scan pruning, text/C zone map +
  scan pruning, online compact/merge blocked, varchar zone map

## Possible Future Work

- Index-only scan equivalent using zone map
- Online compact/merge support for UUID/text PKs (requires log format redesign)
