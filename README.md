# clustered_pg

PostgreSQL extension providing physically sorted storage via custom Table AMs.
The primary access method is **`sorted_heap`** — a heap-based AM that sorts
COPY batches by primary key, maintains per-page zone maps, and uses a custom
scan provider to skip irrelevant blocks at query time.

## How it works

`sorted_heap` keeps data physically ordered by primary key:

1. **Sorted bulk insert** — `multi_insert` (COPY path) sorts each batch by PK
   before delegating to the standard heap. This produces physically sorted runs.

2. **Zone maps** — Block 0 is a meta page storing per-page `(min, max)` of the
   first PK column (int2/int4/int8). Up to 500 page entries fit in the meta
   page's special space.

3. **Compaction** — `sorted_heap_compact(regclass)` does a full CLUSTER rewrite,
   producing a globally sorted table and rebuilding the zone map.

4. **Scan pruning** — A `set_rel_pathlist_hook` injects a `SortedHeapScan`
   custom path when the WHERE clause has PK predicates. The executor calls
   `heap_setscanlimits()` to physically skip pruned blocks, then does per-block
   zone map checks for non-contiguous filtering.

```
COPY → sort by PK → heap insert → update zone map
                                        ↓
compact → CLUSTER rewrite → rebuild zone map → set valid flag
                                                    ↓
SELECT WHERE pk op const → planner hook → extract bounds
    → zone map lookup → block range → heap_setscanlimits → skip I/O
```

## Performance

Benchmark on 500K rows, integer PK, ~100 byte payload per row (~78 MB table):

| Query | Heap (Seq Scan) | sorted_heap (SortedHeapScan) | Speedup |
|-------|----------------|------------------------------|---------|
| Narrow range (100 rows) | 12.6 ms / 8621 bufs | 0.053 ms / 3 bufs | **237x** |
| Medium range (5K rows) | 12.5 ms / 8621 bufs | 0.634 ms / 87 bufs | **20x** |
| Point query (1 row) | 12.3 ms / 8621 bufs | 0.015 ms / 1 buf | **820x** |
| Wide range (20%, 100K rows) | 13.3 ms / 8621 bufs | 13.3 ms / 8622 bufs | ~1x |
| Full scan (no WHERE) | 14.3 ms | 14.8 ms | ~1x |

Zone map pruning eliminates I/O for selective queries while adding no overhead
for full scans.

## Quick start

### Requirements

- PostgreSQL 17+ (developed on 18devel)
- Standard PGXS build toolchain (`pg_config` in PATH)

### Build and install

```bash
make && make install
```

### Create a sorted_heap table

```sql
CREATE EXTENSION clustered_pg;

CREATE TABLE events (
    id      int PRIMARY KEY,
    ts      timestamptz,
    payload text
) USING sorted_heap;

-- Bulk load (COPY path sorts by PK automatically)
INSERT INTO events
SELECT i, now() - (i || ' seconds')::interval, repeat('x', 80)
FROM generate_series(1, 100000) i;

-- Compact to globally sort and build zone map
SELECT clustered_pg.sorted_heap_compact('events'::regclass);

-- Zone map pruning kicks in automatically
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM events WHERE id BETWEEN 500 AND 600;
-- Custom Scan (SortedHeapScan)
-- Zone Map: 2 of 1946 blocks (pruned 1944)
```

### Run tests

```bash
make installcheck
```

## SQL API

### sorted_heap (Table AM)

```sql
-- Create table with sorted storage
CREATE TABLE t (id int PRIMARY KEY, val text) USING sorted_heap;

-- Compact: full rewrite + zone map rebuild
SELECT clustered_pg.sorted_heap_compact('t'::regclass);

-- Inspect zone map stats
SELECT clustered_pg.sorted_heap_zonemap_stats('t'::regclass);

-- Manual zone map rebuild (without compaction)
SELECT clustered_pg.sorted_heap_rebuild_zonemap('t'::regclass);
```

Zone map scan pruning is automatic — the planner hook detects `sorted_heap`
tables with a valid zone map and injects a `SortedHeapScan` custom path when
the WHERE clause contains PK predicates (`=`, `<`, `<=`, `>`, `>=`, `BETWEEN`).

### clustered_heap (Table AM) — legacy

The extension also provides `clustered_heap`, a directed-placement Table AM
that routes INSERTs to the block where the same key already lives. This
requires a companion `clustered_pk_index` for key discovery and a standard
btree for query serving.

```sql
CREATE TABLE t (...) USING clustered_heap;
CREATE INDEX ON t USING clustered_pk_index (key_col);
CREATE INDEX ON t USING btree (key_col);
```

### Observability

```sql
SELECT clustered_pg.version();
SELECT clustered_pg.observability();
```

## Architecture

### Source files

```
sorted_heap.h           – Meta page layout, zone map structs, SortedHeapRelInfo
sorted_heap.c           – Table AM: sorted multi_insert, zone map, compact
sorted_heap_scan.c      – Custom scan provider: planner hook, block pruning
clustered_pg.c          – Extension entry, legacy clustered_heap AM
sql/clustered_pg--0.1.0.sql – Extension install SQL
sql/clustered_pg.sql    – Regression tests
Makefile                – PGXS build
```

### Zone map details

- Stored in block 0's special space as `SortedHeapMetaPageData`
- 500 entries max (24-byte header + 500 * 16-byte entries = 8024 bytes)
- Each entry: `(int64 min, int64 max)` for one data page
- Updated atomically via GenericXLog during `multi_insert`
- Validity flag (`SHM_FLAG_ZONEMAP_VALID`): set by compact/rebuild, cleared
  on first single-row INSERT — prevents stale pruning

### Custom scan provider

- Hooks into `set_rel_pathlist_hook`
- Extracts PK bounds from `baserestrictinfo` OpExprs
- Maps operator OIDs to btree strategies via `get_op_opfamily_strategy()`
- Computes contiguous block range from zone map overlap
- Uses `heap_setscanlimits(start, nblocks)` for physical I/O skip
- Per-block zone map check in `ExecCustomScan` for fine-grained pruning
- EXPLAIN shows: `Zone Map: N of M blocks (pruned P)`

## Limitations

- Zone map capacity: 500 pages. Larger tables have an uncovered tail that
  is always scanned (unless the query's upper bound falls within covered range).
- Zone map tracks first PK column only, integer types only (int2/int4/int8).
- Single-row INSERT does not update the zone map — invalidates scan pruning
  until the next compact.
- `heap_setscanlimits()` only supports contiguous block ranges.
- UPDATE does not re-sort; use compact periodically for write-heavy workloads.

## License

Experimental. Not yet licensed for redistribution.
