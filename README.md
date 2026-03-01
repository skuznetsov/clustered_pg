# clustered_pg

PostgreSQL extension providing physically sorted storage via custom Table AMs.
The primary access method is **`sorted_heap`** — a heap-based AM that sorts
bulk inserts by primary key, maintains per-page zone maps with unlimited
capacity, and uses a custom scan provider to skip irrelevant blocks at query
time. Supports parallel scans, prepared statements, online compaction, and
incremental merge.

## How it works

`sorted_heap` keeps data physically ordered by primary key:

1. **Sorted bulk insert** — `multi_insert` (COPY path) sorts each batch by PK
   before delegating to the standard heap. Produces physically sorted runs.

2. **Zone maps** — Block 0 is a meta page storing per-page `(col1_min,
   col1_max, col2_min, col2_max)` for the first two PK columns. Unlimited
   capacity via overflow page chain (v6 format). Supported types: int2, int4,
   int8, timestamp, timestamptz, date, uuid, text/varchar (C collation).

3. **Compaction** — `sorted_heap_compact(regclass)` does a full CLUSTER rewrite;
   `sorted_heap_merge(regclass)` does incremental two-way merge of sorted
   prefix + unsorted tail. Both have online (non-blocking) variants.

4. **Scan pruning** — A `set_rel_pathlist_hook` injects a `SortedHeapScan`
   custom path when the WHERE clause has PK predicates. The executor calls
   `heap_setscanlimits()` to physically skip pruned blocks, then does per-block
   zone map checks for fine-grained filtering. Supports both literal constants
   and parameterized queries (prepared statements).

```
COPY → sort by PK → heap insert → update zone map
                                        ↓
compact/merge → rewrite → rebuild zone map → set valid flag
                                                  ↓
SELECT WHERE pk op const → planner hook → extract bounds
    → zone map lookup → block range → heap_setscanlimits → skip I/O
```

## Performance

PostgreSQL 18, Apple M-series (12 CPU, 64 GB RAM), zone map v6.
shared_buffers=4GB, work_mem=256MB, maintenance_work_mem=2GB.

### EXPLAIN ANALYZE (warm cache, avg 5 runs)

**1M rows** (71 MB sorted_heap, 71 MB heap+btree)

| Query | sorted_heap | heap+btree | heap seqscan |
|-------|------------|-----------|-------------|
| Point (1 row) | 0.038ms / 1 buf | 0.045ms / 7 bufs | 15.3ms / 6,370 bufs |
| Narrow (100) | 0.043ms / 2 bufs | 0.067ms / 8 bufs | 16.7ms / 6,370 bufs |
| Medium (5K) | 0.438ms / 33 bufs | 0.528ms / 52 bufs | 16.5ms / 6,370 bufs |
| Wide (100K) | 7.5ms / 638 bufs | 9.1ms / 917 bufs | 17.4ms / 6,370 bufs |

**10M rows** (714 MB sorted_heap, 712 MB heap+btree)

| Query | sorted_heap | heap+btree | heap seqscan |
|-------|------------|-----------|-------------|
| Point (1 row) | 0.034ms / 1 buf | 0.054ms / 7 bufs | 118.8ms / 63,695 bufs |
| Narrow (100) | 0.040ms / 1 buf | 0.062ms / 7 bufs | 129.9ms / 63,695 bufs |
| Medium (5K) | 0.449ms / 32 bufs | 0.594ms / 51 bufs | 121.9ms / 63,695 bufs |
| Wide (100K) | 7.6ms / 638 bufs | 9.3ms / 917 bufs | 122.7ms / 63,695 bufs |

**100M rows** (7.8 GB sorted_heap, 7.8 GB heap+btree)

| Query | sorted_heap | heap+btree | heap seqscan |
|-------|------------|-----------|-------------|
| Point (1 row) | 0.128ms / 1 buf | 1.6ms / 8 bufs | 1,242ms / 519,909 bufs |
| Narrow (100) | 0.265ms / 2 bufs | 0.183ms / 9 bufs | 1,380ms / 520,778 bufs |
| Medium (5K) | 0.516ms / 38 bufs | 0.730ms / 58 bufs | 1,350ms / 519,855 bufs |
| Wide (100K) | 9.0ms / 737 bufs | 10.3ms / 1,017 bufs | 1,347ms / 518,896 bufs |

sorted_heap reads fewer blocks than btree at all selectivities. Zone map
prunes to exact block range; btree traverses 3-4 index pages per lookup. At
100M rows: point query reads 1 buffer vs 8 for btree, 519,909 for seqscan.

### pgbench Throughput (10s, 1 client)

**Prepared mode** (`-M prepared`): query planned once, re-executed with parameters.

| Query | 1M sh / btree | 10M sh / btree | 100M sh / btree |
|-------|-------------:|--------------:|---------------:|
| Point (1 row) | 46.6K / 61.0K | 44.6K / 55.4K | 16.0K / 30.4K |
| Narrow (100) | 22.4K / 28.8K | 22.2K / 28.1K | 9.7K / 17.1K |
| Medium (5K) | 3.3K / 5.0K | 3.2K / 4.7K | 1,583 / 2,095 |
| Wide (100K) | 287 / 277 | 278 / 278 | 156 / 148 |

**Simple mode** (`-M simple`): each query parsed, planned, and executed.

| Query | 1M sh / btree | 10M sh / btree | 100M sh / btree |
|-------|-------------:|--------------:|---------------:|
| Point (1 row) | 28.7K / 40.9K | 29.2K / 39.4K | 8,148 / 6,711 |
| Narrow (100) | 22.0K / 26.5K | 20.6K / 27.2K | 4,616 / 3,987 |
| Medium (5K) | 3.3K / 3.9K | 3.1K / 4.6K | 844 / 1,214 |
| Wide (100K) | 198 / 289 | 192 / 279 | 143 / 140 |

In prepared mode, sorted_heap point queries reach 44.6K TPS at 10M rows (+53%
vs simple mode). At 100M with simple mode, sorted_heap wins point (+21%) and
narrow (+16%) queries since execution dominates over planning. Wide (100K row)
queries show parity at all scales.

## Quick start

### Requirements

- PostgreSQL 18+ (uses PG 18 `ExecCustomScan` API)
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
make installcheck              # regression tests (17 suites)
make test-crash-recovery       # crash recovery (4 scenarios)
make test-concurrent           # concurrent DML + online ops
make test-toast                # TOAST integrity + concurrent guard
make test-alter-table          # ALTER TABLE DDL (36 checks)
make test-dump-restore         # pg_dump/restore lifecycle (10 checks)
```

## SQL API

### Compaction

```sql
-- Offline compact: full CLUSTER rewrite (AccessExclusiveLock)
SELECT clustered_pg.sorted_heap_compact('t'::regclass);

-- Online compact: trigger-based, non-blocking (ShareUpdateExclusiveLock,
-- brief AccessExclusiveLock for final swap)
CALL clustered_pg.sorted_heap_compact_online('t'::regclass);

-- Offline merge: two-way merge of sorted prefix + unsorted tail
SELECT clustered_pg.sorted_heap_merge('t'::regclass);

-- Online merge: non-blocking variant
CALL clustered_pg.sorted_heap_merge_online('t'::regclass);
```

### Zone map inspection

```sql
-- Human-readable zone map stats (flags, entry count, ranges)
SELECT clustered_pg.sorted_heap_zonemap_stats('t'::regclass);

-- Manual zone map rebuild (without compaction)
SELECT clustered_pg.sorted_heap_rebuild_zonemap('t'::regclass);
```

### Scan statistics

```sql
-- Structured stats: total_scans, blocks_scanned, blocks_pruned, source
SELECT * FROM clustered_pg.sorted_heap_scan_stats();

-- Reset counters
SELECT clustered_pg.sorted_heap_reset_stats();
```

### Configuration

```sql
-- Disable scan pruning (default: on)
SET sorted_heap.enable_scan_pruning = off;

-- Disable autovacuum zone map rebuild (default: on)
SET sorted_heap.vacuum_rebuild_zonemap = off;
```

### Observability

```sql
SELECT clustered_pg.version();
SELECT clustered_pg.observability();
```

### clustered_heap (Table AM) — legacy

The extension also provides `clustered_heap`, a directed-placement Table AM
that routes INSERTs to the block where the same key already lives. Requires
a companion `clustered_pk_index` for key discovery and a standard btree for
query serving.

```sql
CREATE TABLE t (...) USING clustered_heap;
CREATE INDEX ON t USING clustered_pk_index (key_col);
CREATE INDEX ON t USING btree (key_col);
```

## Architecture

### Source files

| File | Lines | Purpose |
|------|------:|---------|
| `sorted_heap.h` | 183 | Meta page layout, zone map structs (v6), SortedHeapRelInfo |
| `sorted_heap.c` | 2,452 | Table AM: sorted multi_insert, zone map persistence, compact, merge, vacuum |
| `sorted_heap_scan.c` | 1,547 | Custom scan provider: planner hook, parallel scan, multi-col pruning, runtime params |
| `sorted_heap_online.c` | 1,053 | Online compact + online merge: trigger, copy, replay, swap |
| `clustered_pg.c` | 1,537 | Extension entry point, legacy clustered index AM, GUC registration |

### Zone map details

- **v6 format**: 32-byte entries with col1 + col2 min/max per page
- Meta page (block 0): 250 entries in special space
- Overflow pages: 254 entries/page, linked via `shmo_next_block` chain
- No capacity limit — overflow chain extends as needed
- Updated atomically via GenericXLog during `multi_insert`
- Validity flag (`SHM_FLAG_ZONEMAP_VALID`): set by compact/rebuild, cleared
  on first single-row INSERT into uncovered page
- Autovacuum rebuilds zone map when flag is not set

### Custom scan provider

- Hooks into `set_rel_pathlist_hook`
- Extracts PK bounds from `baserestrictinfo` (both `Const` and `Param` nodes)
- Maps operator OIDs to btree strategies via `get_op_opfamily_strategy()`
- Computes contiguous block range from zone map overlap
- Uses `heap_setscanlimits(start, nblocks)` for physical I/O skip
- Per-block zone map check in `ExecCustomScan` for fine-grained pruning
- Parallel-aware: `add_partial_path` + Gather for multi-worker scans
- Prepared statements: runtime parameter resolution via `ExecEvalExprSwitchContext`
- EXPLAIN shows: `Zone Map: N of M blocks (pruned P)`

## Limitations

- Zone map tracks first two PK columns. Supported types: int2, int4, int8,
  timestamp, timestamptz, date, uuid, text/varchar (`COLLATE "C"` required
  for text). UUID/text use lossy first-8-byte mapping.
- Online compact/merge not supported for UUID/text/varchar PKs (lossy int64
  hash causes collisions in replay). Use offline variants.
- Single-row INSERT into a covered page updates zone map in-place. INSERT
  into an uncovered page invalidates scan pruning until next compact (or
  autovacuum rebuild).
- `sorted_heap_compact()` and `sorted_heap_merge()` acquire
  AccessExclusiveLock. Use `_online` variants for non-blocking operation.
- `heap_setscanlimits()` only supports contiguous block ranges. Non-contiguous
  pruning handled per-block in ExecCustomScan.
- UPDATE does not re-sort; use compact/merge periodically for write-heavy
  workloads.
- pg_dump/restore: data restored via COPY, zone map needs compact after
  restore to re-enable scan pruning.
- pg_upgrade: untested. Expected to work (data files copied as-is).

## License

Experimental. Not yet licensed for redistribution.
