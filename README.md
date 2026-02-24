# clustered_pg

PostgreSQL extension implementing physically clustered storage via a custom Table AM. Rows with the same key are placed on the same (or adjacent) heap blocks at INSERT time, so standard btree and BRIN indexes operate at near-optimal I/O efficiency without periodic `CLUSTER` maintenance.

## How it works

`clustered_pg` provides two access methods:

- **`clustered_heap`** (Table AM) — wraps the standard heap with a **directed placement** layer. On INSERT and COPY, the extension extracts the clustering key from each tuple, looks up a per-relation in-memory **zone map** (`key → block number`), and calls `RelationSetTargetBlock` to steer PostgreSQL's buffer manager toward the block where that key already lives. This keeps rows physically grouped by key.

- **`clustered_pk_index`** (Index AM) — maintains segment-level metadata for the clustering key. In practice, the planner always prefers a standard btree when one exists, so this index serves as a **key discovery** mechanism (the table AM finds it at first INSERT to learn which column is the clustering key). The segment map, locator, and SPI-based scan path are retained for completeness but are dormant in the recommended setup.

## Performance

Benchmarks on 100K rows, 1000 distinct keys, interleaved insert order, 100-byte payload:

| Metric | `clustered_heap` + btree | Standard heap + btree | Improvement |
|---|---|---|---|
| Block scatter (blocks/key) | 3.0 | 100.0 | **33x fewer** |
| Point lookup (1 key) | 0.135 ms | 0.127 ms | same |
| Range scan (10% rows) | 0.618 ms | 0.785 ms | 1.3x faster |
| Bitmap scan (1% rows) | 24 buffers | 121 buffers | **5x fewer** |
| JOIN 200 keys (UNNEST) | 1.7 ms / 1K buffers | 5.4 ms / 20K buffers | **3.2x faster / 20x fewer buffers** |

INSERT overhead is ~10% (282 ms vs 257 ms for 100K rows), acceptable given the clustering benefit.

BRIN indexes are also highly effective on directed-placement tables: 22 heap blocks for 1% selectivity vs 119 on standard heap.

## Quick start

### Requirements

- PostgreSQL 17+ (developed on 18)
- Standard PGXS build toolchain (`pg_config` in PATH)

### Build and install

```bash
make
make install
```

### Create a clustered table

```sql
CREATE EXTENSION clustered_pg;

-- 1. Table with clustered storage
CREATE TABLE events (
    tenant_id  int,
    event_time timestamptz,
    payload    text
) USING clustered_heap;

-- 2. Clustering key index (tells the table AM which column to cluster on)
CREATE INDEX events_pkidx ON events
    USING clustered_pk_index (tenant_id);

-- 3. Standard btree for query serving
CREATE INDEX events_btree ON events USING btree (tenant_id);

-- Rows inserted for the same tenant_id will be physically co-located.
INSERT INTO events
SELECT (i % 100), now(), repeat('x', 80)
FROM generate_series(1, 10000) i;
```

The three-object pattern is the recommended production setup:
1. `clustered_heap` — directed placement on INSERT/COPY
2. `clustered_pk_index` — key discovery for the table AM
3. `btree` — query serving (planner always prefers it)

### Run tests

```bash
make installcheck PGPORT=5432
```

This runs 15 regression test groups covering: int2/int4/int8 index types, locator edge cases, JOIN UNNEST rescan correctness, delete + vacuum + GC consistency, segment split boundaries, empty tables, directed placement verification, COPY bulk path, UPDATE/DELETE, NULL keys, many-key fast path, VACUUM + re-insert, TRUNCATE + re-insert, and JOIN + btree production pattern.

## Architecture

### Directed placement (Table AM)

The core mechanism is in `tuple_insert` and `multi_insert` overrides:

1. On first INSERT, the table AM discovers the `clustered_pk_index` via catalog lookup and caches the key attribute number and type OID.
2. For each tuple, it extracts the clustering key, looks up the zone map (in-memory HTAB: `int64 key → BlockNumber`), and sets `RelationSetTargetBlock` before delegating to the standard heap insert.
3. After insertion, it records the actual block where the tuple landed.

The `multi_insert` path (used by COPY) has an adaptive strategy:
- **≤64 distinct keys** → sort + group: full clustering with per-group `ReleaseBulkInsertStatePin`
- **>64 distinct keys** → fast path: single heap_multi_insert call with lightweight zone map recording

### Zone map

- Per-relation HTAB stored in `TopMemoryContext` (survives transactions)
- Overflow guard: max 1M keys per relation (auto-reset on overflow)
- Cross-relation guard: max 256 relations (full cleanup on overflow)
- Stale block validation: before each `RelationSetTargetBlock`, checks `block < RelationGetNumberOfBlocks(rel)` and evicts stale entries (protects against VACUUM heap truncation)
- Invalidated on: `set_new_filelocator`, `TRUNCATE`, `copy_data`, `copy_for_cluster`

### Segment map (legacy path)

The original architecture uses SQL metadata tables (`segment_map`, `segment_map_tids`) accessed via SPI to track key-to-segment mappings. This path includes locator helpers, split policies, and an index AM scan path with multi-level caching (local hint cache, rescan keycache, segment TID fastpath). In the recommended btree setup, the planner never selects this path — it is retained for edge cases where no btree exists.

## SQL API

### Access methods

```sql
CREATE TABLE t (...) USING clustered_heap;
CREATE INDEX ON t USING clustered_pk_index (key_column);
```

Supported key types: `int2`, `int4`, `int8`.

### Locator functions

```sql
locator_pack(major bigint, minor bigint) → clustered_locator
locator_pack_int8(bigint) → clustered_locator
locator_major(clustered_locator) → bigint
locator_minor(clustered_locator) → bigint
locator_to_hex(clustered_locator) → text
locator_cmp(a, b) → int
locator_advance_major(locator, delta) → clustered_locator
locator_next_minor(locator, delta) → clustered_locator
```

Comparison operators (`<`, `<=`, `=`, `>=`, `>`, `<>`) and a btree operator class (`clustered_locator_ops`) are provided.

### Segment map functions

```sql
segment_map_touch(relation_oid, major, minor, ...)
segment_map_allocate_locator(relation_oid, minor, ...)
segment_map_allocate_locator_regclass(regclass, minor, ...)
segment_map_next_locator(regclass, minor, ...)
segment_map_rebuild_from_index(index_relation, ...)
segment_map_stats(relation_oid)
segment_map_count_repack_due(relation_oid, interval_seconds)
segment_map_tids_gc(relation_oid)
```

### Observability

```sql
SELECT clustered_pg.version();
SELECT * FROM clustered_pg.observability();
```

## GUC parameters

All parameters are in the `clustered_pg.pkidx_*` namespace and control the legacy index AM scan path. In the recommended btree setup, these have no effect since the planner bypasses the custom index scan.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pkidx_enable_segment_fastpath` | bool | off | Enable segment_map_tids-driven scan fastpath |
| `pkidx_assume_unique_keys` | bool | off | Assume probed keys are unique (superuser-only) |
| `pkidx_max_segment_tids` | int | 131072 | Max segment_map_tids entries per scan |
| `pkidx_segment_prefetch_span` | int | 0 | Minor-key span for range prefetch |
| `pkidx_local_hint_max_keys` | int | 65536 | Max per-key local hint entries |
| `pkidx_exact_hint_publish_max_keys` | int | — | Max keys published as exact hints per rescan |
| `pkidx_rescan_keycache_trigger` | int | 2 | Rescan count before building keycache |
| `pkidx_rescan_keycache_min_distinct_keys` | int | 4 | Distinct-key threshold for keycache warmup |
| `pkidx_rescan_keycache_max_tids` | int | 262144 | Max total TIDs in keycache |
| `pkidx_enable_adaptive_sparse_select` | bool | off | Adaptive sparse-select bypass |
| `pkidx_adaptive_sparse_*` | int | — | Tuning knobs for adaptive sparse bypass |

## Limitations

- Clustering key must be a single-column integer (`int2`, `int4`, or `int8`)
- NULL clustering keys are rejected by the index AM
- UPDATE does not re-cluster: PostgreSQL routes updates through `heap_update`, not `tuple_insert`. Directed placement only applies to INSERT/COPY. This is acceptable for append-heavy workloads.
- Zone map is per-backend (not shared memory). Each backend builds its own zone map on first insert. Stale hints degrade to standard heap placement, not errors.
- Transaction rollback leaves stale zone map entries. These are best-effort placement hints — stale entries cause PostgreSQL to find another page, degrading performance but not correctness.

## Project structure

```
clustered_pg.c                  – Extension source (~5900 lines)
sql/clustered_pg--0.1.0.sql     – Extension install SQL (~845 lines)
sql/clustered_pg.sql            – Regression tests (~1080 lines, 15 test groups)
expected/clustered_pg.out       – Expected test output
clustered_pg.control            – Extension metadata
Makefile                        – PGXS build
scripts/                        – Operational shell scripts and selftests
```

## License

Experimental. Not yet licensed for redistribution.
