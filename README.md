# clustered_pg

PostgreSQL extension implementing physically clustered storage via a custom Table AM. Rows with the same key are placed on the same (or adjacent) heap blocks at INSERT time, so standard btree and BRIN indexes operate at near-optimal I/O efficiency without periodic `CLUSTER` maintenance.

## How it works

`clustered_pg` provides two access methods:

- **`clustered_heap`** (Table AM) — wraps the standard heap with a **directed placement** layer. On INSERT and COPY, the extension extracts the clustering key from each tuple, looks up a per-relation in-memory **zone map** (`key → block number`), and calls `RelationSetTargetBlock` to steer PostgreSQL's buffer manager toward the block where that key already lives. This keeps rows physically grouped by key.

- **`clustered_pk_index`** (Index AM) — a lightweight index whose only job is **key discovery**: the table AM finds it at first INSERT to learn which column is the clustering key. Scan callbacks are disabled (`amgettuple=NULL`); the planner always uses a standard btree for queries.

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

This runs 14 regression test groups covering: int2/int4 index types, locator edge cases, JOIN UNNEST correctness, delete + vacuum consistency, split boundaries, empty tables, directed placement verification, COPY bulk path, UPDATE/DELETE, NULL keys, many-key fast path, VACUUM + re-insert, TRUNCATE + zone map invalidation, and JOIN + btree production pattern.

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

### Observability

```sql
SELECT clustered_pg.version();
SELECT * FROM clustered_pg.observability();
```

## Limitations

- Clustering key must be a single-column integer (`int2`, `int4`, or `int8`)
- NULL clustering keys are rejected by the index AM
- UPDATE does not re-cluster: PostgreSQL routes updates through `heap_update`, not `tuple_insert`. Directed placement only applies to INSERT/COPY. This is acceptable for append-heavy workloads.
- Zone map is per-backend (not shared memory). Each backend builds its own zone map on first insert. Stale hints degrade to standard heap placement, not errors.
- Transaction rollback leaves stale zone map entries. These are best-effort placement hints — stale entries cause PostgreSQL to find another page, degrading performance but not correctness.

## Project structure

```
clustered_pg.c                  – Extension source (~1630 lines)
sql/clustered_pg--0.1.0.sql     – Extension install SQL (~230 lines)
sql/clustered_pg.sql            – Regression tests (~405 lines, 14 test groups)
expected/clustered_pg.out       – Expected test output
clustered_pg.control            – Extension metadata
Makefile                        – PGXS build
scripts/                        – Operational shell scripts and selftests
```

## License

Experimental. Not yet licensed for redistribution.
