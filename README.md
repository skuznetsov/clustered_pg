# clustered_pg

Experimental PostgreSQL extension scaffold for custom clustered storage experiments.

## Current state (0.1.0)

- C entry points exist for:
  - version: `version()`
  - table AM handler: `tableam_handler(internal)`
  - index AM handler: `pk_index_handler(internal)`
- Access methods registered:
  - `clustered_heap` (TABLE)
  - `clustered_pk_index` (INDEX)
- TABLE AM handler currently delegates to heap AM (`GetHeapamTableAmRoutine()`),
  intentionally conservative baseline for safe bootstrap.
- INDEX AM insert callback is active for single-column integer keys:
  - `aminsert` assigns ordered segment locators via `segment_map_allocate_locator(...)`.
- INDEX AM insert and build callbacks are wired to the split-policy allocator.
- INDEX AM scan callbacks now run through a table-scan fallback (`RelationGetIndexScan` + `table_beginscan`) so constrained index queries can execute through `clustered_pk_index` while retaining full heap fallback semantics.
- `ammarkpos` and `amrestrpos` are implemented on top of the heap-fallback scan path: mark stores the current tuple TID (or start-of-scan), and restore repositions via an internal table rescan to the saved point.
- Locator helpers are exposed via SQL functions:
  - `locator_pack(major, minor) -> bytea`
  - `locator_pack_int8(bigint) -> bytea` (single PK-value mapping)
  - `locator_major(locator) -> bigint`
  - `locator_minor(locator) -> bigint`
  - `locator_to_hex(locator) -> text`
  - `locator_cmp(a, b) -> int` (lexicographic major/minor compare)
  - `locator_advance_major(locator, delta) -> locator`
  - `locator_next_minor(locator, delta) -> locator`
  - `locator_lt|le|eq|ge|gt|ne(a, b) -> boolean`
  - `clustered_locator_ops` (`btree`) operator class for ordered planner behavior
- Segment metadata and split policy helpers:
  - `segment_map` table (`relation_oid`,`major_key`,`minor_from`,`minor_to`, ... counters and knobs)
  - `segment_map_touch(...)` (metadata upsert)
  - `segment_map_allocate_locator(relation_oid, minor, ...)` (policy-driven major assignment + counter update)
  - `segment_map_allocate_locator_regclass(regclass, minor, ...)` (same API, relation-name friendly)
  - `segment_map_next_locator(regclass, minor, ...)` (alias for one-liner allocation flow)
  - `segment_map_stats(relation_oid)` (ordered map snapshot)

## Safety notes

- The INDEX AM scan callbacks are functional (tuple fetch, bitmap, and rescan) and `amrescan` now reuses the underlying heap scan descriptor through `table_rescan` when possible, reducing per-rescan overhead.
- `ambuildempty` now clears stale segment-map rows for the target relation.
- `amvacuumcleanup` evaluates due segments and runs `segment_map_rebuild_from_index(...)` when maintenance is needed, which rebuilds segment_map state from current table key order.
- Added SQL helpers:
  - `segment_map_count_repack_due(relation_oid, interval_seconds)` to identify relation segments that are eligible for maintenance windows,
  - `segment_map_rebuild_from_index(index_relation, ...)` to rebuild per-relation segment_map metadata.
- This prevents unsupported operations while preserving a safe installable extension scaffold for API evolution.

See `docs/locator-contract.md` for the binary locator contract.

Example:

```sql
CREATE EXTENSION clustered_pg;

SELECT locator_pack(4, 7) AS l;
SELECT locator_major(locator_pack(4, 7)) AS major_part;
SELECT locator_minor(locator_pack_int8(12345)) AS pk_locator_minor;
SELECT locator_to_hex(segment_map_allocate_locator('my_table'::regclass::oid, 42)) AS loc_for_pk;
```

## Next milestones

1. define locator format and clustered key materialization policy
2. implement ordered tuple placement and split-aware table AM callbacks
3. implement logical locator index AM and maintenance lifecycle
4. add background maintenance worker and regression tests

Notes:

- Locator compare/advance and btree operator class are provided to support ordering and planner integration.
- `segment_map_allocate_locator()` is the first concrete split-policy routine in this branch.

Developer test hook:

- `make installcheck` uses `sql/clustered_pg.sql` + `expected/clustered_pg.out` (locator + segment-map fixture).
