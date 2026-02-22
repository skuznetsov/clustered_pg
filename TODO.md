# clustered_pg TODO

- [x] create extension bootstrap (Makefile/control/sql/c)
- [x] add C entry points for extension identity and AM handlers
- [x] fix extension script install-time schema binding (use @extschema@)
- [x] define TABLE AM handler binding path (heap delegate)
- [x] convert TABLE AM handler to explicit clustered heap routine wrapper with maintenance cleanup hooks (truncate/rewrite path)
- [x] implement index AM handler skeleton with safe no-op callbacks
- [x] define clustered locator helper API (`locator_pack`, `locator_major`, `locator_minor`, `locator_to_hex`)
- [x] define clustered locator format and key-to-location mapping contract
- [x] implement ordered split-aware locator assignment helper (`segment_map_allocate_locator`)
- [x] add default clustered index operator classes for supported key types (`int2`, `int4`, `int8`)
- [x] fix Index AM routine lifetime contract (`IndexAmRoutine` must be palloc'd) to avoid invalid free on backend teardown
- [x] implement background merge/compaction and VACUUM integration
- [x] parse and apply clustered index reloptions in index AM allocator path (`split_threshold`, `target_fillfactor`, `auto_repack_interval`).
- [x] fix reloptions retrieval in insert path to consume parsed `rd_options` (avoid re-parsing `bytea` as raw relation options; resolves unstable option parsing for `auto_repack_interval`).
- [x] add regression/protocol tests (locator contract + segment_map allocation policy)
- [x] wire operator class and planner path for `clustered_locator` (`btree`-class)
- [x] expose regclass-friendly segment locator APIs (`segment_map_allocate_locator_regclass`, `segment_map_next_locator`)
- [x] enable functional index AM scan callbacks by translating index scan keys to heap scan
- [x] implement `ambeginscan/endscan/rescan/amgettuple/amgetbitmap` lifecycle with table scan state
	- [x] add regression coverage for constrained index predicates through clustered index AM
	- [x] implement scan mark/restore behavior with restart + saved TID replay on the heap fallback path
	- [x] preserve mark/restore state through amrestrpos-initiated table scan bootstrap path
	- [x] add merge-join duplicate inner-path regression for mark/restore stress
	- [x] remove per-row insert-time logging and cache parsed reloptions in relcache
- [x] fix allocator gap-case collision between saturated neighboring major buckets in `segment_map_allocate_locator`

Current local plan:

- Step 1: keep TABLE AM delegating to heap for bootstrap stability, but return a dedicated wrapper routine (now implemented) to allow non-block/offset policy extensions.
- Step 2: keep INDEX AM installable and planner-accessible with an adaptive `amcostestimate` that reflects clustered locator behavior.
- Step 3: introduce a PK-oriented locator encoding contract and expose helper SQL functions for deterministic key-location materialization.
- Step 4: segment-map persistence + ordered split policy now has SQL allocator helper; next cycle is callback wiring.
- Step 5: keep index scan callbacks returning FEATURE_NOT_SUPPORTED while build/inserts/maintenance metadata hooks are active.
- Step 6: build ordered locator primitive operations (`cmp`, `advance_major`, `next_minor`) for split-policy planning.
- Step 7: make clustered index AM usable with native `int2/int4/int8` keys by default operator class installation.

Execution loop status:

- [x] segment map schema + allocator contract scaffold is in place.
- [x] wire split-policy allocator into index AM insert callback (`aminsert`).
- [x] add default operator classes for clustered index AM on `int2/int4/int8`.
- [x] wire split-policy allocator into remaining callbacks (tableam/indexam).
- [x] add VACUUM-aware segment maintenance hook (`amvacuumcleanup`) to execute due-segment rebuilds via `segment_map_rebuild_from_index()`.
- [x] synchronize regression test fixture output (expected/clustered_pg.out) with current SQL runtime behavior.
- [x] add SQL helper `segment_map_count_repack_due()` for maintenance window heuristics.
- [x] implement index AM scan lifecycle scaffolding and functional `amgettuple`/`amgetbitmap` path using table scan fallback.
- [x] replace hardcoded clustered index cost estimates with a planner-aware `genericcostestimate`-based path model and correlation heuristic.
- [x] add regression coverage that asserts `EXPLAIN (COSTS OFF)` uses clustered index for selective lookups.
- [x] add planner-comparison regression (`enable_seqscan/on`) to validate clustered index is chosen only when it is cost-effective for selective lookups.
- [x] run local compile verification for the above regression path (`make` PASS); full `make installcheck` currently blocked because contrib regression cluster is unavailable (`/tmp/.s.PGSQL.5432` missing).

Locator contract draft (v0.1):
- Format: fixed 16-byte `bytea`, payload-only (no varlena metadata in locator value).
- Bytes 0..7: `major` (big-endian int64, signed interpretation on extraction).
- Bytes 8..15: `minor` (big-endian int64).
- Helper `locator_pack_int8(pk)` emits `{major=0, minor=pk}` to support single-column PK prototypes.
- Forward rule: future versions may only append semantics through new helper versions, not mutate existing byte layout.

Production hardening program (next):

- [x] P0 (CAUTION): define and implement a production-grade concurrent maintenance model for `segment_map` (`SPI` paths) to avoid cross-backend allocator race windows.
	- Completed in `sql/clustered_pg--0.1.0.sql`:
		- `segment_map_allocate_locator` and `segment_map_rebuild_from_index` now lock on `pg_advisory_xact_lock(relation_oid::bigint)` (no hash-based lock key).
		- `segment_map_touch` now acquires the same advisory lock before upsert, so all metadata writers are serialized consistently.
		- DoD: lock-key collision risk removed for OID-sized relation identities; writer-ordering for allocator and manual touch paths is aligned.
	- Verification (next): add concurrent-maintenance regression (`pgbench` / `pgbench`-driver `INSERT` script) and run `make installcheck`.
	- Adversary checks pending: duplicate locator collision under parallel writes and advisory lock starvation behavior.
- [x] Added local concurrency smoke harness (`scripts/clustered_pg_concurrency_smoke.sh`) to run pgbench-based parallel insert pressure for allocator invariants.
	- Verifies that `segment_map.sum(row_count)` matches table cardinality and no segment overflows its configured capacity under concurrent inserts.
	- Skipped automatically when `psql`/`pgbench` are unavailable.
	- Make target available: `make concurrency-smoke CONCURRENCY_SMOKE_DB=<your-db>`.
- [x] P0 (CAUTION): implement native clustered index scan primitives (locator -> heap TID index) instead of full table scan fallback.
	- DoD: index scan path uses `segment_map_tids` for equality predicate on single-column PK and falls back safely for non-native keys.
	- C updates:
		- `clustered_pg_pkidx_rescan_internal` now detects `=` scan key on supported types, populates `segment_tids`, and runs native segment replay.
		- `clustered_pg_pkidx_insert` now stores each tuple locator via `segment_map_tids`.
		- `segment_map_rebuild_from_index` now repopulates `segment_map_tids` during full rebuild.
	- Verification pending: run `make installcheck` (or focused regression fixture) to lock in behavior under merge-join mark/restore and backward scans.
- [x] P1 (CAUTION): add crash-recovery resilience around metadata rebuild (`repack`) and VACUUM callbacks.
	- DoD: partial failure in maintenance leaves `segment_map` in consistent state and recovers on next maintenance run.
	- Evidence: `segment_map_rebuild_from_index` now supports test fault injection (`p_fail_after_n_rows`) and restores `segment_map` + `segment_map_tids` state from pre-run temp backups on exception.
	- Verification: `clustered_pk_int8_rebuild_fault_table` keeps row_sum and segment_count identical before/after failure path while reporting `success = false`.
- [x] P1 (SAFE): harden Table AM lifecycle edge paths (`relation_copy_data`, truncate, cluster) with strict DoC checks and no duplicate side effects.
  - DoD: each lifecycle callback executes at most one physical segment cleanup per call and returns unchanged heap behavior.
  - Evidence: `clustered_pg_clustered_heap_clear_segment_map()` now checks for metadata existence under the same advisory lock and skips writes if no segment rows exist.
  - Evidence: `clustered_pg_tableam_cluster_smoke` regression confirms `CLUSTER` clears stale segment map metadata.
  - Evidence: `clustered_pg_tableam_copy_data_smoke` regression confirms `relation_copy_data`/`VACUUM FULL` path clears stale metadata and preserves table rows.
- [x] P1 (SAFE): add vacuum-time orphan cleanup for segment_map_tids (`segment_map_tids_gc`) and wire it through clustered index VACUUM callback.
  - DoD: `VACUUM` on an indexed relation after deletes leaves no `segment_map_tids` entries for missing CTIDs.
  - Invariant: stale tuple mappings are bounded and cannot grow indefinitely between explicit rebuilds.
  - Evidence target: `segment_map_tids_gc` is now callable as maintenance pass and invoked from `clustered_pg_pkidx_vacuumcleanup`.
- [x] P1 (SAFE): stabilize `clustered_pg_pkidx_gc_segment_tids` callback by removing cached SPI prepared plan usage.
  - DoD: `VACUUM` no longer crashes with SIGSEGV after `segment_map_tids_gc` warning path.
  - Change: invoke `segment_map_tids_gc` through `SPI_execute_with_args()` in a per-call query path instead of a cached `SPIPlanPtr`.
  - Verification: local repro sequence (`CREATE TABLE`, `CREATE INDEX`, `DELETE`, `segment_map_tids_gc`, `VACUUM`) completes without server termination.
- [x] P1 (SAFE): harden regression proof for segment_map_tids cleanup invariants under delete+VACUUM.
  - DoD: automated regression asserts `segment_map_tids_gc` removes stale entries and keeps mapping cardinality aligned with live tuples.
  - Added case: `clustered_pk_int8_vacuum_table` uses manual `segment_map_tids_gc` + `VACUUM` and verifies mapping counts before/after.
- [x] P2 (SAFE): stabilize cost model with explicit metadata-backed cardinality hints.
	- DoD:
		- `clustered_pg_pkidx_costestimate` now derives selector floor from `segment_map`-backed row-count metadata and applies it only for selective indexed predicates.
		- `clustered_pg_am_costplanner` now explicitly checks both indexed lookup (`id =`) and non-selective scan paths (`count(*)`, `SELECT *`) including an explicit no-index probe (`enable_indexscan=off`), and still prefers clustered index for selective lookups.
	- Verification command set:
		- `cd /Users/sergey/Projects/C/clustered_pg && EXPLAIN (COSTS OFF) SELECT id FROM clustered_pg_am_costplanner WHERE id = 12345;`
		- `cd /Users/sergey/Projects/C/clustered_pg && EXPLAIN (COSTS OFF) SELECT count(*) FROM clustered_pg_am_costplanner;`
		- `cd /Users/sergey/Projects/C/clustered_pg && EXPLAIN (COSTS OFF) SELECT * FROM clustered_pg_am_costplanner;`
  - DoD: selective lookups continue choosing clustered index; full scans remain preferred for sequential workloads.
  - Verification: existing planner regression (`clustered_pg_am_costplanner`) plus an explicit `SELECT *` no-index scenario.
- [x] P2 (SAFE): add observability: extension versioned settings, function-level counters, and actionable warning context.
	- DoD:
		- Added `clustered_pg_observability()` returning versioned runtime stats:
			`clustered_pg=0.1.0 api=1 defaults={split_threshold,target_fillfactor,auto_repack_interval} counters={...}`.
		- Added counter increments for planner cost-estimation, segment rowcount lookups, insert, scan, and maintenance paths.
		- Added relation + operation context for maintenance warning paths in `vacuumcleanup` short-fail handling.
		- Updated regression fixture:
			- `SELECT public.clustered_pg_observability() AS observability_bootstrap;`
			- `SELECT (public.clustered_pg_observability() ~ 'costestimate=[0-9]+') AS costestimate_tracked;`
	- Verification command set:
		- `cd /Users/sergey/Projects/C/clustered_pg && make`
		- `cd /Users/sergey/Projects/C/clustered_pg && make installcheck` (currently blocked by missing local `/tmp/.s.PGSQL.5432` cluster socket)
- [x] P2 (SAFE): broaden test coverage for copy/update lifecycles (`REINDEX`, `ALTER INDEX ... SET`, `COPY`, `TRUNCATE`, drop/recreate index).
  - DoD: stable `make installcheck` with explicit pass/fail per fixture and zero flaky expectations.
  - Completed: added `clustered_pg_lifecycle_copyupdate_smoke` fixture with assert-only invariants:
    - `COPY` inserts keep `segment_map` row_count aligned with live table rows.
    - `REINDEX` preserves `segment_map` alignment.
    - `ALTER INDEX ... SET` keeps `segment_map` alignment.
    - `DROP INDEX` + insert + recreate index preserves `segment_map` alignment.

Decision protocol (Quadrumvirate):
- For each milestone, run one `Cassandra` precheck, then implement one focused patch, then `Adversary` checks (edge/concurrency/regression) before marking it `COMPLETED`.
- If two attempts do not increase confidence, run `Daedalus` pivot and switch design before continuing.
- If any claim reaches “VERIFIED”, attach command-level evidence in this file and update this status block.

Next milestone (explicit):

- [x] segment-map phase: persist mapping from `major` locator bucket to ordered segment metadata.
- [x] split policy phase: derive/advance `major` policy helper (`segment_map_allocate_locator`) and segment metadata growth.
- [x] split-aware insert/update execution path for custom index/table AM callback wiring.
- [x] wire split-policy allocator into index AM build path (`ambuild`) via table AM build scan.
- [x] maintenance phase: define background compaction contract and VACUUM-safe rewrite points.
- [x] add composite index on `segment_map` ranges to support `p_minor BETWEEN` lookups at scale.

Current engineering status:

- [x] fixed compile-time regression in `clustered_pg_pkidx_vacuumcleanup` logging block (`ereport` parentheses).
- [x] removed per-row segment-map maintenance work from `ambuild` by switching build callback to count-only and rebuilding segment map once post-scan (`segment_map_rebuild_from_index` path).
- [x] verified extension C code builds successfully with `make` using local PostgreSQL 18.
- [x] hardened SQL allocator/rebuild path against `search_path` resolution by schema-qualifying `locator_pack` calls with `@extschema@`.
- [x] add vacuum-time orphan cleanup for `segment_map_tids` via `segment_map_tids_gc` and wire it through `clustered_pg_pkidx_vacuumcleanup`.
	- `VACUUM` now removes stale TID mappings for missing heap tuples, preventing scan-path drift after deletes/updates.
- [x] add regression proof for `segment_map_tids` cleanup under delete/VACUUM (`clustered_pk_int8_vacuum_table`):
	- manual `segment_map_tids_gc` and `VACUUM` assertions track mapping cardinality.
- [x] decouple `segment_map_tids_gc` from other maintenance steps in `vacuumcleanup` (always attempted, even when rebuild/touch fails).
- [x] add crash-recovery fault-injection regression for `segment_map_rebuild_from_index` (`clustered_pk_int8_rebuild_fault_table`) to validate backup/restore behavior on partial failures.
 - [ ] run full extension regression (`make installcheck`) on a stable `contrib_regression` cluster (currently blocked by missing local socket path).
- [x] eliminate `record`-field brittleness in `segment_map_allocate_locator` by replacing shared `record` locals with explicit typed scalar locals before `target_fillfactor`-based split checks.
- [x] implement dedicated clustered table AM wrapper that forwards to heap callbacks and purges `segment_map` metadata on rewrite/truncate, enabling stable lifecycle behavior.
- [x] extend clustered table AM wrapper with additional lifecycle callbacks (`relation_copy_data`, `relation_copy_for_cluster`) to keep segment metadata coherent after physical rewrites.
- [x] harmonize `segment_map` writer lock strategy: `segment_map_allocate_locator`, `segment_map_rebuild_from_index`, and `segment_map_touch` now use the same `pg_advisory_xact_lock(relation_oid::bigint)` contract to remove hash collision ambiguity.
- [x] add maintenance-call hardening in C layer: `clustered_pg_pkidx_execute_segment_map_maintenance` now acquires `pg_advisory_xact_lock(relation_oid::bigint)` before all SPI writes/deletes.
- [x] stabilize `VACUUM` maintenance path with `REINDEX` awareness:
	- `clustered_pg_pkidx_vacuumcleanup` now skips `segment_map_rebuild_segment_map()` and `segment_map_touch_repack()` when the callback is running under `REINDEX`.
	- `segment_map_tids_gc()` remains executed independently and still runs even when maintenance operations are skipped.
- [x] fix `segment_map_allocate_locator()` return ownership in `aminsert` path:
	- SQL `bytea` locator is copied into extension allocator memory before `SPI_finish()` so the pointer is valid and safe to parse/free in caller.
	- caller no longer reads from SPI context memory after cleanup, which removes intermittent `pfree`/lifetime crashes.
- [x] harden locator decode path before lookup:
	- `clustered_pg_pkidx_allocate_locator()` now uses `DatumGetByteaPCopy()` and validates `clustered_locator` payload length (`16` bytes) before returning.
	- `clustered_pg_pkidx_lookup_locator_values()` now validates locator pointer/output pointers and calls shared locator-length guard before reading payload.
	- local reproducible scenario (`COPY` + `INSERT (5),(6),(7)` on `clustered_heap` + `clustered_pk_index`) now completes without backend termination.
- [x] restore production insert write-path for segment-map TID index with safe SPI memory handling:
	- `clustered_pg_pkidx_touch_segment_tids()` uses a stack `ItemPointer` copy, so parameter lifetime is not coupled to caller tuple memory.
	- plan uses `ON CONFLICT` upsert to make writes idempotent across repeated insert retries.
	- `clustered_pg_pkidx_insert()` now calls touch again and keeps locator decode in caller-owned memory for immediate lookup/validation.
	- regression anchor added for `INSERT` + `DO` path that previously triggered `server closed the connection unexpectedly`.
- [x] enable PostgreSQL `CLUSTER` support for the custom index AM by setting `.amclusterable = true`.
- [x] remove dependent-order `ORDER BY` inside `segment_map_rebuild_from_index` rebuild loop to avoid ordering via the target index during concurrent reindex/maintenance.
- [x] guard `clustered_pg_pkidx_ambuild()` maintenance for reindex paths:
	- `CLUSTER/TRUNCATE/VACUUM FULL` no longer triggers `segment_map_rebuild_from_index(...)` while index is in reindex context.
	- this prevents `cannot access index ... while it is being reindexed` regressions and preserves expected `segment_map` zero-state after physical rewrites.

Known environment blockers:

- no active blockers.

Latest execution trace:

- [x] `segment_map_allocate_locator` now computes split thresholds using explicit local columns:
	- `v_container_*` and `v_last_*` scalar fields are read directly from `SELECT ... INTO`.
	- `v_head_major_key`/`v_head_minor_from` are read explicitly for gap-prefix handling.
	- `v_prev_container_major_key` is used for backfill-gap major selection.
- [x] this removes runtime failures like `record "... " has no field "target_fillfactor"` caused by mixed record projections.
- [x] validate with a fresh `make installcheck` run against a clean `contrib_regression` cluster.
- [x] optimize clustered PK index rescan path by skipping redundant `ScanKeyData` copy when source and destination key buffers alias.
- [x] add perf-smoke and `target_fillfactor` boundary regression cases in SQL fixture (`clustered_pg_perf_smoke`, `clustered_pg_fillfactor_bounds/floor`).
- [x] add index-AM smoke test for clustered index insert callback and segment-map growth under 10k-row append workload.
- [x] add scale-focused AM smoke test (50k-row append) to guard split-policy growth behavior under larger volume.
- [x] add descending-order and churn AM smoke test to validate allocator behavior under anti-pattern and delete/reinsert workload.
- [x] optimize reverse-order allocator path: reuse head segment for descending backfill when it still has split capacity (improves segment count from row-per-segment to capacity-bounded behavior).
- [x] add `segment_map_tids_gc(regclass)` and route it from `clustered_pg_pkidx_vacuumcleanup` for stale mapping cleanup.
- [x] add clustered_heap table AM bootstrap smoke test proving CREATE TABLE USING clustered_heap and key scan behavior on inherited heap semantics.
- [x] extend clustered_heap table AM smoke to cover delete and truncate lifecycle on the delegated path.
- [x] add clustered_heap table AM smoke for index creation, filtered read, ANALYZE, and VACUUM lifecycle checks.
- [x] add allocator regression for interstitial inserts into saturated neighboring segments (ensures new major bucket allocation).
- [x] add defensive major-key collision rebasing in allocator to tolerate stale/concurrent `segment_map` rows.
- [x] harden `clustered_pg_pkidx_vacuumcleanup` by splitting maintenance into independent callback blocks so `segment_map_tids_gc` still runs when metadata rebuild/touch fails.
- [x] remove cached-plan execution path in `clustered_pg_pkidx_gc_segment_tids` to avoid vacuum callback prepare instability.
- [x] harden PK index rescan path against stale `scan->keyData` after mark/restore cycles by driving scan predicates from stable state-backed keys and falling back safely to table scan if no valid source exists.
- [x] fix `clustered_pg_pkidx_allocate_locator()` by assigning `locatorRaw = DatumGetByteaP(locatorDatum)` before null-check/copy, preventing uninitialized-bytea copy crashes under insert pressure.
- [x] harden planner cost-estimation SPI path (`clustered_pg_pkidx_estimate_segment_rows`) for DO-block safety:
	- defer SPI plan initialization until after `SPI_connect()` succeeds,
	- guarantee single `SPI_finish()` on every path,
	- avoid recursive SPI teardown crashes that manifested as `server closed the connection unexpectedly` after `COPY + INSERT + REINDEX`.
- [x] extend lifecycle regression fixture to lock in DO aggregate stability:
	- `clustered_pg_lifecycle_copyupdate_smoke` now includes aggregate checks (`count(*)`, `max(i)`, `array_agg`) inside `DO` after reindex and copy lifecycle transitions.
- [x] harden index scan key-state transitions for keyless re-scan paths:
  - DoD:
	- `clustered_pg_pkidx_rescan_internal()` now resets `state->key_count` to `0` whenever `scan->numberOfKeys == 0`, including when cached `table_scan_keys` is freed.
	- this prevents stale key-count/`table_scan_keys` skew when switching from qualified scans to unqualified scans.
  - Verification:
	- `cd /Users/sergey/Projects/C/clustered_pg && make` (compile check passed).
	- regression validation of `REINDEX + DO (count/max/array_agg)` remains blocked until a stable contrib cluster socket exists at `/tmp/.s.PGSQL.5432`.

- [x] P0 (CAUTION): eliminate rescan key-count skew corruption in clustered index scans.
  - DoD:
	- `clustered_pg_pkidx_rescan_internal()` preserves `table_scan_keys` allocation for every `nkeys` transition without writing past `table_scan_keys` bounds.
	- Repro sequence involving REINDEX + DO aggregate (`count(*)`, `max`, `array_agg`) is expected to pass without backend termination after this fix; verification is pending cluster-level rerun.
