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
- [ ] P0 (CAUTION): implement native clustered index scan primitives (locator -> heap TID index) instead of full table scan fallback.
	- DoD: add path with low fan-out direct scan and regression for `SELECT ... WHERE id = ...` under large tables.
	- Adversary checks: reorder-heavy inserts + duplicate inner merge join + mark/restore under backward scan.
- [ ] P1 (CAUTION): add crash-recovery resilience around metadata rebuild (`repack`) and VACUUM callbacks.
	- DoD: partial failure in maintenance leaves `segment_map` in consistent state and recovers on next maintenance run.
	- Verification: inject SPI failures in unit harness and assert no orphaned map rows for dropped/rebuilt relations.
- [ ] P1 (SAFE): harden Table AM lifecycle edge paths (`relation_copy_data`, truncate, cluster) with strict DoC checks and no duplicate side effects.
	- DoD: each lifecycle callback executes at most one physical segment cleanup per call and returns unchanged heap behavior.
	- IN_PROGRESS: `clustered_pg_tableam_cluster_smoke` regression added to confirm `CLUSTER` clears stale segment map metadata.
- [ ] P2 (SAFE): stabilize cost model with explicit metadata-backed cardinality hints.
	- DoD: selective lookups continue choosing clustered index; full scans remain preferred for sequential workloads.
	- Verification: existing planner regression (`clustered_pg_am_costplanner`) plus an explicit `SELECT *` no-index scenario.
- [ ] P2 (SAFE): add observability: extension versioned settings, function-level counters, and actionable warning context.
	- DoD: every maintenance short-fail path logs relation OID + operation context and does not suppress root cause.
- [ ] P2 (SAFE): broaden test coverage for copy/update lifecycles (`REINDEX`, `ALTER INDEX ... SET`, `COPY`, `TRUNCATE`, drop/recreate index).
	- DoD: stable `make installcheck` with explicit pass/fail per fixture and zero flaky expectations.

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
- [ ] run full extension regression (`make installcheck`) on a stable `contrib_regression` cluster (currently blocked by missing local socket path).
- [x] eliminate `record`-field brittleness in `segment_map_allocate_locator` by replacing shared `record` locals with explicit typed scalar locals before `target_fillfactor`-based split checks.
- [x] implement dedicated clustered table AM wrapper that forwards to heap callbacks and purges `segment_map` metadata on rewrite/truncate, enabling stable lifecycle behavior.
- [x] extend clustered table AM wrapper with additional lifecycle callbacks (`relation_copy_data`, `relation_copy_for_cluster`) to keep segment metadata coherent after physical rewrites.
- [x] harmonize `segment_map` writer lock strategy: `segment_map_allocate_locator`, `segment_map_rebuild_from_index`, and `segment_map_touch` now use the same `pg_advisory_xact_lock(relation_oid::bigint)` contract to remove hash collision ambiguity.
- [x] add maintenance-call hardening in C layer: `clustered_pg_pkidx_execute_segment_map_maintenance` now acquires `pg_advisory_xact_lock(relation_oid::bigint)` before all SPI writes/deletes.

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
- [x] add clustered_heap table AM bootstrap smoke test proving CREATE TABLE USING clustered_heap and key scan behavior on inherited heap semantics.
- [x] extend clustered_heap table AM smoke to cover delete and truncate lifecycle on the delegated path.
- [x] add clustered_heap table AM smoke for index creation, filtered read, ANALYZE, and VACUUM lifecycle checks.
- [x] add allocator regression for interstitial inserts into saturated neighboring segments (ensures new major bucket allocation).
- [x] add defensive major-key collision rebasing in allocator to tolerate stale/concurrent `segment_map` rows.
