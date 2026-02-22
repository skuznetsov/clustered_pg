# clustered_pg TODO

- [x] create extension bootstrap (Makefile/control/sql/c)
- [x] add C entry points for extension identity and AM handlers
- [x] fix extension script install-time schema binding (use @extschema@)
- [x] define TABLE AM handler binding path (heap delegate)
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

Current local plan:

- Step 1: keep TABLE AM delegating to heap for bootstrap stability.
- Step 2: keep INDEX AM installable and non-usable by planner via large cost, while unsupported index operations fail fast.
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

Locator contract draft (v0.1):
- Format: fixed 16-byte `bytea`, payload-only (no varlena metadata in locator value).
- Bytes 0..7: `major` (big-endian int64, signed interpretation on extraction).
- Bytes 8..15: `minor` (big-endian int64).
- Helper `locator_pack_int8(pk)` emits `{major=0, minor=pk}` to support single-column PK prototypes.
- Forward rule: future versions may only append semantics through new helper versions, not mutate existing byte layout.

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
- [x] run full extension regression (`make installcheck`) after all pending SQL/runtime fixes (pass on PG 18 local temp cluster).
- [x] eliminate `record`-field brittleness in `segment_map_allocate_locator` by replacing shared `record` locals with explicit typed scalar locals before `target_fillfactor`-based split checks.

Known environment blockers:

- no active blockers.

Latest execution trace:

- [x] `segment_map_allocate_locator` now computes split thresholds using explicit local columns:
	- `v_container_*` and `v_last_*` scalar fields are read directly from `SELECT ... INTO`.
	- `v_head_major_key`/`v_head_minor_from` are read explicitly for gap-prefix handling.
	- `v_prev_container_major_key` is used for backfill-gap major selection.
- [x] this removes runtime failures like `record "... " has no field "target_fillfactor"` caused by mixed record projections.
- [x] validate with a fresh `make installcheck` run against a clean `contrib_regression` cluster.
- [x] add perf-smoke and `target_fillfactor` boundary regression cases in SQL fixture (`clustered_pg_perf_smoke`, `clustered_pg_fillfactor_bounds/floor`).
- [x] add index-AM smoke test for clustered index insert callback and segment-map growth under 10k-row append workload.
- [x] add scale-focused AM smoke test (50k-row append) to guard split-policy growth behavior under larger volume.
- [x] add descending-order and churn AM smoke test to validate allocator behavior under anti-pattern and delete/reinsert workload.
- [x] optimize reverse-order allocator path: reuse head segment for descending backfill when it still has split capacity (improves segment count from row-per-segment to capacity-bounded behavior).
