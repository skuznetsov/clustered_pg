# Clustered locator contract (v0.1)

The locator is an opaque 16-byte identifier intended to decouple logical
key order from physical heap TID placement.

## Binary layout

- `bytea` payload is exactly 16 bytes.
- `bytes 0..7`: `major_key` (signed 64-bit integer, encoded big-endian).
- `bytes 8..15`: `minor_key` (signed 64-bit integer, encoded big-endian).

## API

- `clustered_pg.locator_pack(major int8, minor int8) -> clustered_locator`
- `clustered_pg.locator_pack_int8(pk int8) -> clustered_locator`
- `clustered_pg.locator_major(loc clustered_locator) -> int8`
- `clustered_pg.locator_minor(loc clustered_locator) -> int8`
- `clustered_pg.locator_to_hex(loc clustered_locator) -> text`
- `clustered_pg.locator_cmp(a, b) -> int`
- `clustered_pg.locator_lt|le|eq|ge|gt|ne(a, b) -> bool`
- `clustered_pg.segment_map_allocate_locator(relation_oid, minor, ...) -> clustered_locator`
- `clustered_pg.segment_map_allocate_locator_regclass(regclass, minor, ...) -> clustered_locator`
- `clustered_pg.segment_map_next_locator(regclass, minor, ...) -> clustered_locator`
- `clustered_pg.segment_map_rebuild_from_index(index_relation, ...) -> bigint`
- `clustered_pg.clustered_locator_ops` btree operator class

## Segment-map allocator policy (v0.1)

- `segment_map_allocate_locator` selects an existing segment when `p_minor` already belongs to it and that segment still fits requested `p_split_threshold` (+ default from argument).
- When full, allocator opens a new `major_key` according to ordered boundaries:
  - before first range -> prepend a new `major_key = first_major - 1`;
  - after last range and full -> append new `major_key = last_major + 1`;
  - between or in gap -> insert after the last range whose `minor_to < p_minor`.
- `row_count` is updated atomically under advisory lock (`pg_advisory_xact_lock` on relation OID hash).

## Versioning note

- This is format v0.1.
- Future format changes should be introduced via new helper versions and must not mutate
  the existing layout.
