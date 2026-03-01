# Clustered locator contract (v0.1)

The locator is an opaque 16-byte identifier intended to decouple logical
key order from physical heap TID placement.

## Binary layout

- `bytea` payload is exactly 16 bytes.
- `bytes 0..7`: `major_key` (signed 64-bit integer, encoded big-endian).
- `bytes 8..15`: `minor_key` (signed 64-bit integer, encoded big-endian).

## API

- `pg_sorted_heap.locator_pack(major int8, minor int8) -> clustered_locator`
- `pg_sorted_heap.locator_pack_int8(pk int8) -> clustered_locator`
- `pg_sorted_heap.locator_major(loc clustered_locator) -> int8`
- `pg_sorted_heap.locator_minor(loc clustered_locator) -> int8`
- `pg_sorted_heap.locator_to_hex(loc clustered_locator) -> text`
- `pg_sorted_heap.locator_cmp(a, b) -> int`
- `pg_sorted_heap.locator_lt|le|eq|ge|gt|ne(a, b) -> bool`
- `pg_sorted_heap.clustered_locator_ops` btree operator class

## Versioning note

- This is format v0.1.
- Future format changes should be introduced via new helper versions and must not mutate
  the existing layout.
