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
- `clustered_pg.clustered_locator_ops` btree operator class

## Versioning note

- This is format v0.1.
- Future format changes should be introduced via new helper versions and must not mutate
  the existing layout.
