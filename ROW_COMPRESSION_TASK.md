# Per-table in-memory row compression (`DbRow` as enum)

## Goal
A table can be flagged **`compressed`**. Rows of such a table are kept in RAM
**DEFLATE-compressed per row** (`flate2`), transparently decompressed on read. Opt-in per table,
**master-node only**. Persisted/wire formats stay plain JSON — only the in-memory
representation changes.

## Status
Implemented and released — part of the workspace version and git tag `0.5.1`, which
`my-no-sql-server` pins. It compiles in every configuration:
- `cargo build -p my-no-sql-core --features master-node` ✓
- `cargo build -p my-no-sql-core` (default features, no `flate2` pulled in by this crate) ✓
- `cargo build` (whole workspace) ✓

Nothing is left to finish in this repo: the unit tests are in place (see [Unit tests](#unit-tests))
and the version is bumped and tagged. The spec + API contract below is what matters for the server.

---

## What was changed (files in `my-no-sql-core`)

- **`Cargo.toml`** — `flate2` (`default-features = false`, feature `zlib-rs`) added as an **optional**
  dep, enabled only by `master-node` (`master-node = ["dep:flate2"]`). Built without master-node,
  `my-no-sql-core` does **not** pull `flate2` (TCP readers still get `flate2` through
  `my-no-sql-tcp-shared`, which uses it for wire payload compression).

- **`src/db/db_table/db_table_attributes.rs`**
  - new field `pub compressed: bool`
  - `create_default()` → `compressed: false`
  - `new(...)` gained a `compressed` parameter (**signature change — see Breaking change**)
  - added `set_compressed(&mut self, compressed: bool) -> bool` (returns true if changed)

- **`src/db/db_row/db_row.rs`** — `DbRow` is now an enum:
  ```rust
  pub enum DbRow {
      Plain(DbRowPlain),                       // the historical layout (raw + positions)
      #[cfg(feature = "master-node")]
      Compressed(DbRowCompressed),             // DEFLATE body + uncompressed keys/metadata
  }
  ```
  - `DbRowPlain` = the historical `DbRow` layout (`raw` + positions; `time_stamp` made private). It
    has since gained `partition_key_unescaped`/`row_key_unescaped` (the logical key, kept only when
    the JSON key carries escape sequences) and the parsed `time_stamp_value`.
  - `DbRowCompressed` keeps the keys as **owned strings** (`partition_key_str`/`row_key_str`, so
    indexing never decompresses), the `partition_key`/`row_key`/`expires`/`time_stamp` **positions**
    (valid against the decompressed bytes), the parsed `time_stamp_value`, the
    `expires_value`/`last_read_access` **atomics** (runtime updates preserved across
    compress↔decompress), the compressed body and the original `content_len`.
  - Every public method matches on the variant. The tricky `write_json` expires-injection
    logic was factored into one free fn `write_json_raw(raw, expires, expires_value, out)`
    used by both variants (compressed decompresses into a scratch `Vec<u8>` first).
  - New: `content_bytes(&self) -> Cow<[u8]>` (logical JSON, decompressed if needed),
    `get_content_size(&self) -> usize` (logical length, O(1)),
    `is_compressed()`, `compress_arc(Arc<DbRow>)`, `decompress_arc(Arc<DbRow>)`.
  - `get_src_as_slice()` now returns the **physical** bytes (compressed for a compressed
    row). All in-repo accounting callers were moved to `get_content_size()`.
  - DEFLATE level = `const DEFLATE_COMPRESSION_LEVEL: u32 = 6` (flate2's default level; the
    comment on the constant says why).

- **`src/db/db_table/db_table_inner.rs`** — the three insert choke points
  (`insert_or_replace_row`, `insert_row`, `bulk_insert_or_replace`) compress the incoming
  row(s) **iff `self.attributes.compressed`**. These are the only `DbTableInner` methods that take
  rows, so a caller that writes through them needs no per-call-site changes. `restore_partition` /
  `init_partition` (a ready `DbPartition`) and inserts made directly on a `DbPartition` do not compress.
  Added `apply_rows_compression(&mut self)` (re-encodes all stored rows to match the flag).

- **`src/db/db_partition/db_partition.rs`** — content-size accounting via `get_content_size()`;
  added `apply_rows_compression(bool)`.

- **`src/db/db_partition/db_rows_container.rs`** — added `apply_compression(bool)`: rebuilds
  the sorted vec **and the expiration index** with each row re-encoded.

- **`src/db/db_table/avg_size.rs`** + `db_table_master_node.rs` tests — `get_content_size()`.

---

## Design invariants (why it's correct)
- Keys are owned on the compressed variant ⇒ sorted-vec lookups / `EntityWithStrKey` /
  GC never decompress. Only emitting a row (`write_json`/`to_vec`/`content_bytes`) or re-encoding
  it (`decompress_arc`) does.
- `get_content_size()` is the **logical** length ⇒ `get_table_size()` and `AvgSize` are
  unaffected by compression, and an in-place toggle does not perturb `content_size`.
- A table may freely hold a **mix** of Plain and Compressed rows — both render identically.
- Persistence stays plain: every way to emit a row (`write_json`/`to_vec`/`content_bytes()`, and the
  `as_json_array()` builders, which go through `write_json`) yields the decompressed JSON; only
  `get_src_as_slice()` returns the compressed bytes.

---

## Breaking change to flag
`DbTableAttributes::new()` signature changed (added `compressed`) and the struct has a new
field, so **every struct-literal / `new()` construction must set `compressed`**. Inside this
workspace nothing calls `new()`, so the SDK builds clean. Downstream `my-no-sql-server`
(which pins tag `0.5.1`) sets it in its own `new(...)` / `DbTableAttributes { ... }` constructions. If any
other repo constructs `DbTableAttributes`, add the field there too.

---

## API contract for the server (keep stable)
- `DbTableAttributes`: field `compressed: bool`; `new(persist, max_partitions_amount, max_rows_per_partition_amount, compressed, created)`; `set_compressed(bool) -> bool`.
- `DbRow`: `content_bytes() -> Cow<[u8]>`, `get_content_size() -> usize` (every build); `is_compressed()`, `compress_arc(Arc<DbRow>) -> Arc<DbRow>`, `decompress_arc(Arc<DbRow>) -> Arc<DbRow>` (master-node).
- `DbTableInner::apply_rows_compression(&mut self)` (master-node) — call after toggling.
- Insert methods auto-compress based on `attributes.compressed`.

---

## Unit tests
Compiled only with the feature: `cargo test -p my-no-sql-core --features master-node`.
- `db_row.rs`: `compressed_renders_identically_without_expires` — `to_vec()`/`write_json()` of a
  Plain row == that of its `compress_arc` copy; `compressed_renders_identically_with_expires` —
  the same for `to_vec()` with an `Expires` field.
- `db_row.rs`: `keys_and_content_size_are_correct_on_a_compressed_row` —
  `get_partition_key`/`get_row_key`/`get_content_size` (and `content_bytes`) on a compressed row.
- `db_row.rs`: `decompress_arc_round_trips_back_to_plain`, `update_expires_is_preserved_across_compression`.
- `db_rows_container.rs`: `apply_compression_round_trips_and_keeps_expiration_index` —
  `DbRowsContainer::apply_compression(true)` then `(false)` round-trips and keeps the
  expiration index length correct.
- `test_escaped_keys.rs`: `compression_round_trip_keeps_the_logical_key`;
  `db_json_entity.rs`: `keep_date_time_round_trips_after_compression`.

The server-side plumbing (table attribute end-to-end, the API that toggles the flag, persistence,
UI surfacing) lives in `my-no-sql-server`.
