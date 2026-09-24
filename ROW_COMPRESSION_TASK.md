# Task: per-table in-memory row compression (`DbRow` as enum)

## Goal
A table can be flagged **`compressed`**. Rows of such a table are kept in RAM
**zstd-compressed per row**, transparently decompressed on read. Opt-in per table,
**master-node only**. Persisted/wire formats stay plain JSON — only the in-memory
representation changes.

## Status
A complete reference implementation is **already in this working copy** and compiles:
- `cargo build -p my-no-sql-core --features master-node` ✓
- `cargo build -p my-no-sql-core` (default / reader clients, no zstd pulled in) ✓
- `cargo build` (whole workspace) ✓

So "finishing" = review, optionally adjust, **add unit tests**, then **bump version + tag**.
Feel free to redo any part — the spec + API contract below is what matters for the server.

---

## What was changed (files in `my-no-sql-core`)

- **`Cargo.toml`** — `zstd` added as an **optional** dep, enabled only by `master-node`
  (`master-node = ["dep:zstd"]`). Reader clients (no master-node) do **not** pull zstd.

- **`src/db/db_table/db_table_attributes.rs`**
  - new field `pub compressed: bool`
  - `create_default()` → `compressed: false`
  - `new(...)` gained a `compressed` parameter (**signature change — see Breaking change**)
  - added `set_compressed(&mut self, compressed: bool) -> bool` (returns true if changed)

- **`src/db/db_row/db_row.rs`** — `DbRow` is now an enum:
  ```rust
  pub enum DbRow {
      Plain(DbRowPlain),                       // == the historical struct (raw + positions)
      #[cfg(feature = "master-node")]
      Compressed(DbRowCompressed),             // zstd body + uncompressed keys/metadata
  }
  ```
  - `DbRowPlain` = the old `DbRow` verbatim (just renamed; `time_stamp` made private).
  - `DbRowCompressed` keeps **owned `partition_key`/`row_key` strings** (so indexing never
    decompresses), the `expires`/`time_stamp` **positions** (valid against the decompressed
    bytes), the `expires_value`/`last_read_access` **atomics** (runtime updates preserved
    across compress↔decompress), the compressed body and the original `content_len`.
  - Every public method matches on the variant. The tricky `write_json` expires-injection
    logic was factored into one free fn `write_json_raw(raw, expires, expires_value, out)`
    used by both variants (compressed decompresses into a scratch `Vec<u8>` first).
  - New: `content_bytes(&self) -> Cow<[u8]>` (logical JSON, decompressed if needed),
    `get_content_size(&self) -> usize` (logical length, O(1)),
    `is_compressed()`, `compress_arc(Arc<DbRow>)`, `decompress_arc(Arc<DbRow>)`.
  - `get_src_as_slice()` now returns the **physical** bytes (compressed for a compressed
    row). All in-repo accounting callers were moved to `get_content_size()`.
  - zstd level = `const ZSTD_COMPRESSION_LEVEL: i32 = 3` (zstd default; tweak if desired).

- **`src/db/db_table/db_table_inner.rs`** — the three insert choke points
  (`insert_or_replace_row`, `insert_row`, `bulk_insert_or_replace`) compress the incoming
  row(s) **iff `self.attributes.compressed`**. This covers every server write path (HTTP,
  gRPC, TCP, transactions, init-from-sqlite, backup-restore) with no per-call-site changes.
  Added `apply_rows_compression(&mut self)` (re-encodes all stored rows to match the flag).

- **`src/db/db_partition/db_partition.rs`** — content-size accounting via `get_content_size()`;
  added `apply_rows_compression(bool)`.

- **`src/db/db_partition/db_rows_container.rs`** — added `apply_compression(bool)`: rebuilds
  the sorted vec **and the expiration index** with each row re-encoded.

- **`src/db/db_table/avg_size.rs`** + `db_table_master_node.rs` tests — `get_content_size()`.

---

## Design invariants (why it's correct)
- Keys are owned on the compressed variant ⇒ sorted-vec lookups / `EntityWithStrKey` /
  GC never decompress. Only emitting a row (`write_json`/`to_vec`/`content_bytes`) does.
- `get_content_size()` is the **logical** length ⇒ `get_table_size()` and `AvgSize`
  (which pre-sizes the decompressed output buffer) are unaffected by compression, and an
  in-place toggle does not perturb `content_size`.
- A table may freely hold a **mix** of Plain and Compressed rows — both render identically.
- Persistence stays plain: the server reads `content_bytes()` (decompressed) when saving.

---

## Breaking change to flag
`DbTableAttributes::new()` signature changed (added `compressed`) and the struct has a new
field, so **every struct-literal / `new()` construction must set `compressed`**. Inside this
workspace nothing calls `new()`, so the SDK builds clean. Downstream `my-no-sql-server`
literals (`new(...)`, `DbTableAttributes { ... }`) will be updated on the server side. If any
other repo constructs `DbTableAttributes`, add the field there too.

---

## API contract the server will depend on (keep stable for the tag)
- `DbTableAttributes`: field `compressed: bool`; `new(persist, max_partitions, max_rows, compressed, created)`; `set_compressed(bool) -> bool`.
- `DbRow`: `content_bytes() -> Cow<[u8]>`, `get_content_size() -> usize`, `is_compressed()`, `compress_arc(Arc<DbRow>) -> Arc<DbRow>`, `decompress_arc(Arc<DbRow>) -> Arc<DbRow>`.
- `DbTableInner::apply_rows_compression(&mut self)` (master-node) — call after toggling.
- Insert methods auto-compress based on `attributes.compressed`.

---

## To finish
1. Review / adjust (zstd level, names, gating).
2. **Add unit tests** (master-node):
   - `to_vec()`/`write_json()` of a Plain row == that of its `compress_arc` copy — with and
     without an `Expires` field (exercise both expires-injection branches).
   - `get_partition_key`/`get_row_key`/`get_content_size` correct on a compressed row.
   - `DbRowsContainer::apply_compression(true)` then `(false)` round-trips and keeps the
     expiration index length correct (reuse the existing expiration tests as a template).
3. Bump `my-no-sql-core` / `my-no-sql-sdk` version, **tag, push**.

➡️ **Give me the new tag** and I'll bump `my-no-sql-server/Cargo.toml` and finish the
server-side plumbing (table attribute end-to-end, `UpdateCompress` API, persist via
`content_bytes()`, optional UI/MCP surfacing).
