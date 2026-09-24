# MyNoSqlServer Rust SDK

Rust client SDK and shared core for **MyNoSqlServer** — an in-memory NoSql server where data is written over HTTP and read over TCP (each client keeps a live local copy of the tables it subscribes to).

This is a cargo workspace of 9 crates. Application code normally depends on **one** of them — `my-no-sql-sdk` — and turns on the features it needs.

---

## Packages

| Crate | What it is | Use it when |
|---|---|---|
| **my-no-sql-sdk** | Facade — re-exports all the other crates behind cargo features | Always. This is the crate your service depends on |
| **my-no-sql-abstractions** | `MyNoSqlEntity`, `MyNoSqlEntitySerializer`, `DataSynchronizationPeriod`, `Timestamp` | Pulled in automatically; depend on it directly only in a crate that must stay dependency-light |
| **my-no-sql-macros** | Proc-macros: `my_no_sql_entity`, `enum_of_my_no_sql_entity`, `enum_model` | Defining entities. Enable via feature `macros` |
| **my-no-sql-data-writer** | HTTP writer — insert / replace / delete / read against the server REST API | The service writes data. Feature `data-writer` |
| **my-no-sql-tcp-reader** | TCP reader — subscribes to a table and keeps an in-memory copy; reads are local and synchronous | The service reads data. Feature `data-reader` |
| **my-no-sql-tcp-shared** | TCP protocol contracts, serializer, payload compression, sync-to-main-node handler | Rarely direct — it is what the reader and the server speak. Feature `tcp-contracts` |
| **my-no-sql-core** | The data model itself: `DbTable`, `DbPartition`, `DbRow`, JSON entity parsing, entity serializer | Always present (re-exported as `my_no_sql_sdk::core`) |
| **my-no-sql-server-core** | `DbInstance` / `DbTable` wrappers and table snapshots used by server-side nodes | You are building a MyNoSql master node or read node. Features `master-node` / `read-node` |
| **my-no-sql-tests** | Internal integration tests for the macros and serializers | Never — not part of the public API |

Dependency direction:

```
my-no-sql-sdk
 ├── my-no-sql-abstractions      (entity traits)
 ├── my-no-sql-core ─────────────┐
 ├── my-no-sql-macros            │
 ├── my-no-sql-data-writer       │ (HTTP, via flurl)
 ├── my-no-sql-tcp-reader ── my-no-sql-tcp-shared
 └── my-no-sql-server-core ──────┘
```

---

## Adding to a project

The repo is released as a single repo-wide git tag; every crate in it carries the same version. Pin the tag:

```toml
[dependencies]
my-no-sql-sdk = { tag = "0.5.1", git = "https://github.com/MyJetTools/my-no-sql-sdk.git", features = [
    "macros",
    "data-writer",
    "data-reader",
] }
serde = { version = "*", features = ["derive"] }
```

### Features of `my-no-sql-sdk`

| Feature | Enables | Re-exported as |
|---|---|---|
| `macros` | entity proc-macros | `my_no_sql_sdk::macros` |
| `data-writer` | HTTP writer | `my_no_sql_sdk::data_writer` |
| `data-reader` | TCP reader | `my_no_sql_sdk::reader` |
| `tcp-contracts` | raw TCP contracts | `my_no_sql_sdk::tcp_contracts` |
| `master-node` | server-side model + per-table row compression (zstd) | `my_no_sql_sdk::server` |
| `read-node` | server-side model for a read node | `my_no_sql_sdk::server` |
| `with-ssh` | writer can reach the server through an SSH tunnel | — |
| `debug_db_row` | extra `DbRow` diagnostics | — |

`my_no_sql_sdk::core` and `my_no_sql_sdk::abstractions` are always available.

---

## Defining entities

Entities belong in a **separate shared crate** that both the writing service and the reading service depend on — never copy-paste the struct.

```rust
use my_no_sql_sdk::macros::my_no_sql_entity;
use serde::{Deserialize, Serialize};

#[my_no_sql_entity("instruments")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentEntity {
    pub name: String,
    pub digits: i32,
}
```

The macro adds `partition_key`, `row_key` and `time_stamp` fields and implements `MyNoSqlEntity` + `MyNoSqlEntitySerializer`:

```rust
let entity = InstrumentEntity {
    partition_key: "instruments".to_string(),
    row_key: "EURUSD".to_string(),
    time_stamp: Default::default(),   // Default for normal writes (server stamps its own time);
                                      // set a real value only for the *_if_new methods below
    name: "Euro vs Dollar".to_string(),
    digits: 5,
};
```

Auto-expiring entities add an `expires: Timestamp` field:

```rust
#[my_no_sql_entity(table_name: "sessions", with_expires: true)]
```

One table can also hold several shapes at once — `enum_of_my_no_sql_entity` on the enum, `enum_model` on each case,
which fixes that case's partition key (and optionally row key). Working example:
[my-no-sql-tests/src/macros_tests/enum_test.rs](my-no-sql-tests/src/macros_tests/enum_test.rs).

---

## Namespaces

Every table and every row lives inside a namespace. When none is configured it is the `default`
namespace — exactly what every existing service does today.

The namespace is a part of the connection string, and the format is the same for the writer
(`MyNoSqlWriterSettings::get_url`) and for the reader (`MyNoSqlTcpConnectionSettings::get_host_port`).
Both traits stay as they are — only the value in your `settings.yaml` changes:

```yaml
# legacy — the whole string is the host, namespace is `default`
MyNoSqlWriterUrl: http://10.0.0.1:5123

# new format — `;` separated `key=value` pairs, starts with `host=`
MyNoSqlWriterUrl: host=http://10.0.0.1:5123;ns=alpha
MyNoSqlReaderUrl: host=10.0.0.1:5125;ns=alpha
```

Rules:

* a string starting with `host=` is the new format, anything else is the legacy host as it always was;
* keys are case-insensitive, spaces around `;` and `=` are trimmed, `host` is mandatory, `ns` is optional;
* an unknown key is an error — better not to start than to silently read the wrong namespace;
* a namespace name follows the table-name rules (`[a-z0-9-]` only, no leading/trailing `-`, no `--`),
  1–63 symbols. Upper case is an error and is never silently lower-cased: namespaces are auto-created
  by the server, so a typo has to fail instead of bringing a garbage namespace to life;
* the `default` namespace is never transmitted — the writer sends no `ns` header and the reader sends
  no `SetNamespace` packet, so servers which know nothing about namespaces are not affected at all.

A non-default namespace does require a server which supports them: the writer sends it as the `ns`
header of every request, and the reader sends the `SetNamespace` TCP packet right after the `Greeting`
and before the first `Subscribe`. An old server breaks the connection on that packet instead of
silently serving the default namespace.

Helpers are re-exported at the root of `my-no-sql-sdk`: `DEFAULT_NAMESPACE`, `DbNamespaceName`,
`validate_namespace_name`, `parse_connection_string` / `ConnectionString`.

---

## Writing data

Implement the settings trait (usually on your `SettingsReader`):

```rust
#[async_trait::async_trait]
impl MyNoSqlWriterSettings for SettingsReader {
    async fn get_url(&self) -> String { self.get_my_no_sql_url().await }
    fn get_app_name(&self) -> &'static str { crate::APP_NAME }
    fn get_app_version(&self) -> &'static str { crate::APP_VERSION }
}
```

Build the writer:

```rust
use my_no_sql_sdk::abstractions::DataSynchronizationPeriod;
use my_no_sql_sdk::data_writer::MyNoSqlDataWriter;

let writer = MyNoSqlDataWriter::<InstrumentEntity>::create_with_builder(settings.clone())
    .set_sync_period(DataSynchronizationPeriod::Immediately)
    .persist_table(true)
    .build();
```

Builder options: `set_sync_period`, `persist_table`, `set_max_partitions_amount`,
`set_max_row_per_partitions_amount`, `do_not_auto_create_table`, `use_h1`.
By default the table is auto-created on the first request.

**Always go through `with_retries`** — the writer itself does not retry:

```rust
let w = writer.with_retries(3);

w.insert_or_replace_entity(&entity).await?;
w.bulk_insert_or_replace(&entities).await?;

let one = w.get_entity("instruments", "EURUSD", None).await?;      // Result<Option<T>>
let part = w.get_by_partition_key("instruments", None).await?;     // Result<Option<Vec<T>>>

let n = w.get_rows_count(Some("instruments")).await?;              // Result<Option<usize>> — count only

w.delete_row("instruments", "EURUSD").await?;
w.delete_partitions(&["instruments"]).await?;

// Bulk delete: PartitionKey -> RowKeys
let mut rows_to_delete = std::collections::BTreeMap::new();
rows_to_delete.insert("instruments".to_string(), vec!["EURUSD".to_string()]);
w.bulk_delete(&rows_to_delete).await?;
```

These deletes are unconditional. To delete only a row which has not changed since you read
it, see [Optimistic-concurrency delete](#optimistic-concurrency-delete-delete_entity_if--bulk_delete_if).
To ask how many rows a partition holds without moving the rows, see
[Counting rows without reading them](#counting-rows-without-reading-them-get_rows_count).

The writer returns **owned** entities (`Result<Option<T>, DataWriterError>`), unlike the reader which hands out `Arc<T>`.

### Insert-or-replace-if-new (client-versioned writes)

`InsertOrReplaceIfNew` upserts a row **only when it is missing, or when the incoming `TimeStamp` is strictly greater than the stored one** — a last-writer-by-version upsert for distributed systems. Here the `TimeStamp` is the object's version assigned **by the client**, and it is **mandatory**.

> This is the one exception to the "`time_stamp: Default::default()` — never now" rule. Every other write lets the server stamp its own time; these methods require you to set `time_stamp` to your real version. A default/unset `Timestamp` serializes to `null` / is omitted, and the server rejects the request with **HTTP 400** (`Entity with PartitionKey '..' RowKey '..' does not contain TimeStamp`).

```rust
use my_no_sql_sdk::core::rust_extensions::date_time::DateTimeAsMicroseconds;

let entity = InstrumentEntity {
    partition_key: "instruments".to_string(),
    row_key: "EURUSD".to_string(),
    time_stamp: DateTimeAsMicroseconds::now().into(),   // your version — REQUIRED here
    name: "Euro vs Dollar".to_string(),
    digits: 5,
};

// Single entity (200) and array (202, empty slice = no-op) — both also on with_retries.
w.insert_or_replace_entity_if_new(&entity).await?;
w.bulk_insert_or_replace_if_new(&entities).await?;

// Large snapshots: upload in chunks and commit atomically. On any failure it makes a
// best-effort Cancel of the half-uploaded process and returns the original error.
// The chunked flow lives on the base writer (NOT on with_retries — blindly re-sending a
// chunk after a partial success would double-append rows into the server-side accumulator).
writer.bulk_insert_or_replace_if_new_by_chunks(&entities, 1000).await?;

// Or drive the process yourself (streaming):
let pid = writer.insert_or_replace_if_new_by_chunks_start(&first_chunk).await?;
writer.insert_or_replace_if_new_by_chunks_append(&pid, &next_chunk).await?;
writer.insert_or_replace_if_new_by_chunks_commit(&pid).await?;   // or ..._cancel(&pid)
```

### Atomic snapshot replace (`clean_*_and_bulk_insert`)

The clean-and-insert family exists for exactly one job: **swap one snapshot of data for another
transactionally**. `clean_table_and_bulk_insert` replaces the whole table,
`clean_partition_and_bulk_insert` replaces a single partition and leaves the rest of the table
alone.

The clean and the insert are **one server-side operation**, and it reaches subscribers as a
single `InitTable` / `InitPartition` packet which the reader applies under **one lock** — the
old snapshot is swapped for the new one in a single step. **There is no window in which the
table or the partition is observed empty or half-filled**: a concurrent read gets either the
entire previous snapshot or the entire new one.

```rust
// Whole table: readers see the old set of instruments until the moment they see the new one.
w.clean_table_and_bulk_insert(&entities).await?;

// One partition — the rest of the table is untouched.
w.clean_partition_and_bulk_insert("instruments", &entities).await?;
```

> ⚠️ **Do not emulate this** with `delete_partitions` + `bulk_insert_or_replace` (or
> `bulk_delete` + bulk insert). Those are two independent operations and two separate reader
> updates, so every reader spends the gap between them looking at an empty partition/table.
> That gap is precisely what the clean-and-insert methods remove.

Chunking does not weaken the guarantee: in
`clean_and_bulk_insert_by_chunks_with_own_timestamp` (below) the uploaded chunks are invisible
until the commit, and the commit performs the same single swap. A cancelled or failed process
leaves the previous snapshot exactly as it was.

An empty slice is still a full replace for the non-chunked methods — it cleans and inserts
nothing. The chunked variant is a no-op on an empty slice instead (no process is started).

### Bulk replace keeping the client `TimeStamp`

`bulk_insert_or_update_with_own_timestamp` is `bulk_insert_or_replace` with the server's `useTimestamp=true` flag: every row is written **unconditionally** (no "if new" check), but the stored row keeps the **client-supplied `TimeStamp`** instead of the server clock. Use it to replay a snapshot while preserving each row's original version. Like `*_if_new`, the `TimeStamp` is mandatory — a default one → **HTTP 400**; empty slice is a no-op. Available on the writer and `with_retries`.

```rust
// entities each carry their own time_stamp (a real value, not Default)
w.bulk_insert_or_update_with_own_timestamp(&entities).await?;
```

The `useTimestamp=true` flag applies to the [clean-and-insert family](#atomic-snapshot-replace-clean__and_bulk_insert) too — same mandatory-`TimeStamp` rule, same transactional snapshot swap:

```rust
// Clean the table (or one partition) and re-insert, keeping each row's own TimeStamp.
w.clean_table_and_bulk_insert_with_own_timestamp(&entities).await?;
w.clean_partition_and_bulk_insert_with_own_timestamp("instruments", &entities).await?;

// Chunked variant for large snapshots — starts a process, uploads the rest, commits so the
// clean + insert land as one swap (nothing is visible before the commit, and the table/
// partition is never empty in between); best-effort Cancel on any failure. Base writer only (not
// with_retries). `partition_key: None` cleans the whole table; `Some(pk)` only that partition.
writer.clean_and_bulk_insert_by_chunks_with_own_timestamp(None, &entities, 1000).await?;
// Or stream it:
let pid = writer.clean_and_bulk_insert_by_chunks_with_own_timestamp_start(None, &first).await?;
writer.clean_and_bulk_insert_by_chunks_with_own_timestamp_append(&pid, &next).await?;
writer.clean_and_bulk_insert_by_chunks_with_own_timestamp_commit(&pid).await?; // or ..._cancel(&pid)
```

The three bulk-write modes at a glance:

| Method | Write condition | Stored `TimeStamp` |
|---|---|---|
| `bulk_insert_or_replace` | always | server clock (`now`) |
| `bulk_insert_or_update_with_own_timestamp` | always | the client's `TimeStamp` |
| `bulk_insert_or_replace_if_new` | only if missing or incoming `TimeStamp` is strictly greater | the client's `TimeStamp` |

### Optimistic-concurrency replace (`update_entity` / `replace_entity`)

`Replace` writes a row **only if its stored `TimeStamp` still equals the one you read** — the classic *read version → change fields → write with that version → on conflict re-read and retry* pattern. This differs from InsertOrReplaceIfNew: a version mismatch here is an **error** (409), not a silent skip.

`update_entity` runs the whole loop for you — read, apply your closure, replace, and on a 409 conflict re-read the fresh version and re-apply, up to a retry limit (default 5):

```rust
// Increment a counter safely under concurrent writers.
let updated = w.update_entity("instruments", "EURUSD", |e| {
    e.digits += 1;              // mutate whatever you need…
    // …but DO NOT touch e.time_stamp — it carries the read version and must go back as-is.
}).await?;                      // Result<Option<T>>: None if the row does not exist

// Custom retry budget:
w.update_entity_with_max_attempts("instruments", "EURUSD", 10, |e| e.digits += 1).await?;
```

The closure must not overwrite `time_stamp` — on every retry the version comes from the fresh read, and `get_entity` deserializes it back (`#[serde(rename = "TimeStamp")]`). Errors: **409** → `DataWriterError::RecordIsChanged` (surfaced only after the attempts are exhausted), **404** → `DataWriterError::RecordNotFound`, missing `TimeStamp` → **400**.

The low-level `replace_entity(&entity)` is also available (both on the writer and `with_retries`) when you want to drive the loop yourself — the entity must carry the `TimeStamp` it was read with.

### Insert-or-update in one call (`insert_or_update`)

When the row may or may not be there and you have something to do in either case, `insert_or_update` runs the whole dance in one call: it reads, then either **creates** the row with your `create` closure or **changes** it with your `update` closure — and re-reads its way out of every race it loses to another writer.

```rust
let mut created = false;

let entity = w.insert_or_update(
    "instruments",
    "EURUSD",
    || {                                        // the row is not there — build it
        created = true;
        InstrumentEntity {
            partition_key: "instruments".to_string(),
            row_key: "EURUSD".to_string(),
            time_stamp: Default::default(),     // the server stamps it
            name: "Euro vs Dollar".to_string(),
            digits: 5,
        }
    },
    |e| {                                       // the row is there — change it, or don't
        if e.digits == 5 {
            return false;                       // already right → nothing is written at all
        }
        e.digits = 5;                           // …and never assign e.time_stamp
        true                                    // true → write it back
    },
).await?;                                       // Result<T> — the entity as it was written

// Custom limit on lost races (default 5) — same two closures:
w.insert_or_update_with_max_attempts("instruments", "EURUSD", 10, create_fn, update_fn).await?;
```

Everything about concurrency is inside the method:

| The read found | It writes with | Another writer got there first | What the loop does |
|---|---|---|---|
| nothing | `Insert` (`create()`) | `RecordAlreadyExists` (400) | re-reads, sees their row, applies `update` to **it** — the winner's row is never overwritten blindly |
| the row | `Replace` with the read `TimeStamp` | `RecordIsChanged` (409) | re-reads the fresh version and re-applies `update` |
| the row | `Replace` with the read `TimeStamp` | `RecordNotFound` (404) — deleted meanwhile | re-reads, now there is nothing, so `create()` + `Insert` |
| the row, and `update` answered `false` | nothing at all | — | returns the row as read |

`Insert` is safe to race on: the server re-checks the key **under the table write lock**, so of two concurrent inserts of the same partition+row exactly one succeeds and the other gets `RecordAlreadyExists` — which is the signal this loop turns into "switch to the update branch".

**`update` returns whether to write.** It is handed the row it would change, so it can read the fields, decide there is nothing to do, and answer `false` — no `Replace` is sent, no race is entered, and the call returns the entity as it was read. `true` means "write what I just changed". That is the cheap way to do read-and-maybe-write: one round trip when the row already says what it should.

Both closures can therefore run more than once, always on state that was just read, so write them as *the end state you want* rather than as a step away from a state they remember (`e.digits = 5`, not `e.digits += 1` — an increment would be applied again on every retry). They are `FnMut`: a flag set inside them, like `created` above, is how you find out afterwards which branch won.

`create()` must build the entity under the same `partition_key`/`row_key` the call was made for and leave `time_stamp: Default::default()`; `update` must not assign `time_stamp` at all — it carries the read version this whole protocol stands on. Neither closure may move the row to another key: an entity whose keys do not match the call is refused with an error instead of being written to the wrong place. After `max_attempts` lost races the last one comes back as it came — whichever of the three the loop was retrying: `RecordAlreadyExists`, `RecordIsChanged`, or `RecordNotFound` when the row kept being deleted before the replace could land. All three mean "still contended", not "impossible", so they are the ones to back off and retry on.

Two things worth knowing before you use the returned entity:

- **Its `TimeStamp` is stale.** What comes back is the entity that was *sent*, not a fresh read: `time_stamp` is the version this attempt read, or the default the created entity carried — never the one the server has just stamped. Passing it straight to `replace_entity` / `delete_entity_if` is a guaranteed 409; read the row again first.
- **The table still has to exist.** As with every other write: a writer built with `CreateTableParams` creates it on its first request (the read this loop starts with counts as one), and without them a missing table is a `TableNotFound` error, not a reason to create one.

Available on the writer and on `with_retries`, where the two kinds of retry stay separate: the wrapper re-sends a request whose *transport* failed, `max_attempts` counts only races actually lost to another writer. FlUrl replays idempotent requests only, so those transport retries cover the read (GET) and the replace (PUT) but not the insert (POST) — a POST that may already have landed must not be sent twice.

### Optimistic-concurrency delete (`delete_entity_if` / `bulk_delete_if`)

The same *read version → act on that version* rule applied to deletes: a row is removed **only while its stored `TimeStamp` is still the one you read**. Use it whenever the decision to delete was made from data you read — a row somebody rewrote in the meantime is a row you have not seen, and deleting it blindly would throw that write away.

For a single row the version mismatch is an **error**, exactly like `replace_entity`:

```rust
// The main case: read it, decide it should go, delete exactly that version.
let entity = w.get_entity("instruments", "EURUSD", None).await?.unwrap();

match w.delete_entity_if(&entity).await {
    Ok(Some(deleted)) => { /* gone — `deleted` is the row as it was */ }
    Ok(None) => { /* 404: there was no such row */ }
    Err(DataWriterError::RecordIsChanged(_)) => {
        // 409: rewritten since the read. Re-read and decide again — it may no longer be
        // a row you want to delete.
    }
    Err(err) => return Err(err),
}

// Same thing addressed by keys, when the version comes from somewhere else than the entity:
w.delete_row_if("instruments", "EURUSD", time_stamp).await?;
```

For a batch a mismatch is **data, not an error**: `bulk_delete_if` always answers `200` and reports which rows it left alone. The matching rows are deleted regardless.

```rust
// Versions come from the entities themselves — pass them exactly as they were read.
let result = w.bulk_delete_if(&[&first, &second]).await?;

if !result.is_all_deleted() {
    println!("deleted {}, left {} in place", result.deleted, result.skipped.len());

    for row in result.conflicts() {      // rewritten meanwhile — worth re-reading
        println!("conflict: {}/{}", row.partition_key, row.row_key);
    }
    for row in result.not_found() {      // already gone — nothing to do
        println!("already gone: {}/{}", row.partition_key, row.row_key);
    }
}

// Or with the keys and versions on their own:
let rows = vec![RowToDeleteIf::new("instruments", "EURUSD", time_stamp)];
let result = w.bulk_delete_if_rows(&rows).await?;
```

`SkippedRow::reason` is a `DeleteIfSkipReason` — `NotFound`, `TimeStampMismatch`, or `Unknown(String)` for a reason a newer server may add, so an old client never fails to parse a new response. All four methods are on the writer and on `with_retries`.

The `TimeStamp` is **mandatory** here, like in the `*_if_new` family: an unreadable version can never match a stored one, so a default one comes back as **HTTP 400** — and for `bulk_delete_if` it fails the **whole batch** rather than being reported as one skipped row. An empty batch is a no-op (no request at all).

| Method | Version mismatch | Row missing |
|---|---|---|
| `delete_row` / `bulk_delete` | n/a — deletes unconditionally | `Ok(None)` / ignored |
| `delete_entity_if` / `delete_row_if` | `DataWriterError::RecordIsChanged` (409) | `Ok(None)` (404) |
| `bulk_delete_if` / `bulk_delete_if_rows` | `skipped` + `TimeStampMismatch` (200) | `skipped` + `NotFound` (200) |

### Counting rows without reading them (`get_rows_count`)

`get_rows_count` answers "how many rows are in this partition?" over `GET /api/Count` — the number comes back
on its own, the rows never leave the server. It exists for the reconciliation shape: a job which every minute
asks whether table A and table B still agree on one partition, and whose answer in the steady state is "they do,
do nothing". Reading both partitions to count them would move hundreds of thousands of rows across the network
to learn that nothing needs doing.

```rust
let w = writer.with_retries(3);

let in_partition = w.get_rows_count(Some("instruments")).await?;   // Result<Option<usize>>
let in_table = w.get_rows_count(None).await?;                      // omit the partition → whole table
```

The `Option` carries a distinction the caller of a counter needs and a bare number cannot express:

| Result | Meaning |
|---|---|
| `Ok(None)` | The **table** does not exist |
| `Ok(Some(0))` | The table exists and the partition (or the whole table) is empty |
| `Ok(Some(n))` | `n` rows |

"There is no table" and "the table is empty" are different facts, so the first is never reported as a zero. It is
the same shape as `get_by_partition_key`, deliberately, so the two read alike.

Two behaviours are specific to this method:

- **It never auto-creates the table**, even though the writer does so by default on every other call. A counter
  must not bring into existence the thing it was asked to count — going through the usual path would create the
  table empty, answer `Some(0)`, and make `None` unreachable. So `Ok(None)` is a real answer here, on any writer.
- **It does not touch the partition's last-read moment**, so counting on a timer never keeps a partition alive
  against `set_max_partitions_amount` collection the way a read would.

The count is exact, not an estimate: the server returns the length of the very same in-memory row collection a
download would serialize, taken under one read lock. Two separate calls are still two moments in time — concurrent
writes move the number between them, which is why a mismatch is worth re-checking before acting on it.

A missing table is a normal answer here and is **not** written to the error log, unlike everywhere else in the
writer, where a missing table is a genuine failure. Anything else — an unreachable host, an unknown namespace, a
server still loading (`503`), a `404` from a proxy which does not forward `/api/Count` — comes back as `Err`, never
as `Ok(None)`: a reconciler must not read "I could not ask" as "the table is gone".

---

### HTTP/2

Since the h2 switch the writer talks **HTTP/2 by default** — one multiplexed connection per endpoint. Against a server that does not speak h2, ask for HTTP/1.1 explicitly:

```rust
let writer = MyNoSqlDataWriter::<InstrumentEntity>::create_with_builder(settings)
    .use_h1()
    .build();
```

The 30-second background ping loop follows the same choice.

---

## Reading data

One TCP connection serves any number of tables; every `get_reader::<T>()` subscribes to `T::TABLE_NAME`.

```rust
use my_no_sql_sdk::reader::MyNoSqlTcpConnection;

// settings: Arc<dyn MyNoSqlTcpConnectionSettings> — async fn get_host_port() -> String
let connection = MyNoSqlTcpConnection::new(crate::APP_NAME, settings.clone());

let instruments = connection.get_reader::<InstrumentEntity>();

connection.start().await;                          // create the readers first, then start
instruments.wait_until_first_data_arrives().await; // the only async read-side call
```

Reads are served from the local copy — synchronous and lock-free, no `.await`:

```rust
let one = instruments.get_entity("instruments", "EURUSD");         // Option<Arc<T>>
let map = instruments.get_by_partition_key("instruments");         // Option<BTreeMap<String, Arc<T>>>
let vec = instruments.get_by_partition_key_as_vec("instruments");  // Option<Vec<Arc<T>>>
let all = instruments.get_table_snapshot_as_vec();                 // Option<Vec<Arc<T>>>
```

To react to changes, assign a `MyNoSqlDataReaderCallBacks` implementation — see the callbacks section of
[MY_NO_SQL_ENTITY_DESIGN_PATTERNS.md](MY_NO_SQL_ENTITY_DESIGN_PATTERNS.md#5-reader-change-callbacks).

Feature `mocks` on `my-no-sql-tcp-reader` provides `MyNoSqlDataReaderMock` for unit tests.

### Connection latency

The reader keeps the connection alive with `Ping` → `Pong`, and `my-tcp-sockets` measures the round
trip of every exchange. Every keep-alive ping after the first one carries the round trip of the
previous exchange — it is sent as `PingWithLatency { micros }` instead of a plain `Ping`. The server
treats it as a ping (answers `Pong`), stores the number per connection and shows it next to the
reader. A node pings the main node the same way. All of it lives in `MyNoSqlReaderTcpSerializer::get_ping`;
neither a reader nor a node does anything on its own.

A server which predates the packet (`my-no-sql-sdk` < 0.5.2) drops the connection on it
(`InvalidPacketId`), so **servers and nodes are upgraded before the readers** which ping them.

---

## Server-side crates

`my-no-sql-core` and `my-no-sql-server-core` are the data model the server itself runs on: `DbInstance` (table registry),
`DbTable` / `DbPartition` / `DbRow`, and `db_snapshots` for persistence and replication. A service that only reads and
writes data never touches them directly — they are enabled by `master-node` / `read-node` when building a node.

With `master-node`, `my-no-sql-core` additionally compresses rows in memory per table (zstd); client readers do not pull that in.

---

## Building

```bash
cargo check --workspace --all-targets
cargo test --workspace
cargo check -p my-no-sql-data-writer --features with-ssh   # unix-only feature
```

## Releasing

All crates share one version, and the repo is tagged once (`0.4.2`, `0.5.1`, …) — downstream projects pin that tag.
Bump `version` in every `Cargo.toml` of the workspace, commit, then tag `main`.

## Further reading

- [MY_NO_SQL_ENTITY_DESIGN_PATTERNS.md](MY_NO_SQL_ENTITY_DESIGN_PATTERNS.md) — entity patterns, reader/writer API details, common mistakes
