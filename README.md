# MyNoSqlServer Rust SDK

Rust client SDK and shared core for **MyNoSqlServer** — an in-memory NoSql server where data is written over HTTP and read over TCP (each client keeps a live local copy of the tables it subscribes to).

This is a cargo workspace of 9 crates. Application code normally depends on **one** of them — `my-no-sql-sdk` — and turns on the features it needs.

---

## Packages

| Crate | What it is | Use it when |
|---|---|---|
| **my-no-sql-sdk** | Facade — re-exports the other library crates: `abstractions` and `core` always, the rest behind cargo features | Always. This is the crate your service depends on |
| **my-no-sql-abstractions** | `MyNoSqlEntity`, `MyNoSqlEntitySerializer`, `DataSynchronizationPeriod`, `Timestamp`, connection-string and namespace helpers | Pulled in automatically; depend on it directly only in a crate that must stay dependency-light |
| **my-no-sql-macros** | Proc-macros: `my_no_sql_entity`, `enum_of_my_no_sql_entity`, `enum_model` | Defining entities. Enable via feature `macros` |
| **my-no-sql-data-writer** | HTTP writer — insert / replace / delete / read against the server REST API | The service writes data. Feature `data-writer` |
| **my-no-sql-tcp-reader** | TCP reader — subscribes to a table and keeps an in-memory copy; reads are local and synchronous | The service reads data. Feature `data-reader` |
| **my-no-sql-tcp-shared** | TCP protocol contracts, serializer, payload compression, sync-to-main-node handler | Rarely direct — it is what the reader and the server speak. Feature `tcp-contracts` |
| **my-no-sql-core** | The data model itself: `DbTableInner`, `DbPartition`, `DbRow`, JSON entity parsing, entity serializer | Always present (re-exported as `my_no_sql_sdk::core`) |
| **my-no-sql-server-core** | `DbInstance` / `DbTable` wrappers and table snapshots used by server-side nodes | You are building a MyNoSql master node or read node. Features `master-node` / `read-node` |
| **my-no-sql-tests** | Internal integration tests for the macros, the serializers and the reader's enum API | Never — not part of the public API |

Dependency direction:

```
my-no-sql-sdk
 ├── my-no-sql-abstractions                           (entity traits)
 ├── my-no-sql-core ────────── my-no-sql-abstractions
 ├── my-no-sql-macros
 ├── my-no-sql-data-writer ─── my-no-sql-abstractions (HTTP, via flurl)
 ├── my-no-sql-tcp-reader ──── my-no-sql-tcp-shared, my-no-sql-core, my-no-sql-abstractions
 ├── my-no-sql-tcp-shared
 └── my-no-sql-server-core ─── my-no-sql-core
```

---

## Adding to a project

The repo is released as a single repo-wide git tag; every crate in it carries the same version. Pin the tag:

```toml
[dependencies]
my-no-sql-sdk = { tag = "0.5.1", git = "https://github.com/my-jet-tools/my-no-sql-sdk.git", features = [
    "macros",
    "data-writer",
    "data-reader",
] }
serde = { version = "*", features = ["derive"] }
async-trait = "*" # the settings traits (MyNoSqlWriterSettings, MyNoSqlTcpConnectionSettings) are #[async_trait]
```

### Features of `my-no-sql-sdk`

| Feature | Enables | Re-exported as |
|---|---|---|
| `macros` | entity proc-macros | `my_no_sql_sdk::macros` (also `my_no_sql_sdk::rust_extensions`) |
| `data-writer` | HTTP writer | `my_no_sql_sdk::data_writer` |
| `data-reader` | TCP reader | `my_no_sql_sdk::reader` |
| `tcp-contracts` | raw TCP contracts | `my_no_sql_sdk::tcp_contracts` |
| `master-node` | server-side model + per-table row compression (DEFLATE via `flate2`) | `my_no_sql_sdk::server` |
| `read-node` | server-side model for a read node | `my_no_sql_sdk::server` |
| `with-ssh` | writer can reach the server through an SSH tunnel (needs `data-writer`; unix only) | — |
| `with-ring-tls` | TLS for `https://` writer urls, `ring` crypto provider (needs `data-writer`) | — |
| `with-rust-tls` | TLS for `https://` writer urls, pure-Rust crypto provider, x86_64/aarch64 only (needs `data-writer`) | — |
| `debug_db_row` | extra `DbRow` diagnostics | — |

`my_no_sql_sdk::core` and `my_no_sql_sdk::abstractions` are always available.

Pick one of `with-ring-tls` / `with-rust-tls` when the writer url is `https://` — without either, every request to such a url fails with `FlUrlError::UnsupportedScheme`: the call returns `DataWriterError::FlUrlError(FlUrlError::UnsupportedScheme(..))`, and the background ping of that writer prints it and goes on.

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
                                      // set a real value only for the *_if_new and
                                      // *_with_own_timestamp methods below
    name: "Euro vs Dollar".to_string(),
    digits: 5,
};
```

For an auto-expiring entity the macro also adds an `expires: Timestamp` field (do not declare it yourself). A default (unset) `expires` is left out of the JSON, like a default `time_stamp` — such a row does not expire:

```rust
#[my_no_sql_entity(table_name: "sessions", with_expires: true)]
```

One table can also hold several shapes at once — `enum_of_my_no_sql_entity` on the enum, `enum_model` on each case,
which fixes that case's partition key (and optionally row key). A case with a row key is one row, a case without one
is a whole partition; a row which fits both is read as the case with the row key, whatever the order of declaration.
Keep the two in different partitions: `get_enum_case_models_by_partition_key` (reader and writer) turns every row of the
partition into the partition's case and panics on a row of another case. Working example:
[my-no-sql-tests/src/macros_tests/enum_test.rs](my-no-sql-tests/src/macros_tests/enum_test.rs).

`#[enum_model]` has no `with_expires`: a case which expires declares the field itself, with the attributes the
macro gives the generated one — it is the `skip_serializing_if` which keeps an unset `expires` out of the JSON:

```rust
#[serde(rename = "Expires")]
#[serde(skip_serializing_if = "my_no_sql_sdk::abstractions::skip_timestamp_serializing")]
pub expires: my_no_sql_sdk::abstractions::Timestamp,
```

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

* a string starting with `host=` is the new format, anything else is the legacy host as it always was. Empty elements in front do not count — `;host=http://10.0.0.1:5123;ns=alpha` is the new format as well. A legacy string is not split into elements: the whole of it is the host, a `;` included;
* in the new format keys are case-insensitive, spaces around `;` and `=` are trimmed, empty elements are skipped (`;;`, a `;` in front of `host=` or at the end), `host` is mandatory, `ns` is optional;
* an unknown key is an error — better not to start than to silently read the wrong namespace;
* a namespace name follows the table-name rules (`[a-z0-9-]` only, no leading/trailing `-`, no `--`),
  but may be shorter: 1–63 symbols (a table name needs at least 3). Upper case is an error and is never silently lower-cased: namespaces are auto-created
  by the server, so a typo has to fail instead of bringing a garbage namespace to life;
* the `default` namespace is never transmitted — the writer sends no `ns` header and the reader sends
  no `SetNamespace` packet, so servers which know nothing about namespaces are not affected at all.

A connection string which breaks these rules is never used. The writer returns `DataWriterError::InvalidConnectionString`
from the call — also for a host which is not an url: an empty or blank string, a bare `http://`, or what a legacy string
like `ns=alpha;host=http://..` comes to (without a scheme such a string is a host name to the HTTP client and is not
refused here). The reader writes the error to `my_logger` and fails that connect attempt — the settings are read again
before the next one, 3 seconds later, so a corrected string is picked up without a restart. Until then the readers keep
the data they have, and a reader which has none keeps waiting in `wait_until_first_data_arrives`.

A non-default namespace does require a server which supports them: the writer sends it as the `ns`
header of every request, and the reader sends the `SetNamespace` TCP packet right after the `Greeting`
and before the first `Subscribe`. An old server breaks the connection on that packet instead of
silently serving the default namespace.

Helpers are re-exported at the root of `my-no-sql-sdk`: `DEFAULT_NAMESPACE`, `DbNamespaceName`,
`validate_namespace_name` / `ValidationError`, `parse_connection_string` / `ConnectionString` / `ConnectionStringError`.

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
`set_max_row_per_partitions_amount`, `do_not_auto_create_table`, `use_h1`, `set_body_size_limit`.
By default the table is auto-created on the first request — `create_table` / `create_table_if_not_exists` excepted:
they send only their own request, the table gets the parameters passed to them, and once one of them succeeds that
writer (its `with_retries` wrappers included) does not auto-create the table any more (`get_rows_count` never creates
it either). The builder is a front for
`MyNoSqlDataWriter::new(settings, Option<CreateTableParams>, DataSynchronizationPeriod)`, which can also be
called directly (`None` is what `do_not_auto_create_table()` sets).

**Always go through `with_retries`** — the writer itself sets no retry policy. `with_retries(n)` gives a
request which got no response up to `n` more attempts, and only an idempotent one (GET / PUT / DELETE): a
POST-based write (`insert_entity`, `insert_or_replace_entity`, every `bulk_*`, `clean_*` and `*_if_new`
call) goes out once either way. An attempt is not always a single send: when the connection breaks, the
HTTP client under FlUrl reconnects and replays an idempotent request on its own — on the base writer too.
Only what exists on the base writer alone is called without the wrapper — the chunked `*_by_chunks*`
flows below and `create_table` / `create_table_if_not_exists`. Everything else:

```rust
let w = writer.with_retries(3);

w.insert_or_replace_entity(&entity).await?;
w.bulk_insert_or_replace(&entities).await?;

let one = w.get_entity("instruments", "EURUSD", None).await?;      // Option<T>
let part = w.get_by_partition_key("instruments", None).await?;     // Option<Vec<T>> — always Some;
                                                                   // a missing partition is an empty Vec

let n = w.get_rows_count(Some("instruments")).await?;              // Option<usize> — count only

w.delete_row("instruments", "EURUSD").await?;                      // Option<T>: the deleted row, None if there was none
w.delete_partitions(&["instruments"]).await?;                      // whole partitions; a missing one is skipped

// Bulk delete: PartitionKey -> RowKeys
let mut rows_to_delete = std::collections::BTreeMap::new();
rows_to_delete.insert("instruments".to_string(), vec!["EURUSD".to_string()]);
w.bulk_delete(&rows_to_delete).await?;
```

`delete_partitions` sends its keys in the query string of a DELETE request, and a request head has a
size limit. A list which does not fit into one request — about 4 KB of keys, some 80 keys of 36
characters — is sent as several, one after another. So a long list is not deleted in one step: if a
request fails, the call returns its error and the partitions of the requests before it are already
gone. Calling it again with the same list is safe — a partition which is gone is skipped.

These deletes are unconditional. To delete only a row which has not changed since you read
it, see [Optimistic-concurrency delete](#optimistic-concurrency-delete-delete_entity_if--bulk_delete_if).
To ask how many rows a partition holds without moving the rows, see
[Counting rows without reading them](#counting-rows-without-reading-them-get_rows_count).

The writer returns **owned** entities (`get_entity` → `Result<Option<T>, DataWriterError>`), unlike the reader which hands out `Arc<T>`.

An answer the call has no meaning for — a `503` while the server is still loading its tables, a 5xx, a `404` of a
route nobody serves, a `2xx` with `Content-Type: text/html` (the page of its UI, which is what the server answers
an unknown route with) — is always `Err(DataWriterError::Error(..))`
naming the call, the table, the status and the body (`bulk_insert_or_replace of table instruments returned status
code 503. Body: Application is not initialized yet`; a body over 1 KB is cut and the message says how long it
was). That goes for every call of the writer — the writes, the chunked flows and `create_table*` included — and it
is never folded into `Ok(None)`, an empty list or `Ok(())`. The call named is the request which failed:
`update_entity` and `insert_or_update` report `get_entity`, `insert_entity` or `replace_entity`,
`delete_entity_if` reports `delete_row_if`, the counter calls itself `Rows count`, and the auto-creation in front
of a writer's first request reports `create_table_if_not_exists`.

So `Ok(None)` from `get_entity` / `delete_entity_if` means exactly "no such row" (the server's `404 Record not
found`), and a missing table is `Err(DataWriterError::TableNotFound)` whichever call met it (`get_rows_count`
excepted, see below). A `409` is `Err(DataWriterError::RecordIsChanged)` — the conflict of an
optimistic-concurrency write — from every call but the chunked flows and `create_table*`. The chunked flows have a
`409` of their own, `Bulk process <id> does not match` (the process belongs to another writer session): it comes
back as the `DataWriterError::Error` above, and so does a `409` in answer to `create_table*`, which the API never
gives them. `with_retries` does not replay such an answer — a response did arrive — so do not unwrap writer results
blindly around a server restart.

What a proxy says in the server's place is read by its status, the way the server's answer is — so three of its
answers are not that error: a `400` whose body is not the server's contract is `DataWriterError::Error(<body>)` —
the body alone, as it came and uncut (`DataWriterError::Utf8Error` when the body is not text); a `409` is
`RecordIsChanged` (above); and a `2xx` which is not `text/html` is a success to a call which does not read the
body — the writes, `delete_partitions`, `bulk_delete`, the commits, `create_table*` (`delete_row` reads a `2xx`
other than `200` as "there was no such row").

### Insert-or-replace-if-new (client-versioned writes)

`InsertOrReplaceIfNew` upserts a row **only when it is missing, or when the incoming `TimeStamp` is strictly greater than the stored one** — a last-writer-by-version upsert for distributed systems. Here the `TimeStamp` is the object's version assigned **by the client**, and it is **mandatory**.

> This is an exception to the "`time_stamp: Default::default()` — never now" rule (the `*_with_own_timestamp` methods below are the other one). Every other write lets the server stamp its own time; these methods require you to set `time_stamp` to your real version. A default/unset `time_stamp` is left out of the JSON (by `#[my_no_sql_entity]` and `#[enum_model]` alike), and the server rejects the request with **HTTP 400** (`Entity with PartitionKey '..' RowKey '..' does not contain TimeStamp`). In a debug build the writer does not get that far: it `debug_assert!`s a non-default `time_stamp` and panics before the request is sent.

```rust
use my_no_sql_sdk::core::rust_extensions::date_time::DateTimeAsMicroseconds;

let entity = InstrumentEntity {
    partition_key: "instruments".to_string(),
    row_key: "EURUSD".to_string(),
    time_stamp: DateTimeAsMicroseconds::now().into(),   // your version — REQUIRED here
    name: "Euro vs Dollar".to_string(),
    digits: 5,
};

// Single entity and array (empty slice = no-op) — both also on with_retries.
w.insert_or_replace_entity_if_new(&entity).await?;
w.bulk_insert_or_replace_if_new(&entities).await?;

// Large snapshots: upload in chunks and commit atomically. If a later chunk or the commit fails,
// it makes a best-effort Cancel of the half-uploaded process and returns the original error.
// The chunked flow lives on the base writer only — it is NOT offered on with_retries: its requests
// are POSTs, which the wrapper would not re-send anyway, and a chunk must never be sent twice (it
// would double-append rows into the server-side accumulator).
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
nothing. Readers follow that only for the table-wide calls: for
`clean_partition_and_bulk_insert(pk, &[])` (and its `_with_own_timestamp` twin) the server empties
the partition but publishes nothing to subscribers, so a reader keeps serving the old rows of that
partition until it reconnects or the partition is replaced again with a non-empty set — to empty a
partition for readers as well, use `delete_partitions(&[pk])`. The chunked variant is a no-op on an
empty slice instead (no process is started).

### Bulk replace keeping the client `TimeStamp`

`bulk_insert_or_update_with_own_timestamp` is `bulk_insert_or_replace` with the server's `useTimestamp=true` flag: every row is written **unconditionally** (no "if new" check), but the stored row keeps the **client-supplied `TimeStamp`** instead of the server clock. Use it to replay a snapshot while preserving each row's original version. Like `*_if_new`, the `TimeStamp` is mandatory — a default one → **HTTP 400** (a `debug_assert!` panic in a debug build); empty slice is a no-op. Available on the writer and `with_retries`.

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
// partition is never empty in between); best-effort Cancel if a later chunk or the commit fails.
// Base writer only (not with_retries). `partition_key: None` cleans the whole table; `Some(pk)`
// only that partition.
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

`update_entity` runs the whole loop for you — read, apply your closure, replace, and on a 409 conflict re-read the fresh version and re-apply, up to an attempt limit (default 5):

```rust
// Increment a counter under concurrent writers: a lost race is re-read and the increment re-applied.
// Not exactly-once: if a `Replace` lands but its answer is lost, the re-sent request conflicts with
// its own first copy and the increment is applied twice — set an absolute value where that matters.
let updated = w.update_entity("instruments", "EURUSD", |e| {
    e.digits += 1;              // mutate whatever you need…
    // …but DO NOT touch e.time_stamp — it carries the read version and must go back as-is.
}).await?;                      // Option<T>: None if the row does not exist

// Custom attempt limit:
w.update_entity_with_max_attempts("instruments", "EURUSD", 10, |e| e.digits += 1).await?;
```

The closure must not overwrite `time_stamp` — on every retry the version comes from the fresh read, and `get_entity` deserializes it back (`#[serde(rename = "TimeStamp")]`). Errors: **409** → `DataWriterError::RecordIsChanged` (surfaced only after the attempts are exhausted), the server's **404** `Record not found` → `DataWriterError::RecordNotFound` (a 404 with any other body is an answer the call has no meaning for — `DataWriterError::Error`), missing `TimeStamp` → **400**.

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
).await?;                                       // T — the entity as it was written
                                                // (or as it was read, when `update` said false)

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

Both closures can therefore run more than once, always on state that was just read, so write them as *the end state you want* rather than as a step away from a state they remember (`e.digits = 5` rather than `e.digits += 1`: a relative step lands on whatever row the attempt has just read — possibly one another writer created — and when a `Replace` lands but its answer is lost, the re-sent request conflicts with its own first copy and the step is applied twice). They are `FnMut`: a flag set inside them, like `created` above, tells you afterwards that the closure ran — not yet which branch won, because an `Insert` which loses the race hands over to `update` with the flag already set.

`create()` must build the entity under the same `partition_key`/`row_key` the call was made for and leave `time_stamp: Default::default()`; `update` must not assign `time_stamp` at all — it carries the read version this whole protocol stands on. Neither closure may move the row to another key: an entity whose keys do not match the call is refused with an error instead of being written to the wrong place. After `max_attempts` lost races the last one comes back as it came — whichever of the three the loop was retrying: `RecordAlreadyExists`, `RecordIsChanged`, or `RecordNotFound` when the row kept being deleted before the replace could land. All three mean "still contended", not "impossible", so they are the ones to back off and retry on. A `404` which is not the server's `Record not found` — a route nobody serves, a proxy — is none of them: the loop does not read again because of it, and the call ends at once with the `DataWriterError::Error` of the `Replace`.

Two things worth knowing before you use the returned entity:

- **Its `TimeStamp` is stale.** What comes back is the entity that was *sent*, not a fresh read: `time_stamp` is the version this attempt read, or the default the created entity carried — never the one the server has just stamped. Passing it straight to `replace_entity` / `delete_entity_if` is guaranteed to fail — a 409 for the stale read version, and the default one is not a version the server accepts at all (a 400 when the `TimeStamp` is missing from the request; in a debug build `delete_entity_if` trips its `debug_assert!` instead of sending it) — so read the row again first. Only when `update` answered `false` is it the row exactly as it was just read.
- **The table still has to exist.** As with every other write: a writer built with `CreateTableParams` creates it on its first request (the read this loop starts with counts as one), and without them a missing table is a `TableNotFound` error, not a reason to create one.

Available on the writer and on `with_retries`, where the two kinds of retry stay separate: the wrapper re-sends a request whose *transport* failed, `max_attempts` counts only the conflicts the server answers with — races lost to another writer (or, rarely, to the writer's own re-sent `Replace`). FlUrl replays idempotent requests only, so those transport retries cover the read (GET) and the replace (PUT) but not the insert (POST) — a POST that may already have landed must not be sent twice.

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

A lost answer can hide a delete: when a `DeleteIf` (or a plain `delete_row`) removes the row but its answer is lost and the request is re-sent, the second copy finds no row — the call returns `Ok(None)` although it was this call that deleted it.

For a batch a mismatch is **data, not an error**: `bulk_delete_if` answers `200` whatever the versions turn out to be and reports which rows it left alone. The matching rows are deleted regardless.

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

The `TimeStamp` is **mandatory** here, like in the `*_if_new` family: an unreadable version can never match a stored one, so a default one comes back as **HTTP 400** (a `debug_assert!` panic in a debug build) — and for `bulk_delete_if` it fails the **whole batch** rather than being reported as one skipped row. An empty batch is a no-op (no `DeleteIf` request is sent).

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

let in_partition = w.get_rows_count(Some("instruments")).await?;   // Option<usize>
let in_table = w.get_rows_count(None).await?;                      // omit the partition → whole table
```

The `Option` carries a distinction the caller of a counter needs and a bare number cannot express:

| Result | Meaning |
|---|---|
| `Ok(None)` | The **table** does not exist |
| `Ok(Some(0))` | The table exists and the partition (or the whole table) is empty |
| `Ok(Some(n))` | `n` rows |

"There is no table" and "the table is empty" are different facts, so the first is never reported as a zero. It has
the same `Result<Option<_>>` shape as `get_by_partition_key`, deliberately — but only the shape:
`get_by_partition_key` reports a missing table as `Err(DataWriterError::TableNotFound)`.

Two behaviours are specific to this method:

- **It never auto-creates the table**, even though the writer does so by default on the first request made through any other method but `create_table` / `create_table_if_not_exists`. A counter
  must not bring into existence the thing it was asked to count — going through the usual path would create the
  table empty, answer `Some(0)`, and make `None` unreachable. So `Ok(None)` is a real answer here, on any writer.
- **It does not touch the partition's last-read moment** — unlike `get_entity` / `get_by_partition_key`, it has
  no `UpdateReadStatistics` argument to ask for that — so counting on a timer never keeps a partition alive
  against `set_max_partitions_amount` collection.

The count is exact, not an estimate: the server returns the length of the very same in-memory row collection a
download would serialize, taken under one read lock. Two separate calls are still two moments in time — concurrent
writes move the number between them, which is why a mismatch is worth re-checking before acting on it.

A missing table is a normal answer here and is **not** written to the error log; in every other call of the
writer a missing table is a genuine failure. Anything else — an unreachable host, an unknown namespace, a
server still loading (`503`), a `404` from a proxy which does not forward `/api/Count` — comes back as `Err`, never
as `Ok(None)`: a reconciler must not read "I could not ask" as "the table is gone".

### HTTP/2

Since the h2 switch the writer talks **HTTP/2 by default** — one multiplexed connection per endpoint. Against a server that does not speak h2, ask for HTTP/1.1 explicitly:

```rust
let writer = MyNoSqlDataWriter::<InstrumentEntity>::create_with_builder(settings)
    .use_h1()
    .build();
```

The 30-second background ping loop follows the same choice. It names a table once per endpoint, however many
writers of that table the application has built.

### Size of an answer

The writer reads answers of up to **100 MB** (`DEFAULT_BODY_SIZE_LIMIT`) — FlUrl alone stops at 10 MB, which a
table, or a partition, easily outgrows. A bigger answer fails the call with
`DataWriterError::FlUrlError(FlUrlError::ResponseBodyTooLarge { limit })`. The limit is set on the builder or on
the writer, and the `with_retries` wrappers made after that read with it; `usize::MAX` lifts it:

```rust
let writer = MyNoSqlDataWriter::<InstrumentEntity>::create_with_builder(settings)
    .set_body_size_limit(500 * 1024 * 1024)
    .build();
```

---

## Reading data

One TCP connection serves any number of tables; `get_reader::<T>()` subscribes to `T::TABLE_NAME` — call it once
per table (a second call for the same table panics).

```rust
use my_no_sql_sdk::reader::{MyNoSqlDataReader, MyNoSqlTcpConnection};

// settings: Arc<dyn MyNoSqlTcpConnectionSettings + Send + Sync> — async fn get_host_port(&self) -> String
let connection = MyNoSqlTcpConnection::new(crate::APP_NAME, settings.clone());

let instruments = connection.get_reader::<InstrumentEntity>();

connection.start().await;                          // create the readers first, then start
instruments.wait_until_first_data_arrives().await; // async — a method of the MyNoSqlDataReader trait
```

Reads are served from the local copy — synchronous, no `.await` (each read takes a short `parking_lot` mutex on that copy, no I/O):

```rust
let one = instruments.get_entity("instruments", "EURUSD");         // Option<Arc<T>>
let map = instruments.get_by_partition_key("instruments");         // Option<BTreeMap<String, Arc<T>>>
let vec = instruments.get_by_partition_key_as_vec("instruments");  // Option<Vec<Arc<T>>>
let all = instruments.get_table_snapshot_as_vec();                 // Option<Vec<Arc<T>>>
```

Reads which also report last-read / expiration moments back to the server — `get_entities(..)` and
`get_entity_with_callback_to_server(..)` — are builders (`GetEntitiesBuilder` / `GetEntityBuilder`, exported from
`my_no_sql_sdk::reader`) whose final call is synchronous, like every other read (`.get_as_vec()`, `.execute()`); see
[my-no-sql-tcp-reader/README.md](my-no-sql-tcp-reader/README.md).

The reports are queued and sent in the background, one at a time, each after the server has confirmed the one
before it. A row gets the expiration moment which was asked for it last. Rows of a partition which wait for
their turn together and whose moments fall into the same second leave in one report, with the latest of those
moments — so a row may get a moment which is later than the one its read asked for by less than a second, never
an earlier one; `None` (no expiration) is never mixed with a moment. So while a report is on its way, the reads
of a sliding expiration (`now + ttl` on every read) pile up into a report per partition and second, not into a
report per read. A read which finds nothing on its way is reported at once: reads which come further apart than
a round trip to the server takes still cost a report each.

A report of row expiration moments names one partition and carries one moment, and the partitions take turns. So
when the partitions are read faster than their reports are confirmed, a row waits for the turn of its partition,
and the rows of a partition whose moments fall into different seconds need a turn per second. A time to live which
is not well above that wait lets the server expire rows which are still being read. The moments of the partitions
themselves (`set_partition_expiration_moment(..)`) do not take turns: one report names every partition of the
table which waits, each with its own moment.

To react to changes, assign a `MyNoSqlDataReaderCallBacks` implementation — see the callbacks section of
[MY_NO_SQL_ENTITY_DESIGN_PATTERNS.md](MY_NO_SQL_ENTITY_DESIGN_PATTERNS.md#5-reader-change-callbacks).

Feature `mocks` on `my-no-sql-tcp-reader` provides `MyNoSqlDataReaderMock` for unit tests. `my-no-sql-sdk` has no
feature which forwards it — add `my-no-sql-tcp-reader` itself with `features = ["mocks"]`.

The mock is filled with `update(..)` and emptied with `delete(..)`, and both report to the assigned
`MyNoSqlDataReaderCallBacks` the way the TCP reader reports an `UpdateRows` / a `DeleteRows` packet: `update` — one
`inserted_or_replaced` call per partition with the rows written, `delete` — one `deleted` call per partition with the
rows which were there (a key which was not there is not reported), never a call with an empty list. The rows are
always handed over as `LazyMyNoSqlEntity::Deserialized` — also for an entity with lazy deserialization, which the TCP
reader may hand over as `Raw`. The calls are made from the callbacks events loop, not by `update` / `delete`
themselves — assign the callbacks inside the Tokio runtime of the test and wait for the call. A mock which outlives
that runtime (one shared by several `#[tokio::test]`s) panics on the first `update` / `delete` which has rows to
report. The mock has no counterpart of a snapshot packet (`InitTable` / `InitPartition`).

The reads of the mock answer the way the reads of the TCP reader do. That goes for a read through a filter as well
(`get_entities(..).get_as_vec_with_filter(..)` / `.get_as_btree_map_with_filter(..)`): `None` is "no such
partition", and a partition none of whose rows passed the filter is `Some` of an empty list — the mock used to
answer `None` for that too.

### Connection latency

The reader keeps the connection alive with `Ping` → `Pong`, and `my-tcp-sockets` measures the round
trip of every exchange. Every keep-alive ping sent after the first `Pong` has arrived carries the last
measured round trip — it is sent as `PingWithLatency { micros }` instead of a plain `Ping`. The server
treats it as a ping (answers `Pong`), stores the number per connection and shows it next to the
reader. A node pings the main node the same way. All of it lives in `MyNoSqlReaderTcpSerializer::get_ping`;
neither a reader nor a node does anything on its own.

A server which predates the packet (`my-no-sql-sdk` < 0.5.1) drops the connection on it
(`InvalidPacketId`), so **servers and nodes are upgraded before the readers** which ping them.

---

## Server-side crates

`my-no-sql-core` and `my-no-sql-server-core` are the data model the server itself runs on: `DbInstance` (table registry),
`DbTable` and `db_snapshots` for persistence and replication in `my-no-sql-server-core`, `DbTableInner` / `DbPartition` /
`DbRow` in `my-no-sql-core`. A service that only reads and writes data never touches these types directly.
`my-no-sql-core` is always compiled in — the reader and the macro-generated serializers call into it;
`my-no-sql-server-core` is enabled by `master-node` / `read-node` when building a node.

With `master-node`, `my-no-sql-core` can additionally keep rows compressed in memory, opt-in per table (raw DEFLATE
via `flate2`); that code is not compiled into client readers.

---

## Building

```bash
cargo check --workspace --all-targets
cargo test --workspace
cargo test --workspace --features master-node              # also runs the master-node-only tests
cargo test -p my-no-sql-tcp-reader --features mocks        # the mock-reader tests run only with `mocks`
cargo check -p my-no-sql-data-writer --features with-ssh   # unix-only feature
```

## Releasing

All crates share one version, and the repo is tagged once per release (`0.5.1`, …) — downstream projects pin that tag.
Bump `version` in every `Cargo.toml` of the workspace, commit, then tag `main` — and put the tag into the install
snippets of this README, of [MY_NO_SQL_ENTITY_DESIGN_PATTERNS.md](MY_NO_SQL_ENTITY_DESIGN_PATTERNS.md) and of
[my-no-sql-data-writer/readme.md](my-no-sql-data-writer/readme.md).

## Behaviour changes (October 2026)

Code written against commit `7724144` (tag `0.5.1` as of 2026-10-01) compiles unchanged — with the exceptions at
the end of the list — but behaves differently. A project gets these changes when its `Cargo.lock` moves to a
commit which has them (`cargo update -p my-no-sql-sdk`).

Writer:

- `delete_partitions` now deletes. It used to ask for a route the server does not have and return `Ok(())` with
  nothing deleted — a call which was inert removes the partitions now. A long list is sent as several requests.
- A failed request is no longer reported as "nothing there" or "done". For a `503`, a 5xx or a `404` which is not
  the API's, `get_entity`, `get_by_partition_key`, `get_by_row_key`, `get_all`, `delete_row` and `delete_row_if`
  used to return `Ok(None)`, `get_partition_keys` an empty list (`TableNotFound` for such a `404`) and the
  `clean_*` calls `Ok(())`. All of them return `Err` now, and `get_by_partition_key` / `get_by_row_key` /
  `get_all` never return `Ok(None)`.
- Every call reads an answer by one rule. `insert_or_replace_entity`, the `bulk_*` writes, `bulk_delete`, the
  `*_if_new` calls and the chunked flows used to return `Error(<body>)` for whatever was not a success — the
  server's `400` contract included, as its JSON text. `insert_entity`, `replace_entity`, `bulk_delete_if` and
  `create_table*` did give the typed error for that `400`, and `Error(<body>)` for a status they had no meaning
  for. Now a `400` with the server's contract is the typed error from each of them (`TableNotFound`, …), a `409`
  is `RecordIsChanged` (from the chunked flows and `create_table*` it is the `Error` which follows), and any
  other status is `Error("<call> of table <table> returned status code <status>. Body: <body>")`, a body over
  1 KB cut. Only a `400` whose body is not the contract still comes back as `Error(<body>)`. Code which compares
  the text of `DataWriterError::Error` has to be checked.
- A `2xx` with `Content-Type: text/html` — the page of the server's UI, which is what it answers an unknown route
  with — is an `Err` from every call. The writes, the commits, `delete_partitions`, `clean_*` and `create_table*`
  used to return `Ok(())` for it.
- A `2xx` whose body is not what the call reads (a `204` where a row is expected, a row which does not fit the
  entity) is an `Err`; it used to panic.
- `replace_entity`: only the server's `404 Record not found` is `RecordNotFound`; any other `404` is `Error`, and
  `insert_or_update` ends on it at once instead of spending its attempts. `get_partition_keys`: only a `404` which
  carries the server's `TableNotFound` contract is `TableNotFound`.
- `create_table` / `create_table_if_not_exists` send their own request only — no auto-creation in front of it, so
  `create_table` on a new table succeeds instead of answering `TableAlreadyExists` — and once one of them succeeds
  that writer does not auto-create the table any more.
- `delete_row`, the enum-case deletes and `delete_partitions` carry the writer's `syncPeriod` (the server used its
  own default of 5 seconds for them).
- `with_retries(n).get_partition_keys(..)` is retried like the other reads.
- The ping names a table once per endpoint, and a writer built again from the same settings is registered once.
- A connection string whose host is not an url — an empty or blank string, a bare `http://`, a blank name in
  front of `@<ip>` (`" @10.0.0.5:5123"`, it used to connect to the ip), a host with `->` whose ssh part can not be
  read, or `ns=alpha;host=http://..`, which is a legacy string and taken whole — is
  `InvalidConnectionString` from the call. It used to panic in the task of the caller, and in the ping loop —
  which ended the ping of every writer of the process (a blank string did not panic: it was an error of the HTTP
  client).
- A ping which panics no longer ends the ping loop: each ping runs in a task of its own, and the other writers of
  the process are pinged as before. (An `https://` url in a build without a TLS feature does not panic any more:
  with fl-url `0.7.0` the request fails with `FlUrlError::UnsupportedScheme`.)

Reader:

- Callbacks: a snapshot (`InitTable` / `InitPartition`) makes one `inserted_or_replaced` call per partition; for a
  partition the reader already had it used to be one call per row.
- A partition which was deleted is dropped from the local copy: `has_partition` is `false` and
  `get_by_partition_key` is `None` for it. It used to stay as an empty partition until the next reconnect.
- Reads which report back (`set_row_expiration_moment(..)`, `set_row_last_read_moment()` and the partition ones):
  a row gets the expiration moment which was asked for it — reads of one partition which were queued together
  used to be delivered with the moment of the first one, also when another read had asked for no expiration.
  Rows of a partition which wait together and whose moments fall into the same second still leave in one report,
  with the latest of those moments: a row may get a moment later than asked by less than a second, never an
  earlier one. Rows of a partition whose moments fall into different seconds take a report each, where one report
  used to take them all — "Reading data" says what that costs when many partitions are read at once. The four
  kinds of reports are delivered in turns — row reports used to wait for as long as partition reports kept
  coming; a read which returned no row sends no empty row report.
- A read which reports back wakes the delivery only when its report can leave — there is a connection and no
  report is on its way. It used to post a message per read and kind of report: reads in a tight loop kept the
  server's confirmations waiting behind those messages, so the reports fell further and further behind, the memory
  of the process grew, and the server expired rows which were being read.
- A connection string which can not be parsed no longer kills the connect loop: the error is logged and the
  settings are read again at the next attempt. It used to panic, and the reader stayed on its old data for good.
- `MyNoSqlDataReaderMock` calls the assigned callbacks on `update(..)` / `delete(..)`; it never did. Its reads
  through a filter answer `Some` of an empty list for a partition none of whose rows passed the filter, the way
  the TCP reader does; they used to answer `None`.

Connection string:

- Empty elements in front of `host=` do not make the string a legacy one: `;host=..;ns=..` is read as the new
  format, namespace included. It used to be taken for a legacy host with the default namespace.

Entities:

- `#[enum_model]` leaves a default `time_stamp` out of the JSON like `#[my_no_sql_entity]` (it used to send
  `"TimeStamp":null`): `replace_entity` of such an entity is answered `400`, not `409`.
- `with_expires`: a default `expires` is left out of the JSON as well (it used to go as `"Expires":null`). The
  row means the same — it does not expire.
- `#[enum_of_my_no_sql_entity]`: a case with both keys is matched before a case with a partition key only,
  whatever the order of declaration; `deserialize_entity` returns `Err` for a body which is not an entity (it used
  to panic), and the error of a case which does not parse names the table. A case without a model is a compile
  error which says so (`Enum case must have a model`) instead of `custom attribute panicked`.
  Check your enums when you update: in one which declares a whole-partition case before a row case of the same
  partition, the rows of the row case used to be read as the whole-partition case. They are read as their own
  case now, so `get_enum_case_models_by_partition_key` of the whole-partition case panics (`Expected case …`) on
  them — on the reader and on the writer.

Server side (`my-no-sql-core`, once the server or a node is built on it):

- An entity a client writes has to carry its `PartitionKey` and `RowKey` as JSON strings: a number, `true`, an
  object are refused with `400 JsonParseFail` (`PartitionKey must be a json string`). They used to panic the
  handler (`5`) or to be stored under the value with its first and last character cut off (`123` under `2`). Rows
  which are already stored that way are still loaded, and sent to readers, under the key they were stored under.
  What a reader does with such a row has not changed either: its keys are not strings, so an entity which
  declares them as `String` can not be deserialized from it. A reader of a plain `#[my_no_sql_entity]` panics on
  every snapshot which holds the row — the connection is dropped and made again, the table never arrives, and
  neither do the tables of that connection which are subscribed after it; a reader with lazy deserialization
  keeps the row and panics when the row is read. Rewrite such a row with its keys as strings (`"2"` for the row
  stored as `123`) or delete it by the key it lies under — the server takes both.
- `Tables/MigrateFrom` parses the rows it fetches as a client's write: a source table which holds a row stored
  that way is refused as a whole (`400`, nothing is copied). It used to be copied, such rows included.
- An entity a client writes has to be utf-8 as a whole, not in its keys only: bytes which are not utf-8 in any
  value which goes into the row are refused with `400 JsonParseFail` (`The entity must be utf-8`) — the `TimeStamp`
  of a plain write does not go into it, the server puts its own in. Such an entity used to be stored, and a reader
  whose entity declares that field could not parse the row (one which does not declare it reads the row). Rows
  which are already stored that way are still loaded.
- A `TimeStamp` which is not a JSON string is not read as one: `Replace` with `"TimeStamp":null` is answered
  `400`, not `409`, and the writes which keep the client's `TimeStamp` (`*_if_new`, `*_with_own_timestamp`)
  refuse it as a missing one. `Expires` is read as it was: `null`, `true`, an object mean no expiration, and a
  number is still taken for a unix time.
- The writes which keep the client's `TimeStamp` store the moment they were sent also when it is spelled
  without seconds (`2020-05-06T07:08Z` is stored as `2020-05-06T07:08:00`). Such a value used to pass the
  check, and the row got the server clock instead.
- A key spelled `NULL` or `Null` is refused as a `null` by every call (`Insert`, `Replace` and the writes which
  keep the client's `TimeStamp` did refuse it, the other writes stored the row under `UL` / `ul`). Of several
  fields with the name of a key the last one counts for every call: `Insert`, `Replace` and the writes which keep
  the client's `TimeStamp` used to answer `400` for a `null` in front of the key, the other writes stored such a
  row — and it could not be loaded again. It is loaded now.
- An entity which is refused either way may be refused for another reason: a key which is not utf-8 is
  `JsonParseFail` (`PartitionKey must be a json string`), not `PartitionKey can not be null`, and broken json
  behind a `null` key is `JsonParseFail` as well.
- A field name which is not utf-8 and an array element which is not an object are refused instead of panicking
  the handler.

Not source compatible:

- `my_no_sql_macros::time_stamp_init!` is removed.
- `InitPartitionResult` (returned by `DataReaderEntitiesSet::init_partition`): its two fields have swapped names
  — `partition_now` is the partition which is in the table, `partition_before` the one it has replaced. They
  were named the other way round.
- `SyncToMainNodeQueue` got a private field: it is built through `SyncToMainNodeQueue::new()` only.

## Further reading

- [MY_NO_SQL_ENTITY_DESIGN_PATTERNS.md](MY_NO_SQL_ENTITY_DESIGN_PATTERNS.md) — entity patterns, reader/writer API details, common mistakes
- [my-no-sql-data-writer/readme.md](my-no-sql-data-writer/readme.md) — minimal writer setup
- [my-no-sql-tcp-reader/README.md](my-no-sql-tcp-reader/README.md) — minimal reader setup, reads which report last-read / expiration moments
- [my-no-sql-macros/README.md](my-no-sql-macros/README.md) — the entity macros
- [ROW_COMPRESSION_TASK.md](ROW_COMPRESSION_TASK.md) — design note of the per-table in-memory row compression
