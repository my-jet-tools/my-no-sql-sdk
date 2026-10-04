# MyNoSql Entity Design Patterns

## 1. Reader & Writer overview

### Writer (my-no-sql-data-writer)

HTTP writer — sends requests to MyNoSql server.

**Crate:** `my-no-sql-data-writer` (or via `my-no-sql-sdk` with feature `data-writer`)

#### MyNoSqlWriterSettings trait

```rust
#[async_trait::async_trait]
pub trait MyNoSqlWriterSettings {
    async fn get_url(&self) -> String;       // MyNoSql server URL (HTTP), or `host=<url>;ns=<namespace>`
    fn get_app_name(&self) -> &'static str;
    fn get_app_version(&self) -> &'static str;
}
```

#### Creating a writer

```rust
use my_no_sql_sdk::{
    abstractions::DataSynchronizationPeriod,
    data_writer::{CreateTableParams, MyNoSqlDataWriter},
};

let writer = MyNoSqlDataWriter::<InstrumentEntity>::new(
    settings_reader.clone(),   // Arc<dyn MyNoSqlWriterSettings + Send + Sync>
    Some(CreateTableParams {
        persist: true,                        // persist to disk
        max_partitions_amount: None,          // None = no limit
        max_rows_per_partition_amount: None,
    }),
    DataSynchronizationPeriod::Immediately,   // how long the server may postpone persisting the change
);
```

The same writer through the builder (its defaults: `DataSynchronizationPeriod::Sec5`, table auto-created with `persist: true`):

```rust
let writer = MyNoSqlDataWriter::<InstrumentEntity>::create_with_builder(settings_reader.clone())
    .set_sync_period(DataSynchronizationPeriod::Immediately)
    .build();
```

Create the writer inside a Tokio runtime: `new` / `build()` register it for the background ping loop, which the first writer starts with `tokio::spawn`.

#### with_retries — REQUIRED

**NEVER** call methods on writer directly. Always via `.with_retries(N)` — the only exception is what exists on the base writer alone: the chunked `*_by_chunks*` flows (below) and `create_table` / `create_table_if_not_exists`:

```rust
// ✅ CORRECT — via with_retries
let writer_with_retries = writer.with_retries(3);
writer_with_retries.insert_or_replace_entity(&entity).await.unwrap();

// ❌ WRONG — directly
writer.insert_or_replace_entity(&entity).await.unwrap();
```

What the wrapper adds: up to `N` more attempts for a request which got no response, and only for an idempotent one (GET / PUT / DELETE — `get_entity`, `get_by_partition_key`, `replace_entity`, `delete_row`, …). A POST-based write (`insert_entity`, `insert_or_replace_entity`, `bulk_*`, `*_if_new`, `clean_*`) goes out once either way. An attempt is not always a single send: when the connection breaks, the HTTP client under FlUrl reconnects and replays an idempotent request on its own — on the base writer too — so a GET / PUT / DELETE may reach the server more than once with or without the wrapper.

AppContext pattern:
```rust
pub struct AppContext {
    instruments: MyNoSqlDataWriter<InstrumentEntity>,
}

impl AppContext {
    pub fn get_instruments(&self) -> MyNoSqlDataWriterWithRetries<InstrumentEntity> {
        self.instruments.with_retries(3)
    }
}
```

#### Writer API (MyNoSqlDataWriterWithRetries)

```rust
let w = app_ctx.get_instruments();

// Insert or replace
w.insert_or_replace_entity(&entity).await.unwrap();

// Bulk insert or replace
w.bulk_insert_or_replace(&entities).await.unwrap();

// Insert-or-replace-IF-NEW — writes only when the row is missing or the incoming
// TimeStamp is strictly greater than the stored one (client-versioned upsert).
// TimeStamp is MANDATORY here — see the note below.
w.insert_or_replace_entity_if_new(&entity).await.unwrap();     // single
w.bulk_insert_or_replace_if_new(&entities).await.unwrap();     // array, empty = no-op

// Bulk replace that keeps each row's OWN TimeStamp (unconditional write, not "if new").
// TimeStamp mandatory — see the note below.
w.bulk_insert_or_update_with_own_timestamp(&entities).await.unwrap();

// Optimistic-concurrency update: read → mutate via closure → replace → retry on 409.
// Do NOT touch time_stamp in the closure — it carries the read version. See note below.
let updated = w.update_entity("pk", "rk", |e| e.digits = 5).await.unwrap(); // Option<T>

// Create-or-change in one call: read → create-closure if missing, update-closure if there,
// retrying every race (a lost Insert becomes an update of the winner's row). The update
// closure returns bool: false = the row is already right, nothing is written. See below.
let entity = w.insert_or_update("pk", "rk", || build_entity(), |e| { e.digits = 5; true })
    .await.unwrap();                                                             // T

// Get one entity → Result<Option<T>>
let entity = w.get_entity("partition_key", "row_key", None).await.unwrap();

// Get all in partition → Result<Option<Vec<T>>>. The Option is always Some: a partition
// which is not there is an empty Vec, a missing table is Err(TableNotFound).
let items = w.get_by_partition_key("pk", None).await.unwrap().unwrap_or_default();

// How many rows in a partition, WITHOUT fetching them -> Result<Option<usize>>
// None = no such table; Some(0) = table exists, partition empty. Does not auto-create the table.
let n = w.get_rows_count(Some("pk")).await.unwrap();
let total = w.get_rows_count(None).await.unwrap();   // whole table

// Delete row
w.delete_row("partition_key", "row_key").await.unwrap();
```

**Important:** Writer returns **owned T** (not Arc): `get_entity` → `Result<Option<T>, DataWriterError>`, `get_by_partition_key` → `Result<Option<Vec<T>>, DataWriterError>`.

**Errors are errors:** an answer the call has no meaning for — a `503` while the server is still loading its tables, a 5xx, a `404` of a route nobody serves, a `2xx` with `Content-Type: text/html` (the UI page the server answers an unknown route with) — is `Err(DataWriterError::Error(..))` naming the call, the table, the status and the body (a body over 1 KB is cut), from every call of the writer — never `Ok(None)`, an empty list or `Ok(())`. `Ok(None)` from `get_entity` means exactly "no such row", and a missing table is `Err(DataWriterError::TableNotFound)` whichever call met it (`get_rows_count` answers it with `Ok(None)`).

Not shown here: `delete_partitions`, `bulk_delete` and the optimistic-concurrency deletes `delete_entity_if` / `delete_row_if` / `bulk_delete_if` / `bulk_delete_if_rows` — see the "Writing data" section of the SDK [README.md](https://github.com/my-jet-tools/my-no-sql-sdk/blob/main/README.md#writing-data).

#### InsertOrReplaceIfNew — client-versioned upsert (mandatory TimeStamp)

`*_if_new` writes a row **only when it is missing, or when the incoming `TimeStamp` is strictly greater than the stored one**. `TimeStamp` is the object's version in a distributed system and is assigned **by the client**.

> ⚠️ **These are the cases where you must set a real `time_stamp`** (e.g. `DateTimeAsMicroseconds::now().into()`) instead of `Default::default()` — the `*_if_new` methods and every `*_with_own_timestamp` method. Every other write lets the server stamp its own time; here a default/unset `time_stamp` is left out of the JSON (by `#[my_no_sql_entity]` and `#[enum_model]` alike) and the server answers **HTTP 400** (`... does not contain TimeStamp`). In a debug build the writer does not get that far: it `debug_assert!`s a non-default `time_stamp` and panics before the request is sent.

```rust
use my_no_sql_sdk::core::rust_extensions::date_time::DateTimeAsMicroseconds;

let entity = InstrumentEntity {
    partition_key: "instruments".to_string(),
    row_key: "EURUSD".to_string(),
    time_stamp: DateTimeAsMicroseconds::now().into(),   // REQUIRED for *_if_new
    name: "Euro vs Dollar".to_string(),
    digits: 5,
};

// On with_retries:
w.insert_or_replace_entity_if_new(&entity).await.unwrap();
w.bulk_insert_or_replace_if_new(&entities).await.unwrap();

// Chunked (base writer only — NOT offered on with_retries: its requests are POSTs, which the
// wrapper would not re-send anyway, and a chunk must never be sent twice — it would
// double-append into the server-side accumulator).
// One call: slices, starts, uploads the rest, commits; if a later chunk or the commit
// fails - best-effort Cancel.
writer.bulk_insert_or_replace_if_new_by_chunks(&entities, 1000).await.unwrap();
// Or stream it: insert_or_replace_if_new_by_chunks_start(..) → ..._append(pid, ..) → ..._commit(pid) / ..._cancel(pid).
```

**`bulk_insert_or_update_with_own_timestamp`** sits between plain bulk and `*_if_new`: it writes every row **unconditionally** (no "if new" gate) but stores the **client's `TimeStamp`** rather than the server clock — handy for replaying a snapshot while keeping each row's original version. Same mandatory-`TimeStamp` rule (default → HTTP 400, a `debug_assert!` panic in a debug build); empty slice = no-op; on writer and `with_retries`.

#### Clean-and-insert — transactional snapshot replace

`clean_table_and_bulk_insert` / `clean_partition_and_bulk_insert` **atomically swap one snapshot of data for another**: the whole table, or a single partition with the rest of the table untouched. That is the whole point of these methods.

The clean and the insert are **one server-side operation**, delivered to subscribers as a single `InitTable` / `InitPartition` packet and applied by the reader **under one lock** — old snapshot out, new snapshot in, in one step. **The table/partition is never observed empty or half-filled**: a concurrent read sees either the entire old snapshot or the entire new one.

```rust
w.clean_table_and_bulk_insert(&entities).await.unwrap();            // whole table
w.clean_partition_and_bulk_insert("pk", &entities).await.unwrap();  // one partition
```

> ⚠️ **Never emulate it** with `delete_partitions` + `bulk_insert_or_replace` — two operations means two reader updates, and readers spend the gap between them looking at an empty partition/table. Removing that gap is exactly why the clean-and-insert methods exist.

The same `useTimestamp=true` behavior is available on this family, for replaying a snapshot that fully replaces a table/partition while keeping each row's original version:

```rust
w.clean_table_and_bulk_insert_with_own_timestamp(&entities).await.unwrap();
w.clean_partition_and_bulk_insert_with_own_timestamp("pk", &entities).await.unwrap();

// Chunked (base writer only), atomic clean+insert at commit; None = whole table, Some(pk) = partition:
writer.clean_and_bulk_insert_by_chunks_with_own_timestamp(None, &entities, 1000).await.unwrap();
// low-level: _start(pk, ..) → _append(pid, ..) → _commit(pid) / _cancel(pid)
```

Chunking keeps the guarantee: uploaded chunks are invisible until the commit, the commit does the same single swap, and a cancelled/failed process leaves the previous snapshot untouched. One difference: an empty slice makes the chunked call a no-op (no process is started, nothing is cleaned), while the non-chunked methods still send the clean with an empty array.

> ⚠️ **Do not empty a partition with `clean_partition_and_bulk_insert("pk", &[])`** (or its `_with_own_timestamp` twin): the server empties the partition but publishes nothing to subscribers, so readers keep serving its old rows until they reconnect or the partition is replaced again with a non-empty set. Use `delete_partitions(&["pk"])` — readers get the `deleted` callback and drop the partition. The table-wide `clean_table_and_bulk_insert(&[])` is followed by readers.

All of the `*_with_own_timestamp` ones keep the client `TimeStamp` and require a real (non-default) one — a default → HTTP 400 (a `debug_assert!` panic in a debug build).

| Method | Writes when | Stored `TimeStamp` |
|---|---|---|
| `bulk_insert_or_replace` | always | server clock (`now`) — `time_stamp: Default::default()` |
| `bulk_insert_or_update_with_own_timestamp` | always | client's `TimeStamp` (must be real) |
| `bulk_insert_or_replace_if_new` | only if missing or incoming `TimeStamp` strictly greater | client's `TimeStamp` (must be real) |

#### Optimistic concurrency — `update_entity` / `replace_entity`

Concurrent-safe field updates. The row's `TimeStamp` is a **version token**: you send back the one you read, and the server replaces the row **only if it still matches**. A mismatch means someone else wrote in between → you re-read and retry. (Contrast with `*_if_new`, where a version conflict is a silent skip, not an error.)

`update_entity` runs the read → mutate → replace → retry loop for you:

```rust
// Reads the row, applies your closure, replaces it; on a 409 conflict it re-reads the
// fresh version and re-applies the closure, up to an attempt limit (default 5).
let updated = w.update_entity("pk", "rk", |e| {
    e.digits = 5;        // mutate whatever you need
    // ⚠️ never assign e.time_stamp — it is the read version and must go back untouched
}).await?;               // Option<T>: None if the row does not exist

// Explicit attempt limit:
w.update_entity_with_max_attempts("pk", "rk", 10, |e| e.digits = 5).await?;
```

On every retry the version comes from the fresh read (`get_entity` deserializes `TimeStamp` back), so the closure must **not** overwrite `time_stamp`.

Errors: **409** → `DataWriterError::RecordIsChanged` (returned only after attempts are exhausted); the server's **404** `Record not found` → `DataWriterError::RecordNotFound` (any other 404 → `DataWriterError::Error`); missing `TimeStamp` → **400**.

The low-level `replace_entity(&entity)` (on the writer and `with_retries`) is available when you want to drive the loop yourself — the entity must carry the `TimeStamp` it was read with.

> This is **not** the "set a real time_stamp" exception that `*_if_new` is: with `update_entity` you never set `time_stamp` at all — it comes from the read entity and flows straight back.

#### Insert-or-update in one call — `insert_or_update`

For the very common "create it if it is not there, otherwise change it" — where doing it by hand means a `get_entity`, an `if`, and a race with every other writer doing the same. `insert_or_update` takes both closures and keeps the whole protocol inside:

```rust
let mut created = false;

let entity = w.insert_or_update(
    "instruments",
    "EURUSD",
    || {                                   // row missing → build it (same pk/rk, default TimeStamp)
        created = true;
        InstrumentEntity {
            partition_key: "instruments".to_string(),
            row_key: "EURUSD".to_string(),
            time_stamp: Default::default(),
            name: "Euro vs Dollar".to_string(),
            digits: 5,
        }
    },
    |e| {                                  // row there → look at it and decide
        if e.digits == 5 {
            return false;                  // nothing to change → nothing is written
        }
        e.digits = 5;
        true                               // → Replace, with the TimeStamp it was read with
    },
).await.unwrap();                          // T, not Option — there is always a row after this
```

| Situation | What happens |
|---|---|
| row missing | `create()` → `Insert` |
| another writer inserted the same key first | `RecordAlreadyExists` → re-read → `update` runs on **their** row |
| row there | `update()` → `Replace` with the read `TimeStamp` |
| row rewritten between read and write | 409 → re-read → `update` re-applied to the fresh version |
| row deleted between read and write | `404 Record not found` → re-read → `create()` + `Insert` |
| `update` returned `false` | nothing is sent; the row is returned as read |

`Insert` is race-proof by design — the server re-checks the key under the table write lock, so of two concurrent inserts exactly one wins and the loser is told so, which is what this loop switches on.

Because a retry always starts from a fresh read, both closures may run several times: write them as "the state I want" (`e.digits = 5`) rather than as a relative step (`e.digits += 1`) — a step lands on whatever row the attempt has just read, and is applied twice when a `Replace` lands but its answer is lost and the request is re-sent. `create` must use the same pk/rk the call was made for, `update` must never assign `time_stamp` — and neither closure may change the keys (that is refused, not written to the wrong row). The returned entity never carries the `TimeStamp` the server just stamped: on the update branch it is the version this attempt **read**, on the create branch it is the `Default::default()` the created entity was built with. Re-read before using it for another optimistic-concurrency write. Default budget is 5 lost races (`insert_or_update_with_max_attempts` to change it); after that the last one is returned as it came — `RecordAlreadyExists`, `RecordIsChanged` or `RecordNotFound` (the row kept being deleted before the replace landed). All three mean "still contended". On writer and `with_retries`.

#### DataSynchronizationPeriod

| Value | Description |
|---|---|
| `Immediately` | No delay: the change is due for persisting at once (the server's background persist loop writes it — not the request itself) |
| `Sec1` / `Sec5` / `Sec15` / `Sec30` | Persisting may be postponed up to N seconds |
| `Min1` | Up to 1 minute |
| `Asap` | No delay either (on the current server the same deadline as `Immediately`) |

It travels as the `syncPeriod` query parameter of a write request (the chunked flows send it with the commit) and only says how long the server may postpone **persisting** the change; readers are notified right after the write whatever the period is. Every write carries it, the deletes included.

#### CreateTableParams

```rust
CreateTableParams {
    persist: true,   // true = survives server restart
    max_partitions_amount: None,
    max_rows_per_partition_amount: None,
}
```

`Some(params)` — the table is auto-created on the writer's first request (a read counts too; `get_rows_count` never creates it, and `create_table` / `create_table_if_not_exists` create it with the parameters passed to them instead — once one of them succeeds, the writer and its `with_retries` wrappers do not auto-create the table any more). `None` — table must already exist.

---

### Reader (my-no-sql-tcp-reader)

TCP reader — subscribes to a table, keeps a local copy. Reads are local, no network requests.

**Crate:** `my-no-sql-tcp-reader` (or via `my-no-sql-sdk` with feature `data-reader`)

#### Reader API (MyNoSqlDataReaderTcp)

All reader read-methods are **synchronous** (a short `parking_lot` mutex on the in-memory copy, no I/O). The final calls of the `get_entities(..)` / `get_entity_with_callback_to_server(..)` builders (`get_as_vec()`, `get_as_btree_map()`, `execute()`, …) are synchronous too: the moments they report are queued and sent in the background. `async` is only `wait_until_first_data_arrives`.

```rust
// Get all in partition → Option<BTreeMap<String, Arc<T>>>
// Key: row_key, value: entity
let items = reader.get_by_partition_key("pk");

match items {
    Some(entities) => {
        for (row_key, entity) in entities {
            // entity: Arc<T>
        }
    }
    None => { /* partition not found */ }
}

// Same partition, but as a Vec
let items = reader.get_by_partition_key_as_vec("pk"); // Option<Vec<Arc<T>>>

// Get one entity → Option<Arc<T>>
let entity = reader.get_entity("partition_key", "row_key");

// Is the partition in the local copy → bool. A partition which was deleted, or lost its last
// row, is not there any more: the copy keeps no empty partitions.
let exists = reader.has_partition("pk");

// Full table snapshot as a Vec → Option<Vec<Arc<T>>>
let all = reader.get_table_snapshot_as_vec();

// Async method — waits until the subscription receives its first snapshot.
// It comes from the `MyNoSqlDataReader` trait: `use my_no_sql_sdk::reader::MyNoSqlDataReader;`
reader.wait_until_first_data_arrives().await;
```

**CRITICAL:** `get_by_partition_key` returns `Option<BTreeMap<String, Arc<T>>>` (key = row_key), **NOT** `Vec<Arc<T>>` and **NOT** `Vec<(String, Arc<T>)>`. Use `get_by_partition_key_as_vec` when you just need the values.

---

## 2. Shared entity crate

> Entities MUST live in a separate shared crate. Duplicating entity structs across multiple projects is an anti-pattern.
> Reader (service) and writer (admin) import the same crate.

```
my-no-sql-entities/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── instrument.rs
    └── ...
```

```toml
# my-no-sql-entities/Cargo.toml
[dependencies]
my-no-sql-sdk = { tag = "0.5.1", git = "https://github.com/my-jet-tools/my-no-sql-sdk.git", features = ["macros"] }
serde = { version = "*", features = ["derive"] }
```

```toml
# Consuming service Cargo.toml
my-no-sql-entities = { path = "../my-no-sql-entities" }
```

---

## 3. Entity with expiration (with_expires)

When an entity should auto-expire after a certain time, use `with_expires:true`. This adds an `expires: Timestamp` field to the struct. A default (`Default::default()`) `expires` is left out of the JSON, like a default `time_stamp` — such a row does not expire.

**Important:** when using `with_expires`, all parameters must be named — positional syntax does not work with multiple parameters.

```rust
// ✅ CORRECT — all parameters named
#[my_no_sql_entity(table_name:"sessions", with_expires:true)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionEntity {
    pub trader_id: String,
}

// ✅ CORRECT — single positional parameter (no expires)
#[my_no_sql_entity("sessions")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionEntity {
    pub trader_id: String,
}

// ❌ WRONG — mixing positional and named parameters
#[my_no_sql_entity("sessions", with_expires:true)]
```

Setting the expiration value:

```rust
use std::time::Duration;
use my_no_sql_sdk::core::rust_extensions::date_time::DateTimeAsMicroseconds;

let entity = SessionEntity {
    partition_key: "pk".to_string(),
    row_key: "rk".to_string(),
    time_stamp: Default::default(),
    expires: DateTimeAsMicroseconds::now()
        .add(Duration::from_secs(300))  // expires in 5 minutes
        .into(),
    trader_id: "trader-1".to_string(),
};
```

---

## 4. Common Mistakes / Anti-patterns

| Mistake | Fix |
|---|---|
| Calling writer directly without retries | Always `writer.with_retries(3).method()` — the base writer is only for what the wrapper does not have (the chunked `*_by_chunks*` flows, `create_table*`) |
| Awaiting reader read-methods (`reader.get_entity(...).await`) | Reader reads are **sync** now — no `.await`. The final calls of the `get_entities(..)` / `get_entity_with_callback_to_server(..)` builders too. Only `wait_until_first_data_arrives` is async |
| Expecting `Vec<Arc<T>>` from reader `get_by_partition_key` | Returns `Option<BTreeMap<String, Arc<T>>>` (row_key → entity). Use `get_by_partition_key_as_vec` for just values |
| Expecting `Option<Vec<Arc<T>>>` from writer `get_by_partition_key` | Writer returns `Result<Option<Vec<T>>>` — owned T, not Arc |
| Duplicating entity struct across multiple projects | Shared crate |
| Setting `time_stamp: DateTimeAsMicroseconds::now().into()` for normal writes | Always `time_stamp: Default::default()` — the server stamps its own time |
| Calling any `*_if_new` or `*_with_own_timestamp` method with `time_stamp: Default::default()` | The **opposite** rule: these send the client's version — `time_stamp: DateTimeAsMicroseconds::now().into()`. A default TimeStamp → HTTP 400 (a `debug_assert!` panic in a debug build) |
| Assigning `e.time_stamp` inside an `update_entity` closure | Never touch it — it is the read version that detects concurrent writes. Overwriting it makes the `Replace` fail — the server no longer gets the version it stored (a 409 conflict; a missing `TimeStamp` is an HTTP 400) — so the update never lands (and re-reads carry a fresh version anyway) |
| Loading a whole partition just to count its rows | `w.get_rows_count(Some("pk"))` — the number comes back on its own, the rows never leave the server. `None` = no such table, `Some(0)` = table exists, partition is empty |
| Reading `Ok(None)` from `get_rows_count(..)` as "empty" | It means the **table is not there**. Empty is `Ok(Some(0))`. An unreachable server / unknown namespace is `Err`, never `None` |
| Treating a 409 from `replace_entity` as fatal | It means the row changed under you — re-read and retry (or just use `update_entity`, which loops for you) |
| Hand-rolling `get_entity` → `if None { insert } else { update }` | That is a race: between your read and your insert another writer can create the row. `insert_or_update` does the whole loop, including turning a lost `Insert` into an update of the winner's row |
| Treating `RecordAlreadyExists` from `insert_entity` as fatal | It means somebody created that key first — re-read and update it instead (`insert_or_update` does exactly this for you) |
| Writing the row back from an `insert_or_update` update closure when nothing changed | Return `false` — the row is not sent, no race is entered, and the entity comes back as read |

---

## 5. Reader Change Callbacks

When a service maintains an in-memory cache based on NoSql data and must react to changes — use `MyNoSqlDataReaderCallBacks`.

### Trait

```rust
// Plain trait, plain `fn`s — no #[async_trait], nothing can be awaited inside the callbacks
pub trait MyNoSqlDataReaderCallBacks<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
> {
    fn inserted_or_replaced(&self, partition_key: &str, entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>);
    fn deleted(&self, partition_key: &str, entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>);
}
```

One packet from the server makes at most one `inserted_or_replaced` call and at most one `deleted` call per partition, never with an empty list. `UpdateRows` — the rows written; `DeleteRows` — the rows removed. A snapshot packet — `InitTable` (the first snapshot, every reconnect, `clean_table_and_bulk_insert`) or `InitPartition` (`clean_partition_and_bulk_insert`, a deleted partition) — gives `inserted_or_replaced` with **every** row the partition has now, changed or not (rows are not compared), and then `deleted` with the rows it had and has no more. So a reconnect costs one `inserted_or_replaced` call per partition of the table.

`MyNoSqlDataReaderMock` (feature `mocks` of `my-no-sql-tcp-reader`) keeps the same contract in a unit test: `update(..)` — one `inserted_or_replaced` call per partition with the rows written, `delete(..)` — one `deleted` call per partition with the rows which were there, the rows always as `LazyMyNoSqlEntity::Deserialized`. The calls come from an events loop of the same kind, started by `assign_callback` in the Tokio runtime of the test — so the test waits for the call instead of expecting it right after `update` / `delete`, and a mock which outlives that runtime panics on the next change it has to report.

### Pattern: full cache reload

On any event (insert/replace/delete) — fully re-read all data from the reader and replace the cache. Do not attempt incremental updates.

```rust
use std::sync::Arc;
use my_no_sql_entities::MyEntityNoSqlEntity;
use my_no_sql_sdk::reader::{LazyMyNoSqlEntity, MyNoSqlDataReaderCallBacks};

pub struct MyEntityNoSqlCallback {
    app: Arc<AppContext>,
}

impl MyEntityNoSqlCallback {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

impl MyNoSqlDataReaderCallBacks<MyEntityNoSqlEntity> for MyEntityNoSqlCallback {
    fn inserted_or_replaced(
        &self,
        _partition_key: &str,
        _entities: Vec<LazyMyNoSqlEntity<MyEntityNoSqlEntity>>,
    ) {
        tokio::spawn(reload_my_entities(self.app.clone()));
    }

    fn deleted(
        &self,
        _partition_key: &str,
        _entities: Vec<LazyMyNoSqlEntity<MyEntityNoSqlEntity>>,
    ) {
        tokio::spawn(reload_my_entities(self.app.clone()));
    }
}

// Reload script — reads all from reader, replaces cache
pub async fn reload_my_entities(app: Arc<AppContext>) {
    // Take the cache lock FIRST and read the reader second: reloads spawned back to back
    // then land in order — one which read earlier can never overwrite a newer snapshot.
    let mut cache = app.cache.lock().await;

    // Reader reads are sync — no .await.
    // PARTITION_KEY is a constant declared by hand in the entities crate
    // (`impl MyEntityNoSqlEntity { pub const PARTITION_KEY: &'static str = "..."; }`) —
    // #[my_no_sql_entity] does not generate it, only #[enum_model] does
    let items = app.my_entity_reader
        .get_by_partition_key(MyEntityNoSqlEntity::PARTITION_KEY);

    match items {
        Some(entities) => cache.reload_all(entities.values().map(|e| e.as_ref().into())),
        None => cache.reload_all(std::iter::empty()),
    }
}
```

### Registration — before the connection starts

Assign the callback between `MyNoSqlTcpConnection::get_reader()` and `MyNoSqlTcpConnection::start()`.
The first snapshot is handed to the callbacks which are assigned when it arrives; a callback assigned
after that gets only the changes which come later, and a cache filled by it stays empty until the
table changes.

```rust
use my_no_sql_sdk::reader::MyNoSqlDataReader;

// assign_callback is sync — no .await. It starts the callbacks events loop with
// tokio::spawn, so call it inside the Tokio runtime.
app.my_entity_reader
    .assign_callback(Arc::new(MyEntityNoSqlCallback::new(app.clone())));
```

### The cache is filled after the reader

The callbacks are delivered after the reader's own copy of the table has been updated — one at a time,
from the reader's own events loop — and the full reload above runs in a `tokio::spawn` on top of that.
`wait_until_first_data_arrives()` covers the reader's copy, not a cache filled from the callbacks: code
which reads such a cache right after the first snapshot can find it empty.

Until the cache has been loaded once, read from the reader — it holds the data by then:

```rust
pub async fn get_my_entities(app: &AppContext) -> Vec<MyEntity> {
    {
        let cache = app.cache.lock().await;
        if cache.is_loaded() {
            return cache.get_all();
        }
    }

    // The first reload has not run yet — the reader has the data already
    app.my_entity_reader
        .get_by_partition_key_as_vec(MyEntityNoSqlEntity::PARTITION_KEY)
        .unwrap_or_default()
        .iter()
        .map(|e| e.as_ref().into())
        .collect()
}
```

### Rules (in case of full reload to local cache)

| Rule | Why |
|---|---|
| **ALWAYS** `tokio::spawn` in callback | Callbacks are plain (non-async) `fn`s, called one at a time from the reader's callbacks events loop — the async reload has to be spawned, and a slow callback delays the next ones |
| Reload script takes `Arc<AppContext>` (owned) | Required for `tokio::spawn` — move semantics |
| **ALWAYS** full reload, not incremental | Simpler, more reliable, no edge cases with event ordering |
| Reload locks the cache **before** it reads the reader | Every callback spawns its own reload; with the read first, two of them can interleave and leave the older snapshot in the cache |
| Callback lives in `scripts/` | Not a flow — no HTTP/gRPC context |
| Cache must have a `reload_all` method | Clears and re-populates from an iterator, and marks the cache as loaded |
| Read from the reader until the cache is loaded once | The cache is filled after the reader: right after the first snapshot it can still be empty |
| Assign the callback before the connection starts | A callback assigned after the first snapshot does not get it |
