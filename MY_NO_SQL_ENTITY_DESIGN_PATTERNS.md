## 1. Reader & Writer overview

After "Where entities live and how they flow" add a full section with API.

### Writer (my-no-sql-data-writer)

HTTP writer — sends requests to MyNoSql server.

**Crate:** `my-no-sql-data-writer` (or via `my-no-sql-sdk` with feature `data-writer`)

#### MyNoSqlWriterSettings trait

```rust
#[async_trait::async_trait]
pub trait MyNoSqlWriterSettings {
    async fn get_url(&self) -> String;       // MyNoSql server URL (HTTP)
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
    DataSynchronizationPeriod::Immediately,   // how fast reader sees changes
);
```

#### with_retries — REQUIRED

**NEVER** call methods on writer directly. Always via `.with_retries(N)`:

```rust
// ✅ CORRECT — via with_retries
let writer_with_retries = writer.with_retries(3);
writer_with_retries.insert_or_replace_entity(&entity).await.unwrap();

// ❌ WRONG — directly
writer.insert_or_replace_entity(&entity).await.unwrap();
```

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
w.insert_or_replace_entity_if_new(&entity).await.unwrap();     // single  → 200
w.bulk_insert_or_replace_if_new(&entities).await.unwrap();     // array   → 202, empty = no-op

// Bulk replace that keeps each row's OWN TimeStamp (unconditional write, not "if new").
// TimeStamp mandatory — see the note below.
w.bulk_insert_or_update_with_own_timestamp(&entities).await.unwrap();

// Optimistic-concurrency update: read → mutate via closure → replace → retry on 409.
// Do NOT touch time_stamp in the closure — it carries the read version. See note below.
let updated = w.update_entity("pk", "rk", |e| e.some_field = 42).await.unwrap(); // Option<T>

// Create-or-change in one call: read → create-closure if missing, update-closure if there,
// retrying every race (a lost Insert becomes an update of the winner's row). The update
// closure returns bool: false = the row is already right, nothing is written. See below.
let entity = w.insert_or_update("pk", "rk", || build_entity(), |e| { e.some_field = 42; true })
    .await.unwrap();                                                             // T

// Get one entity → Result<Option<T>>
let entity = w.get_entity("partition_key", "row_key", None).await.unwrap();

// Get all in partition → Result<Option<Vec<T>>>
let items = w.get_by_partition_key("pk", None).await.unwrap().unwrap_or_default();

// How many rows in a partition, WITHOUT fetching them -> Result<Option<usize>>
// None = no such table; Some(0) = table exists, partition empty. Does not auto-create the table.
let n = w.get_rows_count(Some("pk")).await.unwrap();
let total = w.get_rows_count(None).await.unwrap();   // whole table

// Delete row
w.delete_row("partition_key", "row_key").await.unwrap();
```

**Important:** Writer returns **owned T** (not Arc), wrapped in `Result<Option<T>, DataWriterError>`.

#### InsertOrReplaceIfNew — client-versioned upsert (mandatory TimeStamp)

`*_if_new` writes a row **only when it is missing, or when the incoming `TimeStamp` is strictly greater than the stored one**. `TimeStamp` is the object's version in a distributed system and is assigned **by the client**.

> ⚠️ **These are the cases where you must set a real `time_stamp`** (e.g. `DateTimeAsMicroseconds::now().into()`) instead of `Default::default()` — the `*_if_new` methods and `bulk_insert_or_update_with_own_timestamp`. Every other write lets the server stamp its own time; here a default/unset `Timestamp` serializes to `null` / is omitted and the server answers **HTTP 400** (`... does not contain TimeStamp`).

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

// Chunked (base writer only, NOT with_retries — re-sending a chunk after a partial
// success would double-append into the server-side accumulator).
// One call: slices, starts, uploads the rest, commits; on error best-effort Cancel.
writer.bulk_insert_or_replace_if_new_by_chunks(&entities, 1000).await.unwrap();
// Or stream it: _start → _append(pid, ..) → _commit(pid) / _cancel(pid).
```

**`bulk_insert_or_update_with_own_timestamp`** sits between plain bulk and `*_if_new`: it writes every row **unconditionally** (no "if new" gate) but stores the **client's `TimeStamp`** rather than the server clock — handy for replaying a snapshot while keeping each row's original version. Same mandatory-`TimeStamp` rule (default → HTTP 400); empty slice = no-op; on writer and `with_retries`.

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

Chunking keeps the guarantee: uploaded chunks are invisible until the commit, the commit does the same single swap, and a cancelled/failed process leaves the previous snapshot untouched.

All of the `*_with_own_timestamp` ones keep the client `TimeStamp` and require a real (non-default) one — a default → HTTP 400.

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
// fresh version and re-applies the closure, up to a retry limit (default 5).
let updated = w.update_entity("pk", "rk", |e| {
    e.some_field = 42;   // mutate whatever you need
    // ⚠️ never assign e.time_stamp — it is the read version and must go back untouched
}).await?;               // Result<Option<T>>: None if the row does not exist

// Explicit retry budget:
w.update_entity_with_max_attempts("pk", "rk", 10, |e| e.some_field = 42).await?;
```

On every retry the version comes from the fresh read (`get_entity` deserializes `TimeStamp` back), so the closure must **not** overwrite `time_stamp`.

Errors: **409** → `DataWriterError::RecordIsChanged` (returned only after attempts are exhausted); **404** → `DataWriterError::RecordNotFound`; missing `TimeStamp` → **400**.

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
).await.unwrap();                          // Result<T>, not Option — there is always a row after this
```

| Situation | What happens |
|---|---|
| row missing | `create()` → `Insert` |
| another writer inserted the same key first | `RecordAlreadyExists` → re-read → `update` runs on **their** row |
| row there | `update()` → `Replace` with the read `TimeStamp` |
| row rewritten between read and write | 409 → re-read → `update` re-applied to the fresh version |
| row deleted between read and write | 404 → re-read → `create()` + `Insert` |
| `update` returned `false` | nothing is sent; the row is returned as read |

`Insert` is race-proof by design — the server re-checks the key under the table write lock, so of two concurrent inserts exactly one wins and the loser is told so, which is what this loop switches on.

Because a retry always starts from a fresh read, both closures may run several times: write them as "the state I want" (`e.digits = 5`), never as a step from remembered state (`e.digits += 1`). `create` must use the same pk/rk the call was made for, `update` must never assign `time_stamp` — and neither closure may change the keys (that is refused, not written to the wrong row). The returned entity never carries the `TimeStamp` the server just stamped: on the update branch it is the version this attempt **read**, on the create branch it is the `Default::default()` the created entity was built with. Re-read before using it for another optimistic-concurrency write. Default budget is 5 lost races (`insert_or_update_with_max_attempts` to change it); after that the last one is returned as it came — `RecordAlreadyExists`, `RecordIsChanged` or `RecordNotFound` (the row kept being deleted before the replace landed). All three mean "still contended". On writer and `with_retries`.

#### DataSynchronizationPeriod

| Value | Description |
|---|---|
| `Immediately` | Reader sees changes ASAP |
| `Sec1` / `Sec5` / `Sec15` / `Sec30` | Delay N seconds |
| `Min1` | 1 minute |
| `Asap` | Best-effort |

#### CreateTableParams

```rust
CreateTableParams {
    persist: true,   // true = survives server restart
    max_partitions_amount: None,
    max_rows_per_partition_amount: None,
}
```

`Some(params)` — auto-create table on first write. `None` — table must already exist.

---

### Reader (my-no-sql-tcp-reader)

TCP reader — subscribes to a table, keeps a local copy. Reads are local, no network requests.

**Crate:** `my-no-sql-tcp-reader` (or via `my-no-sql-sdk` with feature `data-reader`)

#### Reader API (MyNoSqlDataReaderTcp)

All reader read-methods are **synchronous** (lock-free via `parking_lot` + in-memory cache). Only `wait_until_first_data_arrives` remains `async`.

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

// Check partition existence → bool
let exists = reader.has_partition("pk");

// Full table snapshot as a Vec → Option<Vec<Arc<T>>>
let all = reader.get_table_snapshot_as_vec();

// Async-only method — waits until the subscription receives its first snapshot
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
my-no-sql-sdk = { ..., features = ["macros"] }
serde = { version = "*", features = ["derive"] }
```

```toml
# Consuming service Cargo.toml
my-no-sql-entities = { path = "../my-no-sql-entities" }
```

---

## 3. Entity with expiration (with_expires)

When an entity should auto-expire after a certain time, use `with_expires:true`. This adds an `expires: Timestamp` field to the struct.

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
| Calling writer directly without retries | Always `writer.with_retries(3).method()` |
| Awaiting reader read-methods (`reader.get_entity(...).await`) | Reader reads are **sync** now — no `.await`. Only `wait_until_first_data_arrives` is async |
| Expecting `Vec<Arc<T>>` from reader `get_by_partition_key` | Returns `Option<BTreeMap<String, Arc<T>>>` (row_key → entity). Use `get_by_partition_key_as_vec` for just values |
| Expecting `Option<Vec<Arc<T>>>` from writer `get_by_partition_key` | Writer returns `Result<Option<Vec<T>>>` — owned T, not Arc |
| Duplicating entity struct across multiple projects | Shared crate |
| Setting `time_stamp: Timestamp::now()` for normal writes | Always `time_stamp: Default::default()` — the server stamps its own time |
| Calling any `*_if_new` or `*_with_own_timestamp` method with `time_stamp: Default::default()` | The **opposite** rule: these send the client's version — `time_stamp: DateTimeAsMicroseconds::now().into()`. A default TimeStamp → HTTP 400 |
| Assigning `e.time_stamp` inside an `update_entity` closure | Never touch it — it is the read version that detects concurrent writes. Overwriting it defeats optimistic concurrency (and re-reads carry a fresh version anyway) |
| Loading a whole partition just to count its rows | `w.get_rows_count(Some("pk"))` — the number comes back on its own, the rows never leave the server. `None` = no such table, `Some(0)` = table exists and is empty |
| Reading `get_rows_count() == Ok(None)` as "empty" | It means the **table is not there**. Empty is `Ok(Some(0))`. An unreachable server / unknown namespace is `Err`, never `None` |
| Treating a 409 from `replace_entity` as fatal | It means the row changed under you — re-read and retry (or just use `update_entity`, which loops for you) |
| Hand-rolling `get_entity` → `if None { insert } else { update }` | That is a race: between your read and your insert another writer can create the row. `insert_or_update` does the whole loop, including turning a lost `Insert` into an update of the winner's row |
| Treating `RecordAlreadyExists` from `insert_entity` as fatal | It means somebody created that key first — re-read and update it instead (`insert_or_update` does exactly this for you) |
| Writing the row back from an `insert_or_update` update closure when nothing changed | Return `false` — the row is not sent, no race is entered, and the entity comes back as read |

---

## 5. Reader Change Callbacks

When a service maintains an in-memory cache based on NoSql data and must react to changes — use `MyNoSqlDataReaderCallBacks`.

### Trait

```rust
#[async_trait::async_trait]
pub trait MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> {
    async fn inserted_or_replaced(&self, partition_key: &str, entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>);
    async fn deleted(&self, partition_key: &str, entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>);
}
```

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

#[async_trait::async_trait]
impl MyNoSqlDataReaderCallBacks<MyEntityNoSqlEntity> for MyEntityNoSqlCallback {
    async fn inserted_or_replaced(
        &self,
        _partition_key: &str,
        _entities: Vec<LazyMyNoSqlEntity<MyEntityNoSqlEntity>>,
    ) {
        tokio::spawn(reload_my_entities(self.app.clone()));
    }

    async fn deleted(
        &self,
        _partition_key: &str,
        _entities: Vec<LazyMyNoSqlEntity<MyEntityNoSqlEntity>>,
    ) {
        tokio::spawn(reload_my_entities(self.app.clone()));
    }
}

// Reload script — reads all from reader, replaces cache
pub async fn reload_my_entities(app: Arc<AppContext>) {
    // Reader reads are sync — no .await
    let items = app.my_entity_reader
        .get_by_partition_key(MyEntityNoSqlEntity::PARTITION_KEY);

    let mut cache = app.cache.lock().await;
    match items {
        Some(entities) => cache.reload_all(entities.values().map(|e| e.as_ref().into())),
        None => cache.reload_all(std::iter::empty()),
    }
}
```

### Registration — in main.rs after init, before start_application

```rust
use my_no_sql_sdk::reader::MyNoSqlDataReader;

// assign_callback is sync — no .await
app.my_entity_reader
    .assign_callback(Arc::new(MyEntityNoSqlCallback::new(app.clone())));
```

### Rules (in case of full reload to local cache)

| Rule | Why |
|---|---|
| **ALWAYS** `tokio::spawn` in callback | Callback must not block the NoSql reader |
| Reload script takes `Arc<AppContext>` (owned) | Required for `tokio::spawn` — move semantics |
| **ALWAYS** full reload, not incremental | Simpler, more reliable, no edge cases with event ordering |
| Callback lives in `scripts/` | Not a flow — no HTTP/gRPC context |
| Cache must have a `reload_all` method | Clears and re-populates from an iterator |
