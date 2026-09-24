use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use flurl::FlUrl;

use my_no_sql_abstractions::{
    DataSynchronizationPeriod, MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp,
};

use serde::{Deserialize, Serialize};

use crate::{
    BulkDeleteIfResult, MyNoSqlDataWriterBuilder, MyNoSqlDataWriterWithRetries,
    MyNoSqlWriterSettings, RowToDeleteIf,
};

use super::{fl_url_factory::FlUrlFactory, DataWriterError, UpdateReadStatistics};

/// Default number of read-modify-write attempts for [`MyNoSqlDataWriter::update_entity`] and
/// [`MyNoSqlDataWriter::insert_or_update`] before a persistent conflict with another writer is
/// surfaced.
pub const DEFAULT_UPDATE_ENTITY_MAX_ATTEMPTS: usize = 5;

pub struct CreateTableParams {
    pub persist: bool,
    pub max_partitions_amount: Option<usize>,
    pub max_rows_per_partition_amount: Option<usize>,
}

impl CreateTableParams {
    pub fn populate_params(&self, mut fl_url: FlUrl) -> FlUrl {
        if let Some(max_partitions_amount) = self.max_partitions_amount {
            fl_url = fl_url.append_query_param(
                "maxPartitionsAmount",
                Some(max_partitions_amount.to_string()),
            )
        };

        if let Some(max_rows_per_partition_amount) = self.max_rows_per_partition_amount {
            fl_url = fl_url.append_query_param(
                "maxRowsPerPartitionAmount",
                Some(max_rows_per_partition_amount.to_string()),
            )
        };

        if !self.persist {
            fl_url = fl_url.append_query_param("persist", Some("false"));
        };

        fl_url
    }
}

pub struct MyNoSqlDataWriter<TEntity: MyNoSqlEntity + Sync + Send> {
    sync_period: DataSynchronizationPeriod,
    phantom: PhantomData<TEntity>,
    fl_url_factory: FlUrlFactory,
}

impl<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send> MyNoSqlDataWriter<TEntity> {
    pub fn create_with_builder(
        settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
    ) -> MyNoSqlDataWriterBuilder<TEntity> {
        MyNoSqlDataWriterBuilder::new(settings)
    }
    pub fn new(
        settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
        auto_create_table_params: Option<CreateTableParams>,
        sync_period: DataSynchronizationPeriod,
    ) -> Self {
        crate::PING_POOL.register(settings.clone(), TEntity::TABLE_NAME);

        Self {
            phantom: PhantomData,
            sync_period,
            fl_url_factory: FlUrlFactory::new(
                settings,
                auto_create_table_params.map(|itm| itm.into()),
                TEntity::TABLE_NAME,
            ),
        }
    }

    /// Falls back to HTTP/1.1 - for MyNoSqlServer instances which do not support HTTP/2.
    pub fn use_h1(&mut self) {
        self.fl_url_factory.use_h1();
        crate::PING_POOL.use_h1(self.fl_url_factory.get_settings(), TEntity::TABLE_NAME);
    }

    pub async fn create_table(&self, params: CreateTableParams) -> Result<(), DataWriterError> {
        let (fl_url, url) = self.fl_url_factory.get_fl_url().await?;

        super::execution::create_table(
            fl_url,
            url.as_str(),
            TEntity::TABLE_NAME,
            params,
            &self.sync_period,
        )
        .await
    }

    #[cfg(all(unix, feature = "with-ssh"))]
    pub fn set_ssh_security_credentials_resolver(
        &mut self,
        resolver: Arc<
            dyn flurl::my_ssh::ssh_settings::SshSecurityCredentialsResolver + Send + Sync,
        >,
    ) {
        self.fl_url_factory.ssh_security_credentials_resolver = Some(resolver);
    }

    pub async fn create_table_if_not_exists(
        &self,
        params: &CreateTableParams,
    ) -> Result<(), DataWriterError> {
        let (fl_url, url) = self.fl_url_factory.get_fl_url().await?;
        super::execution::create_table_if_not_exists(
            fl_url,
            url.as_str(),
            TEntity::TABLE_NAME,
            params,
            self.sync_period,
        )
        .await
    }

    pub fn with_retries(&self, max_attempts: usize) -> MyNoSqlDataWriterWithRetries<TEntity> {
        MyNoSqlDataWriterWithRetries::new(
            self.fl_url_factory.clone(),
            self.sync_period,
            max_attempts,
        )
    }

    pub async fn insert_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::insert_entity(fl_url, entity, &self.sync_period).await
    }

    pub async fn insert_or_replace_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::insert_or_replace_entity(fl_url, entity, &self.sync_period).await
    }

    /// Optimistic-concurrency replace: the entity must carry the `TimeStamp` it was read
    /// with. If the stored row's `TimeStamp` still matches, it is replaced; otherwise this
    /// fails with [`DataWriterError::RecordIsChanged`] (409). A missing row yields
    /// [`DataWriterError::RecordNotFound`] (404), a missing `TimeStamp` an HTTP 400.
    ///
    /// Prefer [`Self::update_entity`] which drives the read → mutate → replace → retry loop
    /// for you.
    pub async fn replace_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::replace_entity(fl_url, entity, &self.sync_period).await
    }

    /// Read-modify-write with optimistic concurrency, retrying `RecordIsChanged` conflicts
    /// with the default attempt limit ([`DEFAULT_UPDATE_ENTITY_MAX_ATTEMPTS`]).
    ///
    /// Reads the row, calls `update` on it (mutate whatever fields you need — do **not**
    /// touch `time_stamp`, it carries the read version), and replaces it. On a concurrent
    /// change (409) it re-reads the fresh version and re-applies `update`. Returns
    /// `Ok(None)` if the row does not exist, `Ok(Some(updated))` on success.
    pub async fn update_entity<TFn: FnMut(&mut TEntity)>(
        &self,
        partition_key: &str,
        row_key: &str,
        update: TFn,
    ) -> Result<Option<TEntity>, DataWriterError> {
        self.update_entity_with_max_attempts(
            partition_key,
            row_key,
            DEFAULT_UPDATE_ENTITY_MAX_ATTEMPTS,
            update,
        )
        .await
    }

    /// [`Self::update_entity`] with an explicit retry limit for the optimistic-concurrency
    /// loop. On exhaustion the last [`DataWriterError::RecordIsChanged`] is returned.
    pub async fn update_entity_with_max_attempts<TFn: FnMut(&mut TEntity)>(
        &self,
        partition_key: &str,
        row_key: &str,
        max_attempts: usize,
        update: TFn,
    ) -> Result<Option<TEntity>, DataWriterError> {
        super::execution::run_read_modify_write(
            max_attempts,
            update,
            || self.get_entity(partition_key, row_key, None),
            |entity| async move {
                let result = self.replace_entity(&entity).await;
                (entity, result)
            },
        )
        .await
    }

    /// Insert-or-update in one call: read the row, then either create it with `create` or
    /// change it with `update`, retrying every race with another writer until one of the two
    /// writes lands. The whole conflict protocol lives in here, so the caller never has to know
    /// in advance which of the two operations it is doing:
    ///
    /// * the row is not there -> `create()` builds it and it goes in through `Insert`. The
    ///   server re-checks the key under the table write lock, so when another writer created
    ///   the same key first this call gets `RecordAlreadyExists`, re-reads, and applies
    ///   `update` to the row that writer wrote - the winner's row is never overwritten blindly.
    /// * the row is there -> `update(&mut entity)` and `Replace`, carrying the `TimeStamp` the
    ///   entity was read with. Rewritten in between (409) -> re-read the fresh version and
    ///   apply `update` to it. Deleted in between (404) -> re-read and fall into the `create`
    ///   branch.
    ///
    /// `update` decides whether the row is written at all. It is handed the row it would
    /// change, so it can read the fields, find that they already say what they should, and
    /// answer `false` - nothing is sent, no race is entered, and the entity comes back as it
    /// was read. `true` means "write what I just changed".
    ///
    /// Both closures can therefore run more than once, each time on state that was just read,
    /// so they must express the wanted end state ("set this field to X") rather than a step
    /// away from a state they remember. They are `FnMut`: a flag flipped inside them is how you
    /// find out afterwards which branch won.
    ///
    /// `create` must build the entity under the `partition_key` / `row_key` given here - any
    /// other key is refused - and leave `time_stamp` at `Default::default()`, which lets the
    /// server stamp it. `update` must not touch `time_stamp` at all: it carries the read
    /// version the whole protocol stands on.
    ///
    /// Returns the entity as it was written - or as it was read, when `update` answered
    /// `false`. After `max_attempts` lost races the last one is returned as it came, so it is
    /// whichever of the three the loop was retrying: [`DataWriterError::RecordIsChanged`],
    /// [`DataWriterError::RecordAlreadyExists`] or [`DataWriterError::RecordNotFound`] (the row
    /// kept being deleted before the replace landed). All three mean "still contended", never
    /// "impossible".
    ///
    /// What comes back is the entity which was sent, not a fresh read of the stored row: its
    /// `time_stamp` is the version this attempt read (or the default the created entity
    /// carried), never the one the server has just stamped. Handing it straight to
    /// [`Self::replace_entity`] or `delete_entity_if` is a guaranteed conflict - read it again
    /// first.
    ///
    /// The table has to exist, like for every other write: a writer built with
    /// [`CreateTableParams`] creates it on its first request - the read this loop starts with
    /// counts as one - and without them a missing table is a
    /// [`DataWriterError::TableNotFound`], not a reason to create anything.
    pub async fn insert_or_update<
        TCreate: FnMut() -> TEntity,
        TUpdate: FnMut(&mut TEntity) -> bool,
    >(
        &self,
        partition_key: &str,
        row_key: &str,
        create: TCreate,
        update: TUpdate,
    ) -> Result<TEntity, DataWriterError> {
        self.insert_or_update_with_max_attempts(
            partition_key,
            row_key,
            DEFAULT_UPDATE_ENTITY_MAX_ATTEMPTS,
            create,
            update,
        )
        .await
    }

    /// [`Self::insert_or_update`] with an explicit limit on how many races it may lose before
    /// the conflict is surfaced.
    pub async fn insert_or_update_with_max_attempts<
        TCreate: FnMut() -> TEntity,
        TUpdate: FnMut(&mut TEntity) -> bool,
    >(
        &self,
        partition_key: &str,
        row_key: &str,
        max_attempts: usize,
        create: TCreate,
        update: TUpdate,
    ) -> Result<TEntity, DataWriterError> {
        super::execution::run_insert_or_update(
            max_attempts,
            create,
            update,
            || self.get_entity(partition_key, row_key, None),
            |entity| async move {
                if let Err(err) = super::execution::ensure_entity_keys_match(
                    &entity,
                    partition_key,
                    row_key,
                    "create",
                ) {
                    return (entity, Err(err));
                }

                let result = self.insert_entity(&entity).await;
                (entity, result)
            },
            |entity| async move {
                if let Err(err) = super::execution::ensure_entity_keys_match(
                    &entity,
                    partition_key,
                    row_key,
                    "update",
                ) {
                    return (entity, Err(err));
                }

                let result = self.replace_entity(&entity).await;
                (entity, result)
            },
        )
        .await
    }

    pub async fn bulk_insert_or_replace(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::bulk_insert_or_replace(fl_url, entities, &self.sync_period).await
    }

    /// Same as [`Self::bulk_insert_or_replace`], but each row is stored with **its own
    /// `TimeStamp`** (server sends `useTimestamp=true`) instead of the server clock. This
    /// is still an unconditional replace — every row is written — it just preserves the
    /// client-supplied version. Every entity must carry a real (non-default) `time_stamp`,
    /// otherwise the server rejects the request with HTTP 400. Empty slice is a no-op.
    pub async fn bulk_insert_or_update_with_own_timestamp(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::bulk_insert_or_update_with_own_timestamp(
            fl_url,
            entities,
            &self.sync_period,
        )
        .await
    }

    /// Deletes rows described as PartitionKey -> RowKeys
    pub async fn bulk_delete(
        &self,
        rows_to_delete: &BTreeMap<String, Vec<String>>,
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::bulk_delete::<TEntity>(fl_url, rows_to_delete, &self.sync_period).await
    }

    /// Conditional bulk delete: each row is deleted only while the `TimeStamp` stored in the
    /// table is still the one the entity carries. Pass the entities exactly as they were
    /// read.
    ///
    /// This always succeeds as a **partial** result: the matching rows are gone, and every
    /// row which was left in place is listed in [`BulkDeleteIfResult::skipped`] with the
    /// reason — [`TimeStampMismatch`](crate::DeleteIfSkipReason::TimeStampMismatch)
    /// (rewritten meanwhile) or [`NotFound`](crate::DeleteIfSkipReason::NotFound) (no such
    /// row). Use
    /// [`BulkDeleteIfResult::is_all_deleted`] / [`BulkDeleteIfResult::conflicts`] instead of
    /// picking the list apart by hand.
    ///
    /// Every entity must carry a real (non-default) `time_stamp` — an unreadable version can
    /// never match a stored one, so the server rejects the **whole batch** with HTTP 400
    /// rather than reporting that row as skipped. An empty slice is a no-op.
    pub async fn bulk_delete_if(
        &self,
        entities: &[&TEntity],
    ) -> Result<BulkDeleteIfResult, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::bulk_delete_if(fl_url, entities, &self.sync_period).await
    }

    /// [`Self::bulk_delete_if`] taking keys and versions on their own instead of whole
    /// entities — for when the versions come from somewhere else than the entities
    /// themselves. Same contract in every other respect.
    pub async fn bulk_delete_if_rows(
        &self,
        rows: &[RowToDeleteIf],
    ) -> Result<BulkDeleteIfResult, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::bulk_delete_if_rows::<TEntity>(fl_url, rows, &self.sync_period).await
    }

    pub async fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<TEntity>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::get_entity(
            fl_url,
            partition_key,
            row_key,
            update_read_statistics.as_ref(),
        )
        .await
    }

    pub async fn get_by_partition_key(
        &self,
        partition_key: &str,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::get_by_partition_key(
            fl_url,
            partition_key,
            update_read_statistics.as_ref(),
        )
        .await
    }

    /// How many rows the table holds in `partition_key` - or in the whole table, when it is
    /// `None` - without a single row crossing the network. This is the cheap way to ask "do
    /// these two tables still agree?" on a schedule: reading the partition to count it costs
    /// its whole contents for an answer which, in the steady state, is "nothing to do".
    ///
    /// Same shape as [`Self::get_by_partition_key`], and the same distinction, which is the
    /// point of the `Option`:
    ///
    /// - `None` - the **table** does not exist.
    /// - `Some(0)` - it exists and the partition (or the whole table) is empty.
    ///
    /// Those are different facts, so a missing table is never folded into a zero.
    ///
    /// This is the one writer method which does **not** auto-create the table on its way out,
    /// even when the writer was built with auto-creation on (the default). A counter must not
    /// bring into existence the thing it was asked to count: going through the usual path
    /// would create the table empty and answer `Some(0)`, making `None` unreachable and the
    /// first call a write. Every other method keeps auto-creating as before.
    ///
    /// Unlike a read it also does not touch the partition's last-read moment, so counting on a
    /// timer never keeps a partition alive against `max_partitions_amount` collection.
    pub async fn get_rows_count(
        &self,
        partition_key: Option<&str>,
    ) -> Result<Option<usize>, DataWriterError> {
        let (fl_url, _) = self
            .fl_url_factory
            .get_fl_url_without_auto_create_table()
            .await?;
        super::execution::get_rows_count(fl_url, TEntity::TABLE_NAME, partition_key).await
    }

    pub async fn get_enum_case_models_by_partition_key<
        TResult: MyNoSqlEntity
            + my_no_sql_abstractions::GetMyNoSqlEntitiesByPartitionKey
            + From<TEntity>
            + Sync
            + Send
            + 'static,
    >(
        &self,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<Vec<TResult>>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::get_enum_case_models_by_partition_key(
            fl_url,
            update_read_statistics.as_ref(),
        )
        .await
    }

    pub async fn get_enum_case_model<
        TResult: MyNoSqlEntity
            + From<TEntity>
            + my_no_sql_abstractions::GetMyNoSqlEntity
            + Sync
            + Send
            + 'static,
    >(
        &self,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<TResult>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::get_enum_case_model(fl_url, update_read_statistics.as_ref()).await
    }

    pub async fn get_by_row_key(
        &self,
        row_key: &str,
    ) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::get_by_row_key(fl_url, row_key).await
    }

    pub async fn get_partition_keys(
        &self,
        skip: Option<i32>,
        limit: Option<i32>,
    ) -> Result<Vec<String>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::get_partition_keys(fl_url, TEntity::TABLE_NAME, skip, limit).await
    }

    pub async fn delete_enum_case<
        TResult: MyNoSqlEntity
            + From<TEntity>
            + my_no_sql_abstractions::GetMyNoSqlEntity
            + Sync
            + Send
            + 'static,
    >(
        &self,
    ) -> Result<Option<TResult>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::delete_enum_case(fl_url).await
    }

    pub async fn delete_enum_case_with_row_key<
        TResult: MyNoSqlEntity
            + From<TEntity>
            + my_no_sql_abstractions::GetMyNoSqlEntitiesByPartitionKey
            + Sync
            + Send
            + 'static,
    >(
        &self,
        row_key: &str,
    ) -> Result<Option<TResult>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::delete_enum_case_with_row_key(fl_url, row_key).await
    }

    pub async fn delete_row(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<TEntity>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::delete_row(fl_url, partition_key, row_key).await
    }

    /// Optimistic-concurrency delete of the row this entity stands for: it is deleted only
    /// while the stored `TimeStamp` is still the one the entity carries. This is the main
    /// conditional-delete case — read the row, decide it should go, and delete exactly the
    /// version that was read.
    ///
    /// `Ok(Some(deleted))` when it was deleted, `Ok(None)` when there was no such row (404).
    /// A row which was rewritten in the meantime is **not** deleted and comes back as
    /// [`DataWriterError::RecordIsChanged`] (409) — re-read it and decide again, since it may
    /// no longer be a row you want to delete. A missing `time_stamp` is an HTTP 400.
    pub async fn delete_entity_if(
        &self,
        entity: &TEntity,
    ) -> Result<Option<TEntity>, DataWriterError> {
        self.delete_row_if(
            entity.get_partition_key(),
            entity.get_row_key(),
            entity.get_time_stamp(),
        )
        .await
    }

    /// [`Self::delete_entity_if`] addressed by keys instead of by entity — for when the
    /// version comes from somewhere else than the entity itself. Same codes: `Ok(None)` on
    /// 404, [`DataWriterError::RecordIsChanged`] on 409.
    pub async fn delete_row_if(
        &self,
        partition_key: &str,
        row_key: &str,
        time_stamp: Timestamp,
    ) -> Result<Option<TEntity>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::delete_row_if(
            fl_url,
            partition_key,
            row_key,
            time_stamp,
            &self.sync_period,
        )
        .await
    }

    pub async fn delete_partitions(&self, partition_keys: &[&str]) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::delete_partitions(fl_url, TEntity::TABLE_NAME, partition_keys).await
    }

    pub async fn get_all(&self) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::get_all(fl_url).await
    }

    /// **Atomically replaces the whole table** with `entities` — this is the point of the
    /// method, not a side effect. The clean and the insert are one server-side operation and
    /// reach subscribers as a single `InitTable` packet, which the reader applies under one
    /// lock (old snapshot swapped for the new one in a single step). **There is no moment at
    /// which the table is observed empty or half-filled**: a concurrent read sees either the
    /// entire previous snapshot or the entire new one.
    ///
    /// Do **not** emulate it with `delete_partitions` + `bulk_insert_or_replace` — that leaves
    /// a window in which readers see an empty table.
    pub async fn clean_table_and_bulk_insert(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::clean_table_and_bulk_insert(fl_url, entities, &self.sync_period).await
    }

    /// **Atomically replaces one partition** with `entities`, leaving the rest of the table
    /// untouched. Same guarantee as [`Self::clean_table_and_bulk_insert`], scoped to
    /// `partition_key`: it is one server-side operation, delivered as a single `InitPartition`
    /// packet and applied by the reader under one lock, so **the partition is never observed
    /// empty or half-filled** — a concurrent read gets the whole old snapshot or the whole new
    /// one.
    pub async fn clean_partition_and_bulk_insert(
        &self,
        partition_key: &str,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::clean_partition_and_bulk_insert(
            fl_url,
            partition_key,
            entities,
            &self.sync_period,
        )
        .await
    }

    /// Same as [`Self::clean_table_and_bulk_insert`] — including the atomic snapshot swap
    /// (one `InitTable` packet, applied under one reader lock, table never seen empty) — but
    /// each re-inserted row keeps its **own `TimeStamp`** (server sends `useTimestamp=true`)
    /// instead of the server clock. Every entity must carry a real (non-default)
    /// `time_stamp`, otherwise the server rejects the request with HTTP 400.
    pub async fn clean_table_and_bulk_insert_with_own_timestamp(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::clean_table_and_bulk_insert_with_own_timestamp(
            fl_url,
            entities,
            &self.sync_period,
        )
        .await
    }

    /// Same as [`Self::clean_partition_and_bulk_insert`] — including the atomic swap of the
    /// partition (one `InitPartition` packet, applied under one reader lock, partition never
    /// seen empty) — but each re-inserted row keeps its **own `TimeStamp`**
    /// (`useTimestamp=true`). Every entity must carry a real (non-default) `time_stamp`,
    /// otherwise the server rejects the request with HTTP 400.
    pub async fn clean_partition_and_bulk_insert_with_own_timestamp(
        &self,
        partition_key: &str,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::clean_partition_and_bulk_insert_with_own_timestamp(
            fl_url,
            partition_key,
            entities,
            &self.sync_period,
        )
        .await
    }

    /// One-call chunked clean-and-bulk-insert that keeps each row's **own `TimeStamp`**
    /// (`useTimestamp=true`). Slices `entities` into chunks of `chunk_size`, starts a
    /// server-side process with the first chunk (scoped to `partition_key`, or the whole
    /// table when `None`), uploads the rest under the issued `processId`, then commits so
    /// the clean + insert happen atomically. On any failure a best-effort `Cancel` is issued
    /// and the original error is returned. Every entity must carry a non-default `time_stamp`.
    ///
    /// Chunking does **not** weaken the snapshot-swap guarantee: nothing an uploaded chunk
    /// carries is visible until the commit, and the commit applies the clean and the insert
    /// together — one `InitTable` / `InitPartition` packet, applied by the reader under one
    /// lock. **The table (or partition) is never observed empty or partially uploaded**; a
    /// cancelled or failed process leaves the previous snapshot exactly as it was.
    ///
    /// An empty slice is a no-op — note that, unlike the non-chunked clean, it does **not**
    /// clean the table (no process is started). Use `clean_*_with_own_timestamp(&[])` to
    /// clean without inserting.
    pub async fn clean_and_bulk_insert_by_chunks_with_own_timestamp(
        &self,
        partition_key: Option<&str>,
        entities: &[TEntity],
        chunk_size: usize,
    ) -> Result<(), DataWriterError> {
        assert!(chunk_size > 0, "chunk_size must be greater than 0");

        if entities.is_empty() {
            return Ok(());
        }

        let mut chunks = entities.chunks(chunk_size);

        let process_id = self
            .clean_and_bulk_insert_by_chunks_with_own_timestamp_start(
                partition_key,
                chunks.next().unwrap(),
            )
            .await?;

        let result = async {
            for chunk in chunks {
                self.clean_and_bulk_insert_by_chunks_with_own_timestamp_append(&process_id, chunk)
                    .await?;
            }
            self.clean_and_bulk_insert_by_chunks_with_own_timestamp_commit(&process_id)
                .await
        }
        .await;

        if let Err(err) = result {
            let _ = self
                .clean_and_bulk_insert_by_chunks_with_own_timestamp_cancel(&process_id)
                .await;
            return Err(err);
        }

        Ok(())
    }

    /// Low-level chunked clean flow: starts a new process by uploading the first chunk
    /// (cleaning `partition_key`, or the whole table when `None`) and returns the
    /// server-issued `processId`. Nothing is applied until the commit — readers keep being
    /// served the current snapshot for the whole upload, however long it takes. Keeps each
    /// row's own `TimeStamp`; each row must carry a non-default one.
    pub async fn clean_and_bulk_insert_by_chunks_with_own_timestamp_start(
        &self,
        partition_key: Option<&str>,
        entities: &[TEntity],
    ) -> Result<String, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::clean_and_bulk_insert_by_chunks_with_own_timestamp_upload(
            fl_url,
            entities,
            partition_key,
            None,
        )
        .await
    }

    /// Low-level chunked clean flow: appends a further chunk to an already-started process.
    /// Still invisible to readers — the accumulated rows only become the live snapshot at the
    /// commit.
    pub async fn clean_and_bulk_insert_by_chunks_with_own_timestamp_append(
        &self,
        process_id: &str,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::clean_and_bulk_insert_by_chunks_with_own_timestamp_upload(
            fl_url,
            entities,
            None,
            Some(process_id),
        )
        .await?;
        Ok(())
    }

    /// Low-level chunked clean flow: commits the process (cleans + inserts every uploaded row).
    /// This is the atomic point of the whole flow — the clean and the insert land together and
    /// readers get one snapshot swap, so the table/partition is never observed empty.
    pub async fn clean_and_bulk_insert_by_chunks_with_own_timestamp_commit(
        &self,
        process_id: &str,
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::clean_and_bulk_insert_by_chunks_commit(
            fl_url,
            process_id,
            &self.sync_period,
        )
        .await
    }

    /// Low-level chunked clean flow: cancels the process, dropping the uploaded rows. The live
    /// snapshot is left exactly as it was — an abandoned replace never empties the table or
    /// the partition.
    pub async fn clean_and_bulk_insert_by_chunks_with_own_timestamp_cancel(
        &self,
        process_id: &str,
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::clean_and_bulk_insert_by_chunks_cancel(fl_url, process_id).await
    }

    /// Insert-or-replace-if-new for a single entity: the row is written only when it is
    /// missing or the incoming `TimeStamp` is strictly greater than the stored one.
    ///
    /// The `TimeStamp` here is the object's version in a distributed system and is
    /// **mandatory** — set `entity.time_stamp` to your real version (e.g.
    /// `DateTimeAsMicroseconds::now().into()`) before calling. A default/unset `Timestamp`
    /// serializes to `null`/omitted and the server rejects the request with HTTP 400.
    pub async fn insert_or_replace_entity_if_new(
        &self,
        entity: &TEntity,
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::insert_or_replace_entity_if_new(fl_url, entity, &self.sync_period).await
    }

    /// Bulk insert-or-replace-if-new. Same per-row rule and the same mandatory-`TimeStamp`
    /// contract as [`Self::insert_or_replace_entity_if_new`]. An empty slice is a no-op.
    pub async fn bulk_insert_or_replace_if_new(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::bulk_insert_or_replace_if_new(fl_url, entities, &self.sync_period).await
    }

    /// One-call chunked insert-or-replace-if-new: slices `entities` into chunks of
    /// `chunk_size`, starts a server-side process with the first chunk, uploads the rest
    /// under the issued `processId`, then commits. If any step fails, a best-effort
    /// `Cancel` is issued and the original error is returned (so a half-uploaded process
    /// never lingers on the server). An empty slice is a no-op.
    ///
    /// Same mandatory-`TimeStamp` contract as [`Self::insert_or_replace_entity_if_new`].
    /// Use this when you already have all entities in memory; for streaming, drive the
    /// [`Self::insert_or_replace_if_new_by_chunks_start`] / `..._append` / `..._commit` /
    /// `..._cancel` primitives directly.
    pub async fn bulk_insert_or_replace_if_new_by_chunks(
        &self,
        entities: &[TEntity],
        chunk_size: usize,
    ) -> Result<(), DataWriterError> {
        assert!(chunk_size > 0, "chunk_size must be greater than 0");

        if entities.is_empty() {
            return Ok(());
        }

        let mut chunks = entities.chunks(chunk_size);

        // The first chunk starts the process and yields the id every following request
        // must carry. Once it succeeds the process exists on the server, so from here on
        // any failure has to be cleaned up with a Cancel.
        let process_id = self
            .insert_or_replace_if_new_by_chunks_start(chunks.next().unwrap())
            .await?;

        let result = async {
            for chunk in chunks {
                self.insert_or_replace_if_new_by_chunks_append(&process_id, chunk)
                    .await?;
            }
            self.insert_or_replace_if_new_by_chunks_commit(&process_id)
                .await
        }
        .await;

        if let Err(err) = result {
            // Best-effort cleanup — keep the original error regardless of the cancel result.
            let _ = self
                .insert_or_replace_if_new_by_chunks_cancel(&process_id)
                .await;
            return Err(err);
        }

        Ok(())
    }

    /// Low-level chunked flow: starts a new process by uploading the first chunk and
    /// returns the server-issued `processId` to be replayed on every following call.
    /// Nothing is applied to the table until [`Self::insert_or_replace_if_new_by_chunks_commit`].
    /// Each row must carry its own non-default `TimeStamp`.
    pub async fn insert_or_replace_if_new_by_chunks_start(
        &self,
        entities: &[TEntity],
    ) -> Result<String, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::insert_or_replace_if_new_by_chunks_upload(fl_url, entities, None).await
    }

    /// Low-level chunked flow: appends a further chunk to an already-started process.
    pub async fn insert_or_replace_if_new_by_chunks_append(
        &self,
        process_id: &str,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::insert_or_replace_if_new_by_chunks_upload(
            fl_url,
            entities,
            Some(process_id),
        )
        .await?;
        Ok(())
    }

    /// Low-level chunked flow: commits an accumulated process (applies every uploaded row).
    pub async fn insert_or_replace_if_new_by_chunks_commit(
        &self,
        process_id: &str,
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::insert_or_replace_if_new_by_chunks_commit(
            fl_url,
            process_id,
            &self.sync_period,
        )
        .await
    }

    /// Low-level chunked flow: cancels an accumulated process, dropping the uploaded rows.
    pub async fn insert_or_replace_if_new_by_chunks_cancel(
        &self,
        process_id: &str,
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::insert_or_replace_if_new_by_chunks_cancel(fl_url, process_id).await
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OperationFailHttpContract {
    pub reason: String,
    pub message: String,
}
