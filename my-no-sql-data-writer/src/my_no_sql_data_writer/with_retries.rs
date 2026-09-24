use std::{collections::BTreeMap, marker::PhantomData};

use my_no_sql_abstractions::{
    DataSynchronizationPeriod, MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp,
};

use crate::{BulkDeleteIfResult, DataWriterError, RowToDeleteIf, UpdateReadStatistics};

use super::fl_url_factory::FlUrlFactory;

pub struct MyNoSqlDataWriterWithRetries<TEntity: MyNoSqlEntity + Sync + Send> {
    fl_url_factory: FlUrlFactory,
    sync_period: DataSynchronizationPeriod,
    phantom: PhantomData<TEntity>,
    max_attempts: usize,
}

impl<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>
    MyNoSqlDataWriterWithRetries<TEntity>
{
    pub fn new(
        fl_url_factory: FlUrlFactory,
        sync_period: DataSynchronizationPeriod,
        max_attempts: usize,
    ) -> Self {
        Self {
            phantom: PhantomData,
            sync_period,

            max_attempts,
            fl_url_factory,
        }
    }

    pub async fn insert_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::insert_entity(fl_url, entity, &self.sync_period).await
    }

    pub async fn insert_or_replace_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::insert_or_replace_entity(fl_url, entity, &self.sync_period).await
    }

    /// Optimistic-concurrency replace. See
    /// [`super::MyNoSqlDataWriter::replace_entity`]. The `with_retries` here retries the
    /// underlying HTTP request on transport errors; a 409 `RecordIsChanged` is a real
    /// conflict and is returned to the caller (drive the read-modify-write loop with
    /// [`Self::update_entity`]).
    pub async fn replace_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::replace_entity(fl_url, entity, &self.sync_period).await
    }

    /// Read-modify-write with optimistic concurrency (default attempt limit). See
    /// [`super::MyNoSqlDataWriter::update_entity`]. Each `get`/`replace` in the loop also
    /// goes through the HTTP-level retries of this wrapper.
    pub async fn update_entity<TFn: FnMut(&mut TEntity)>(
        &self,
        partition_key: &str,
        row_key: &str,
        update: TFn,
    ) -> Result<Option<TEntity>, DataWriterError> {
        self.update_entity_with_max_attempts(
            partition_key,
            row_key,
            crate::DEFAULT_UPDATE_ENTITY_MAX_ATTEMPTS,
            update,
        )
        .await
    }

    /// [`Self::update_entity`] with an explicit optimistic-concurrency retry limit.
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

    /// Insert-or-update in one call (default attempt limit). See
    /// [`super::MyNoSqlDataWriter::insert_or_update`] for the protocol and for what the two
    /// closures may and may not do - including `update` answering `false` to say the row is
    /// already right and nothing has to be written.
    ///
    /// The two kinds of retry do not multiply: the `with_retries` of this wrapper re-sends a
    /// single HTTP request when the transport fails, while `max_attempts` here counts only
    /// races actually lost to another writer. FlUrl replays idempotent requests only, so those
    /// transport retries cover the read (GET) and the replace (PUT) of this loop but not the
    /// insert (POST) - a POST which may already have landed must not be sent twice.
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
            crate::DEFAULT_UPDATE_ENTITY_MAX_ATTEMPTS,
            create,
            update,
        )
        .await
    }

    /// [`Self::insert_or_update`] with an explicit limit on lost races.
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
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::bulk_insert_or_replace(fl_url, entities, &self.sync_period).await
    }

    /// Bulk insert-or-replace that keeps each entity's own `TimeStamp` (`useTimestamp=true`).
    /// See [`super::MyNoSqlDataWriter::bulk_insert_or_update_with_own_timestamp`]. Every
    /// entity must carry a real (non-default) `time_stamp`; empty slice is a no-op.
    pub async fn bulk_insert_or_update_with_own_timestamp(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::bulk_insert_or_update_with_own_timestamp(
            fl_url,
            entities,
            &self.sync_period,
        )
        .await
    }

    /// Insert-or-replace-if-new for a single entity. The `TimeStamp` is the object's
    /// version and is mandatory — a default/unset `Timestamp` makes the server answer
    /// HTTP 400. See [`super::MyNoSqlDataWriter::insert_or_replace_entity_if_new`].
    pub async fn insert_or_replace_entity_if_new(
        &self,
        entity: &TEntity,
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::insert_or_replace_entity_if_new(fl_url, entity, &self.sync_period).await
    }

    /// Bulk insert-or-replace-if-new. Mandatory-`TimeStamp` contract as above; empty slice
    /// is a no-op. The chunked flow is intentionally not offered on the retries wrapper:
    /// re-sending a chunk after a partial success would double-append rows into the
    /// server-side accumulator, so those requests must not be blindly retried.
    pub async fn bulk_insert_or_replace_if_new(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::bulk_insert_or_replace_if_new(fl_url, entities, &self.sync_period).await
    }

    /// Deletes rows described as PartitionKey -> RowKeys
    pub async fn bulk_delete(
        &self,
        rows_to_delete: &BTreeMap<String, Vec<String>>,
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::bulk_delete::<TEntity>(fl_url, rows_to_delete, &self.sync_period).await
    }

    /// Conditional bulk delete — each row is deleted only while it is still at the version
    /// its entity carries. See [`super::MyNoSqlDataWriter::bulk_delete_if`]: the result is a
    /// partial success, the leftovers come back in [`BulkDeleteIfResult::skipped`]. Every
    /// entity must carry a real (non-default) `time_stamp`; an empty slice is a no-op.
    ///
    /// The `with_retries` here retries the underlying HTTP request on transport errors, which
    /// is safe: a retry re-sends the same versions, and a row deleted by the first attempt
    /// simply comes back as `NotFound` in the second one.
    pub async fn bulk_delete_if(
        &self,
        entities: &[&TEntity],
    ) -> Result<BulkDeleteIfResult, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::bulk_delete_if(fl_url, entities, &self.sync_period).await
    }

    /// [`Self::bulk_delete_if`] taking keys and versions on their own instead of whole
    /// entities. See [`super::MyNoSqlDataWriter::bulk_delete_if_rows`].
    pub async fn bulk_delete_if_rows(
        &self,
        rows: &[RowToDeleteIf],
    ) -> Result<BulkDeleteIfResult, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::bulk_delete_if_rows::<TEntity>(fl_url, rows, &self.sync_period).await
    }

    pub async fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<TEntity>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
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
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::get_by_partition_key(
            fl_url,
            partition_key,
            update_read_statistics.as_ref(),
        )
        .await
    }

    /// See [`super::MyNoSqlDataWriter::get_rows_count`]. `None` = the table does not exist,
    /// `Some(0)` = it exists and the partition is empty; and, as there, the table is not
    /// auto-created by the asking. The retries are safe because the request only reads.
    pub async fn get_rows_count(
        &self,
        partition_key: Option<&str>,
    ) -> Result<Option<usize>, DataWriterError> {
        let (fl_url, _) = self
            .fl_url_factory
            .get_fl_url_without_auto_create_table()
            .await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
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
        let fl_url = fl_url.with_retries(self.max_attempts);
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
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::get_enum_case_model(fl_url, update_read_statistics.as_ref()).await
    }

    pub async fn get_by_row_key(
        &self,
        row_key: &str,
    ) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::get_by_row_key(fl_url, row_key).await
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
        let fl_url = fl_url.with_retries(self.max_attempts);
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
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::delete_enum_case_with_row_key(fl_url, row_key).await
    }

    pub async fn delete_row(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<TEntity>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::delete_row(fl_url, partition_key, row_key).await
    }

    /// Optimistic-concurrency delete of the row this entity stands for. See
    /// [`super::MyNoSqlDataWriter::delete_entity_if`]. The `with_retries` here retries the
    /// underlying HTTP request on transport errors; a 409 `RecordIsChanged` is a real
    /// conflict and is returned to the caller.
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

    /// [`Self::delete_entity_if`] addressed by keys instead of by entity. See
    /// [`super::MyNoSqlDataWriter::delete_row_if`].
    pub async fn delete_row_if(
        &self,
        partition_key: &str,
        row_key: &str,
        time_stamp: Timestamp,
    ) -> Result<Option<TEntity>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
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
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::delete_partitions(fl_url, TEntity::TABLE_NAME, partition_keys).await
    }

    pub async fn get_all(&self) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::get_all(fl_url).await
    }

    /// Atomically replaces the whole table with `entities` — see
    /// [`super::MyNoSqlDataWriter::clean_table_and_bulk_insert`]. The clean and the insert are
    /// one operation and reach readers as a single snapshot swap, so the table is never
    /// observed empty.
    pub async fn clean_table_and_bulk_insert(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::clean_table_and_bulk_insert(fl_url, entities, &self.sync_period).await
    }

    /// Atomically replaces one partition with `entities`, leaving the rest of the table alone —
    /// see [`super::MyNoSqlDataWriter::clean_partition_and_bulk_insert`]. One operation, one
    /// snapshot swap on the reader: the partition is never observed empty.
    pub async fn clean_partition_and_bulk_insert(
        &self,
        partition_key: &str,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::clean_partition_and_bulk_insert(
            fl_url,
            partition_key,
            entities,
            &self.sync_period,
        )
        .await
    }

    /// Clean-and-bulk-insert keeping each row's own `TimeStamp` (`useTimestamp=true`). See
    /// [`super::MyNoSqlDataWriter::clean_table_and_bulk_insert_with_own_timestamp`]. Same
    /// atomic snapshot swap — the table is never observed empty. Every entity must carry a
    /// real (non-default) `time_stamp`.
    pub async fn clean_table_and_bulk_insert_with_own_timestamp(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::clean_table_and_bulk_insert_with_own_timestamp(
            fl_url,
            entities,
            &self.sync_period,
        )
        .await
    }

    /// Clean-partition-and-bulk-insert keeping each row's own `TimeStamp` (`useTimestamp=true`).
    /// See [`super::MyNoSqlDataWriter::clean_partition_and_bulk_insert_with_own_timestamp`].
    /// Same atomic snapshot swap — the partition is never observed empty.
    /// Every entity must carry a real (non-default) `time_stamp`. (The chunked clean flow is
    /// only on the base writer — re-sending a chunk after a partial success would double-append
    /// into the server-side accumulator.)
    pub async fn clean_partition_and_bulk_insert_with_own_timestamp(
        &self,
        partition_key: &str,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        let fl_url = fl_url.with_retries(self.max_attempts);
        super::execution::clean_partition_and_bulk_insert_with_own_timestamp(
            fl_url,
            partition_key,
            entities,
            &self.sync_period,
        )
        .await
    }

    pub async fn get_partition_keys(
        &self,
        skip: Option<i32>,
        limit: Option<i32>,
    ) -> Result<Vec<String>, DataWriterError> {
        let (fl_url, _) = self.fl_url_factory.get_fl_url().await?;
        super::execution::get_partition_keys(fl_url, TEntity::TABLE_NAME, skip, limit).await
    }
}
