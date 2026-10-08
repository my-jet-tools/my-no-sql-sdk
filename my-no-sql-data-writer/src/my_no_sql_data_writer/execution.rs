use flurl::{body::HttpRequestBody, FlUrlResponse};
use my_json::{
    json_reader::JsonArrayIterator,
    json_writer::{JsonArrayWriter, RawJsonObject},
};
use my_logger::LogEventCtx;
use my_no_sql_abstractions::{
    DataSynchronizationPeriod, MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{CreateTableParams, DataWriterError, OperationFailHttpContract, UpdateReadStatistics};

use super::delete_if::{
    deserialize_bulk_delete_if_result, serialize_rows_to_delete_if, BulkDeleteIfResult,
    RowToDeleteIf,
};
use super::fl_url_ext::FlUrlExt;
use super::{WriterFlUrl, WriterFlUrlResponse};

const API_SEGMENT: &str = "api";

const ROW_CONTROLLER: &str = "Row";
const ROWS_CONTROLLER: &str = "Rows";
const BULK_CONTROLLER: &str = "Bulk";
const PARTITIONS_CONTROLLER: &str = "Partitions";

/// The rows counter sits at the root of the api - `/api/Count` - not under `Row`. The `Row`
/// it is grouped under in the server's swagger is not part of its path.
const COUNT_SEGMENT: &str = "Count";

pub async fn create_table_if_not_exists(
    flurl: WriterFlUrl,
    url: &str,
    table_name: &'static str,
    params: &CreateTableParams,
    sync_period: DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let fl_url = flurl
        .append_path_segment("Tables")
        .append_path_segment("CreateIfNotExists")
        .append_data_sync_period(&sync_period)
        .with_table_name_as_query_param(table_name);

    let fl_url = fl_url.map(|fl_url| params.populate_params(fl_url));

    let mut response = fl_url.post(HttpRequestBody::Empty).await?;

    create_table_errors_handler(&mut response, "create_table_if_not_exists", table_name, url).await
}

pub async fn create_table(
    flurl: WriterFlUrl,
    url: &str,
    table_name: &str,
    params: CreateTableParams,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let fl_url = flurl
        .append_path_segment("Tables")
        .append_path_segment("Create")
        .with_table_name_as_query_param(table_name)
        .append_data_sync_period(sync_period);

    let fl_url = fl_url.map(|fl_url| params.populate_params(fl_url));

    let mut response = fl_url.post(HttpRequestBody::Empty).await?;

    create_table_errors_handler(&mut response, "create_table", table_name, url).await
}

/// POST /Row/Insert - writes the row only if it is not there yet. A key which is already
/// taken comes back as [`DataWriterError::RecordAlreadyExists`], and it is a reliable answer:
/// the server re-checks the key under the table write lock while inserting, so of two
/// concurrent inserts of the same partition+row exactly one succeeds and the other gets that
/// error. That is what makes `Insert` usable as the create half of an insert-or-update loop
/// (see `MyNoSqlDataWriter::insert_or_update`).
///
/// Any answer which is neither a 2xx nor the typed error of [`check_error`] is an error which
/// names the call - see [`unexpected_response`].
pub async fn insert_entity<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: WriterFlUrl,
    entity: &TEntity,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let mut response = flurl
        .append_path_segment(ROW_CONTROLLER)
        .append_path_segment("Insert")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(HttpRequestBody::Json(entity.serialize_entity()))
        .await?;

    // Turns the server's `RecordAlreadyExists` contract into the typed variant instead of an
    // opaque `Error(<body>)` - a caller which is racing another writer has to be able to tell
    // "the key is taken" from "the write failed".
    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(&mut response, "insert_entity", TEntity::TABLE_NAME).await
}

/// POST /Row/InsertOrReplace - writes the row whatever is stored under its keys.
///
/// 2xx → `Ok(())`; 400 → the typed error of [`check_error`] (a missing table is
/// [`DataWriterError::TableNotFound`]). Any other answer is an error, see
/// [`unexpected_response`].
pub async fn insert_or_replace_entity<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    entity: &TEntity,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let entity = entity.serialize_entity();

    let mut response = flurl
        .append_path_segment(ROW_CONTROLLER)
        .append_path_segment("InsertOrReplace")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(HttpRequestBody::Json(entity))
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "insert_or_replace_entity",
        TEntity::TABLE_NAME,
    )
    .await
}

/// PUT /Row/Replace — optimistic-concurrency replace. The entity must carry the
/// `TimeStamp` it was read with; the server compares it to the stored row's `TimeStamp`:
/// equal → replaced (200); different → 409 [`DataWriterError::RecordIsChanged`]; row
/// missing → `404 Record not found` [`DataWriterError::RecordNotFound`]; no `TimeStamp` →
/// 400. Use it as read-version → mutate → write-with-that-version; on a conflict re-read and
/// retry (see `MyNoSqlDataWriter::update_entity`).
///
/// Any other answer - a 404 which is not the one of the API included, see
/// [`is_record_not_found`] - says nothing about the row and is an error, see
/// [`unexpected_response`]. It is not a lost race, and `insert_or_update` does not read again
/// because of it.
pub async fn replace_entity<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: WriterFlUrl,
    entity: &TEntity,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let mut response = flurl
        .append_path_segment(ROW_CONTROLLER)
        .append_path_segment("Replace")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .put(HttpRequestBody::Json(entity.serialize_entity()))
        .await?;

    if is_record_not_found(&mut response).await? {
        return Err(DataWriterError::RecordNotFound(
            RECORD_NOT_FOUND_BODY.to_string(),
        ));
    }

    // Handles 400 (deserialize_error) and 409 (RecordIsChanged).
    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(&mut response, "replace_entity", TEntity::TABLE_NAME).await
}

/// POST /Bulk/InsertOrReplace - writes every row of the slice whatever is stored under its
/// keys. An empty slice is a no-op (no request at all).
///
/// Read the way [`insert_or_replace_entity`] is.
pub async fn bulk_insert_or_replace<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    entities: &[TEntity],
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    if entities.is_empty() {
        return Ok(());
    }

    let mut response = flurl
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("InsertOrReplace")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(serialize_entities_to_body(entities))
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(&mut response, "bulk_insert_or_replace", TEntity::TABLE_NAME).await
}

/// POST /Bulk/InsertOrReplace with `useTimestamp=true` — bulk insert-or-replace that
/// KEEPS each entity's own `TimeStamp` instead of letting the server stamp its own clock.
///
/// Unlike [`bulk_insert_or_replace_if_new`], this is an **unconditional** replace (no
/// "strictly greater" check): every row is written, but the stored row carries the
/// client-supplied `TimeStamp`. Because of that the `TimeStamp` is **mandatory** — a
/// default/unset one is left out of the JSON by the entity macros and the server answers
/// **HTTP 400** (a `debug_assert!` panic in a debug build). An empty slice is a no-op.
///
/// Read the way [`insert_or_replace_entity`] is.
pub async fn bulk_insert_or_update_with_own_timestamp<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    entities: &[TEntity],
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    if entities.is_empty() {
        return Ok(());
    }

    debug_assert!(
        entities.iter().all(|e| !e.get_time_stamp().is_default()),
        "bulk_insert_or_update_with_own_timestamp requires every entity to carry its own \
         (non-default) TimeStamp; a default one is left out of the JSON by the entity macros \
         and the server rejects the request with HTTP 400"
    );

    let mut response = flurl
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("InsertOrReplace")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .append_query_param("useTimestamp", Some("true"))
        .post(serialize_entities_to_body(entities))
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "bulk_insert_or_update_with_own_timestamp",
        TEntity::TABLE_NAME,
    )
    .await
}

/// Deletes rows described as PartitionKey -> RowKeys
///
/// Read the way [`insert_or_replace_entity`] is.
pub async fn bulk_delete<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: WriterFlUrl,
    rows_to_delete: &BTreeMap<String, Vec<String>>,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    if rows_to_delete.is_empty() {
        return Ok(());
    }

    let body = match serde_json::to_vec(rows_to_delete) {
        Ok(body) => body,
        Err(err) => {
            return Err(DataWriterError::Error(format!(
                "Failed to serialize rows to delete: {:?}",
                err
            )))
        }
    };

    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("Delete")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(HttpRequestBody::Json(body))
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(&mut response, "bulk_delete", TEntity::TABLE_NAME).await
}

/// POST /api/Bulk/DeleteIf — optimistic-concurrency delete of a whole batch. Each entity's
/// own `TimeStamp` is the version to match, so pass the entities exactly as they were read.
///
/// Whatever the versions turn out to be, the answer is a **partial success** (HTTP 200): rows
/// which are still at the version sent are deleted, every other one stays in the table and
/// comes back in
/// [`BulkDeleteIfResult::skipped`] with the reason - `TimeStampMismatch` (rewritten
/// meanwhile) or `NotFound` (no such row). A conflict here is data, not an error - unlike
/// [`delete_row_if`], which answers 409 for the single row it addresses.
///
/// Every entity must carry a real (non-default) `TimeStamp`: an unreadable version could
/// never match a stored one, so the server refuses the **whole batch** with HTTP 400 instead
/// of reporting that row as skipped (a `debug_assert!` panic in a debug build). An empty slice
/// is a no-op (no request at all), like [`bulk_delete`].
pub async fn bulk_delete_if<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: WriterFlUrl,
    entities: &[&TEntity],
    sync_period: &DataSynchronizationPeriod,
) -> Result<BulkDeleteIfResult, DataWriterError> {
    if entities.is_empty() {
        return Ok(BulkDeleteIfResult::nothing_to_delete());
    }

    debug_assert!(
        entities.iter().all(|e| !e.get_time_stamp().is_default()),
        "bulk_delete_if compares against the version each row was read at; a default \
         Timestamp is not a version the server can read and it rejects the whole batch \
         with HTTP 400"
    );

    let body = serialize_rows_to_delete_if(
        entities
            .iter()
            .map(|e| (e.get_partition_key(), e.get_row_key(), e.get_time_stamp())),
    )?;

    send_bulk_delete_if::<TEntity>(flurl, body, sync_period, "bulk_delete_if").await
}

/// [`bulk_delete_if`] taking the keys and versions on their own instead of whole entities —
/// for when the versions come from somewhere else than the entities (a projection, a change
/// log, another service). Same contract in every other respect.
pub async fn bulk_delete_if_rows<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: WriterFlUrl,
    rows: &[RowToDeleteIf],
    sync_period: &DataSynchronizationPeriod,
) -> Result<BulkDeleteIfResult, DataWriterError> {
    if rows.is_empty() {
        return Ok(BulkDeleteIfResult::nothing_to_delete());
    }

    debug_assert!(
        rows.iter().all(|row| !row.time_stamp.is_default()),
        "bulk_delete_if_rows compares against the version each row was read at; a default \
         Timestamp is not a version the server can read and it rejects the whole batch \
         with HTTP 400"
    );

    let body = serialize_rows_to_delete_if(rows.iter().map(|row| {
        (
            row.partition_key.as_str(),
            row.row_key.as_str(),
            row.time_stamp,
        )
    }))?;

    send_bulk_delete_if::<TEntity>(flurl, body, sync_period, "bulk_delete_if_rows").await
}

/// The request of [`bulk_delete_if`] and of [`bulk_delete_if_rows`]. `operation` is the one of
/// them which sends it - the name an answer the call has no meaning for is reported under, see
/// [`unexpected_response`].
async fn send_bulk_delete_if<TEntity: MyNoSqlEntity>(
    flurl: WriterFlUrl,
    body: Vec<u8>,
    sync_period: &DataSynchronizationPeriod,
    operation: &str,
) -> Result<BulkDeleteIfResult, DataWriterError> {
    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("DeleteIf")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(HttpRequestBody::Json(body))
        .await?;

    // 400 - the table is not there, or a row of the batch carries no readable TimeStamp.
    check_error(&mut response).await?;

    if !is_ok_result(&response) {
        return unexpected_response(&mut response, operation, TEntity::TABLE_NAME).await;
    }

    deserialize_bulk_delete_if_result(response.get_body_as_slice().await?)
}

/// GET /Row by both keys - reads one row.
///
/// A 2xx carries the row → `Ok(Some(entity))`; `404 Record not found` - the row, or its whole
/// partition, is not there → `Ok(None)`; 400 → the typed error of [`check_error`] (a missing
/// table is [`DataWriterError::TableNotFound`]). Any other answer says nothing about the row
/// and comes back as an error - see [`unexpected_response`]; so does a 2xx whose body is not
/// the row - see [`read_entity`].
pub async fn get_entity<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: WriterFlUrl,
    partition_key: &str,
    row_key: &str,
    update_read_statistics: Option<&UpdateReadStatistics>,
) -> Result<Option<TEntity>, DataWriterError> {
    let mut request = flurl
        .append_path_segment(ROW_CONTROLLER)
        .with_partition_key_as_query_param(partition_key)
        .with_row_key_as_query_param(row_key)
        .with_table_name_as_query_param(TEntity::TABLE_NAME);

    if let Some(update_read_statistics) = update_read_statistics {
        request = request.map(|request| update_read_statistics.fill_fields(request));
    }

    let mut response = request.get().await?;

    if is_record_not_found(&mut response).await? {
        return Ok(None);
    }

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        let entity = read_entity(&mut response, "get_entity").await?;
        return Ok(Some(entity));
    }

    unexpected_response(&mut response, "get_entity", TEntity::TABLE_NAME).await
}

/// GET /Row by the partition key - reads the rows of one partition.
///
/// A 2xx carries the rows → `Ok(Some(entities))`, and that covers a partition which is not
/// there as well: the server answers it with an empty array, so it is `Ok(Some(vec![]))`. 400 →
/// the typed error of [`check_error`] (a missing table is [`DataWriterError::TableNotFound`]).
/// Any other answer - a 404 too, which the API never gives to this request - is an error, see
/// [`unexpected_response`]. So the `Option` is always `Some`; it is kept for the callers which
/// match on it.
pub async fn get_by_partition_key<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    partition_key: &str,
    update_read_statistics: Option<&UpdateReadStatistics>,
) -> Result<Option<Vec<TEntity>>, DataWriterError> {
    let mut request = flurl
        .append_path_segment(ROW_CONTROLLER)
        .with_partition_key_as_query_param(partition_key)
        .with_table_name_as_query_param(TEntity::TABLE_NAME);

    if let Some(update_read_statistics) = update_read_statistics {
        request = request.map(|request| update_read_statistics.fill_fields(request));
    }

    let mut response = request.get().await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        let entities = deserialize_entities(response.get_body_as_slice().await?)?;
        return Ok(Some(entities));
    }

    unexpected_response(&mut response, "get_by_partition_key", TEntity::TABLE_NAME).await
}

/// `GET /api/Count` - how many rows the table holds in `partition_key`, or in the whole table
/// when `partition_key` is `None`. Only the number travels: the rows are never serialized,
/// which is the whole point of asking this instead of reading the partition and counting it.
///
/// `Ok(None)` means the **table** does not exist. `Ok(Some(0))` means it does and the
/// partition is empty. Those are different facts - a caller reconciling two tables acts on
/// the first and not on the second - so a missing table is never folded into a zero. Callers
/// must therefore hand this an `FlUrl` built **without** auto-creating the table
/// ([`super::fl_url_factory::FlUrlFactory::get_fl_url_without_auto_create_table`]), or the
/// table would exist by the time it is counted and `None` could never be returned.
pub async fn get_rows_count(
    flurl: WriterFlUrl,
    table_name: &str,
    partition_key: Option<&str>,
) -> Result<Option<usize>, DataWriterError> {
    let mut request = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(COUNT_SEGMENT)
        .with_table_name_as_query_param(table_name);

    if let Some(partition_key) = partition_key {
        request = request.with_partition_key_as_query_param(partition_key);
    }

    let mut response = request.get().await?;

    if is_table_not_found(&mut response).await? {
        return Ok(None);
    }

    check_error(&mut response).await?;

    // Everything check_error lets through which is not a 2xx - 503 while the server is still
    // loading, a 5xx - is a failure to answer, not an answer of "no such table". Reporting it
    // as `None` would tell the caller the table is gone.
    if !is_ok_result(&response) {
        return unexpected_response(&mut response, "Rows count", table_name).await;
    }

    let rows_count = parse_rows_count(response.get_body_as_slice().await?)?;

    Ok(Some(rows_count))
}

pub async fn get_enum_case_models_by_partition_key<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
    TResult: MyNoSqlEntity
        + my_no_sql_abstractions::GetMyNoSqlEntitiesByPartitionKey
        + From<TEntity>
        + Sync
        + Send
        + 'static,
>(
    flurl: WriterFlUrl,
    update_read_statistics: Option<&UpdateReadStatistics>,
) -> Result<Option<Vec<TResult>>, DataWriterError> {
    let result: Option<Vec<TEntity>> =
        get_by_partition_key(flurl, TResult::PARTITION_KEY, update_read_statistics).await?;

    match result {
        Some(entities) => {
            let mut result = Vec::with_capacity(entities.len());

            for entity in entities {
                result.push(entity.into());
            }

            Ok(Some(result))
        }
        None => Ok(None),
    }
}

pub async fn get_enum_case_model<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
    TResult: MyNoSqlEntity
        + From<TEntity>
        + my_no_sql_abstractions::GetMyNoSqlEntity
        + Sync
        + Send
        + 'static,
>(
    flurl: WriterFlUrl,
    update_read_statistics: Option<&UpdateReadStatistics>,
) -> Result<Option<TResult>, DataWriterError> {
    let entity: Option<TEntity> = get_entity(
        flurl,
        TResult::PARTITION_KEY,
        TResult::ROW_KEY,
        update_read_statistics,
    )
    .await?;

    match entity {
        Some(entity) => Ok(Some(entity.into())),
        None => Ok(None),
    }
}

/// GET /api/Row by the row key - reads the rows which have that key, from every partition.
///
/// Read the way [`get_by_partition_key`] is: a 2xx is `Ok(Some(entities))` - `Ok(Some(vec![]))`
/// when no partition holds such a row - and the `Option` is always `Some`.
pub async fn get_by_row_key<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: WriterFlUrl,
    row_key: &str,
) -> Result<Option<Vec<TEntity>>, DataWriterError> {
    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(ROW_CONTROLLER)
        .with_row_key_as_query_param(row_key)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .get()
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        let entities = deserialize_entities(response.get_body_as_slice().await?)?;
        return Ok(Some(entities));
    }

    unexpected_response(&mut response, "get_by_row_key", TEntity::TABLE_NAME).await
}

/// GET /api/Partitions - the partition keys of the table, `skip` / `limit` applied.
///
/// A table which holds no partitions is `Ok(vec![])`. Nothing else is: an answer which is not a
/// 2xx is an error - the typed one of [`check_error`] for a 400 (which is how the server
/// reports a missing table), [`DataWriterError::TableNotFound`] for a 404 which carries the
/// same `TableNotFound` contract, [`unexpected_response`] for the rest (the 404 of a route
/// nobody serves included) - and never an empty list, which would read as "the table is
/// empty".
pub async fn get_partition_keys(
    flurl: WriterFlUrl,
    table_name: &str,
    skip: Option<i32>,
    limit: Option<i32>,
) -> Result<Vec<String>, DataWriterError> {
    #[derive(Serialize, Deserialize)]
    pub struct GetPartitionsJsonResult {
        pub amount: usize,
        pub data: Vec<String>,
    }
    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(PARTITIONS_CONTROLLER)
        .with_table_name_as_query_param(table_name)
        .with_skip_as_query_param(skip)
        .with_limit_as_query_param(limit)
        .get()
        .await?;

    // A 404 says "there is no such table" only when it carries the `TableNotFound` contract of
    // the server - the rule of `is_table_not_found`. Any other 404 is a route nobody serves or
    // a proxy answering in the server's place: it says nothing about the table and is left to
    // `unexpected_response` below.
    if response.get_status_code() == 404 && is_table_not_found(&mut response).await? {
        return Err(DataWriterError::TableNotFound(table_name.to_string()));
    }

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        let result: Result<GetPartitionsJsonResult, _> =
            serde_json::from_slice(response.get_body_as_slice().await?);
        match result {
            Ok(result) => return Ok(result.data),
            Err(err) => {
                return Err(DataWriterError::Error(format!(
                    "Failed to deserialize: {:?}",
                    err
                )))
            }
        }
    }

    unexpected_response(&mut response, "get_partition_keys", table_name).await
}

pub async fn delete_enum_case<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
    TResult: MyNoSqlEntity
        + From<TEntity>
        + my_no_sql_abstractions::GetMyNoSqlEntity
        + Sync
        + Send
        + 'static,
>(
    flurl: WriterFlUrl,
    sync_period: &DataSynchronizationPeriod,
) -> Result<Option<TResult>, DataWriterError> {
    let entity: Option<TEntity> =
        delete_row(flurl, TResult::PARTITION_KEY, TResult::ROW_KEY, sync_period).await?;

    match entity {
        Some(entity) => Ok(Some(entity.into())),
        None => Ok(None),
    }
}

pub async fn delete_enum_case_with_row_key<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
    TResult: MyNoSqlEntity
        + From<TEntity>
        + my_no_sql_abstractions::GetMyNoSqlEntitiesByPartitionKey
        + Sync
        + Send
        + 'static,
>(
    flurl: WriterFlUrl,
    row_key: &str,
    sync_period: &DataSynchronizationPeriod,
) -> Result<Option<TResult>, DataWriterError> {
    let entity: Option<TEntity> =
        delete_row(flurl, TResult::PARTITION_KEY, row_key, sync_period).await?;

    match entity {
        Some(entity) => Ok(Some(entity.into())),
        None => Ok(None),
    }
}

/// DELETE /api/Row - deletes the row whatever its version is; the deleted row comes back in
/// the body.
///
/// 200 → `Ok(Some(deleted))`; any other 2xx → `Ok(None)`: the row, or its whole partition, was
/// not there, which the server answers with a 204 and no body; 400 → the typed error of
/// [`check_error`]. Any other answer - a 404 too, which the API never gives to this request -
/// is an error, see [`unexpected_response`].
pub async fn delete_row<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: WriterFlUrl,
    partition_key: &str,
    row_key: &str,
    sync_period: &DataSynchronizationPeriod,
) -> Result<Option<TEntity>, DataWriterError> {
    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(ROW_CONTROLLER)
        .append_data_sync_period(sync_period)
        .with_partition_key_as_query_param(partition_key)
        .with_row_key_as_query_param(row_key)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .delete()
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) && response.get_status_code() == 200 {
        let entity = read_entity(&mut response, "delete_row").await?;
        return Ok(Some(entity));
    }

    if is_ok_result(&response) {
        return Ok(None);
    }

    unexpected_response(&mut response, "delete_row", TEntity::TABLE_NAME).await
}

/// DELETE /api/Row/DeleteIf — optimistic-concurrency delete: the row goes away only while
/// the `TimeStamp` stored in the table is still `time_stamp`, and the deleted row comes back
/// in the body.
///
/// Same codes as [`replace_entity`]: 200 → the row was deleted → `Ok(Some(entity))`;
/// `404 Record not found` → there is no such row → `Ok(None)`; 409 → the row is there but at
/// another version, somebody rewrote it between the read and this call →
/// [`DataWriterError::RecordIsChanged`]; 400 → the table is missing, or `time_stamp` is not
/// a readable `TimeStamp` (a default one trips the `debug_assert!` below in a debug build).
/// Any other answer says nothing about the row - not that it is gone, not that it is still
/// there - and is an error, see [`unexpected_response`].
///
/// Use it as read → decide → delete exactly the version that was read. On a conflict re-read
/// and decide again — the row may no longer be one you want to delete.
pub async fn delete_row_if<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: WriterFlUrl,
    partition_key: &str,
    row_key: &str,
    time_stamp: Timestamp,
    sync_period: &DataSynchronizationPeriod,
) -> Result<Option<TEntity>, DataWriterError> {
    debug_assert!(
        !time_stamp.is_default(),
        "DeleteIf matches against the version the row was read at; a default Timestamp is \
         not a version the server can read and it answers HTTP 400"
    );

    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(ROW_CONTROLLER)
        .append_path_segment("DeleteIf")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .with_partition_key_as_query_param(partition_key)
        .with_row_key_as_query_param(row_key)
        .append_query_param("timeStamp", Some(time_stamp.to_string()))
        .delete()
        .await?;

    if is_record_not_found(&mut response).await? {
        return Ok(None);
    }

    // Handles 400 (deserialize_error) and 409 (RecordIsChanged).
    check_error(&mut response).await?;

    if is_ok_result(&response) && response.get_status_code() == 200 {
        let entity = read_entity(&mut response, "delete_row_if").await?;
        return Ok(Some(entity));
    }

    unexpected_response(&mut response, "delete_row_if", TEntity::TABLE_NAME).await
}

/// DELETE /api/Rows/DeletePartitions - deletes whole partitions, the keys travelling as the
/// repeated `partitionKeys` query parameter. A key which is not in the table is skipped: the
/// answer does not say which partitions were there.
///
/// 2xx (the server answers 204) → `Ok(())`; 400 → the typed error of [`check_error`]. Any
/// other answer - a 404 of a server which does not serve this route included - is an error, see
/// [`unexpected_response`]: nothing was deleted. An empty slice is a no-op (no request at all),
/// like in [`bulk_delete`].
///
/// This is one request: the keys have to fit into its head. The callers hand it one list of
/// [`split_partition_keys`] at a time.
pub async fn delete_partitions(
    flurl: WriterFlUrl,
    table_name: &str,
    partition_keys: &[&str],
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    if partition_keys.is_empty() {
        return Ok(());
    }

    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(ROWS_CONTROLLER)
        .append_path_segment("DeletePartitions")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(table_name)
        .with_partition_keys_as_query_param(partition_keys)
        .delete()
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(&mut response, "delete_partitions", table_name).await
}

/// How many bytes of `partitionKeys=..` pairs go into one `DeletePartitions` request.
///
/// The keys travel in the query string, and the head of a request has a size limit which is
/// not the API's to lift: over HTTP/2 - which the writer speaks by default - the server
/// refuses a head of more than about 16 KB with a 431, the HTTP client itself refuses an url
/// of more than 64 KB, and a proxy in front of the server may take less than either. A list
/// which does not fit is sent as several requests instead of failing as a whole.
const DELETE_PARTITIONS_QUERY_BUDGET: usize = 4 * 1024;

/// Splits the keys of a `delete_partitions` call into the lists of the single requests - see
/// [`DELETE_PARTITIONS_QUERY_BUDGET`]. The keys keep their order and every key goes out once; a
/// key which is over the budget on its own still gets a request of its own. An empty slice
/// gives no request at all.
pub(crate) fn split_partition_keys<'s, 'k>(partition_keys: &'s [&'k str]) -> Vec<&'s [&'k str]> {
    let mut result = Vec::new();
    let mut from = 0;
    let mut spent = 0;

    for (index, partition_key) in partition_keys.iter().enumerate() {
        let cost = partition_key_query_pair_size(partition_key);

        if index > from && spent + cost > DELETE_PARTITIONS_QUERY_BUDGET {
            result.push(&partition_keys[from..index]);
            from = index;
            spent = 0;
        }

        spent += cost;
    }

    if from < partition_keys.len() {
        result.push(&partition_keys[from..]);
    }

    result
}

/// The most a `&partitionKeys=<key>` pair can take in the query string: a byte which every url
/// encoder leaves as it is counts as one, any other byte as the three of its `%XX` escape.
fn partition_key_query_pair_size(partition_key: &str) -> usize {
    const PAIR_OVERHEAD: usize = "&partitionKeys=".len();

    let key_size: usize = partition_key
        .bytes()
        .map(|b| match b {
            b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'-' | b'_' | b'.' => 1,
            _ => 3,
        })
        .sum();

    PAIR_OVERHEAD + key_size
}

/// GET /Row with nothing but the table name - reads every row of the table.
///
/// Read the way [`get_by_partition_key`] is: a 2xx is `Ok(Some(entities))` - `Ok(Some(vec![]))`
/// for a table which holds no rows - and the `Option` is always `Some`.
pub async fn get_all<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: WriterFlUrl,
) -> Result<Option<Vec<TEntity>>, DataWriterError> {
    let mut response = flurl
        .append_path_segment(ROW_CONTROLLER)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .get()
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        let entities = deserialize_entities(response.get_body_as_slice().await?)?;
        return Ok(Some(entities));
    }

    unexpected_response(&mut response, "get_all", TEntity::TABLE_NAME).await
}

/// POST /Bulk/CleanAndBulkInsert — **transactionally replaces the whole table** with
/// `entities`. The clean and the insert are one server-side operation, published to
/// subscribers as a single `InitTable` packet which the reader applies under one lock, so the
/// table is never observed empty or half-filled: a concurrent read sees either the entire
/// previous snapshot or the entire new one. That atomic swap is the reason this endpoint
/// exists — a `DeletePartitions` + `BulkInsertOrReplace` pair does not give it.
pub async fn clean_table_and_bulk_insert<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    entities: &[TEntity],
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let mut response = flurl
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("CleanAndBulkInsert")
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .append_data_sync_period(sync_period)
        .post(serialize_entities_to_body(entities))
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "clean_table_and_bulk_insert",
        TEntity::TABLE_NAME,
    )
    .await
}

/// [`clean_table_and_bulk_insert`] scoped to one partition (`partitionKey` query param): the
/// partition is **transactionally replaced** with `entities` and the rest of the table is left
/// untouched. Published as a single `InitPartition` packet and applied by the reader under one
/// lock — the partition is never observed empty or half-filled.
pub async fn clean_partition_and_bulk_insert<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    partition_key: &str,
    entities: &[TEntity],
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let mut response = flurl
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("CleanAndBulkInsert")
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .append_data_sync_period(sync_period)
        .with_partition_key_as_query_param(partition_key)
        .post(serialize_entities_to_body(entities))
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "clean_partition_and_bulk_insert",
        TEntity::TABLE_NAME,
    )
    .await
}

/// [`clean_table_and_bulk_insert`] with `useTimestamp=true`: the whole table is cleaned and
/// re-inserted in the same transactional swap (never observed empty), but each row keeps its
/// **own `TimeStamp`** instead of the server clock.
/// Every entity must carry a real (non-default) `TimeStamp`, otherwise the server rejects
/// the request with HTTP 400 (a `debug_assert!` panic in a debug build). (An empty slice
/// still cleans the table — nothing to insert.)
pub async fn clean_table_and_bulk_insert_with_own_timestamp<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    entities: &[TEntity],
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    debug_assert!(
        entities.iter().all(|e| !e.get_time_stamp().is_default()),
        "clean_table_and_bulk_insert_with_own_timestamp requires every entity to carry its \
         own (non-default) TimeStamp; a default one → HTTP 400"
    );

    let mut response = flurl
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("CleanAndBulkInsert")
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .append_data_sync_period(sync_period)
        .append_query_param("useTimestamp", Some("true"))
        .post(serialize_entities_to_body(entities))
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "clean_table_and_bulk_insert_with_own_timestamp",
        TEntity::TABLE_NAME,
    )
    .await
}

/// [`clean_partition_and_bulk_insert`] with `useTimestamp=true`: the partition is cleaned and
/// re-inserted in the same transactional swap (never observed empty), but each row keeps its
/// **own `TimeStamp`**. Every entity must carry a real
/// (non-default) `TimeStamp`, otherwise the server rejects the request with HTTP 400 (a
/// `debug_assert!` panic in a debug build).
pub async fn clean_partition_and_bulk_insert_with_own_timestamp<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    partition_key: &str,
    entities: &[TEntity],
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    debug_assert!(
        entities.iter().all(|e| !e.get_time_stamp().is_default()),
        "clean_partition_and_bulk_insert_with_own_timestamp requires every entity to carry its \
         own (non-default) TimeStamp; a default one → HTTP 400"
    );

    let mut response = flurl
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("CleanAndBulkInsert")
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .append_data_sync_period(sync_period)
        .with_partition_key_as_query_param(partition_key)
        .append_query_param("useTimestamp", Some("true"))
        .post(serialize_entities_to_body(entities))
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "clean_partition_and_bulk_insert_with_own_timestamp",
        TEntity::TABLE_NAME,
    )
    .await
}

/// POST /api/Row/InsertOrReplaceIfNew — inserts a missing row, or replaces the stored
/// one only when the incoming `TimeStamp` is strictly greater.
///
/// Unlike the plain writes (and like the `useTimestamp=true` ones), the server does NOT stamp
/// its own time here: the client's `TimeStamp` is the object's version and is mandatory. A
/// default (unset) `Timestamp` is left out of the JSON by the entity macros, and the server
/// answers HTTP 400 (a `debug_assert!` panic in a debug build). The caller must set
/// `entity.time_stamp` to a real value (e.g. `DateTimeAsMicroseconds::now().into()`).
///
/// Read the way [`insert_or_replace_entity`] is.
pub async fn insert_or_replace_entity_if_new<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    entity: &TEntity,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    debug_assert!(
        !entity.get_time_stamp().is_default(),
        "InsertOrReplaceIfNew requires the entity to carry its own (non-default) TimeStamp; \
         a default one is left out of the JSON by the entity macros and the server rejects \
         the request with HTTP 400"
    );

    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(ROW_CONTROLLER)
        .append_path_segment("InsertOrReplaceIfNew")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(HttpRequestBody::Json(entity.serialize_entity()))
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "insert_or_replace_entity_if_new",
        TEntity::TABLE_NAME,
    )
    .await
}

/// POST /api/Bulk/InsertOrReplaceIfNew — same rule as [`insert_or_replace_entity_if_new`]
/// applied per row. Every entity must carry its own non-default `TimeStamp`. An empty
/// slice is a no-op (early `Ok(())`, like `bulk_insert_or_replace`).
pub async fn bulk_insert_or_replace_if_new<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    entities: &[TEntity],
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    if entities.is_empty() {
        return Ok(());
    }

    debug_assert!(
        entities.iter().all(|e| !e.get_time_stamp().is_default()),
        "InsertOrReplaceIfNew requires every entity to carry its own (non-default) TimeStamp; \
         a default one is left out of the JSON by the entity macros and the server rejects \
         the request with HTTP 400"
    );

    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("InsertOrReplaceIfNew")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(serialize_entities_to_body(entities))
        .await?;

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "bulk_insert_or_replace_if_new",
        TEntity::TABLE_NAME,
    )
    .await
}

/// POST /api/Bulk/InsertOrReplaceIfNewByChunks — uploads one chunk of rows aside from the
/// table. Pass `process_id = None` to start a new process (the server issues an id and
/// returns it in `{ "processId": ".." }`); pass the previously issued id to append a
/// further chunk. Either way the (echoed) process id is returned. Nothing is applied
/// until the commit. Each row must carry its own non-default `TimeStamp`.
///
/// 400 → the typed error of [`check_error`]; any other answer which is not a 2xx - the 404 of
/// a process the server does not know (any more) and the 409 of a process which belongs to
/// another writer session included - is an error, see [`unexpected_response`] and
/// [`check_error_of_a_bulk_process`].
pub async fn insert_or_replace_if_new_by_chunks_upload<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    entities: &[TEntity],
    process_id: Option<&str>,
) -> Result<String, DataWriterError> {
    debug_assert!(
        entities.iter().all(|e| !e.get_time_stamp().is_default()),
        "InsertOrReplaceIfNew requires every entity to carry its own (non-default) TimeStamp; \
         a default one is left out of the JSON by the entity macros and the server rejects \
         the chunk with HTTP 400"
    );

    let mut request = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("InsertOrReplaceIfNewByChunks")
        .with_table_name_as_query_param(TEntity::TABLE_NAME);

    if let Some(process_id) = process_id {
        request = request.append_query_param("processId", Some(process_id));
    }

    let mut response = request.post(serialize_entities_to_body(entities)).await?;

    check_error_of_a_bulk_process(&mut response).await?;

    if !is_ok_result(&response) {
        // The writer makes this request under two names: the chunk which starts a process and
        // the chunks which follow it.
        let operation = match process_id {
            Some(_) => "insert_or_replace_if_new_by_chunks_append",
            None => "insert_or_replace_if_new_by_chunks_start",
        };

        return unexpected_response(&mut response, operation, TEntity::TABLE_NAME).await;
    }

    let body = response.get_body_as_slice().await?;

    let contract: BulkProcessResponseContract =
        serde_json::from_slice(body).map_err(|err| {
            DataWriterError::Error(format!(
                "Failed to deserialize BulkProcessResponse: {:?}",
                err
            ))
        })?;

    Ok(contract.process_id)
}

/// POST /api/Bulk/InsertOrReplaceIfNewByChunksCommit — applies all accumulated chunks in
/// one operation (insert when missing, replace only when the row's `TimeStamp` is greater).
///
/// Read the way [`insert_or_replace_if_new_by_chunks_upload`] is. `table_name` is only what an
/// error names: the request carries the process and nothing else.
pub async fn insert_or_replace_if_new_by_chunks_commit(
    flurl: WriterFlUrl,
    table_name: &str,
    process_id: &str,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("InsertOrReplaceIfNewByChunksCommit")
        .append_query_param("processId", Some(process_id))
        .append_data_sync_period(sync_period)
        .post(HttpRequestBody::Empty)
        .await?;

    check_error_of_a_bulk_process(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "insert_or_replace_if_new_by_chunks_commit",
        table_name,
    )
    .await
}

/// POST /api/Bulk/InsertOrReplaceIfNewByChunksCancel — drops the accumulated chunks. The
/// table is not touched.
///
/// Read the way [`insert_or_replace_if_new_by_chunks_commit`] is.
pub async fn insert_or_replace_if_new_by_chunks_cancel(
    flurl: WriterFlUrl,
    table_name: &str,
    process_id: &str,
) -> Result<(), DataWriterError> {
    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("InsertOrReplaceIfNewByChunksCancel")
        .append_query_param("processId", Some(process_id))
        .post(HttpRequestBody::Empty)
        .await?;

    check_error_of_a_bulk_process(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "insert_or_replace_if_new_by_chunks_cancel",
        table_name,
    )
    .await
}

/// POST /api/Bulk/CleanAndBulkInsertByChunks with `useTimestamp=true` — uploads one chunk of
/// a clean-and-bulk-insert process that keeps each row's own `TimeStamp`. `process_id = None`
/// starts a new process (the server issues the id and returns it); `partition_key` is honored
/// only on that first (start) chunk and scopes the clean to that partition (`None` = whole
/// table). Pass the issued id on every following chunk. Nothing is applied until the commit —
/// the chunks sit in a server-side accumulator while readers keep being served the current
/// snapshot. Each row must carry a non-default `TimeStamp`.
///
/// Read the way [`insert_or_replace_if_new_by_chunks_upload`] is.
pub async fn clean_and_bulk_insert_by_chunks_with_own_timestamp_upload<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: WriterFlUrl,
    entities: &[TEntity],
    partition_key: Option<&str>,
    process_id: Option<&str>,
) -> Result<String, DataWriterError> {
    debug_assert!(
        entities.iter().all(|e| !e.get_time_stamp().is_default()),
        "clean_and_bulk_insert_by_chunks_with_own_timestamp requires every entity to carry its \
         own (non-default) TimeStamp; a default one → the chunk is rejected with HTTP 400"
    );

    let mut request = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("CleanAndBulkInsertByChunks")
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .append_query_param("useTimestamp", Some("true"));

    // partitionKey is taken into account only when a new process is started.
    if let Some(partition_key) = partition_key {
        request = request.with_partition_key_as_query_param(partition_key);
    }

    if let Some(process_id) = process_id {
        request = request.append_query_param("processId", Some(process_id));
    }

    let mut response = request.post(serialize_entities_to_body(entities)).await?;

    check_error_of_a_bulk_process(&mut response).await?;

    if !is_ok_result(&response) {
        // The writer makes this request under two names: the chunk which starts a process and
        // the chunks which follow it.
        let operation = match process_id {
            Some(_) => "clean_and_bulk_insert_by_chunks_with_own_timestamp_append",
            None => "clean_and_bulk_insert_by_chunks_with_own_timestamp_start",
        };

        return unexpected_response(&mut response, operation, TEntity::TABLE_NAME).await;
    }

    let body = response.get_body_as_slice().await?;

    let contract: BulkProcessResponseContract = serde_json::from_slice(body).map_err(|err| {
        DataWriterError::Error(format!(
            "Failed to deserialize BulkProcessResponse: {:?}",
            err
        ))
    })?;

    Ok(contract.process_id)
}

/// POST /api/Bulk/CleanAndBulkInsertByChunksCommit — cleans the table (or the partition the
/// process was started with) and inserts every accumulated row atomically. This is where the
/// uploaded chunks become visible, all at once: readers get a single `InitTable` /
/// `InitPartition` snapshot swap, so the table/partition is never observed empty or partially
/// uploaded, no matter how many chunks the process took.
///
/// Read the way [`insert_or_replace_if_new_by_chunks_commit`] is, `table_name` included. An
/// error names the call the writer makes it under -
/// `clean_and_bulk_insert_by_chunks_with_own_timestamp_commit`.
pub async fn clean_and_bulk_insert_by_chunks_commit(
    flurl: WriterFlUrl,
    table_name: &str,
    process_id: &str,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("CleanAndBulkInsertByChunksCommit")
        .append_query_param("processId", Some(process_id))
        .append_data_sync_period(sync_period)
        .post(HttpRequestBody::Empty)
        .await?;

    check_error_of_a_bulk_process(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "clean_and_bulk_insert_by_chunks_with_own_timestamp_commit",
        table_name,
    )
    .await
}

/// POST /api/Bulk/CleanAndBulkInsertByChunksCancel — drops the accumulated chunks. The table
/// is not touched.
///
/// Read the way [`clean_and_bulk_insert_by_chunks_commit`] is.
pub async fn clean_and_bulk_insert_by_chunks_cancel(
    flurl: WriterFlUrl,
    table_name: &str,
    process_id: &str,
) -> Result<(), DataWriterError> {
    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("CleanAndBulkInsertByChunksCancel")
        .append_query_param("processId", Some(process_id))
        .post(HttpRequestBody::Empty)
        .await?;

    check_error_of_a_bulk_process(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    unexpected_response(
        &mut response,
        "clean_and_bulk_insert_by_chunks_with_own_timestamp_cancel",
        table_name,
    )
    .await
}

#[derive(Deserialize)]
struct BulkProcessResponseContract {
    #[serde(rename = "processId")]
    process_id: String,
}

/// A 2xx - unless it is a page of html. The API answers none of its calls with html, while a
/// route nobody serves is answered with exactly that: the deployed server hands out the page of
/// its UI - a `200 text/html` - for every path it has no action for, and so does many a proxy.
/// That is how `delete_partitions` asked for a route which did not exist and was told "done"
/// for years. Such a 2xx is not a success of any call; it is left to [`unexpected_response`]
/// like every other answer a call has no meaning for.
fn is_ok_result(response: &FlUrlResponse) -> bool {
    response.get_status_code() >= 200 && response.get_status_code() < 300 && !is_html(response)
}

fn is_html(response: &FlUrlResponse) -> bool {
    match response.get_header("content-type") {
        Ok(Some(content_type)) => is_html_content_type(content_type),
        // No `Content-Type` at all - a 204 has none - or one which can not be read as text.
        _ => false,
    }
}

/// `text/html` in any case and with whatever parameters follow it - `text/html; charset=utf-8`.
fn is_html_content_type(content_type: &str) -> bool {
    let media_type = match content_type.split_once(';') {
        Some((media_type, _)) => media_type,
        None => content_type,
    };

    media_type.trim().eq_ignore_ascii_case("text/html")
}

fn serialize_entities_to_body<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer>(
    entities: &[TEntity],
) -> HttpRequestBody {
    if entities.len() == 0 {
        return HttpRequestBody::Json(vec![b'[', b']']);
    }

    let mut json_array_writer = JsonArrayWriter::new();

    for entity in entities {
        let payload = entity.serialize_entity();
        let payload: RawJsonObject = payload.into();
        json_array_writer = json_array_writer.write(payload);
    }

    HttpRequestBody::Json(json_array_writer.build().into_bytes())
}

/// [`check_error`] for the routes of a chunked flow. They do answer 409, and with a meaning of
/// their own - `Bulk process <id> does not match: <reason>`: the process belongs to another
/// writer session, another table or another namespace. That is not a row somebody has
/// rewritten, so it is not [`DataWriterError::RecordIsChanged`]: it is left to
/// [`unexpected_response`], which names the call and carries what the server said.
async fn check_error_of_a_bulk_process(
    response: &mut WriterFlUrlResponse,
) -> Result<(), DataWriterError> {
    if response.get_status_code() == 409 {
        return Ok(());
    }

    check_error(response).await
}

async fn check_error(response: &mut WriterFlUrlResponse) -> Result<(), DataWriterError> {
    let result = match response.get_status_code() {
        400 => Err(deserialize_error(response).await?),

        // 409 is an optimistic-concurrency conflict ("Record is changed"), not a missing
        // table. The body is a plain-text message, not an OperationFailHttpContract.
        409 => {
            let body = response.get_body_as_slice().await?;
            Err(DataWriterError::RecordIsChanged(
                String::from_utf8_lossy(body).to_string(),
            ))
        }
        _ => Ok(()),
    };

    if let Err(err) = &result {
        if !is_expected_outcome(err) {
            my_logger::LOGGER.write_error(
                format!("FlUrlRequest to {}", response.url.to_string()),
                format!("{:?}", err),
                None.into(),
            );
        }
    }

    result
}

/// Errors which are the API answering normally rather than something going wrong. They are
/// carried to the caller to act on, and are deliberately not written to the log - a routine
/// outcome must not look like a failure of the service which is using the writer.
///
/// [`DataWriterError::RecordIsChanged`] is exactly that: `DeleteIf` / `Replace` answer 409
/// whenever the row was rewritten between the read and the write, which is what the
/// optimistic-concurrency protocol is for. The caller re-reads and decides again, and
/// `update_entity` even retries it in a loop - logging it once per attempt turned an ordinary
/// conflict into a stream of errors in the console.
///
/// [`DataWriterError::RecordNotFound`] is the same kind of answer ("it is not there any
/// more"); `check_error` never produces it - it turns only 400 and 409 into errors, and the
/// callers which report a missing row map 404 themselves - so it is listed here to keep the
/// rule in one place rather than because a 404 reaches this function.
///
/// [`DataWriterError::RecordAlreadyExists`] joined them when `Insert` started reporting it
/// typed: it is the answer `insert_or_update` expects whenever another writer created the same
/// key first, and under contention that happens on a normal path, once per lost race.
fn is_expected_outcome(err: &DataWriterError) -> bool {
    match err {
        DataWriterError::RecordIsChanged(_)
        | DataWriterError::RecordNotFound(_)
        | DataWriterError::RecordAlreadyExists(_) => true,
        _ => false,
    }
}

/// "There is no such table" is the one error which is an answer rather than a failure to a
/// counter, so [`get_rows_count`] peels it off before [`check_error`] - which would both turn
/// it into an error and log it, once per call, about the very absence being asked about. It is
/// done here rather than by adding [`DataWriterError::TableNotFound`] to
/// [`is_expected_outcome`], which would silence it for every write path too, where a missing
/// table really is a failure worth the log line.
///
/// **The status code alone never decides it - the body has to carry the `TableNotFound`
/// contract.** This server says "no such table" with 400 plus that contract
/// (`OPERATION_FAIL_HTTP_STATUS_CODE`), and answers a bare 404 for something else entirely: a
/// reverse proxy which does not forward `/api/Count`, or a server predating the `/api` prefix.
/// Reading such a 404 as "the table is gone" would have a reconciler rebuild a table which is
/// present and full, so a body which is not the contract falls through to be reported as the
/// failure it is. 404 is accepted next to 400 only under that condition, so this keeps working
/// if the server ever moves the status.
async fn is_table_not_found(response: &mut WriterFlUrlResponse) -> Result<bool, DataWriterError> {
    match response.get_status_code() {
        400 | 404 => match deserialize_error(response).await {
            Ok(DataWriterError::TableNotFound(_)) => Ok(true),
            // Some other error the server named: hand it back to `check_error` to report and
            // log the usual way.
            Ok(_) => Ok(false),
            // The body could not be read off the wire at all. That must surface as itself - a
            // second read of a consumed body reports something else entirely.
            Err(err @ DataWriterError::FlUrlError(_)) => Err(err),
            // A body which could not even be read as utf8. It is not an answer about the
            // table either, so it falls through the same way. (A body which is simply not the
            // contract - a proxy's "404 - Not Found", a plain-text validation message - is not
            // here: `deserialize_error` hands it back as `Ok(Error(<body>))`, caught above.)
            Err(_) => Ok(false),
        },
        _ => Ok(false),
    }
}

/// The body of the 404 a missing row is answered with - by the main server and by a node alike.
const RECORD_NOT_FOUND_BODY: &str = "Record not found";

/// "There is no such row" is an answer, and a 404 is how the API gives it to a read by both
/// keys, to `Replace` and to `DeleteIf`: the row - or its whole partition - is not in the
/// table. [`get_entity`] and [`delete_row_if`] turn it into `Ok(None)`, [`replace_entity`] into
/// [`DataWriterError::RecordNotFound`].
///
/// **The status code alone never decides it - the body has to be the one the API sends.** A
/// 404 is also what comes back when nobody serves the route: a server which does not have it
/// (`DeleteIf` is much younger than the rest of the api), or a proxy answering in the server's
/// place. That says nothing about the row, and reading it as "not there" would report a row as
/// missing - or as already deleted - while it sits in the table, and would have
/// `insert_or_update` take it for a race it has lost and read again, attempt after attempt.
/// Such a 404 is left to [`unexpected_response`], which makes an error of it. The same rule as
/// in [`is_table_not_found`].
async fn is_record_not_found(response: &mut WriterFlUrlResponse) -> Result<bool, DataWriterError> {
    if response.get_status_code() != 404 {
        return Ok(false);
    }

    Ok(is_record_not_found_body(
        response.get_body_as_slice().await?,
    ))
}

/// Trimmed for the reason [`parse_rows_count`] trims: the body is plain text, free to carry
/// trailing whitespace.
fn is_record_not_found_body(body: &[u8]) -> bool {
    match std::str::from_utf8(body) {
        Ok(body) => body.trim() == RECORD_NOT_FOUND_BODY,
        Err(_) => false,
    }
}

/// Where a function ends up when the status of the answer is none of those it has a meaning
/// for. 503 while the server is still loading its tables, a 5xx, a 404 of a route which is not
/// served, whatever a proxy says in the server's place - none of them is the API answering the
/// request, so none of them may be read as "there is nothing there" or as "done". A caller told
/// `Ok(None)` would treat a row which is in the table as missing, and one told `Ok(())` would
/// believe a snapshot was replaced which was not touched.
///
/// The error names the status and carries the body - that is all there is to tell a server
/// which is still starting from a route which does not exist. `operation` and `table_name` say
/// which call it was. A long body is cut, see [`BODY_IN_ERROR_LIMIT`].
async fn unexpected_response<TResult>(
    response: &mut WriterFlUrlResponse,
    operation: &str,
    table_name: &str,
) -> Result<TResult, DataWriterError> {
    let status_code = response.get_status_code();
    let body = response.get_body_as_slice().await?;

    Err(unexpected_status(operation, table_name, status_code, body))
}

/// How much of a body the message of [`unexpected_status`] carries. What the API says about a
/// call it did not serve is a line of text; anything longer is somebody else's - the page of a
/// UI, of a proxy - and can be of any size, while the message ends up in a log line.
const BODY_IN_ERROR_LIMIT: usize = 1024;

fn unexpected_status(
    operation: &str,
    table_name: &str,
    status_code: u16,
    body: &[u8],
) -> DataWriterError {
    if body.len() <= BODY_IN_ERROR_LIMIT {
        return DataWriterError::Error(format!(
            "{} of table {} returned status code {}. Body: {}",
            operation,
            table_name,
            status_code,
            String::from_utf8_lossy(body)
        ));
    }

    let shown = cut_on_char_boundary(body, BODY_IN_ERROR_LIMIT);

    DataWriterError::Error(format!(
        "{} of table {} returned status code {}. Body of {} bytes, cut to the first {}: {}",
        operation,
        table_name,
        status_code,
        body.len(),
        shown.len(),
        String::from_utf8_lossy(shown)
    ))
}

/// The first `limit` bytes of a body which is longer than that - less the bytes of a character
/// the cut would go through.
fn cut_on_char_boundary(body: &[u8], limit: usize) -> &[u8] {
    let cut = &body[..limit];

    match std::str::from_utf8(cut) {
        // The cut went through a character: the text ends where that character starts.
        Err(err) if err.error_len().is_none() => &cut[..err.valid_up_to()],
        // Text which ends on a character - or not text at all, which is shown the way a body
        // which is not text always is.
        _ => cut,
    }
}

/// The body of `/api/Count` is the number and nothing else (the server writes it with
/// `HttpOutput::as_text`), so it is parsed rather than deserialized. Trimmed because a
/// text/plain body is free to carry trailing whitespace.
fn parse_rows_count(body: &[u8]) -> Result<usize, DataWriterError> {
    let body = std::str::from_utf8(body)?;

    match body.trim().parse() {
        Ok(rows_count) => Ok(rows_count),
        Err(_) => Err(DataWriterError::Error(format!(
            "Rows count endpoint returned '{}' which is not a number",
            body
        ))),
    }
}

async fn deserialize_error(
    response: &mut WriterFlUrlResponse,
) -> Result<DataWriterError, DataWriterError> {
    let body = response.get_body_as_slice().await?;

    let body_as_str = std::str::from_utf8(body)?;

    let result = match serde_json::from_str::<OperationFailHttpContract>(body_as_str) {
        Ok(fail_contract) => match fail_contract.reason.as_str() {
            "TableAlreadyExists" => DataWriterError::TableAlreadyExists(fail_contract.message),
            "TableNotFound" => DataWriterError::TableNotFound(fail_contract.message),
            "RecordAlreadyExists" => DataWriterError::RecordAlreadyExists(fail_contract.message),
            "RequiredEntityFieldIsMissing" => {
                DataWriterError::RequiredEntityFieldIsMissing(fail_contract.message)
            }
            "JsonParseFail" => DataWriterError::ServerCouldNotParseJson(fail_contract.message),
            _ => DataWriterError::Error(format!("Not supported error. {:?}", fail_contract)),
        },
        // Not the error contract at all (a plain-text 400 from the HTTP layer, say). The body
        // is the whole diagnostic there, so it is carried through as-is - a parser complaint
        // in its place would throw away the only thing that says what went wrong.
        Err(_) => DataWriterError::Error(body_as_str.to_string()),
    };

    Ok(result)
}

fn deserialize_entities<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer>(
    src: &[u8],
) -> Result<Vec<TEntity>, DataWriterError> {
    let mut result = Vec::new();

    let json_array_iterator = match JsonArrayIterator::new(src) {
        Ok(json_array_iterator) => json_array_iterator,
        Err(err) => return Err(not_an_array_of_entities::<TEntity>(err)),
    };

    while let Some(item) = json_array_iterator.get_next() {
        let itm = match item {
            Ok(itm) => itm,
            Err(err) => return Err(not_an_array_of_entities::<TEntity>(err)),
        };

        match TEntity::deserialize_entity(itm.as_bytes()) {
            Ok(entity) => {
                result.push(entity);
            }
            Err(err) => {
                return Err(DataWriterError::Error(format!(
                    "Table: '{}'. Can not deserialize entity: {}. Entity: {:?}",
                    TEntity::TABLE_NAME,
                    err,
                    std::str::from_utf8(itm.as_bytes())
                )));
            }
        }
    }

    Ok(result)
}

/// The body of a 2xx answer to a read of rows is not a json array - there is none where the
/// rows are expected, it is the page of a proxy - or stops being one half way through.
fn not_an_array_of_entities<TEntity: MyNoSqlEntity>(
    err: my_json::json_reader::JsonParseError,
) -> DataWriterError {
    DataWriterError::Error(format!(
        "Can not deserialize entities for table: {}. Err: {:?}",
        TEntity::TABLE_NAME,
        err
    ))
}

/// Reads the row a 2xx answer carries in its body.
///
/// A 2xx whose body is not the row - a 204 where the row is expected, the page of a proxy, a
/// row which does not fit the entity - is not an answer the call can hand out either. It is
/// reported the way [`unexpected_response`] reports a status, with what the deserializer said,
/// and never by a panic which would take the calling task down.
async fn read_entity<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer>(
    response: &mut WriterFlUrlResponse,
    operation: &str,
) -> Result<TEntity, DataWriterError> {
    let status_code = response.get_status_code();
    let body = response.get_body_as_slice().await?;

    match TEntity::deserialize_entity(body) {
        Ok(entity) => Ok(entity),
        Err(err) => Err(DataWriterError::Error(format!(
            "{} of table {} returned status code {} with a body which is not the row: {}. Body: {}",
            operation,
            TEntity::TABLE_NAME,
            status_code,
            err,
            String::from_utf8_lossy(body)
        ))),
    }
}

/// How `create_table` and `create_table_if_not_exists` read their answer: a 2xx is done, a 400
/// carries what the server refused the table for - [`DataWriterError::TableAlreadyExists`] to
/// `Tables/Create` - and any other answer is not the API's and is reported the way
/// [`unexpected_response`] reports it, under `process_name`. Unlike the other calls these two
/// write every failure to the log themselves, with the url.
async fn create_table_errors_handler(
    response: &mut WriterFlUrlResponse,
    process_name: &'static str,
    table_name: &str,
    url: &str,
) -> Result<(), DataWriterError> {
    if is_ok_result(response) {
        return Ok(());
    }

    let result = match response.get_status_code() {
        400 => deserialize_error(response).await?,
        status_code => unexpected_status(
            process_name,
            table_name,
            status_code,
            response.get_body_as_slice().await?,
        ),
    };

    my_logger::LOGGER.write_error(
        process_name,
        format!("{:?}", result),
        LogEventCtx::new().add("URL", url),
    );

    Err(result)
}

/// The optimistic-concurrency read-modify-write loop shared by every `update_entity`
/// wrapper. Kept transport-agnostic (parameterized by `read` / `replace` closures) so the
/// retry logic can be unit-tested without a live server.
///
/// - `read` fetches the current entity (its `TimeStamp` is the version to send back);
/// - `update` mutates the caller's fields in place — it must NOT touch `time_stamp`;
/// - `replace` writes it and hands the entity back alongside the result.
///
/// On [`DataWriterError::RecordIsChanged`] the loop re-reads (fresh version) and re-applies
/// `update`, up to `max_attempts`; on exhaustion the last `RecordIsChanged` is returned.
/// A missing row (`read` → `None`) yields `Ok(None)`; any other error is propagated as-is.
pub(crate) async fn run_read_modify_write<TEntity, TFn, FRead, RFut, FReplace, PFut>(
    max_attempts: usize,
    mut update: TFn,
    mut read: FRead,
    mut replace: FReplace,
) -> Result<Option<TEntity>, DataWriterError>
where
    TFn: FnMut(&mut TEntity),
    FRead: FnMut() -> RFut,
    RFut: std::future::Future<Output = Result<Option<TEntity>, DataWriterError>>,
    FReplace: FnMut(TEntity) -> PFut,
    PFut: std::future::Future<Output = (TEntity, Result<(), DataWriterError>)>,
{
    let mut attempt: usize = 0;

    loop {
        let mut entity = match read().await? {
            Some(entity) => entity,
            None => return Ok(None),
        };

        update(&mut entity);

        let (entity, replace_result) = replace(entity).await;

        match replace_result {
            Ok(()) => return Ok(Some(entity)),
            Err(DataWriterError::RecordIsChanged(message)) => {
                attempt += 1;
                if attempt >= max_attempts {
                    return Err(DataWriterError::RecordIsChanged(message));
                }
                // Otherwise loop: re-read the fresh version and re-apply `update`.
            }
            Err(err) => return Err(err),
        }
    }
}

/// The insert-or-update loop, one step below `MyNoSqlDataWriter::insert_or_update`: which of
/// the two closures runs is decided by what the read found, and every way two writers can
/// collide is answered by reading again rather than by failing.
///
/// * missing -> `create` -> `insert`. A lost race is `RecordAlreadyExists`, and the next read
///   finds the row the winner wrote, so the `update` branch takes over from there.
/// * present -> `update` -> `replace` with the `TimeStamp` the entity was read with. A lost
///   race is `RecordIsChanged` (rewritten under us) or `RecordNotFound` (deleted under us),
///   and the next read is what says which branch is the right one now.
///
/// `update` returns whether the row has to be written at all: it gets the row it would change
/// and can answer `false` after looking at it - the stored row already says what it should, so
/// there is nothing to write and no reason to spend a `Replace` (or to lose a race over one).
/// The entity is then returned as read.
///
/// Every lost race costs one attempt out of `max_attempts` and the last one is returned as it
/// came. `create` and `update` are only ever called right after a read, so neither of them
/// ever works on a state older than the attempt it belongs to.
pub(crate) async fn run_insert_or_update<
    TEntity,
    TCreate,
    TUpdate,
    FRead,
    RFut,
    FInsert,
    IFut,
    FReplace,
    PFut,
>(
    max_attempts: usize,
    mut create: TCreate,
    mut update: TUpdate,
    mut read: FRead,
    mut insert: FInsert,
    mut replace: FReplace,
) -> Result<TEntity, DataWriterError>
where
    TCreate: FnMut() -> TEntity,
    TUpdate: FnMut(&mut TEntity) -> bool,
    FRead: FnMut() -> RFut,
    RFut: std::future::Future<Output = Result<Option<TEntity>, DataWriterError>>,
    FInsert: FnMut(TEntity) -> IFut,
    IFut: std::future::Future<Output = (TEntity, Result<(), DataWriterError>)>,
    FReplace: FnMut(TEntity) -> PFut,
    PFut: std::future::Future<Output = (TEntity, Result<(), DataWriterError>)>,
{
    let mut attempt: usize = 0;

    loop {
        let (entity, write_result) = match read().await? {
            Some(mut entity) => {
                if !update(&mut entity) {
                    // The closure looked at the row and decided it is already right. Writing it
                    // back would only be a way to lose a race with a writer who has something
                    // to say.
                    return Ok(entity);
                }

                replace(entity).await
            }
            None => {
                let entity = create();
                insert(entity).await
            }
        };

        match write_result {
            Ok(()) => return Ok(entity),
            Err(err) if is_lost_race(&err) => {
                attempt += 1;
                if attempt >= max_attempts {
                    return Err(err);
                }
                // Otherwise loop: read again and let the fresh state pick the branch.
            }
            Err(err) => return Err(err),
        }
    }
}

/// The three ways `insert_or_update` loses a race to another writer. All of them mean "read
/// again"; none of them means the write is impossible.
fn is_lost_race(err: &DataWriterError) -> bool {
    match err {
        // Insert: the key was taken between our read and our insert.
        DataWriterError::RecordAlreadyExists(_)
        // Replace: the row was rewritten between our read and our replace.
        | DataWriterError::RecordIsChanged(_)
        // Replace: the row was deleted between our read and our replace.
        | DataWriterError::RecordNotFound(_) => true,
        _ => false,
    }
}

/// Both closures may shape the entity however they like, but neither may move it to another
/// key: the loop reads `partition_key` / `row_key`, so an entity carrying different keys would
/// be written somewhere else and reported as success while the row which was asked for is
/// still missing - and the next call would do it all over again. `built_by` names the closure
/// which produced the entity, so the message says which one to go and look at.
pub(crate) fn ensure_entity_keys_match<TEntity: MyNoSqlEntity>(
    entity: &TEntity,
    partition_key: &str,
    row_key: &str,
    built_by: &str,
) -> Result<(), DataWriterError> {
    if entity.get_partition_key() != partition_key || entity.get_row_key() != row_key {
        return Err(DataWriterError::Error(format!(
            "insert_or_update for ['{}', '{}'] is about to write an entity with ['{}', '{}'] - the '{}' closure must not change the keys of the row",
            partition_key,
            row_key,
            entity.get_partition_key(),
            entity.get_row_key(),
            built_by,
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp};
    use serde::Serialize;
    use serde_derive::Deserialize;

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct TestEntity {
        partition_key: String,
        row_key: String,
    }

    impl MyNoSqlEntity for TestEntity {
        const TABLE_NAME: &'static str = "test";
        const LAZY_DESERIALIZATION: bool = false;

        fn get_partition_key(&self) -> &str {
            &self.partition_key
        }

        fn get_row_key(&self) -> &str {
            &self.row_key
        }

        fn get_time_stamp(&self) -> Timestamp {
            Timestamp::default()
        }
    }

    impl MyNoSqlEntitySerializer for TestEntity {
        fn serialize_entity(&self) -> Vec<u8> {
            my_no_sql_core::entity_serializer::serialize(self)
        }

        fn deserialize_entity(src: &[u8]) -> Result<Self, String> {
            my_no_sql_core::entity_serializer::deserialize(src)
        }
    }

    #[test]
    fn test() {
        let entities = vec![
            TestEntity {
                partition_key: "1".to_string(),
                row_key: "1".to_string(),
            },
            TestEntity {
                partition_key: "1".to_string(),
                row_key: "2".to_string(),
            },
            TestEntity {
                partition_key: "2".to_string(),
                row_key: "1".to_string(),
            },
            TestEntity {
                partition_key: "2".to_string(),
                row_key: "2".to_string(),
            },
        ];

        let as_json = super::serialize_entities_to_body(&entities);

        let body = as_json.into_vec();

        println!("{}", std::str::from_utf8(&body).unwrap());
    }

    #[test]
    fn test_parse_rows_count() {
        assert_eq!(super::parse_rows_count(b"0").unwrap(), 0);
        assert_eq!(super::parse_rows_count(b"138081").unwrap(), 138081);
        // text/plain is free to carry trailing whitespace
        assert_eq!(super::parse_rows_count(b"51062\n").unwrap(), 51062);
    }

    #[test]
    fn test_parse_rows_count_of_a_body_which_is_not_a_number() {
        // Anything but a number is a failure to answer - it must not be read as a count.
        assert!(super::parse_rows_count(b"").is_err());
        assert!(super::parse_rows_count(b"-1").is_err());
        assert!(super::parse_rows_count(b"{\"amount\":5}").is_err());
    }

    #[test]
    fn partition_keys_are_split_into_requests_which_fit_into_a_request_head() {
        // Nothing to delete - nothing to ask for.
        assert!(super::split_partition_keys(&[]).is_empty());

        // A short list is one request, as it has always been.
        assert_eq!(
            super::split_partition_keys(&["a", "b"]),
            vec![&["a", "b"][..]]
        );

        let keys: Vec<String> = (0..1000)
            .map(|no| format!("partition-{:06}", no))
            .collect();
        let keys: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();

        let requests = super::split_partition_keys(&keys);

        assert!(requests.len() > 1);
        // Every key goes out once, in the order it came in.
        assert_eq!(requests.concat(), keys);

        for request in &requests {
            let spent: usize = request
                .iter()
                .map(|key| "&partitionKeys=".len() + key.len())
                .sum();
            assert!(spent <= super::DELETE_PARTITIONS_QUERY_BUDGET);
        }

        // All of them but the last one are filled: one more key would not fit.
        for request in &requests[..requests.len() - 1] {
            let spent: usize = request
                .iter()
                .map(|key| "&partitionKeys=".len() + key.len())
                .sum();
            assert!(
                spent + "&partitionKeys=".len() + keys[0].len()
                    > super::DELETE_PARTITIONS_QUERY_BUDGET
            );
        }

        // A key which is over the budget on its own still goes out - in a request of its own.
        let huge = "x".repeat(super::DELETE_PARTITIONS_QUERY_BUDGET);
        let keys = ["a", huge.as_str(), "b"];
        let requests = super::split_partition_keys(&keys);
        assert_eq!(requests, vec![&keys[..1], &keys[1..2], &keys[2..]]);
    }

    #[test]
    fn a_partition_key_is_counted_as_what_it_takes_in_the_query_string() {
        const PAIR: usize = "&partitionKeys=".len();

        assert_eq!(super::partition_key_query_pair_size(""), PAIR);
        // Left as they are by every url encoder.
        assert_eq!(
            super::partition_key_query_pair_size("Az09-_."),
            PAIR + 7
        );
        // Escaped - or possibly escaped, which is what a limit has to count with.
        assert_eq!(super::partition_key_query_pair_size("a b"), PAIR + 5);
        assert_eq!(super::partition_key_query_pair_size("a&b=c"), PAIR + 9);
        assert_eq!(super::partition_key_query_pair_size("~"), PAIR + 3);
        // Two bytes of utf8 for the letter - each of them escaped.
        assert_eq!(super::partition_key_query_pair_size("ю"), PAIR + 6);
    }

    #[test]
    fn empty_slice_of_entities_serializes_to_an_empty_array() {
        // The clean-and-insert calls send it: an empty slice still cleans the table.
        let body = super::serialize_entities_to_body::<TestEntity>(&[]);

        assert!(matches!(body, super::HttpRequestBody::Json(_)));
        assert_eq!(body.into_vec(), b"[]");
    }

    #[test]
    fn a_404_is_record_not_found_only_with_the_body_of_the_api() {
        assert!(super::is_record_not_found_body(b"Record not found"));
        // text/plain is free to carry trailing whitespace
        assert!(super::is_record_not_found_body(b"Record not found\n"));

        // What comes back when nobody serves the route says nothing about the row: the
        // server's framework for a path it has no action for, a node's guard, a proxy.
        assert!(!super::is_record_not_found_body(b"404 - Not Found"));
        assert!(!super::is_record_not_found_body(b"Not found"));
        assert!(!super::is_record_not_found_body(
            b"<html><body>404 Not Found</body></html>"
        ));
        assert!(!super::is_record_not_found_body(b""));
        assert!(!super::is_record_not_found_body(&[0xff, 0xfe]));

        // A missing table is not a missing row.
        assert!(!super::is_record_not_found_body(
            br#"{"reason":"TableNotFound","message":"Table 'test' not found"}"#
        ));
    }

    #[test]
    fn unexpected_status_names_the_call_the_table_the_status_and_the_body() {
        match super::unexpected_status(
            "get_entity",
            "test",
            503,
            b"Application is not initialized yet",
        ) {
            DataWriterError::Error(message) => assert_eq!(
                message,
                "get_entity of table test returned status code 503. Body: Application is not initialized yet"
            ),
            other => panic!("expected DataWriterError::Error, got {:?}", other),
        }

        // The message get_rows_count has always had - it goes through the same function now.
        match super::unexpected_status("Rows count", "test", 502, b"") {
            DataWriterError::Error(message) => assert_eq!(
                message,
                "Rows count of table test returned status code 502. Body: "
            ),
            other => panic!("expected DataWriterError::Error, got {:?}", other),
        }

        // A body which is not text must not turn the error about the status into another one.
        match super::unexpected_status("delete_row", "test", 500, &[b'o', b'k', 0xff]) {
            DataWriterError::Error(message) => assert_eq!(
                message,
                "delete_row of table test returned status code 500. Body: ok\u{fffd}"
            ),
            other => panic!("expected DataWriterError::Error, got {:?}", other),
        }
    }

    #[test]
    fn unexpected_status_carries_no_more_than_the_head_of_a_long_body() {
        fn message_of(body: &[u8]) -> String {
            match super::unexpected_status("get_all", "test", 200, body) {
                DataWriterError::Error(message) => message,
                other => panic!("expected DataWriterError::Error, got {:?}", other),
            }
        }

        // 1 KB is carried as it came.
        let body = "x".repeat(1024);

        assert_eq!(
            message_of(body.as_bytes()),
            format!(
                "get_all of table test returned status code 200. Body: {}",
                body
            )
        );

        // A byte more and it is cut - and the message says what it was cut from.
        let body = "x".repeat(1025);

        assert_eq!(
            message_of(body.as_bytes()),
            format!(
                "get_all of table test returned status code 200. Body of 1025 bytes, cut to the first 1024: {}",
                "x".repeat(1024)
            )
        );

        // The cut does not go through a character: each "ю" takes two bytes, and the 1024th
        // byte of this body is the first byte of one.
        let body = format!("x{}", "ю".repeat(1000));

        assert_eq!(
            message_of(body.as_bytes()),
            format!(
                "get_all of table test returned status code 200. Body of 2001 bytes, cut to the first 1023: x{}",
                "ю".repeat(511)
            )
        );

        // A body which is not text is cut where the limit is.
        let body = vec![0xff; 2000];

        assert_eq!(
            message_of(body.as_slice()),
            format!(
                "get_all of table test returned status code 200. Body of 2000 bytes, cut to the first 1024: {}",
                "\u{fffd}".repeat(1024)
            )
        );
    }

    /// Mirrors what `#[my_no_sql_entity]` generates for the TimeStamp field, so we can
    /// verify how a real / default `Timestamp` actually serializes on the InsertOrReplaceIfNew
    /// path (where the server requires a parseable ISO TimeStamp).
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct TimeStampedTestEntity {
        partition_key: String,
        row_key: String,
        #[serde(rename = "TimeStamp")]
        #[serde(skip_serializing_if = "my_no_sql_abstractions::skip_timestamp_serializing")]
        time_stamp: Timestamp,
    }

    impl MyNoSqlEntity for TimeStampedTestEntity {
        const TABLE_NAME: &'static str = "test";
        const LAZY_DESERIALIZATION: bool = false;

        fn get_partition_key(&self) -> &str {
            &self.partition_key
        }

        fn get_row_key(&self) -> &str {
            &self.row_key
        }

        fn get_time_stamp(&self) -> Timestamp {
            self.time_stamp
        }
    }

    impl MyNoSqlEntitySerializer for TimeStampedTestEntity {
        fn serialize_entity(&self) -> Vec<u8> {
            my_no_sql_core::entity_serializer::serialize(self)
        }

        fn deserialize_entity(src: &[u8]) -> Result<Self, String> {
            my_no_sql_core::entity_serializer::deserialize(src)
        }
    }

    #[test]
    fn real_timestamp_serializes_as_parseable_iso() {
        use rust_extensions::date_time::DateTimeAsMicroseconds;

        let entity = TimeStampedTestEntity {
            partition_key: "pk".to_string(),
            row_key: "rk".to_string(),
            time_stamp: DateTimeAsMicroseconds::from_str("2025-01-01T12:00:00.123456")
                .unwrap()
                .into(),
        };

        let body = entity.serialize_entity();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let ts = json
            .get("TimeStamp")
            .expect("TimeStamp must be present for a real value")
            .as_str()
            .expect("TimeStamp must be a string");

        // The server parses it exactly like this; if it were null/empty it would 400.
        assert!(
            DateTimeAsMicroseconds::parse_iso_string(ts).is_some(),
            "serialized TimeStamp '{}' must be a parseable ISO date-time",
            ts
        );
    }

    #[test]
    fn default_timestamp_is_omitted() {
        let entity = TimeStampedTestEntity {
            partition_key: "pk".to_string(),
            row_key: "rk".to_string(),
            time_stamp: Timestamp::default(),
        };

        let body = entity.serialize_entity();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // A default Timestamp is skipped entirely — the server sees no TimeStamp and
        // rejects an InsertOrReplaceIfNew request with HTTP 400. This is exactly why the
        // request functions of this module debug_assert on a non-default TimeStamp.
        assert!(
            json.get("TimeStamp").is_none(),
            "a default Timestamp must not serialize to a value, got: {}",
            String::from_utf8_lossy(&body)
        );
    }

    // What the four calls below say when they refuse a default TimeStamp has to be true of
    // what the test above shows: the field is left out, it does not go out as null. Nothing is
    // sent - the assertion comes before the request - and it is there in a debug build only.

    #[cfg(debug_assertions)]
    fn entity_with_default_time_stamp() -> TimeStampedTestEntity {
        TimeStampedTestEntity {
            partition_key: "pk".to_string(),
            row_key: "rk".to_string(),
            time_stamp: Timestamp::default(),
        }
    }

    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(
        expected = "a default one is left out of the JSON by the entity macros and the server rejects the request with HTTP 400"
    )]
    async fn bulk_insert_or_update_with_own_timestamp_refuses_a_default_timestamp() {
        let _ = super::bulk_insert_or_update_with_own_timestamp(
            super::WriterFlUrl::new(
                flurl::FlUrl::new("http://127.0.0.1:0"),
                crate::DEFAULT_BODY_SIZE_LIMIT,
            ),
            &[entity_with_default_time_stamp()],
            &my_no_sql_abstractions::DataSynchronizationPeriod::Immediately,
        )
        .await;
    }

    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(
        expected = "a default one is left out of the JSON by the entity macros and the server rejects the request with HTTP 400"
    )]
    async fn insert_or_replace_entity_if_new_refuses_a_default_timestamp() {
        let _ = super::insert_or_replace_entity_if_new(
            super::WriterFlUrl::new(
                flurl::FlUrl::new("http://127.0.0.1:0"),
                crate::DEFAULT_BODY_SIZE_LIMIT,
            ),
            &entity_with_default_time_stamp(),
            &my_no_sql_abstractions::DataSynchronizationPeriod::Immediately,
        )
        .await;
    }

    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(
        expected = "a default one is left out of the JSON by the entity macros and the server rejects the request with HTTP 400"
    )]
    async fn bulk_insert_or_replace_if_new_refuses_a_default_timestamp() {
        let _ = super::bulk_insert_or_replace_if_new(
            super::WriterFlUrl::new(
                flurl::FlUrl::new("http://127.0.0.1:0"),
                crate::DEFAULT_BODY_SIZE_LIMIT,
            ),
            &[entity_with_default_time_stamp()],
            &my_no_sql_abstractions::DataSynchronizationPeriod::Immediately,
        )
        .await;
    }

    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(
        expected = "a default one is left out of the JSON by the entity macros and the server rejects the chunk with HTTP 400"
    )]
    async fn a_chunk_of_insert_or_replace_if_new_refuses_a_default_timestamp() {
        let _ = super::insert_or_replace_if_new_by_chunks_upload(
            super::WriterFlUrl::new(
                flurl::FlUrl::new("http://127.0.0.1:0"),
                crate::DEFAULT_BODY_SIZE_LIMIT,
            ),
            &[entity_with_default_time_stamp()],
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn empty_bulk_insert_or_replace_if_new_is_ok() {
        // The empty-slice guard returns before any request is built, so no server is needed.
        let flurl = super::WriterFlUrl::new(
                flurl::FlUrl::new("http://127.0.0.1:0"),
                crate::DEFAULT_BODY_SIZE_LIMIT,
            );
        let result = super::bulk_insert_or_replace_if_new::<TimeStampedTestEntity>(
            flurl,
            &[],
            &my_no_sql_abstractions::DataSynchronizationPeriod::Immediately,
        )
        .await;

        assert!(result.is_ok(), "empty bulk must be a no-op Ok(()), got {:?}", result.err());
    }

    #[tokio::test]
    async fn empty_bulk_delete_if_is_ok() {
        // Same empty-input guard as bulk_delete: the answer is built without a request, so
        // no server is needed.
        let result = super::bulk_delete_if::<TimeStampedTestEntity>(
            super::WriterFlUrl::new(
                flurl::FlUrl::new("http://127.0.0.1:0"),
                crate::DEFAULT_BODY_SIZE_LIMIT,
            ),
            &[],
            &my_no_sql_abstractions::DataSynchronizationPeriod::Immediately,
        )
        .await
        .unwrap();

        assert_eq!(result.deleted, 0);
        assert!(result.is_all_deleted());

        let result = super::bulk_delete_if_rows::<TimeStampedTestEntity>(
            super::WriterFlUrl::new(
                flurl::FlUrl::new("http://127.0.0.1:0"),
                crate::DEFAULT_BODY_SIZE_LIMIT,
            ),
            &[],
            &my_no_sql_abstractions::DataSynchronizationPeriod::Immediately,
        )
        .await
        .unwrap();

        assert_eq!(result.deleted, 0);
        assert!(result.is_all_deleted());
    }

    #[test]
    fn chunking_splits_as_expected() {
        // The chunked one-call method relies on slice::chunks; pin the boundaries it produces.
        let entities: Vec<u32> = (0..10).collect();

        let lens: Vec<usize> = entities.chunks(3).map(|c| c.len()).collect();
        assert_eq!(lens, vec![3, 3, 3, 1]);

        let lens: Vec<usize> = entities.chunks(5).map(|c| c.len()).collect();
        assert_eq!(lens, vec![5, 5]);

        let lens: Vec<usize> = entities.chunks(100).map(|c| c.len()).collect();
        assert_eq!(lens, vec![10]);
    }

    use crate::DataWriterError;
    use std::cell::Cell;

    // A tiny entity for the read-modify-write loop tests. `version` stands in for the
    // stored TimeStamp; `value` is the field the closure mutates.
    #[derive(Debug, PartialEq)]
    struct LoopEntity {
        version: i64,
        value: i32,
    }

    #[tokio::test]
    async fn update_loop_retries_on_conflict_then_succeeds() {
        let read_calls = Cell::new(0i32);
        let replace_calls = Cell::new(0i32);

        // Server-side "stored version" bumps on every write attempt so the first two
        // replaces see a stale version (409) and the third one matches.
        let stored_version = Cell::new(10i64);

        let result: Result<Option<LoopEntity>, DataWriterError> = super::run_read_modify_write(
            5,
            |e: &mut LoopEntity| e.value += 1,
            || {
                read_calls.set(read_calls.get() + 1);
                let version = stored_version.get();
                async move {
                    Ok(Some(LoopEntity {
                        version,
                        value: 100,
                    }))
                }
            },
            |e: LoopEntity| {
                let n = replace_calls.get() + 1;
                replace_calls.set(n);
                // The row moves on under us for the first two attempts.
                stored_version.set(stored_version.get() + 1);
                async move {
                    if n < 3 {
                        (e, Err(DataWriterError::RecordIsChanged("changed".to_string())))
                    } else {
                        (e, Ok(()))
                    }
                }
            },
        )
        .await;

        // Each attempt starts from a fresh read (value 100) and applies the closure once.
        assert_eq!(result.unwrap(), Some(LoopEntity { version: 12, value: 101 }));
        assert_eq!(read_calls.get(), 3, "must re-read on every conflict");
        assert_eq!(replace_calls.get(), 3);
    }

    #[tokio::test]
    async fn update_loop_gives_up_after_max_attempts() {
        let replace_calls = Cell::new(0i32);

        let result: Result<Option<LoopEntity>, DataWriterError> = super::run_read_modify_write(
            3,
            |_e: &mut LoopEntity| {},
            || async { Ok(Some(LoopEntity { version: 1, value: 0 })) },
            |e: LoopEntity| {
                replace_calls.set(replace_calls.get() + 1);
                async move {
                    (e, Err(DataWriterError::RecordIsChanged("still conflicting".to_string())))
                }
            },
        )
        .await;

        match result {
            Err(DataWriterError::RecordIsChanged(msg)) => assert_eq!(msg, "still conflicting"),
            other => panic!("expected RecordIsChanged, got {:?}", other),
        }
        assert_eq!(replace_calls.get(), 3, "must stop exactly at max_attempts");
    }

    #[tokio::test]
    async fn update_loop_returns_none_when_row_missing() {
        let replace_calls = Cell::new(0i32);

        let result: Result<Option<LoopEntity>, DataWriterError> = super::run_read_modify_write(
            5,
            |_e: &mut LoopEntity| panic!("update must not be called when the row is missing"),
            || async { Ok(None) },
            |e: LoopEntity| {
                replace_calls.set(replace_calls.get() + 1);
                async move { (e, Ok(())) }
            },
        )
        .await;

        assert_eq!(result.unwrap(), None);
        assert_eq!(replace_calls.get(), 0, "must not attempt a replace");
    }

    #[tokio::test]
    async fn update_loop_propagates_other_errors_without_retry() {
        let replace_calls = Cell::new(0i32);

        let result: Result<Option<LoopEntity>, DataWriterError> = super::run_read_modify_write(
            5,
            |_e: &mut LoopEntity| {},
            || async { Ok(Some(LoopEntity { version: 1, value: 0 })) },
            |e: LoopEntity| {
                replace_calls.set(replace_calls.get() + 1);
                async move { (e, Err(DataWriterError::RecordNotFound("gone".to_string()))) }
            },
        )
        .await;

        assert!(matches!(result, Err(DataWriterError::RecordNotFound(_))));
        assert_eq!(replace_calls.get(), 1, "a non-conflict error must not be retried");
    }

    // A compile-time check of the public insert-or-update surface. The loop itself is tested
    // below through its closures; what this pins is that the closures a caller actually writes
    // satisfy the bounds - including a flag (`created`) being mutated inside `create`
    // while `update` is live, which is the one shape the borrow checker could refuse. Never
    // called: type-checking it is the whole point.
    #[allow(dead_code)]
    async fn insert_or_update_is_usable_as_documented(
        writer: &crate::MyNoSqlDataWriter<TestEntity>,
        with_retries: &crate::MyNoSqlDataWriterWithRetries<TestEntity>,
    ) -> Result<(), DataWriterError> {
        let mut created = false;

        let entity = writer
            .insert_or_update(
                "pk",
                "rk",
                || {
                    created = true;
                    TestEntity {
                        partition_key: "pk".to_string(),
                        row_key: "rk".to_string(),
                    }
                },
                // Read the row, decide nothing has to change, and say so.
                |e: &mut TestEntity| e.partition_key != "pk",
            )
            .await?;

        let _ = (entity, created);

        with_retries
            .insert_or_update_with_max_attempts(
                "pk",
                "rk",
                10,
                || TestEntity {
                    partition_key: "pk".to_string(),
                    row_key: "rk".to_string(),
                },
                |_e: &mut TestEntity| true,
            )
            .await?;

        Ok(())
    }

    // ---- insert-or-update loop ---------------------------------------------------------
    //
    // The loop's whole job is to pick a branch from what the read just saw and to survive the
    // three ways the other writer can get in the way, so every test below fakes a read which
    // changes its answer between attempts.

    #[tokio::test]
    async fn insert_or_update_creates_the_row_when_it_is_missing() {
        let create_calls = Cell::new(0i32);
        let update_calls = Cell::new(0i32);
        let insert_calls = Cell::new(0i32);
        let replace_calls = Cell::new(0i32);

        let result: Result<LoopEntity, DataWriterError> = super::run_insert_or_update(
            5,
            || {
                create_calls.set(create_calls.get() + 1);
                LoopEntity {
                    version: 0,
                    value: 7,
                }
            },
            |_e: &mut LoopEntity| {
                update_calls.set(update_calls.get() + 1);
                true
            },
            || async { Ok(None) },
            |e: LoopEntity| {
                insert_calls.set(insert_calls.get() + 1);
                async move { (e, Ok(())) }
            },
            |e: LoopEntity| {
                replace_calls.set(replace_calls.get() + 1);
                async move { (e, Ok(())) }
            },
        )
        .await;

        assert_eq!(
            result.unwrap(),
            LoopEntity {
                version: 0,
                value: 7
            }
        );
        assert_eq!(create_calls.get(), 1);
        assert_eq!(insert_calls.get(), 1);
        assert_eq!(update_calls.get(), 0, "update belongs to the other branch");
        assert_eq!(
            replace_calls.get(),
            0,
            "replace belongs to the other branch"
        );
    }

    #[tokio::test]
    async fn insert_or_update_switches_to_update_when_another_writer_inserted_first() {
        // The first read says the row is missing; by the time our insert lands, someone else
        // has created it - and every read after that sees their row (version 42).
        let read_calls = Cell::new(0i32);
        let create_calls = Cell::new(0i32);
        let update_calls = Cell::new(0i32);
        let insert_calls = Cell::new(0i32);
        let replace_calls = Cell::new(0i32);

        let result: Result<LoopEntity, DataWriterError> = super::run_insert_or_update(
            5,
            || {
                create_calls.set(create_calls.get() + 1);
                LoopEntity {
                    version: 0,
                    value: 7,
                }
            },
            |e: &mut LoopEntity| {
                update_calls.set(update_calls.get() + 1);
                e.value += 1;
                true
            },
            || {
                let n = read_calls.get() + 1;
                read_calls.set(n);
                async move {
                    if n == 1 {
                        Ok(None)
                    } else {
                        Ok(Some(LoopEntity {
                            version: 42,
                            value: 100,
                        }))
                    }
                }
            },
            |e: LoopEntity| {
                insert_calls.set(insert_calls.get() + 1);
                async move {
                    (
                        e,
                        Err(DataWriterError::RecordAlreadyExists(
                            "Record already exists".to_string(),
                        )),
                    )
                }
            },
            |e: LoopEntity| {
                replace_calls.set(replace_calls.get() + 1);
                async move { (e, Ok(())) }
            },
        )
        .await;

        // The winner's row is what we ended up updating - the entity `create` built is dropped.
        assert_eq!(
            result.unwrap(),
            LoopEntity {
                version: 42,
                value: 101
            }
        );
        assert_eq!(read_calls.get(), 2, "a lost insert must be re-read");
        assert_eq!(create_calls.get(), 1);
        assert_eq!(insert_calls.get(), 1);
        assert_eq!(update_calls.get(), 1);
        assert_eq!(replace_calls.get(), 1);
    }

    #[tokio::test]
    async fn insert_or_update_retries_the_update_branch_on_a_version_conflict() {
        let read_calls = Cell::new(0i32);
        let replace_calls = Cell::new(0i32);
        let stored_version = Cell::new(10i64);

        let result: Result<LoopEntity, DataWriterError> = super::run_insert_or_update(
            5,
            || panic!("create must not run while the row is there"),
            |e: &mut LoopEntity| {
                e.value += 1;
                true
            },
            || {
                read_calls.set(read_calls.get() + 1);
                let version = stored_version.get();
                async move {
                    Ok(Some(LoopEntity {
                        version,
                        value: 100,
                    }))
                }
            },
            |e: LoopEntity| {
                replace_calls.set(replace_calls.get() + 1);
                async move { (e, Ok(())) }
            },
            |e: LoopEntity| {
                let n = replace_calls.get() + 1;
                replace_calls.set(n);
                // The row moves on under us for the first two attempts.
                stored_version.set(stored_version.get() + 1);
                async move {
                    if n < 3 {
                        (
                            e,
                            Err(DataWriterError::RecordIsChanged("changed".to_string())),
                        )
                    } else {
                        (e, Ok(()))
                    }
                }
            },
        )
        .await;

        assert_eq!(
            result.unwrap(),
            LoopEntity {
                version: 12,
                value: 101
            }
        );
        assert_eq!(read_calls.get(), 3, "must re-read on every conflict");
    }

    #[tokio::test]
    async fn insert_or_update_falls_back_to_create_when_the_row_is_deleted_under_us() {
        // Read sees the row, but it is gone by the time we replace it (404). The loop must not
        // give up on a row which simply has to be created instead.
        let read_calls = Cell::new(0i32);
        let create_calls = Cell::new(0i32);
        let insert_calls = Cell::new(0i32);

        let result: Result<LoopEntity, DataWriterError> =
            super::run_insert_or_update(
                5,
                || {
                    create_calls.set(create_calls.get() + 1);
                    LoopEntity {
                        version: 0,
                        value: 7,
                    }
                },
                |e: &mut LoopEntity| {
                    e.value += 1;
                    true
                },
                || {
                    let n = read_calls.get() + 1;
                    read_calls.set(n);
                    async move {
                        if n == 1 {
                            Ok(Some(LoopEntity {
                                version: 5,
                                value: 100,
                            }))
                        } else {
                            Ok(None)
                        }
                    }
                },
                |e: LoopEntity| {
                    insert_calls.set(insert_calls.get() + 1);
                    async move { (e, Ok(())) }
                },
                |e: LoopEntity| async move {
                    (e, Err(DataWriterError::RecordNotFound("gone".to_string())))
                },
            )
            .await;

        assert_eq!(
            result.unwrap(),
            LoopEntity {
                version: 0,
                value: 7
            }
        );
        assert_eq!(read_calls.get(), 2);
        assert_eq!(create_calls.get(), 1);
        assert_eq!(insert_calls.get(), 1);
    }

    #[tokio::test]
    async fn insert_or_update_gives_up_after_max_attempts() {
        // A pathological writer keeps taking the key back before we get to it.
        let insert_calls = Cell::new(0i32);

        let result: Result<LoopEntity, DataWriterError> = super::run_insert_or_update(
            3,
            || LoopEntity {
                version: 0,
                value: 7,
            },
            |_e: &mut LoopEntity| true,
            || async { Ok(None) },
            |e: LoopEntity| {
                insert_calls.set(insert_calls.get() + 1);
                async move {
                    (
                        e,
                        Err(DataWriterError::RecordAlreadyExists("taken".to_string())),
                    )
                }
            },
            |e: LoopEntity| async move { (e, Ok(())) },
        )
        .await;

        match result {
            Err(DataWriterError::RecordAlreadyExists(msg)) => assert_eq!(msg, "taken"),
            other => panic!("expected RecordAlreadyExists, got {:?}", other),
        }
        assert_eq!(
            insert_calls.get(),
            3,
            "must stop exactly at max_attempts, keeping the last conflict"
        );
    }

    #[tokio::test]
    async fn insert_or_update_propagates_a_real_failure_without_retrying() {
        let insert_calls = Cell::new(0i32);
        let read_calls = Cell::new(0i32);

        let result: Result<LoopEntity, DataWriterError> = super::run_insert_or_update(
            5,
            || LoopEntity {
                version: 0,
                value: 7,
            },
            |_e: &mut LoopEntity| true,
            || {
                read_calls.set(read_calls.get() + 1);
                async { Ok(None) }
            },
            |e: LoopEntity| {
                insert_calls.set(insert_calls.get() + 1);
                async move { (e, Err(DataWriterError::Error("table is dead".to_string()))) }
            },
            |e: LoopEntity| async move { (e, Ok(())) },
        )
        .await;

        assert!(matches!(result, Err(DataWriterError::Error(_))));
        assert_eq!(insert_calls.get(), 1, "a real failure is not a lost race");
        assert_eq!(read_calls.get(), 1);
    }

    #[tokio::test]
    async fn insert_or_update_writes_nothing_when_the_update_closure_declines() {
        // The closure is handed the row, sees it already says what it should, and answers
        // `false`. Nothing may go out then - the point of that answer is to save the write, not
        // just to skip the change.
        let read_calls = Cell::new(0i32);
        let insert_calls = Cell::new(0i32);
        let replace_calls = Cell::new(0i32);

        let result: Result<LoopEntity, DataWriterError> = super::run_insert_or_update(
            5,
            || panic!("create must not run while the row is there"),
            |e: &mut LoopEntity| e.value != 100,
            || {
                read_calls.set(read_calls.get() + 1);
                async {
                    Ok(Some(LoopEntity {
                        version: 3,
                        value: 100,
                    }))
                }
            },
            |e: LoopEntity| {
                insert_calls.set(insert_calls.get() + 1);
                async move { (e, Ok(())) }
            },
            |e: LoopEntity| {
                replace_calls.set(replace_calls.get() + 1);
                async move { (e, Ok(())) }
            },
        )
        .await;

        assert_eq!(
            result.unwrap(),
            LoopEntity {
                version: 3,
                value: 100
            },
            "the row comes back exactly as it was read"
        );
        assert_eq!(read_calls.get(), 1);
        assert_eq!(replace_calls.get(), 0, "declining must not send a Replace");
        assert_eq!(insert_calls.get(), 0);
    }

    #[test]
    fn create_may_not_build_an_entity_under_another_key() {
        let entity = TestEntity {
            partition_key: "pk".to_string(),
            row_key: "rk".to_string(),
        };

        assert!(super::ensure_entity_keys_match(&entity, "pk", "rk", "create").is_ok());

        match super::ensure_entity_keys_match(&entity, "pk", "other-rk", "create") {
            Err(DataWriterError::Error(msg)) => {
                assert!(
                    msg.contains("'other-rk'"),
                    "the message must name both keys: {}",
                    msg
                );
                assert!(
                    msg.contains("'rk'"),
                    "the message must name both keys: {}",
                    msg
                );
            }
            other => panic!("expected a key mismatch error, got {:?}", other),
        }
    }
}
