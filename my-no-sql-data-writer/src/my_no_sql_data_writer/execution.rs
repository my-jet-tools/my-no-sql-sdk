use flurl::{body::HttpRequestBody, FlUrl, FlUrlResponse};
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

const API_SEGMENT: &str = "api";

const ROW_CONTROLLER: &str = "Row";
const ROWS_CONTROLLER: &str = "Rows";
const BULK_CONTROLLER: &str = "Bulk";
const PARTITIONS_CONTROLLER: &str = "Partitions";

/// The rows counter sits at the root of the api - `/api/Count` - not under `Row`. The `Row`
/// it is grouped under in the server's swagger is not part of its path.
const COUNT_SEGMENT: &str = "Count";

pub async fn create_table_if_not_exists(
    flurl: FlUrl,
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

    let fl_url = params.populate_params(fl_url);

    let mut response = fl_url.post(HttpRequestBody::Empty).await?;

    create_table_errors_handler(&mut response, "create_table_if_not_exists", url).await
}

pub async fn create_table(
    flurl: FlUrl,
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

    let fl_url = params.populate_params(fl_url);

    let mut response = fl_url.post(HttpRequestBody::Empty).await?;

    create_table_errors_handler(&mut response, "create_table", url).await
}

/// POST /api/Row/Insert - writes the row only if it is not there yet. A key which is already
/// taken comes back as [`DataWriterError::RecordAlreadyExists`], and it is a reliable answer:
/// the server re-checks the key under the table write lock while inserting, so of two
/// concurrent inserts of the same partition+row exactly one succeeds and the other gets that
/// error. That is what makes `Insert` usable as the create half of an insert-or-update loop
/// (see `MyNoSqlDataWriter::insert_or_update`).
pub async fn insert_entity<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: FlUrl,
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

    if is_ok_result(&response) {
        return Ok(());
    }

    // Turns the server's `RecordAlreadyExists` contract into the typed variant instead of an
    // opaque `Error(<body>)` - a caller which is racing another writer has to be able to tell
    // "the key is taken" from "the write failed".
    check_error(&mut response).await?;

    let reason = response.receive_body().await?;
    let reason = String::from_utf8(reason)?;
    return Err(DataWriterError::Error(reason));
}

pub async fn insert_or_replace_entity<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
    entity: &TEntity,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let entity = entity.serialize_entity();

    let response = flurl
        .append_path_segment(ROW_CONTROLLER)
        .append_path_segment("InsertOrReplace")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(HttpRequestBody::Json(entity))
        .await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    let body = response.receive_body().await?;
    let body = String::from_utf8(body)?;

    return Err(DataWriterError::Error(body));
}

/// PUT /api/Row/Replace — optimistic-concurrency replace. The entity must carry the
/// `TimeStamp` it was read with; the server compares it to the stored row's `TimeStamp`:
/// equal → replaced (200); different → 409 [`DataWriterError::RecordIsChanged`]; row
/// missing → 404 [`DataWriterError::RecordNotFound`]; no `TimeStamp` → 400. Use it as
/// read-version → mutate → write-with-that-version; on a conflict re-read and retry (see
/// `MyNoSqlDataWriter::update_entity`).
pub async fn replace_entity<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: FlUrl,
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

    if response.get_status_code() == 404 {
        let body = response.receive_body().await?;
        let message = String::from_utf8(body)
            .unwrap_or_else(|_| "Record not found".to_string());
        return Err(DataWriterError::RecordNotFound(message));
    }

    // Handles 400 (deserialize_error) and 409 (RecordIsChanged).
    check_error(&mut response).await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    let body = response.receive_body().await?;
    let body = String::from_utf8(body)?;
    return Err(DataWriterError::Error(body));
}

pub async fn bulk_insert_or_replace<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
    entities: &[TEntity],
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    if entities.is_empty() {
        return Ok(());
    }

    let response = flurl
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("InsertOrReplace")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(serialize_entities_to_body(entities))
        .await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    let reason = response.receive_body().await?;
    let reason = String::from_utf8(reason)?;
    return Err(DataWriterError::Error(reason));
}

/// POST /api/Bulk/InsertOrReplace with `useTimestamp=true` — bulk insert-or-replace that
/// KEEPS each entity's own `TimeStamp` instead of letting the server stamp its own clock.
///
/// Unlike [`bulk_insert_or_replace_if_new`], this is an **unconditional** replace (no
/// "strictly greater" check): every row is written, but the stored row carries the
/// client-supplied `TimeStamp`. Because of that the `TimeStamp` is **mandatory** — a
/// default/unset one serializes to `null`/omitted and the server answers **HTTP 400**.
/// An empty slice is a no-op.
pub async fn bulk_insert_or_update_with_own_timestamp<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
    entities: &[TEntity],
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    if entities.is_empty() {
        return Ok(());
    }

    debug_assert!(
        entities.iter().all(|e| !e.get_time_stamp().is_default()),
        "bulk_insert_or_update_with_own_timestamp requires every entity to carry its own \
         (non-default) TimeStamp; a default Timestamp serializes to null/omitted and the \
         server rejects it with HTTP 400"
    );

    let response = flurl
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("InsertOrReplace")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .append_query_param("useTimestamp", Some("true"))
        .post(serialize_entities_to_body(entities))
        .await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    let reason = response.receive_body().await?;
    let reason = String::from_utf8(reason)?;
    return Err(DataWriterError::Error(reason));
}

/// Deletes rows described as PartitionKey -> RowKeys
pub async fn bulk_delete<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: FlUrl,
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

    let response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("Delete")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(HttpRequestBody::Json(body))
        .await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    let reason = response.receive_body().await?;
    let reason = String::from_utf8(reason)?;
    return Err(DataWriterError::Error(reason));
}

/// POST /api/Bulk/DeleteIf — optimistic-concurrency delete of a whole batch. Each entity's
/// own `TimeStamp` is the version to match, so pass the entities exactly as they were read.
///
/// The answer is always a **partial success** (HTTP 200): rows which are still at the
/// version sent are deleted, every other one stays in the table and comes back in
/// [`BulkDeleteIfResult::skipped`] with the reason - `TimeStampMismatch` (rewritten
/// meanwhile) or `NotFound` (no such row). A conflict here is data, not an error - unlike
/// [`delete_row_if`], which answers 409 for the single row it addresses.
///
/// Every entity must carry a real (non-default) `TimeStamp`: an unreadable version could
/// never match a stored one, so the server refuses the **whole batch** with HTTP 400 instead
/// of reporting that row as skipped. An empty slice is a no-op (no request at all), like
/// [`bulk_delete`].
pub async fn bulk_delete_if<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: FlUrl,
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

    send_bulk_delete_if::<TEntity>(flurl, body, sync_period).await
}

/// [`bulk_delete_if`] taking the keys and versions on their own instead of whole entities —
/// for when the versions come from somewhere else than the entities (a projection, a change
/// log, another service). Same contract in every other respect.
pub async fn bulk_delete_if_rows<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: FlUrl,
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

    send_bulk_delete_if::<TEntity>(flurl, body, sync_period).await
}

async fn send_bulk_delete_if<TEntity: MyNoSqlEntity>(
    flurl: FlUrl,
    body: Vec<u8>,
    sync_period: &DataSynchronizationPeriod,
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
        let reason = response.receive_body().await?;
        let reason = String::from_utf8(reason)?;
        return Err(DataWriterError::Error(reason));
    }

    deserialize_bulk_delete_if_result(response.get_body_as_slice().await?)
}

pub async fn get_entity<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: FlUrl,
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
        request = update_read_statistics.fill_fields(request);
    }

    let mut response = request.get().await?;

    if response.get_status_code() == 404 {
        return Ok(None);
    }

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        let entity = TEntity::deserialize_entity(response.get_body_as_slice().await?).unwrap();
        return Ok(Some(entity));
    }

    return Ok(None);
}

pub async fn get_by_partition_key<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
    partition_key: &str,
    update_read_statistics: Option<&UpdateReadStatistics>,
) -> Result<Option<Vec<TEntity>>, DataWriterError> {
    let mut request = flurl
        .append_path_segment(ROW_CONTROLLER)
        .with_partition_key_as_query_param(partition_key)
        .with_table_name_as_query_param(TEntity::TABLE_NAME);

    if let Some(update_read_statistics) = update_read_statistics {
        request = update_read_statistics.fill_fields(request);
    }

    let mut response = request.get().await?;

    if response.get_status_code() == 404 {
        return Ok(None);
    }

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        let entities = deserialize_entities(response.get_body_as_slice().await?)?;
        return Ok(Some(entities));
    }

    return Ok(None);
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
    flurl: FlUrl,
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
        let status_code = response.get_status_code();
        let body = response.get_body_as_slice().await?;
        return Err(DataWriterError::Error(format!(
            "Rows count of table {} returned status code {}. Body: {}",
            table_name,
            status_code,
            String::from_utf8_lossy(body)
        )));
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
    flurl: FlUrl,
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
    flurl: FlUrl,
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

pub async fn get_by_row_key<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: FlUrl,
    row_key: &str,
) -> Result<Option<Vec<TEntity>>, DataWriterError> {
    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(ROW_CONTROLLER)
        .with_row_key_as_query_param(row_key)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .get()
        .await?;

    if response.get_status_code() == 404 {
        return Ok(None);
    }

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        let entities = deserialize_entities(response.get_body_as_slice().await?)?;
        return Ok(Some(entities));
    }

    return Ok(None);
}

pub async fn get_partition_keys(
    flurl: FlUrl,
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

    if response.get_status_code() == 404 {
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

    return Ok(vec![]);
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
    flurl: FlUrl,
) -> Result<Option<TResult>, DataWriterError> {
    let entity: Option<TEntity> =
        delete_row(flurl, TResult::PARTITION_KEY, TResult::ROW_KEY).await?;

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
    flurl: FlUrl,
    row_key: &str,
) -> Result<Option<TResult>, DataWriterError> {
    let entity: Option<TEntity> = delete_row(flurl, TResult::PARTITION_KEY, row_key).await?;

    match entity {
        Some(entity) => Ok(Some(entity.into())),
        None => Ok(None),
    }
}

pub async fn delete_row<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: FlUrl,
    partition_key: &str,
    row_key: &str,
) -> Result<Option<TEntity>, DataWriterError> {
    let mut response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(ROW_CONTROLLER)
        .with_partition_key_as_query_param(partition_key)
        .with_row_key_as_query_param(row_key)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .delete()
        .await?;

    if response.get_status_code() == 404 {
        return Ok(None);
    }

    check_error(&mut response).await?;

    if response.get_status_code() == 200 {
        let entity = TEntity::deserialize_entity(response.get_body_as_slice().await?).unwrap();
        return Ok(Some(entity));
    }

    return Ok(None);
}

/// DELETE /api/Row/DeleteIf — optimistic-concurrency delete: the row goes away only while
/// the `TimeStamp` stored in the table is still `time_stamp`, and the deleted row comes back
/// in the body.
///
/// Same codes as [`replace_entity`], mapped the way [`delete_row`] maps them: 200 → the row
/// was deleted → `Ok(Some(entity))`; 404 → there is no such row → `Ok(None)`; 409 → the row
/// is there but at another version, somebody rewrote it between the read and this call →
/// [`DataWriterError::RecordIsChanged`]; 400 → the table is missing, or `time_stamp` is not
/// a readable `TimeStamp`.
///
/// Use it as read → decide → delete exactly the version that was read. On a conflict re-read
/// and decide again — the row may no longer be one you want to delete.
pub async fn delete_row_if<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: FlUrl,
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

    if response.get_status_code() == 404 {
        return Ok(None);
    }

    // Handles 400 (deserialize_error) and 409 (RecordIsChanged).
    check_error(&mut response).await?;

    if response.get_status_code() == 200 {
        let entity = TEntity::deserialize_entity(response.get_body_as_slice().await?).unwrap();
        return Ok(Some(entity));
    }

    return Ok(None);
}

pub async fn delete_partitions(
    flurl: FlUrl,
    table_name: &str,
    partition_keys: &[&str],
) -> Result<(), DataWriterError> {
    let mut response = flurl
        .append_path_segment(ROWS_CONTROLLER)
        .with_table_name_as_query_param(table_name)
        .with_partition_keys_as_query_param(partition_keys)
        .delete()
        .await?;

    if response.get_status_code() == 404 {
        return Ok(());
    }

    check_error(&mut response).await?;

    return Ok(());
}

pub async fn get_all<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send>(
    flurl: FlUrl,
) -> Result<Option<Vec<TEntity>>, DataWriterError> {
    let mut response = flurl
        .append_path_segment(ROW_CONTROLLER)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .get()
        .await?;

    if response.get_status_code() == 404 {
        return Ok(None);
    }

    check_error(&mut response).await?;

    if is_ok_result(&response) {
        let entities = deserialize_entities(response.get_body_as_slice().await?)?;
        return Ok(Some(entities));
    }

    return Ok(None);
}

/// POST /api/Bulk/CleanAndBulkInsert — **transactionally replaces the whole table** with
/// `entities`. The clean and the insert are one server-side operation, published to
/// subscribers as a single `InitTable` packet which the reader applies under one lock, so the
/// table is never observed empty or half-filled: a concurrent read sees either the entire
/// previous snapshot or the entire new one. That atomic swap is the reason this endpoint
/// exists — a `DeletePartitions` + `BulkInsertOrReplace` pair does not give it.
pub async fn clean_table_and_bulk_insert<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
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

    return Ok(());
}

/// [`clean_table_and_bulk_insert`] scoped to one partition (`partitionKey` query param): the
/// partition is **transactionally replaced** with `entities` and the rest of the table is left
/// untouched. Published as a single `InitPartition` packet and applied by the reader under one
/// lock — the partition is never observed empty or half-filled.
pub async fn clean_partition_and_bulk_insert<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
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

    return Ok(());
}

/// [`clean_table_and_bulk_insert`] with `useTimestamp=true`: the whole table is cleaned and
/// re-inserted in the same transactional swap (never observed empty), but each row keeps its
/// **own `TimeStamp`** instead of the server clock.
/// Every entity must carry a real (non-default) `TimeStamp`, otherwise the server rejects
/// the request with HTTP 400. (An empty slice still cleans the table — nothing to insert.)
pub async fn clean_table_and_bulk_insert_with_own_timestamp<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
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

    return Ok(());
}

/// [`clean_partition_and_bulk_insert`] with `useTimestamp=true`: the partition is cleaned and
/// re-inserted in the same transactional swap (never observed empty), but each row keeps its
/// **own `TimeStamp`**. Every entity must carry a real
/// (non-default) `TimeStamp`, otherwise the server rejects the request with HTTP 400.
pub async fn clean_partition_and_bulk_insert_with_own_timestamp<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
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

    return Ok(());
}

/// POST /api/Row/InsertOrReplaceIfNew — inserts a missing row, or replaces the stored
/// one only when the incoming `TimeStamp` is strictly greater.
///
/// Unlike every other write, the server does NOT stamp its own time here: the client's
/// `TimeStamp` is the object's version and is mandatory. A default (unset) `Timestamp`
/// serializes to `null` / is omitted, and the server answers HTTP 400. The caller must
/// set `entity.time_stamp` to a real value (e.g. `DateTimeAsMicroseconds::now().into()`).
pub async fn insert_or_replace_entity_if_new<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
    entity: &TEntity,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    debug_assert!(
        !entity.get_time_stamp().is_default(),
        "InsertOrReplaceIfNew requires the entity to carry its own (non-default) TimeStamp; \
         a default Timestamp serializes to null/omitted and the server rejects it with HTTP 400"
    );

    let response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(ROW_CONTROLLER)
        .append_path_segment("InsertOrReplaceIfNew")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(HttpRequestBody::Json(entity.serialize_entity()))
        .await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    let reason = response.receive_body().await?;
    let reason = String::from_utf8(reason)?;
    return Err(DataWriterError::Error(reason));
}

/// POST /api/Bulk/InsertOrReplaceIfNew — same rule as [`insert_or_replace_entity_if_new`]
/// applied per row. Every entity must carry its own non-default `TimeStamp`. An empty
/// slice is a no-op (early `Ok(())`, like `bulk_insert_or_replace`).
pub async fn bulk_insert_or_replace_if_new<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
    entities: &[TEntity],
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    if entities.is_empty() {
        return Ok(());
    }

    debug_assert!(
        entities.iter().all(|e| !e.get_time_stamp().is_default()),
        "InsertOrReplaceIfNew requires every entity to carry its own (non-default) TimeStamp; \
         a default Timestamp serializes to null/omitted and the server rejects it with HTTP 400"
    );

    let response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("InsertOrReplaceIfNew")
        .append_data_sync_period(sync_period)
        .with_table_name_as_query_param(TEntity::TABLE_NAME)
        .post(serialize_entities_to_body(entities))
        .await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    let reason = response.receive_body().await?;
    let reason = String::from_utf8(reason)?;
    return Err(DataWriterError::Error(reason));
}

/// POST /api/Bulk/InsertOrReplaceIfNewByChunks — uploads one chunk of rows aside from the
/// table. Pass `process_id = None` to start a new process (the server issues an id and
/// returns it in `{ "processId": ".." }`); pass the previously issued id to append a
/// further chunk. Either way the (echoed) process id is returned. Nothing is applied
/// until the commit. Each row must carry its own non-default `TimeStamp`.
pub async fn insert_or_replace_if_new_by_chunks_upload<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
    entities: &[TEntity],
    process_id: Option<&str>,
) -> Result<String, DataWriterError> {
    debug_assert!(
        entities.iter().all(|e| !e.get_time_stamp().is_default()),
        "InsertOrReplaceIfNew requires every entity to carry its own (non-default) TimeStamp; \
         a default Timestamp serializes to null/omitted and the server rejects the commit"
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

    if !is_ok_result(&response) {
        let reason = response.receive_body().await?;
        let reason = String::from_utf8(reason)?;
        return Err(DataWriterError::Error(reason));
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
pub async fn insert_or_replace_if_new_by_chunks_commit(
    flurl: FlUrl,
    process_id: &str,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("InsertOrReplaceIfNewByChunksCommit")
        .append_query_param("processId", Some(process_id))
        .append_data_sync_period(sync_period)
        .post(HttpRequestBody::Empty)
        .await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    let reason = response.receive_body().await?;
    let reason = String::from_utf8(reason)?;
    return Err(DataWriterError::Error(reason));
}

/// POST /api/Bulk/InsertOrReplaceIfNewByChunksCancel — drops the accumulated chunks. The
/// table is not touched.
pub async fn insert_or_replace_if_new_by_chunks_cancel(
    flurl: FlUrl,
    process_id: &str,
) -> Result<(), DataWriterError> {
    let response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("InsertOrReplaceIfNewByChunksCancel")
        .append_query_param("processId", Some(process_id))
        .post(HttpRequestBody::Empty)
        .await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    let reason = response.receive_body().await?;
    let reason = String::from_utf8(reason)?;
    return Err(DataWriterError::Error(reason));
}

/// POST /api/Bulk/CleanAndBulkInsertByChunks with `useTimestamp=true` — uploads one chunk of
/// a clean-and-bulk-insert process that keeps each row's own `TimeStamp`. `process_id = None`
/// starts a new process (the server issues the id and returns it); `partition_key` is honored
/// only on that first (start) chunk and scopes the clean to that partition (`None` = whole
/// table). Pass the issued id on every following chunk. Nothing is applied until the commit —
/// the chunks sit in a server-side accumulator while readers keep being served the current
/// snapshot. Each row must carry a non-default `TimeStamp`.
pub async fn clean_and_bulk_insert_by_chunks_with_own_timestamp_upload<
    TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send,
>(
    flurl: FlUrl,
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

    if !is_ok_result(&response) {
        let reason = response.receive_body().await?;
        let reason = String::from_utf8(reason)?;
        return Err(DataWriterError::Error(reason));
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
pub async fn clean_and_bulk_insert_by_chunks_commit(
    flurl: FlUrl,
    process_id: &str,
    sync_period: &DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("CleanAndBulkInsertByChunksCommit")
        .append_query_param("processId", Some(process_id))
        .append_data_sync_period(sync_period)
        .post(HttpRequestBody::Empty)
        .await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    let reason = response.receive_body().await?;
    let reason = String::from_utf8(reason)?;
    return Err(DataWriterError::Error(reason));
}

/// POST /api/Bulk/CleanAndBulkInsertByChunksCancel — drops the accumulated chunks. The table
/// is not touched.
pub async fn clean_and_bulk_insert_by_chunks_cancel(
    flurl: FlUrl,
    process_id: &str,
) -> Result<(), DataWriterError> {
    let response = flurl
        .append_path_segment(API_SEGMENT)
        .append_path_segment(BULK_CONTROLLER)
        .append_path_segment("CleanAndBulkInsertByChunksCancel")
        .append_query_param("processId", Some(process_id))
        .post(HttpRequestBody::Empty)
        .await?;

    if is_ok_result(&response) {
        return Ok(());
    }

    let reason = response.receive_body().await?;
    let reason = String::from_utf8(reason)?;
    return Err(DataWriterError::Error(reason));
}

#[derive(Deserialize)]
struct BulkProcessResponseContract {
    #[serde(rename = "processId")]
    process_id: String,
}

fn is_ok_result(response: &FlUrlResponse) -> bool {
    response.get_status_code() >= 200 && response.get_status_code() < 300
}

fn serialize_entities_to_body<TEntity: MyNoSqlEntity + MyNoSqlEntitySerializer>(
    entities: &[TEntity],
) -> HttpRequestBody {
    if entities.len() == 0 {
        HttpRequestBody::Json(vec![b'[', b']']);
    }

    let mut json_array_writer = JsonArrayWriter::new();

    for entity in entities {
        let payload = entity.serialize_entity();
        let payload: RawJsonObject = payload.into();
        json_array_writer = json_array_writer.write(payload);
    }

    HttpRequestBody::Json(json_array_writer.build().into_bytes())
}

async fn check_error(response: &mut FlUrlResponse) -> Result<(), DataWriterError> {
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
/// more"); every caller maps 404 before `check_error` today, so it is listed here to keep the
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
async fn is_table_not_found(response: &mut FlUrlResponse) -> Result<bool, DataWriterError> {
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
    response: &mut FlUrlResponse,
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

    let json_array_iterator = JsonArrayIterator::new(src);

    if let Err(err) = &json_array_iterator {
        panic!(
            "Can not deserialize entities for table: {}. Err: {:?}",
            TEntity::TABLE_NAME,
            err
        );
    }

    let json_array_iterator = json_array_iterator.unwrap();

    while let Some(item) = json_array_iterator.get_next() {
        let itm = item.unwrap();

        match TEntity::deserialize_entity(itm.as_bytes()) {
            Ok(entity) => {
                result.push(entity);
            }
            Err(err) => {
                println!(
                    "Table: '{}', Entity: {:?}",
                    TEntity::TABLE_NAME,
                    std::str::from_utf8(itm.as_bytes())
                );
                panic!("Can not deserialize entity: {}", err);
            }
        }
    }
    Ok(result)

    /*
    let mut result = Vec::new();



    for itm in JsonArrayIterator::new(src) {
        let itm = itm.unwrap();

        result.push(TEntity::deserialize_entity(itm).unwrap());
    }
    Ok(result)
     */
}

async fn create_table_errors_handler(
    response: &mut FlUrlResponse,
    process_name: &'static str,
    url: &str,
) -> Result<(), DataWriterError> {
    if is_ok_result(response) {
        return Ok(());
    }

    let result = deserialize_error(response).await?;

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
        // wrapper methods debug_assert on a non-default TimeStamp.
        assert!(
            json.get("TimeStamp").is_none(),
            "a default Timestamp must not serialize to a value, got: {}",
            String::from_utf8_lossy(&body)
        );
    }

    #[tokio::test]
    async fn empty_bulk_insert_or_replace_if_new_is_ok() {
        // The empty-slice guard returns before any request is built, so no server is needed.
        let flurl = flurl::FlUrl::new("http://127.0.0.1:0");
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
            flurl::FlUrl::new("http://127.0.0.1:0"),
            &[],
            &my_no_sql_abstractions::DataSynchronizationPeriod::Immediately,
        )
        .await
        .unwrap();

        assert_eq!(result.deleted, 0);
        assert!(result.is_all_deleted());

        let result = super::bulk_delete_if_rows::<TimeStampedTestEntity>(
            flurl::FlUrl::new("http://127.0.0.1:0"),
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
    // above through its closures; what this pins is that the closures a caller actually writes
    // satisfy the bounds - including the "which branch won" flag being mutated inside `create`
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
