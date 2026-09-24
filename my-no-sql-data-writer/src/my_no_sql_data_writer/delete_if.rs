use my_no_sql_abstractions::{MyNoSqlEntity, Timestamp};
use serde::{Deserialize, Serialize};

use crate::DataWriterError;

/// One row of a conditional delete: which row, and the version the caller read it at.
///
/// Only these three fields travel to the server - unlike every write operation this is not
/// a whole entity. Use it when the versions come from somewhere other than the entities
/// themselves (a projection, a change log, another service); when the entities are at hand,
/// `bulk_delete_if` takes them directly and reads the version out of each one.
#[derive(Debug, Clone)]
pub struct RowToDeleteIf {
    pub partition_key: String,
    pub row_key: String,
    /// The `TimeStamp` the row was read with. A default one is not a version the server can
    /// read and makes the whole batch fail with HTTP 400.
    pub time_stamp: Timestamp,
}

impl RowToDeleteIf {
    pub fn new(
        partition_key: impl Into<String>,
        row_key: impl Into<String>,
        time_stamp: Timestamp,
    ) -> Self {
        Self {
            partition_key: partition_key.into(),
            row_key: row_key.into(),
            time_stamp,
        }
    }

    /// Takes the keys and the read version off an entity - the same three fields
    /// `bulk_delete_if` would send for it.
    pub fn from_entity<TEntity: MyNoSqlEntity>(entity: &TEntity) -> Self {
        Self {
            partition_key: entity.get_partition_key().to_string(),
            row_key: entity.get_row_key().to_string(),
            time_stamp: entity.get_time_stamp(),
        }
    }
}

/// Why a row of a `bulk_delete_if` batch was left in the table.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "String")]
pub enum DeleteIfSkipReason {
    /// There is no such row in the table (any more).
    NotFound,

    /// The row is there, but not at the version which was sent - somebody rewrote it
    /// between the read and this delete.
    TimeStampMismatch,

    /// A reason this client does not know yet. An older client has to survive a newer
    /// server, so an unexpected value is carried through instead of failing the parse of
    /// the whole response.
    Unknown(String),
}

impl DeleteIfSkipReason {
    /// The value as the server spells it - for the two known reasons it round-trips, and an
    /// [`DeleteIfSkipReason::Unknown`] one gives back whatever arrived.
    pub fn as_str(&self) -> &str {
        match self {
            Self::NotFound => "NotFound",
            Self::TimeStampMismatch => "TimeStampMismatch",
            Self::Unknown(value) => value.as_str(),
        }
    }
}

impl From<String> for DeleteIfSkipReason {
    fn from(src: String) -> Self {
        match src.as_str() {
            "NotFound" => Self::NotFound,
            "TimeStampMismatch" => Self::TimeStampMismatch,
            _ => Self::Unknown(src),
        }
    }
}

/// A row `bulk_delete_if` did not delete, and why.
#[derive(Debug, Clone)]
pub struct SkippedRow {
    pub partition_key: String,
    pub row_key: String,
    pub reason: DeleteIfSkipReason,
}

/// The answer of `POST /api/Bulk/DeleteIf`: a partial success. The rows which were still at
/// the version they were read at are gone; every other one stayed in place and is listed in
/// [`Self::skipped`]. This is never an error - a conflict inside a batch is data, not a
/// failure of the request (unlike the single-row delete, which answers 409).
#[derive(Debug, Clone)]
pub struct BulkDeleteIfResult {
    /// How many rows really left the table.
    pub deleted: usize,

    /// The requested rows which were left in place, each with the reason why.
    pub skipped: Vec<SkippedRow>,
}

impl BulkDeleteIfResult {
    /// Nothing was asked for, so nothing was deleted and nothing was skipped - the answer of
    /// an empty batch, which never reaches the server.
    pub(crate) fn nothing_to_delete() -> Self {
        Self {
            deleted: 0,
            skipped: Vec::new(),
        }
    }

    /// Every requested row was deleted - the batch went through without a single conflict or
    /// missing row.
    pub fn is_all_deleted(&self) -> bool {
        self.skipped.is_empty()
    }

    /// The rows which are still in the table at a newer version - the ones worth re-reading
    /// and deciding about again.
    pub fn conflicts(&self) -> impl Iterator<Item = &SkippedRow> {
        self.skipped
            .iter()
            .filter(|itm| itm.reason == DeleteIfSkipReason::TimeStampMismatch)
    }

    /// The rows which were not in the table at all - usually already deleted by somebody
    /// else, so nothing is left to do about them.
    pub fn not_found(&self) -> impl Iterator<Item = &SkippedRow> {
        self.skipped
            .iter()
            .filter(|itm| itm.reason == DeleteIfSkipReason::NotFound)
    }

    /// Whether anything was rewritten under us. `false` with a non-empty
    /// [`Self::skipped`] means the leftovers are only rows which were already gone.
    pub fn has_conflicts(&self) -> bool {
        self.conflicts().next().is_some()
    }
}

/// The wire form of one row of the request body. `TimeStamp` is written in the canonical
/// form of [`Timestamp`] - the server compares parsed moments, not texts, so nothing has to
/// be normalized on top of that.
#[derive(Serialize)]
struct DeleteIfRowContract<'s> {
    #[serde(rename = "PartitionKey")]
    partition_key: &'s str,

    #[serde(rename = "RowKey")]
    row_key: &'s str,

    #[serde(rename = "TimeStamp")]
    time_stamp: Timestamp,
}

#[derive(Deserialize)]
struct BulkDeleteIfResponseContract {
    deleted: usize,

    // A response which does not carry the field at all reads as "nothing was skipped"
    // instead of failing - the same forgiveness `DeleteIfSkipReason::Unknown` gives.
    #[serde(default)]
    skipped: Vec<SkippedDeleteIfRowContract>,
}

#[derive(Deserialize)]
struct SkippedDeleteIfRowContract {
    #[serde(rename = "PartitionKey")]
    partition_key: String,

    #[serde(rename = "RowKey")]
    row_key: String,

    #[serde(rename = "Reason")]
    reason: DeleteIfSkipReason,
}

/// Builds the flat `[{"PartitionKey":..,"RowKey":..,"TimeStamp":..}]` body of
/// `POST /api/Bulk/DeleteIf`. Note it is a plain array - not the `PartitionKey -> RowKeys`
/// map of the unconditional `POST /api/Bulk/Delete`.
pub(crate) fn serialize_rows_to_delete_if<'s>(
    rows: impl Iterator<Item = (&'s str, &'s str, Timestamp)>,
) -> Result<Vec<u8>, DataWriterError> {
    let rows: Vec<DeleteIfRowContract> = rows
        .map(|(partition_key, row_key, time_stamp)| DeleteIfRowContract {
            partition_key,
            row_key,
            time_stamp,
        })
        .collect();

    serde_json::to_vec(&rows).map_err(|err| {
        DataWriterError::Error(format!(
            "Failed to serialize rows to conditionally delete: {:?}",
            err
        ))
    })
}

pub(crate) fn deserialize_bulk_delete_if_result(
    src: &[u8],
) -> Result<BulkDeleteIfResult, DataWriterError> {
    let contract: BulkDeleteIfResponseContract = serde_json::from_slice(src).map_err(|err| {
        DataWriterError::Error(format!(
            "Failed to deserialize BulkDeleteIfResponse: {:?}",
            err
        ))
    })?;

    Ok(BulkDeleteIfResult {
        deleted: contract.deleted,
        skipped: contract
            .skipped
            .into_iter()
            .map(|itm| SkippedRow {
                partition_key: itm.partition_key,
                row_key: itm.row_key,
                reason: itm.reason,
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use my_no_sql_abstractions::Timestamp;
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use super::*;

    fn time_stamp(src: &str) -> Timestamp {
        DateTimeAsMicroseconds::from_str(src).unwrap().into()
    }

    /// The body is the flat array the server expects, with PascalCase field names and the
    /// canonical TimeStamp text.
    #[test]
    fn request_body_is_a_flat_array_of_pk_rk_and_time_stamp() {
        let ts = time_stamp("2026-08-12T18:19:36.776352");

        let body =
            serialize_rows_to_delete_if([("pk1", "rk1", ts), ("pk1", "rk2", ts)].into_iter())
                .unwrap();

        let expected = concat!(
            r#"[{"PartitionKey":"pk1","RowKey":"rk1","TimeStamp":"2026-08-12T18:19:36.776352"},"#,
            r#"{"PartitionKey":"pk1","RowKey":"rk2","TimeStamp":"2026-08-12T18:19:36.776352"}]"#
        );

        assert_eq!(std::str::from_utf8(&body).unwrap(), expected);
    }

    /// A version spelled with trailing zeros is the same moment; it goes out in the
    /// canonical form and the server compares parsed moments, so no extra normalization is
    /// needed on this side.
    #[test]
    fn time_stamp_goes_out_in_its_canonical_form() {
        let rows = [
            ("2026-08-09T16:44:39.540400", "2026-08-09T16:44:39.5404"),
            ("2026-08-09T16:44:39.5404", "2026-08-09T16:44:39.5404"),
            ("2026-08-09T16:44:39", "2026-08-09T16:44:39"),
        ];

        for (src, expected) in rows {
            let body =
                serialize_rows_to_delete_if([("pk", "rk", time_stamp(src))].into_iter()).unwrap();

            assert_eq!(
                std::str::from_utf8(&body).unwrap(),
                format!(
                    r#"[{{"PartitionKey":"pk","RowKey":"rk","TimeStamp":"{}"}}]"#,
                    expected
                ),
                "source: {}",
                src
            );
        }
    }

    #[test]
    fn empty_batch_serializes_to_an_empty_array() {
        let body = serialize_rows_to_delete_if(std::iter::empty()).unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "[]");
    }

    #[test]
    fn rows_can_be_taken_off_entities() {
        let entity = TestEntity {
            partition_key: "pk".to_string(),
            row_key: "rk".to_string(),
            time_stamp: time_stamp("2026-08-12T18:19:36.776352"),
        };

        let row = RowToDeleteIf::from_entity(&entity);

        assert_eq!(row.partition_key, "pk");
        assert_eq!(row.row_key, "rk");
        assert_eq!(row.time_stamp, entity.time_stamp);
    }

    #[test]
    fn response_with_both_known_reasons_is_parsed() {
        let result = deserialize_bulk_delete_if_result(
            br#"{"deleted":2,"skipped":[
                {"PartitionKey":"pk1","RowKey":"rk2","Reason":"TimeStampMismatch"},
                {"PartitionKey":"pk1","RowKey":"rk9","Reason":"NotFound"}
            ]}"#,
        )
        .unwrap();

        assert_eq!(result.deleted, 2);
        assert_eq!(result.skipped.len(), 2);

        assert_eq!(result.skipped[0].partition_key, "pk1");
        assert_eq!(result.skipped[0].row_key, "rk2");
        assert_eq!(
            result.skipped[0].reason,
            DeleteIfSkipReason::TimeStampMismatch
        );

        assert_eq!(result.skipped[1].row_key, "rk9");
        assert_eq!(result.skipped[1].reason, DeleteIfSkipReason::NotFound);

        assert!(!result.is_all_deleted());
        assert!(result.has_conflicts());
        assert_eq!(result.conflicts().count(), 1);
        assert_eq!(result.not_found().count(), 1);
    }

    #[test]
    fn a_full_success_is_parsed() {
        let result = deserialize_bulk_delete_if_result(br#"{"deleted":3,"skipped":[]}"#).unwrap();

        assert_eq!(result.deleted, 3);
        assert!(result.is_all_deleted());
        assert!(!result.has_conflicts());
    }

    /// The reason this client does not know must not take the response down with it - an
    /// old client has to keep working against a newer server.
    #[test]
    fn an_unknown_reason_survives_deserialization() {
        let result = deserialize_bulk_delete_if_result(
            br#"{"deleted":1,"skipped":[
                {"PartitionKey":"pk","RowKey":"rk","Reason":"SomethingBrandNew"}
            ]}"#,
        )
        .unwrap();

        assert_eq!(result.deleted, 1);
        assert_eq!(
            result.skipped[0].reason,
            DeleteIfSkipReason::Unknown("SomethingBrandNew".to_string())
        );
        assert_eq!(result.skipped[0].reason.as_str(), "SomethingBrandNew");

        // An unknown reason is neither of the two known buckets, but it is still a row
        // which was left in place.
        assert!(!result.is_all_deleted());
        assert!(!result.has_conflicts());
        assert_eq!(result.not_found().count(), 0);
    }

    #[test]
    fn known_reasons_round_trip_through_as_str() {
        for reason in [
            DeleteIfSkipReason::NotFound,
            DeleteIfSkipReason::TimeStampMismatch,
        ] {
            assert_eq!(
                DeleteIfSkipReason::from(reason.as_str().to_string()),
                reason
            );
        }
    }

    #[test]
    fn a_response_without_skipped_reads_as_nothing_skipped() {
        let result = deserialize_bulk_delete_if_result(br#"{"deleted":5}"#).unwrap();

        assert_eq!(result.deleted, 5);
        assert!(result.is_all_deleted());
    }

    #[test]
    fn a_broken_response_is_an_error_not_a_panic() {
        assert!(deserialize_bulk_delete_if_result(b"not json at all").is_err());
    }

    struct TestEntity {
        partition_key: String,
        row_key: String,
        time_stamp: Timestamp,
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
            self.time_stamp
        }
    }
}
