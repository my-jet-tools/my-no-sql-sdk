//! `MyNoSqlDataReaderMock` stands for the tcp reader in the test of a service - so a read has to
//! answer the way the tcp reader answers it. A read through a filter did not: for a partition
//! none of whose rows passed the filter the mock answered `None` - "no such partition" - where
//! the tcp reader answers `Some` of an empty list.

use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp};
use my_no_sql_tcp_shared::sync_to_main::SyncToMainNodeHandler;

use crate::{
    subscribers::UpdateEvent, MyNoSqlDataReader, MyNoSqlDataReaderMock, MyNoSqlDataReaderTcp,
};

#[derive(serde::Serialize, serde::Deserialize)]
struct TestEntity {
    #[serde(rename = "PartitionKey")]
    partition_key: String,
    #[serde(rename = "RowKey")]
    row_key: String,
}

impl MyNoSqlEntity for TestEntity {
    const TABLE_NAME: &'static str = "test-table";
    const LAZY_DESERIALIZATION: bool = false;

    fn get_partition_key(&self) -> &str {
        self.partition_key.as_str()
    }

    fn get_row_key(&self) -> &str {
        self.row_key.as_str()
    }

    fn get_time_stamp(&self) -> Timestamp {
        rust_extensions::date_time::DateTimeAsMicroseconds::new(0).into()
    }
}

impl MyNoSqlEntitySerializer for TestEntity {
    fn serialize_entity(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    fn deserialize_entity(src: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(src).map_err(|err| format!("{}", err))
    }
}

fn row_keys_of_vec(rows: Option<Vec<Arc<TestEntity>>>) -> Option<Vec<String>> {
    rows.map(|rows| rows.iter().map(|row| row.row_key.clone()).collect())
}

fn row_keys_of_map(rows: Option<BTreeMap<String, Arc<TestEntity>>>) -> Option<Vec<String>> {
    rows.map(|rows| rows.into_keys().collect())
}

/// What the reads through a filter answer: for a filter which takes some rows of the partition,
/// for one which takes none of them, and for a partition which is not there.
async fn reads_through_a_filter<TReader: MyNoSqlDataReader<TestEntity>>(
    reader: &TReader,
) -> Vec<Option<Vec<String>>> {
    let mut result = Vec::new();

    for (partition_key, row_key_to_take) in
        [("pk", "rk2"), ("pk", "no-such-row"), ("no-such-pk", "rk2")]
    {
        result.push(row_keys_of_vec(
            reader
                .get_entities(partition_key)
                .get_as_vec_with_filter(|row| row.row_key == row_key_to_take)
                .await,
        ));

        result.push(row_keys_of_map(
            reader
                .get_entities(partition_key)
                .get_as_btree_map_with_filter(|row| row.row_key == row_key_to_take)
                .await,
        ));
    }

    result
}

#[tokio::test]
async fn a_read_through_a_filter_answers_the_way_the_tcp_reader_answers() {
    let mock = MyNoSqlDataReaderMock::<TestEntity>::new();

    mock.update(
        ["rk1", "rk2"]
            .into_iter()
            .map(|row_key| {
                Arc::new(TestEntity {
                    partition_key: "pk".to_string(),
                    row_key: row_key.to_string(),
                })
            })
            .collect::<Vec<_>>()
            .into_iter(),
    );

    let reader = MyNoSqlDataReaderTcp::<TestEntity>::new(Arc::new(SyncToMainNodeHandler::new(
        my_logger::LOGGER.clone(),
    )));

    reader.update_rows(
        br#"[{"PartitionKey":"pk","RowKey":"rk1"},{"PartitionKey":"pk","RowKey":"rk2"}]"#.to_vec(),
    );

    let of_the_reader = reads_through_a_filter(&reader).await;

    assert_eq!(
        of_the_reader,
        vec![
            // some rows pass
            Some(vec!["rk2".to_string()]),
            Some(vec!["rk2".to_string()]),
            // the partition is there, none of its rows passes
            Some(vec![]),
            Some(vec![]),
            // no such partition
            None,
            None,
        ]
    );

    assert_eq!(reads_through_a_filter(&mock).await, of_the_reader);
}
