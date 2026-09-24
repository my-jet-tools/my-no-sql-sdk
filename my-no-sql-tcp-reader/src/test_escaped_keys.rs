//! The reader keeps its own index, keyed by the strings the rows report. A row which arrived
//! over tcp is held as [`LazyMyNoSqlEntity::Raw`] - its keys come out of the raw json - while a
//! mocked one is already deserialized and reports the keys off the struct. Both have to produce
//! the same, logical, key: the master sends delete events by that key, and a mismatch would
//! leave a deleted row in the reader's cache forever.

use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp};
use my_no_sql_core::db_json_entity::DbJsonEntity;
use my_no_sql_tcp_shared::DeleteRowTcpContract;

use crate::{
    subscribers::{EntityRawData, LazyMyNoSqlEntity},
    DataReaderEntitiesSet,
};

/// The escaped spelling the client's serializer produces, and the key it stands for.
const ESCAPED_PARTITION_KEY: &str = "de\\\\mo";
const LOGICAL_PARTITION_KEY: &str = "de\\mo";
const ESCAPED_ROW_KEY: &str = "demo\\\\DIRNGUSDENM50D";
const LOGICAL_ROW_KEY: &str = "demo\\DIRNGUSDENM50D";

#[derive(serde::Serialize, serde::Deserialize)]
struct TestEntity {
    #[serde(rename = "PartitionKey")]
    partition_key: String,
    #[serde(rename = "RowKey")]
    row_key: String,
}

impl MyNoSqlEntity for TestEntity {
    const TABLE_NAME: &'static str = "test-table";
    const LAZY_DESERIALIZATION: bool = true;

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

/// The json the master sends over tcp, with both keys escaped.
fn escaped_json() -> String {
    format!(
        r#"{{"PartitionKey":"{}","RowKey":"{}","TimeStamp":"2020-05-06T07:08:09"}}"#,
        ESCAPED_PARTITION_KEY, ESCAPED_ROW_KEY
    )
}

/// Exactly what the tcp reader builds for a table with lazy deserialization.
fn raw_entity(json: &str) -> LazyMyNoSqlEntity<TestEntity> {
    let data = json.as_bytes().to_vec();
    let db_json_entity = DbJsonEntity::from_slice(&data).unwrap();

    LazyMyNoSqlEntity::Raw(Arc::new(EntityRawData {
        db_json_entity,
        data,
    }))
}

fn index_it(entity: LazyMyNoSqlEntity<TestEntity>) -> DataReaderEntitiesSet<TestEntity> {
    let mut entities = DataReaderEntitiesSet::new(TestEntity::TABLE_NAME);

    let mut src_data = BTreeMap::new();
    src_data.insert(entity.get_partition_key().to_string(), vec![entity]);

    entities.update_rows(src_data, &None);

    entities
}

#[test]
fn a_row_which_arrived_over_tcp_reports_logical_keys() {
    let entity = raw_entity(escaped_json().as_str());

    assert_eq!(entity.get_partition_key(), LOGICAL_PARTITION_KEY);
    assert_eq!(entity.get_row_key(), LOGICAL_ROW_KEY);
}

/// The lazily deserialized row and the deserialized one - the mock path - index identically.
#[test]
fn the_raw_and_the_deserialized_row_agree_on_the_key() {
    let raw = raw_entity(escaped_json().as_str());

    let deserialized: LazyMyNoSqlEntity<TestEntity> =
        TestEntity::deserialize_entity(escaped_json().as_bytes())
            .unwrap()
            .into();

    assert_eq!(raw.get_partition_key(), deserialized.get_partition_key());
    assert_eq!(raw.get_row_key(), deserialized.get_row_key());
}

#[test]
fn a_raw_row_is_found_in_the_index_by_its_logical_key() {
    let entities = index_it(raw_entity(escaped_json().as_str()));

    let table = entities.as_ref().unwrap();

    let partition = table
        .get(LOGICAL_PARTITION_KEY)
        .expect("partition is indexed by the logical key");

    assert!(partition
        .get(LOGICAL_ROW_KEY)
        .is_some(), "row is indexed by the logical key");

    // the raw json spelling is not the key - indexing by it is what used to break point reads
    assert!(table.get(ESCAPED_PARTITION_KEY).is_none());
    assert!(partition.get(ESCAPED_ROW_KEY).is_none());
}

/// The master sends deletes as plain strings; with both sides deriving the same logical key the
/// row actually leaves the cache.
#[test]
fn a_delete_contract_with_the_logical_key_removes_the_row() {
    let mut entities = index_it(raw_entity(escaped_json().as_str()));

    assert_eq!(entities.as_ref().unwrap().len(), 1);

    entities.delete_rows(
        vec![DeleteRowTcpContract {
            partition_key: LOGICAL_PARTITION_KEY.to_string(),
            row_key: LOGICAL_ROW_KEY.to_string(),
        }],
        &None,
    );

    // nothing is left hanging in the cache: the row went, and its now empty partition with it
    assert!(
        entities.as_ref().unwrap().is_empty(),
        "the row survived a delete with its own key"
    );
}

#[test]
fn init_table_indexes_a_raw_row_by_its_logical_key() {
    let mut entities = DataReaderEntitiesSet::new(TestEntity::TABLE_NAME);

    let entity = raw_entity(escaped_json().as_str());

    let mut src_data = BTreeMap::new();
    src_data.insert(entity.get_partition_key().to_string(), vec![entity]);

    entities.init_table(src_data);

    let table = entities.as_ref().unwrap();

    assert!(table
        .get(LOGICAL_PARTITION_KEY)
        .unwrap()
        .get(LOGICAL_ROW_KEY)
        .is_some());
}

/// An InitPartition packet arrives grouped by partition key - the rows inside still have to land
/// under their own row key, or a partition comes back from a clean-and-insert holding a single
/// entry which no point read and no delete event can address.
#[test]
fn init_partition_indexes_a_raw_row_by_its_logical_key() {
    let mut entities = DataReaderEntitiesSet::new(TestEntity::TABLE_NAME);

    let second_row_json = format!(
        r#"{{"PartitionKey":"{}","RowKey":"{}","TimeStamp":"2020-05-06T07:08:09"}}"#,
        ESCAPED_PARTITION_KEY, "second-row"
    );

    let mut src_data = BTreeMap::new();
    src_data.insert(
        LOGICAL_PARTITION_KEY.to_string(),
        vec![
            raw_entity(escaped_json().as_str()),
            raw_entity(second_row_json.as_str()),
        ],
    );

    entities.init_partition(LOGICAL_PARTITION_KEY, src_data);

    let partition = entities
        .as_ref()
        .unwrap()
        .get(LOGICAL_PARTITION_KEY)
        .expect("partition is indexed by the logical key");

    assert_eq!(partition.len(), 2, "every row of the partition is indexed");
    assert!(partition.get(LOGICAL_ROW_KEY).is_some());
    assert!(partition.get("second-row").is_some());

    // the partition key is not a row key - indexing by it is what used to swallow the rows
    assert!(partition.get(LOGICAL_PARTITION_KEY).is_none());
}
