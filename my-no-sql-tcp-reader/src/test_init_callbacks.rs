//! A snapshot packet - `InitTable`, `InitPartition` - is reported to the callbacks partition by
//! partition: one `inserted_or_replaced` call with every row the partition has now, and one
//! `deleted` call with the rows which are gone. For a partition the reader already had it used to
//! be an `inserted_or_replaced` call per row - and a reconnect sends the whole table to a reader
//! which has all of it. A callback which reloads its cache on every call reloaded it once per row.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp};
use my_no_sql_tcp_shared::sync_to_main::SyncToMainNodeHandler;
use parking_lot::Mutex;

use crate::{
    subscribers::{LazyMyNoSqlEntity, MyNoSqlDataReaderCallBacks, UpdateEvent},
    DataReaderEntitiesSet, MyNoSqlDataReader, MyNoSqlDataReaderTcp,
};

/// The partition key and the row key of the mark - see [`TestReader::calls`].
const MARK: &str = "the-mark";

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

/// The payload of a packet - the json array of rows the master sends, given here as
/// (partition key, row key) pairs.
fn packet(rows: &[(&str, &str)]) -> Vec<u8> {
    let rows: Vec<String> = rows
        .iter()
        .map(|(partition_key, row_key)| {
            format!(
                r#"{{"PartitionKey":"{}","RowKey":"{}","TimeStamp":"2020-05-06T07:08:09"}}"#,
                partition_key, row_key
            )
        })
        .collect();

    format!("[{}]", rows.join(",")).into_bytes()
}

/// One call of a callback: which one it was, the partition key and the row keys it was handed.
#[derive(Debug, Clone, PartialEq)]
enum Call {
    InsertedOrReplaced(String, Vec<String>),
    Deleted(String, Vec<String>),
}

fn inserted_or_replaced(partition_key: &str, row_keys: &[&str]) -> Call {
    Call::InsertedOrReplaced(
        partition_key.to_string(),
        row_keys.iter().map(|row_key| row_key.to_string()).collect(),
    )
}

fn deleted(partition_key: &str, row_keys: &[&str]) -> Call {
    Call::Deleted(
        partition_key.to_string(),
        row_keys.iter().map(|row_key| row_key.to_string()).collect(),
    )
}

fn row_keys(entities: &[LazyMyNoSqlEntity<TestEntity>]) -> Vec<String> {
    entities
        .iter()
        .map(|entity| entity.get_row_key().to_string())
        .collect()
}

/// Writes down every call, in the order the calls are made.
struct TestCallbacks {
    calls: Mutex<Vec<Call>>,
}

impl MyNoSqlDataReaderCallBacks<TestEntity> for TestCallbacks {
    fn inserted_or_replaced(
        &self,
        partition_key: &str,
        entities: Vec<LazyMyNoSqlEntity<TestEntity>>,
    ) {
        self.calls.lock().push(Call::InsertedOrReplaced(
            partition_key.to_string(),
            row_keys(&entities),
        ));
    }

    fn deleted(&self, partition_key: &str, entities: Vec<LazyMyNoSqlEntity<TestEntity>>) {
        self.calls.lock().push(Call::Deleted(
            partition_key.to_string(),
            row_keys(&entities),
        ));
    }
}

/// A tcp reader with callbacks assigned, the way an application has it. It is never connected -
/// the tests hand it the packets the socket would.
struct TestReader {
    reader: MyNoSqlDataReaderTcp<TestEntity>,
    callbacks: Arc<TestCallbacks>,
}

impl TestReader {
    fn new() -> Self {
        let reader = MyNoSqlDataReaderTcp::new(Arc::new(SyncToMainNodeHandler::new(
            my_logger::LOGGER.clone(),
        )));

        let callbacks = Arc::new(TestCallbacks {
            calls: Mutex::new(Vec::new()),
        });

        reader.assign_callback(callbacks.clone());

        Self { reader, callbacks }
    }

    /// Every call the packets have made, in the order they were made.
    ///
    /// The callbacks are delivered through an events loop - one by one, in the order they were
    /// queued. So a mark is queued last: a row written to a partition of its own. Once its call
    /// is there, the calls of every packet before it are there as well - none is still on its way
    /// to be missed by the count. The reader is consumed: the mark stays in its table.
    async fn calls(self) -> Vec<Call> {
        self.reader.update_rows(packet(&[(MARK, MARK)]));

        let the_mark = inserted_or_replaced(MARK, &[MARK]);

        for _ in 0..500 {
            {
                let mut calls = self.callbacks.calls.lock();
                if calls.last() == Some(&the_mark) {
                    calls.pop();
                    return calls.clone();
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!(
            "the mark did not reach the callbacks. Got: {:?}",
            self.callbacks.calls.lock()
        );
    }
}

/// A reconnect: the master sends the whole table again, to a reader which has all of it.
#[tokio::test]
async fn an_init_table_packet_makes_one_call_per_partition() {
    let test_reader = TestReader::new();

    let table = [
        ("pk1", "rk1"),
        ("pk1", "rk2"),
        ("pk1", "rk3"),
        ("pk2", "rk1"),
        ("pk2", "rk2"),
    ];

    test_reader.reader.init_table(packet(&table));
    test_reader.reader.init_table(packet(&table));

    assert_eq!(
        test_reader.calls().await,
        vec![
            // the first snapshot
            inserted_or_replaced("pk1", &["rk1", "rk2", "rk3"]),
            inserted_or_replaced("pk2", &["rk1", "rk2"]),
            // the second one used to make five calls, a row each. Not a row has changed, and all
            // of them are reported nevertheless - the reader does not compare a row with the one
            // it replaces
            inserted_or_replaced("pk1", &["rk1", "rk2", "rk3"]),
            inserted_or_replaced("pk2", &["rk1", "rk2"]),
        ]
    );
}

#[tokio::test]
async fn an_init_table_packet_reports_what_is_gone_in_one_call_per_partition() {
    let test_reader = TestReader::new();

    test_reader.reader.init_table(packet(&[
        ("pk1", "rk1"),
        ("pk1", "rk2"),
        ("pk1", "rk3"),
        ("pk2", "rk1"),
        ("pk2", "rk2"),
    ]));

    test_reader.reader.init_table(packet(&[
        ("pk1", "rk3"),
        ("pk1", "rk4"),
        ("pk3", "rk1"),
        ("pk3", "rk2"),
    ]));

    assert_eq!(
        test_reader.calls().await,
        vec![
            // the first snapshot
            inserted_or_replaced("pk1", &["rk1", "rk2", "rk3"]),
            inserted_or_replaced("pk2", &["rk1", "rk2"]),
            // pk1 was there: what it has now, and what it has lost
            inserted_or_replaced("pk1", &["rk3", "rk4"]),
            deleted("pk1", &["rk1", "rk2"]),
            // pk3 is new
            inserted_or_replaced("pk3", &["rk1", "rk2"]),
            // pk2 is not in the table any more
            deleted("pk2", &["rk1", "rk2"]),
        ]
    );
}

/// What a `clean_partition_and_bulk_insert` brings to a subscriber.
#[tokio::test]
async fn an_init_partition_packet_makes_one_call_for_the_rows_and_one_for_the_deleted_ones() {
    let test_reader = TestReader::new();

    test_reader.reader.init_table(packet(&[
        ("pk1", "rk1"),
        ("pk1", "rk2"),
        ("pk1", "rk3"),
        ("pk2", "rk1"),
    ]));

    test_reader.reader.init_partition(
        "pk1",
        packet(&[("pk1", "rk2"), ("pk1", "rk3"), ("pk1", "rk4")]),
    );

    assert_eq!(
        test_reader.calls().await,
        vec![
            // the first snapshot
            inserted_or_replaced("pk1", &["rk1", "rk2", "rk3"]),
            inserted_or_replaced("pk2", &["rk1"]),
            // the partition as the packet left it - used to be three calls, a row each
            inserted_or_replaced("pk1", &["rk2", "rk3", "rk4"]),
            // the row the previous partition had and the new one does not
            deleted("pk1", &["rk1"]),
        ]
    );
}

#[tokio::test]
async fn an_init_partition_packet_for_a_new_partition_makes_one_call() {
    let test_reader = TestReader::new();

    test_reader
        .reader
        .init_table(packet(&[("pk1", "rk1"), ("pk1", "rk2")]));

    test_reader.reader.init_partition(
        "pk2",
        packet(&[("pk2", "rk1"), ("pk2", "rk2"), ("pk2", "rk3")]),
    );

    assert_eq!(
        test_reader.calls().await,
        vec![
            inserted_or_replaced("pk1", &["rk1", "rk2"]),
            inserted_or_replaced("pk2", &["rk1", "rk2", "rk3"]),
        ]
    );
}

/// An `InitPartition` with no rows in it is how the master reports a partition which was deleted.
#[tokio::test]
async fn an_init_partition_packet_without_rows_makes_the_deleted_call_only() {
    let test_reader = TestReader::new();

    test_reader
        .reader
        .init_table(packet(&[("pk1", "rk1"), ("pk1", "rk2"), ("pk2", "rk1")]));

    test_reader.reader.init_partition("pk1", packet(&[]));

    // nothing was there to delete - nothing to report
    test_reader
        .reader
        .init_partition("never-existed", packet(&[]));

    // The partition is gone from the copy - it is not kept as an empty one. It used to be: a
    // deleted partition stayed listed, and stayed `Some` to a read, until the next InitTable.
    assert_eq!(test_reader.reader.get_partition_keys(), vec!["pk2"]);

    for partition_key in ["pk1", "never-existed"] {
        assert!(!test_reader.reader.has_partition(partition_key));
        assert!(test_reader
            .reader
            .get_by_partition_key(partition_key)
            .is_none());
        assert!(test_reader
            .reader
            .get_by_partition_key_as_vec(partition_key)
            .is_none());
    }

    assert!(test_reader.reader.has_partition("pk2"));

    assert_eq!(
        test_reader.calls().await,
        vec![
            inserted_or_replaced("pk1", &["rk1", "rk2"]),
            inserted_or_replaced("pk2", &["rk1"]),
            deleted("pk1", &["rk1", "rk2"]),
        ]
    );
}

/// A partition which comes back after it was deleted is a new one to the callbacks.
#[tokio::test]
async fn a_partition_which_was_deleted_and_comes_back_is_reported_as_a_new_one() {
    let test_reader = TestReader::new();

    test_reader
        .reader
        .init_table(packet(&[("pk1", "rk1"), ("pk1", "rk2")]));

    test_reader.reader.init_partition("pk1", packet(&[]));

    // An empty table is still a table which has arrived.
    assert!(test_reader.reader.get_partition_keys().is_empty());

    test_reader
        .reader
        .init_partition("pk1", packet(&[("pk1", "rk2"), ("pk1", "rk3")]));

    assert_eq!(test_reader.reader.get_partition_keys(), vec!["pk1"]);

    assert_eq!(
        test_reader.calls().await,
        vec![
            inserted_or_replaced("pk1", &["rk1", "rk2"]),
            deleted("pk1", &["rk1", "rk2"]),
            // nothing to compare it with - no `deleted` call for rk1 a second time
            inserted_or_replaced("pk1", &["rk2", "rk3"]),
        ]
    );
}

/// The rows of an `InitPartition` packet, the way `init_partition` takes them.
fn rows(
    partition_key: &str,
    row_keys: &[&str],
) -> BTreeMap<String, Vec<LazyMyNoSqlEntity<TestEntity>>> {
    let rows = row_keys
        .iter()
        .map(|row_key| {
            TestEntity {
                partition_key: partition_key.to_string(),
                row_key: row_key.to_string(),
            }
            .into()
        })
        .collect();

    let mut result = BTreeMap::new();
    result.insert(partition_key.to_string(), rows);
    result
}

fn keys(partition: &BTreeMap<String, LazyMyNoSqlEntity<TestEntity>>) -> Vec<&str> {
    partition.keys().map(|row_key| row_key.as_str()).collect()
}

/// The callbacks are told the difference between the two partitions `init_partition` gives back,
/// so each field has to hold what its name says. They used to be named the other way round.
#[test]
fn init_partition_gives_back_the_partition_it_replaced_and_the_one_it_put() {
    let mut entities = DataReaderEntitiesSet::new(TestEntity::TABLE_NAME);

    let result = entities.init_partition("pk", rows("pk", &["rk1", "rk2"]));

    assert!(result.partition_before.is_none());
    assert_eq!(keys(result.partition_now), vec!["rk1", "rk2"]);

    let result = entities.init_partition("pk", rows("pk", &["rk2", "rk3"]));

    assert_eq!(
        keys(result.partition_before.as_ref().unwrap()),
        vec!["rk1", "rk2"]
    );
    assert_eq!(keys(result.partition_now), vec!["rk2", "rk3"]);
}
