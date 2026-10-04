//! `MyNoSqlDataReaderMock` tells the callbacks what `update` and `delete` did to its copy - the
//! way the tcp reader tells them about an `UpdateRows` and a `DeleteRows` packet. It used to keep
//! the callbacks it was assigned and never call them, so a service which reacts to the callbacks
//! could not be tested through the mock.

use std::{sync::Arc, time::Duration};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp};
use my_no_sql_tcp_shared::{sync_to_main::SyncToMainNodeHandler, DeleteRowTcpContract};
use parking_lot::Mutex;

use crate::{
    subscribers::{LazyMyNoSqlEntity, MyNoSqlDataReaderCallBacks, UpdateEvent},
    MyNoSqlDataReader, MyNoSqlDataReaderMock, MyNoSqlDataReaderTcp,
};

/// The partition key and the row key of the mark - see [`TestCallbacks::calls_before_the_mark`].
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

/// The rows `update` is given, written here as (partition key, row key) pairs.
fn rows(rows: &[(&str, &str)]) -> std::vec::IntoIter<Arc<TestEntity>> {
    let rows: Vec<Arc<TestEntity>> = rows
        .iter()
        .map(|(partition_key, row_key)| {
            Arc::new(TestEntity {
                partition_key: partition_key.to_string(),
                row_key: row_key.to_string(),
            })
        })
        .collect();

    rows.into_iter()
}

/// The keys `delete` is given.
fn keys(keys: &[(&str, &str)]) -> std::vec::IntoIter<(String, String)> {
    let keys: Vec<(String, String)> = keys
        .iter()
        .map(|(partition_key, row_key)| (partition_key.to_string(), row_key.to_string()))
        .collect();

    keys.into_iter()
}

/// The same rows the way a tcp reader gets them - the payload of an `UpdateRows` packet, the json
/// array the master sends.
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

impl TestCallbacks {
    /// Every call made before the call of the mark, in the order they were made.
    ///
    /// The callbacks are delivered through an events loop - one by one, in the order they were
    /// queued. So a mark is queued last: a row written to a partition of its own. Once its call
    /// is there, the calls of everything done before it are there as well - none is still on its
    /// way to be missed by the count.
    async fn calls_before_the_mark(&self) -> Vec<Call> {
        let the_mark = inserted_or_replaced(MARK, &[MARK]);

        for _ in 0..500 {
            {
                let mut calls = self.calls.lock();
                if calls.last() == Some(&the_mark) {
                    calls.pop();
                    return calls.clone();
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!(
            "the mark did not reach the callbacks. Got: {:?}",
            self.calls.lock()
        );
    }
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

/// A mock with callbacks assigned, the way a test of a service which reacts to them has it.
struct TestMock {
    mock: MyNoSqlDataReaderMock<TestEntity>,
    callbacks: Arc<TestCallbacks>,
}

impl TestMock {
    fn new() -> Self {
        let mock = MyNoSqlDataReaderMock::new();

        let callbacks = Arc::new(TestCallbacks {
            calls: Mutex::new(Vec::new()),
        });

        mock.assign_callback(callbacks.clone());

        Self { mock, callbacks }
    }

    /// Every call `update` and `delete` have made, in the order they were made - the mark is
    /// written and waited for, see [`TestCallbacks::calls_before_the_mark`]. The mock is
    /// consumed: the mark stays in it.
    async fn calls(self) -> Vec<Call> {
        self.mock.update(rows(&[(MARK, MARK)]));

        self.callbacks.calls_before_the_mark().await
    }
}

#[tokio::test]
async fn update_makes_one_call_per_partition_with_the_rows_written() {
    let test_mock = TestMock::new();

    // the rows of the two partitions come mixed
    test_mock.mock.update(rows(&[
        ("pk1", "rk1"),
        ("pk2", "rk1"),
        ("pk1", "rk2"),
        ("pk2", "rk2"),
        ("pk1", "rk3"),
    ]));

    // rk2 is replaced and rk4 is inserted - both are rows written
    test_mock
        .mock
        .update(rows(&[("pk1", "rk2"), ("pk1", "rk4")]));

    assert_eq!(
        test_mock.calls().await,
        vec![
            inserted_or_replaced("pk1", &["rk1", "rk2", "rk3"]),
            inserted_or_replaced("pk2", &["rk1", "rk2"]),
            inserted_or_replaced("pk1", &["rk2", "rk4"]),
        ]
    );
}

#[tokio::test]
async fn delete_makes_one_call_per_partition_with_the_rows_removed() {
    let test_mock = TestMock::new();

    test_mock.mock.update(rows(&[
        ("pk1", "rk1"),
        ("pk1", "rk2"),
        ("pk1", "rk3"),
        ("pk2", "rk1"),
    ]));

    test_mock.mock.delete(keys(&[
        ("pk1", "rk1"),
        ("pk2", "rk1"),
        ("pk1", "rk3"),
        // a row which is not there is not a delete - and must not be reported as one
        ("pk1", "never-existed"),
        ("never-existed", "rk1"),
    ]));

    // pk2 went with its last row
    assert_eq!(test_mock.mock.get_partition_keys(), vec!["pk1"]);
    assert!(test_mock.mock.get_entity("pk1", "rk2").is_some());

    assert_eq!(
        test_mock.calls().await,
        vec![
            inserted_or_replaced("pk1", &["rk1", "rk2", "rk3"]),
            inserted_or_replaced("pk2", &["rk1"]),
            deleted("pk1", &["rk1", "rk3"]),
            deleted("pk2", &["rk1"]),
        ]
    );
}

/// No call is made with an empty list of rows.
#[tokio::test]
async fn nothing_written_and_nothing_removed_makes_no_call() {
    let test_mock = TestMock::new();

    test_mock.mock.update(rows(&[("pk1", "rk1")]));

    test_mock.mock.update(rows(&[]));
    test_mock.mock.delete(keys(&[]));

    test_mock
        .mock
        .delete(keys(&[("pk1", "never-existed"), ("never-existed", "rk1")]));

    // a row is removed once, whatever number of times it is asked for
    test_mock
        .mock
        .delete(keys(&[("pk1", "rk1"), ("pk1", "rk1")]));
    test_mock.mock.delete(keys(&[("pk1", "rk1")]));

    assert_eq!(
        test_mock.calls().await,
        vec![
            inserted_or_replaced("pk1", &["rk1"]),
            deleted("pk1", &["rk1"]),
        ]
    );
}

/// The mock stands for the tcp reader in the test of a service, so its callbacks have to hear
/// what the callbacks of a tcp reader hear: `update` and `delete` make the calls which an
/// `UpdateRows` and a `DeleteRows` packet with the same rows make.
#[tokio::test]
async fn the_mock_makes_the_calls_the_tcp_reader_makes() {
    let test_mock = TestMock::new();

    let reader = MyNoSqlDataReaderTcp::<TestEntity>::new(Arc::new(SyncToMainNodeHandler::new(
        my_logger::LOGGER.clone(),
    )));

    let reader_callbacks = Arc::new(TestCallbacks {
        calls: Mutex::new(Vec::new()),
    });

    reader.assign_callback(reader_callbacks.clone());

    // the rows of the two partitions come mixed, and not in the order of their keys
    let written = [
        ("pk2", "rk2"),
        ("pk1", "rk3"),
        ("pk2", "rk1"),
        ("pk1", "rk1"),
    ];

    test_mock.mock.update(rows(&written));
    reader.update_rows(packet(&written));

    // rows which are not there, a row asked for twice, a partition which goes with its last row
    let removed = [
        ("pk2", "rk1"),
        ("pk1", "rk3"),
        ("pk1", "never-existed"),
        ("never-existed", "rk1"),
        ("pk2", "rk2"),
        ("pk2", "rk2"),
    ];

    test_mock.mock.delete(keys(&removed));
    reader.delete_rows(
        removed
            .iter()
            .map(|(partition_key, row_key)| DeleteRowTcpContract {
                partition_key: partition_key.to_string(),
                row_key: row_key.to_string(),
            })
            .collect(),
    );

    // both are left with the same copy
    assert_eq!(test_mock.mock.get_partition_keys(), vec!["pk1"]);
    assert_eq!(reader.get_partition_keys(), vec!["pk1"]);

    reader.update_rows(packet(&[(MARK, MARK)]));
    let reader_calls = reader_callbacks.calls_before_the_mark().await;

    assert_eq!(
        reader_calls,
        vec![
            inserted_or_replaced("pk1", &["rk3", "rk1"]),
            inserted_or_replaced("pk2", &["rk2", "rk1"]),
            deleted("pk1", &["rk3"]),
            deleted("pk2", &["rk1", "rk2"]),
        ]
    );

    assert_eq!(test_mock.calls().await, reader_calls);
}

/// Keeps the rows it is handed, in the order it is handed them.
struct KeepingCallbacks {
    rows: Mutex<Vec<Arc<TestEntity>>>,
}

impl KeepingCallbacks {
    fn keep(&self, entities: Vec<LazyMyNoSqlEntity<TestEntity>>) {
        let mut rows = self.rows.lock();

        for mut entity in entities {
            rows.push(entity.get().clone());
        }
    }

    /// Row number `no` of those handed over - it is waited for, the calls come from an events
    /// loop.
    async fn row(&self, no: usize) -> Arc<TestEntity> {
        for _ in 0..500 {
            if let Some(row) = self.rows.lock().get(no - 1) {
                return row.clone();
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!(
            "row {} was not handed over. Got {} row(s)",
            no,
            self.rows.lock().len()
        );
    }
}

impl MyNoSqlDataReaderCallBacks<TestEntity> for KeepingCallbacks {
    fn inserted_or_replaced(
        &self,
        _partition_key: &str,
        entities: Vec<LazyMyNoSqlEntity<TestEntity>>,
    ) {
        self.keep(entities);
    }

    fn deleted(&self, _partition_key: &str, entities: Vec<LazyMyNoSqlEntity<TestEntity>>) {
        self.keep(entities);
    }
}

/// The callbacks are handed the rows themselves: the row `update` was given - not the one it has
/// replaced - and the row `delete` has taken out of the copy. The keys do not tell the two apart.
#[tokio::test]
async fn the_callbacks_are_handed_the_row_written_and_the_row_removed() {
    let mock = MyNoSqlDataReaderMock::new();

    let callbacks = Arc::new(KeepingCallbacks {
        rows: Mutex::new(Vec::new()),
    });

    mock.assign_callback(callbacks.clone());

    let row = rows(&[("pk", "rk")]).next().unwrap();
    // the same keys - it replaces the first one
    let row_written_over_it = rows(&[("pk", "rk")]).next().unwrap();

    mock.update(std::iter::once(row.clone()));
    mock.update(std::iter::once(row_written_over_it.clone()));
    mock.delete(keys(&[("pk", "rk")]));

    assert!(Arc::ptr_eq(&callbacks.row(1).await, &row));
    assert!(Arc::ptr_eq(&callbacks.row(2).await, &row_written_over_it));
    // the row which was in the copy when it was deleted
    assert!(Arc::ptr_eq(&callbacks.row(3).await, &row_written_over_it));
}

/// Does what a service does in a callback - reads the reader - and writes down the row keys it
/// has found in the partition of the call.
struct ReadingCallbacks {
    mock: Arc<MyNoSqlDataReaderMock<TestEntity>>,
    found: Mutex<Vec<Vec<String>>>,
}

impl ReadingCallbacks {
    fn read_the_partition(&self, partition_key: &str) {
        let found = match self.mock.get_by_partition_key(partition_key) {
            Some(partition) => partition.keys().cloned().collect(),
            None => Vec::new(),
        };

        self.found.lock().push(found);
    }

    /// What call number `no` has found - it is waited for, the calls come from an events loop.
    async fn found_by_call(&self, no: usize) -> Vec<String> {
        for _ in 0..500 {
            if let Some(found) = self.found.lock().get(no - 1) {
                return found.clone();
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!("call {} was not made. Got: {:?}", no, self.found.lock());
    }
}

impl MyNoSqlDataReaderCallBacks<TestEntity> for ReadingCallbacks {
    fn inserted_or_replaced(
        &self,
        partition_key: &str,
        _entities: Vec<LazyMyNoSqlEntity<TestEntity>>,
    ) {
        self.read_the_partition(partition_key);
    }

    fn deleted(&self, partition_key: &str, _entities: Vec<LazyMyNoSqlEntity<TestEntity>>) {
        self.read_the_partition(partition_key);
    }
}

/// The calls are made after the copy has been changed, and not under its lock - a callback which
/// reloads a cache from the reader gets what it was called about.
#[tokio::test]
async fn a_callback_finds_the_mock_already_changed() {
    let mock = Arc::new(MyNoSqlDataReaderMock::new());

    let callbacks = Arc::new(ReadingCallbacks {
        mock: mock.clone(),
        found: Mutex::new(Vec::new()),
    });

    mock.assign_callback(callbacks.clone());

    mock.update(rows(&[("pk", "rk1"), ("pk", "rk2")]));
    assert_eq!(callbacks.found_by_call(1).await, vec!["rk1", "rk2"]);

    mock.delete(keys(&[("pk", "rk1")]));
    assert_eq!(callbacks.found_by_call(2).await, vec!["rk2"]);

    // the last row takes the partition with it
    mock.delete(keys(&[("pk", "rk2")]));
    assert!(callbacks.found_by_call(3).await.is_empty());
}

/// Callbacks are optional: a mock which has none works the way it always did, and needs no
/// runtime - the events loop of the callbacks is the only thing which is spawned.
#[test]
fn a_mock_without_callbacks_needs_no_runtime() {
    let mock = MyNoSqlDataReaderMock::new();

    mock.update(rows(&[("pk1", "rk1"), ("pk1", "rk2"), ("pk2", "rk1")]));
    mock.delete(keys(&[
        ("pk1", "rk1"),
        ("pk2", "rk1"),
        ("pk2", "never-existed"),
    ]));

    assert_eq!(mock.get_partition_keys(), vec!["pk1"]);
    assert!(mock.get_entity("pk1", "rk1").is_none());
    assert!(mock.get_entity("pk1", "rk2").is_some());
}

/// The events loop of the callbacks lives in the runtime `assign_callback` was called in - the
/// runtime of the test. A mock which outlives it, like one shared by several tests, has nobody to
/// make its calls - and says so at the first change it has to report, the way the tcp reader
/// would, instead of leaving a test to wait for a call which never comes.
#[test]
fn a_mock_which_outlives_the_runtime_of_its_callbacks_panics_when_it_has_rows_to_report() {
    let mock = MyNoSqlDataReaderMock::new();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    runtime.block_on(async {
        mock.assign_callback(Arc::new(TestCallbacks {
            calls: Mutex::new(Vec::new()),
        }));
    });

    drop(runtime);

    // nothing to report - nothing to panic about
    mock.update(rows(&[]));
    mock.delete(keys(&[("pk", "never-existed")]));

    let update = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        mock.update(rows(&[("pk", "rk")]));
    }));

    assert!(update.is_err());
    // the copy is changed by then, and is not left locked
    assert!(mock.get_entity("pk", "rk").is_some());

    let delete = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        mock.delete(keys(&[("pk", "rk")]));
    }));

    assert!(delete.is_err());
    assert!(mock.get_entity("pk", "rk").is_none());
}
