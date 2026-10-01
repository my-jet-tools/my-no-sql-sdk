//! `delete_rows` hands the rows it removed to the table's callbacks. It used to read each row
//! back out of the partition it had just removed it from, so the branch was fine only while it
//! was dead: the first delete on a table with a callback assigned panicked and took the reader's
//! socket down with it.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp};
use my_no_sql_tcp_shared::DeleteRowTcpContract;
use parking_lot::Mutex;

use crate::{
    subscribers::{LazyMyNoSqlEntity, MyNoSqlDataReaderCallBacks, MyNoSqlDataReaderCallBacksPusher},
    DataReaderEntitiesSet,
};

const PARTITION_KEY: &str = "pk";
const ROW_KEY: &str = "rk";

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

fn entity(row_key: &str) -> LazyMyNoSqlEntity<TestEntity> {
    TestEntity {
        partition_key: PARTITION_KEY.to_string(),
        row_key: row_key.to_string(),
    }
    .into()
}

/// Records what each callback was handed, as (partition_key, row_key) pairs.
struct TestCallbacks {
    deleted: Mutex<Vec<(String, String)>>,
}

impl TestCallbacks {
    fn new() -> Self {
        Self {
            deleted: Mutex::new(Vec::new()),
        }
    }

    /// The pusher delivers through an events loop - give it the few ticks it needs.
    async fn wait_for_deleted(&self, amount: usize) -> Vec<(String, String)> {
        for _ in 0..500 {
            {
                let deleted = self.deleted.lock();
                if deleted.len() >= amount {
                    return deleted.clone();
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!(
            "deleted callback did not receive {} entities. Got: {:?}",
            amount,
            self.deleted.lock()
        );
    }
}

impl MyNoSqlDataReaderCallBacks<TestEntity> for TestCallbacks {
    fn inserted_or_replaced(
        &self,
        _partition_key: &str,
        _entities: Vec<LazyMyNoSqlEntity<TestEntity>>,
    ) {
    }

    fn deleted(&self, partition_key: &str, entities: Vec<LazyMyNoSqlEntity<TestEntity>>) {
        let mut write_access = self.deleted.lock();

        for entity in entities {
            write_access.push((partition_key.to_string(), entity.get_row_key().to_string()));
        }
    }
}

fn assign_callbacks(
    callbacks: Arc<TestCallbacks>,
) -> Option<Arc<MyNoSqlDataReaderCallBacksPusher<TestEntity>>> {
    Some(Arc::new(MyNoSqlDataReaderCallBacksPusher::new(callbacks)))
}

fn insert_rows(
    entities: &mut DataReaderEntitiesSet<TestEntity>,
    callbacks: &Option<Arc<MyNoSqlDataReaderCallBacksPusher<TestEntity>>>,
    rows: Vec<LazyMyNoSqlEntity<TestEntity>>,
) {
    let mut src_data = BTreeMap::new();
    src_data.insert(PARTITION_KEY.to_string(), rows);

    entities.update_rows(src_data, callbacks);
}

#[tokio::test]
async fn the_deleted_callback_gets_the_row_which_was_removed() {
    let test_callbacks = Arc::new(TestCallbacks::new());
    let callbacks = assign_callbacks(test_callbacks.clone());

    let mut entities = DataReaderEntitiesSet::new(TestEntity::TABLE_NAME);

    insert_rows(&mut entities, &callbacks, vec![entity(ROW_KEY)]);

    // used to panic here: the row was read back out of the partition it had just left
    entities.delete_rows(
        vec![DeleteRowTcpContract {
            partition_key: PARTITION_KEY.to_string(),
            row_key: ROW_KEY.to_string(),
        }],
        &callbacks,
    );

    assert_eq!(
        test_callbacks.wait_for_deleted(1).await,
        vec![(PARTITION_KEY.to_string(), ROW_KEY.to_string())]
    );

    // the row went, and its now empty partition with it
    assert!(entities.as_ref().unwrap().is_empty());
}

#[tokio::test]
async fn every_deleted_row_of_a_partition_reaches_the_callback() {
    let test_callbacks = Arc::new(TestCallbacks::new());
    let callbacks = assign_callbacks(test_callbacks.clone());

    let mut entities = DataReaderEntitiesSet::new(TestEntity::TABLE_NAME);

    insert_rows(
        &mut entities,
        &callbacks,
        vec![entity("rk1"), entity("rk2"), entity("rk3")],
    );

    entities.delete_rows(
        vec![
            DeleteRowTcpContract {
                partition_key: PARTITION_KEY.to_string(),
                row_key: "rk1".to_string(),
            },
            DeleteRowTcpContract {
                partition_key: PARTITION_KEY.to_string(),
                row_key: "rk3".to_string(),
            },
            // a row which is not there is not a delete - and must not be reported as one
            DeleteRowTcpContract {
                partition_key: PARTITION_KEY.to_string(),
                row_key: "never-existed".to_string(),
            },
        ],
        &callbacks,
    );

    assert_eq!(
        test_callbacks.wait_for_deleted(2).await,
        vec![
            (PARTITION_KEY.to_string(), "rk1".to_string()),
            (PARTITION_KEY.to_string(), "rk3".to_string())
        ]
    );

    let partition = entities.as_ref().unwrap().get(PARTITION_KEY).unwrap();
    assert_eq!(partition.len(), 1);
    assert!(partition.get("rk2").is_some());
}
