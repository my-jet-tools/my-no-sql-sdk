use std::collections::BTreeMap;

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};

use super::{LazyMyNoSqlEntity, MyNoSqlDataReaderCallBacksPusher};
#[cfg(test)]
use super::MyNoSqlDataReaderCallBacks;

pub fn trigger_table_difference_sync<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
>(
    pusher: &MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>,
    before: Option<BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>>,
    now_entities: &BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
) {
    match before {
        Some(mut before) => {
            for (now_partition_key, now_partition) in now_entities {
                let before_partition = before.remove(now_partition_key);
                trigger_partition_difference_sync(
                    pusher,
                    now_partition_key,
                    before_partition,
                    now_partition,
                );
            }

            for (before_partition_key, before_partition) in before {
                let mut deleted_entities = Vec::new();
                for (_, db_row) in before_partition {
                    deleted_entities.push(db_row);
                }
                if deleted_entities.len() > 0 {
                    pusher.deleted(before_partition_key.as_str(), deleted_entities);
                }
            }
        }
        None => {
            for (partition_key, now_partition) in now_entities {
                let mut added_entities = Vec::new();
                for entity in now_partition.values() {
                    added_entities.push(entity.clone());
                }
                if added_entities.len() > 0 {
                    pusher.inserted_or_replaced(partition_key, added_entities);
                }
            }
        }
    }
}

pub fn trigger_partition_difference_sync<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
>(
    pusher: &MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>,
    partition_key: &str,
    before_partition: Option<BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
    now_partition: &BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>,
) {
    match before_partition {
        Some(mut before_partition) => {
            for (now_row_key, now_row) in now_partition {
                let mut inserted_or_replaced = Vec::new();
                before_partition.remove(now_row_key);
                inserted_or_replaced.push(now_row.clone());
                if inserted_or_replaced.len() > 0 {
                    pusher.inserted_or_replaced(partition_key, inserted_or_replaced);
                }
            }

            let mut deleted_entities = Vec::new();
            for (_, before_row) in before_partition {
                deleted_entities.push(before_row);
            }
            if deleted_entities.len() > 0 {
                pusher.deleted(partition_key, deleted_entities);
            }
        }
        None => {
            let mut inserted_or_replaced = Vec::new();
            for entity in now_partition.values() {
                inserted_or_replaced.push(entity.clone());
            }
            if inserted_or_replaced.len() > 0 {
                pusher.inserted_or_replaced(partition_key, inserted_or_replaced);
            }
        }
    }
}

#[cfg(test)]
pub async fn trigger_table_difference<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
    TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity>,
>(
    callbacks: &TMyNoSqlDataReaderCallBacks,
    before: Option<BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>>,
    now_entities: &BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
) {
    match before {
        Some(before) => {
            trigger_old_and_new_table_difference(callbacks, before, now_entities).await;
        }
        None => {
            trigger_brand_new_table(callbacks, now_entities).await;
        }
    }
}

#[cfg(test)]
pub async fn trigger_brand_new_table<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
    TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity>,
>(
    callbacks: &TMyNoSqlDataReaderCallBacks,
    now_entities: &BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
) {
    for (partition_key, now_partition) in now_entities {
        let mut added_entities = Vec::new();
        for entity in now_partition.values() {
            added_entities.push(entity.clone());
        }

        if added_entities.len() > 0 {
            callbacks
                .inserted_or_replaced(partition_key, added_entities);
        }
    }
}

#[cfg(test)]
pub async fn trigger_old_and_new_table_difference<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
    TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity>,
>(
    callbacks: &TMyNoSqlDataReaderCallBacks,
    mut before: BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
    now_entities: &BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
) {
    for (now_partition_key, now_partition) in now_entities {
        let before_partition = before.remove(now_partition_key);

        trigger_partition_difference(
            callbacks,
            now_partition_key,
            before_partition,
            now_partition,
        )
        .await;
    }

    for (before_partition_key, before_partition) in before {
        let mut deleted_entities = Vec::new();

        for (_, db_row) in before_partition {
            deleted_entities.push(db_row);
        }

        if deleted_entities.len() > 0 {
            callbacks
                .deleted(before_partition_key.as_str(), deleted_entities);
        }
    }
}

#[cfg(test)]
pub async fn trigger_partition_difference<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
    TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity>,
>(
    callbacks: &TMyNoSqlDataReaderCallBacks,
    partition_key: &str,
    before_partition: Option<BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
    now_partition: &BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>,
) {
    match before_partition {
        Some(mut before_partition) => {
            for (now_row_key, now_row) in now_partition {
                let mut inserted_or_replaced = Vec::new();

                match before_partition.remove(now_row_key) {
                    Some(_) => {
                        inserted_or_replaced.push(now_row.clone());
                    }
                    None => {
                        inserted_or_replaced.push(now_row.clone());
                    }
                }

                if inserted_or_replaced.len() > 0 {
                    callbacks
                        .inserted_or_replaced(partition_key, inserted_or_replaced);
                }
            }

            let mut deleted_entities = Vec::new();

            for (_, before_row) in before_partition {
                deleted_entities.push(before_row);
            }

            if deleted_entities.len() > 0 {
                callbacks.deleted(partition_key, deleted_entities);
            }
        }
        None => {
            trigger_brand_new_partition(callbacks, partition_key, now_partition).await;
        }
    }
}

#[cfg(test)]
pub async fn trigger_brand_new_partition<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
    TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity>,
>(
    callbacks: &TMyNoSqlDataReaderCallBacks,
    partition_key: &str,
    partition: &BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>,
) {
    let mut inserted_or_replaced = Vec::new();
    for entity in partition.values() {
        inserted_or_replaced.push(entity.clone());
    }

    if inserted_or_replaced.len() > 0 {
        callbacks
            .inserted_or_replaced(partition_key, inserted_or_replaced);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp};
    use serde_derive::{Deserialize, Serialize};
    use parking_lot::Mutex;

    use crate::subscribers::{LazyMyNoSqlEntity, MyNoSqlDataReaderCallBacks};

    struct TestCallbacksInner {
        inserted_or_replaced_entities: BTreeMap<String, Vec<LazyMyNoSqlEntity<TestRow>>>,
        deleted: BTreeMap<String, Vec<LazyMyNoSqlEntity<TestRow>>>,
    }

    pub struct TestCallbacks {
        data: Mutex<TestCallbacksInner>,
    }

    impl TestCallbacks {
        pub fn new() -> Self {
            Self {
                data: Mutex::new(TestCallbacksInner {
                    inserted_or_replaced_entities: BTreeMap::new(),
                    deleted: BTreeMap::new(),
                }),
            }
        }
    }

    impl MyNoSqlDataReaderCallBacks<TestRow> for TestCallbacks {
        fn inserted_or_replaced(
            &self,
            partition_key: &str,
            entities: Vec<LazyMyNoSqlEntity<TestRow>>,
        ) {
            let mut write_access = self.data.lock();
            match write_access
                .inserted_or_replaced_entities
                .get_mut(partition_key)
            {
                Some(db_partition) => {
                    db_partition.extend(entities);
                }

                None => {
                    write_access
                        .inserted_or_replaced_entities
                        .insert(partition_key.to_string(), entities);
                }
            }
        }

        fn deleted(&self, partition_key: &str, entities: Vec<LazyMyNoSqlEntity<TestRow>>) {
            let mut write_access = self.data.lock();
            match write_access.deleted.get_mut(partition_key) {
                Some(db_partition) => {
                    db_partition.extend(entities);
                }

                None => {
                    write_access
                        .deleted
                        .insert(partition_key.to_string(), entities);
                }
            }
        }
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct TestRow {
        partition_key: String,
        row_key: String,
        timestamp: i64,
    }

    impl TestRow {
        pub fn new(partition_key: String, row_key: String, timestamp: i64) -> Self {
            TestRow {
                partition_key,
                row_key,
                timestamp,
            }
        }
    }

    impl MyNoSqlEntity for TestRow {
        const TABLE_NAME: &'static str = "Test";
        const LAZY_DESERIALIZATION: bool = false;

        fn get_partition_key(&self) -> &str {
            self.partition_key.as_str()
        }
        fn get_row_key(&self) -> &str {
            self.row_key.as_str()
        }
        fn get_time_stamp(&self) -> Timestamp {
            self.timestamp.into()
        }
    }

    impl MyNoSqlEntitySerializer for TestRow {
        fn serialize_entity(&self) -> Vec<u8> {
            my_no_sql_core::entity_serializer::serialize(self)
        }

        fn deserialize_entity(src: &[u8]) -> Result<Self, String> {
            my_no_sql_core::entity_serializer::deserialize(src)
        }
    }

    #[tokio::test]
    pub async fn test_we_had_data_in_table_and_new_table_is_empty() {
        let test_callback = TestCallbacks::new();

        let mut before_rows: BTreeMap<String, LazyMyNoSqlEntity<TestRow>> = BTreeMap::new();

        before_rows.insert(
            "RK1".to_string(),
            TestRow::new("PK1".to_string(), "RK1".to_string(), 1).into(),
        );
        before_rows.insert(
            "RK2".to_string(),
            TestRow::new("PK1".to_string(), "RK2".to_string(), 1).into(),
        );

        let mut before = BTreeMap::new();

        before.insert("PK1".to_string(), before_rows);

        let after = BTreeMap::new();

        super::trigger_table_difference(&test_callback, Some(before), &after).await;

        let read_access = test_callback.data.lock();

        assert_eq!(2, read_access.deleted.get("PK1").unwrap().len());
    }

    #[tokio::test]
    pub async fn test_brand_new_table() {
        let test_callback = TestCallbacks::new();

        let mut after_rows: BTreeMap<String, LazyMyNoSqlEntity<TestRow>> = BTreeMap::new();

        after_rows.insert(
            "RK1".to_string(),
            TestRow::new("PK1".to_string(), "RK1".to_string(), 1).into(),
        );
        after_rows.insert(
            "RK2".to_string(),
            TestRow::new("PK1".to_string(), "RK2".to_string(), 1).into(),
        );

        let mut after = BTreeMap::new();

        after.insert("PK1".to_string(), after_rows);

        super::trigger_table_difference(&test_callback, None, &after).await;

        let read_access = test_callback.data.lock();
        assert_eq!(
            2,
            read_access
                .inserted_or_replaced_entities
                .get("PK1")
                .unwrap()
                .len()
        );
    }

    #[tokio::test]
    pub async fn test_we_have_updates_in_table() {
        let test_callback = TestCallbacks::new();

        let mut before_partition: BTreeMap<String, LazyMyNoSqlEntity<TestRow>> = BTreeMap::new();

        before_partition.insert(
            "RK1".to_string(),
            TestRow::new("PK1".to_string(), "RK1".to_string(), 1).into(),
        );
        before_partition.insert(
            "RK2".to_string(),
            TestRow::new("PK1".to_string(), "RK2".to_string(), 1).into(),
        );

        let mut before = BTreeMap::new();
        before.insert("PK1".to_string(), before_partition);

        let mut after_partition: BTreeMap<String, LazyMyNoSqlEntity<TestRow>> = BTreeMap::new();
        after_partition.insert(
            "RK2".to_string(),
            TestRow::new("PK1".to_string(), "RK2".to_string(), 2).into(),
        );

        let mut after = BTreeMap::new();
        after.insert("PK1".to_string(), after_partition);

        super::trigger_table_difference(&test_callback, Some(before), &after).await;

        let read_access = test_callback.data.lock();
        assert_eq!(
            1,
            read_access
                .inserted_or_replaced_entities
                .get("PK1")
                .unwrap()
                .len()
        );
        assert_eq!(1, read_access.deleted.get("PK1").unwrap().len());
    }
}
