use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use parking_lot::RwLock;
use rust_extensions::{lazy::LazyVec, AppStates};

use crate::MyNoSqlDataReaderCallBacks;

use super::MyNoSqlDataReaderCallBacksPusher;

pub struct MyNoSqlDataReaderMockInnerData<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
> {
    pub items: BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>,
    pub callbacks: Option<Arc<MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>>>,
}

impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static>
    MyNoSqlDataReaderMockInnerData<TMyNoSqlEntity>
{
    pub fn new() -> Self {
        Self {
            items: BTreeMap::new(),
            callbacks: None,
        }
    }
}

pub struct MyNoSqlDataReaderMockInner<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
> {
    pub inner: RwLock<MyNoSqlDataReaderMockInnerData<TMyNoSqlEntity>>,
    app_states: Arc<AppStates>,
}

impl<TMyNoSqlEntity> MyNoSqlDataReaderMockInner<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(MyNoSqlDataReaderMockInnerData::new()),
            app_states: Arc::new(AppStates::create_initialized()),
        }
    }

    pub fn assign_callback<
        TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    >(
        &self,
        callbacks: Arc<TMyNoSqlDataReaderCallBacks>,
    ) {
        let pusher =
            MyNoSqlDataReaderCallBacksPusher::new(callbacks, self.app_states.clone());

        let mut write_access = self.inner.write();
        write_access.callbacks = Some(Arc::new(pusher));
    }

    pub fn update(&self, items: impl Iterator<Item = Arc<TMyNoSqlEntity>>) {
        let mut write_access = self.inner.write();
        for item in items {
            let partition_key = item.get_partition_key();
            let row_key = item.get_row_key();

            let partition = write_access
                .items
                .entry(partition_key.to_string())
                .or_insert_with(BTreeMap::new);
            partition.insert(row_key.to_string(), item);
        }
    }
    pub fn delete(&self, to_delete: impl Iterator<Item = (String, String)>) {
        let mut write_access = self.inner.write();

        let mut partitions_to_remove = HashSet::new();
        for (partition_key, row_key) in to_delete {
            if let Some(partition) = write_access.items.get_mut(&partition_key) {
                partition.remove(&row_key);
            }

            if let Some(partition) = write_access.items.get(partition_key.as_str()) {
                if partition.is_empty() {
                    partitions_to_remove.insert(partition_key);
                }
            }
        }

        for partition_to_remove in partitions_to_remove {
            write_access.items.remove(partition_to_remove.as_str());
        }
    }

    pub fn get_table_snapshot_as_vec(&self) -> Vec<Arc<TMyNoSqlEntity>> {
        let read_access = self.inner.read();
        let mut result = Vec::new();
        for partition in read_access.items.values() {
            for item in partition.values() {
                result.push(item.clone());
            }
        }

        result
    }

    pub fn get_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let read_access = self.inner.read();
        read_access.items.get(partition_key).cloned()
    }

    pub fn get_partition_keys(&self) -> Vec<String> {
        let read_access = self.inner.read();
        read_access.items.keys().cloned().collect()
    }

    pub fn get_by_partition_key_as_vec(
        &self,
        partition_key: &str,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let read_access = self.inner.read();
        let mut result = LazyVec::new();
        if let Some(partition) = read_access.items.get(partition_key) {
            for item in partition.values() {
                result.add(item.clone());
            }
        }

        result.get_result()
    }

    pub fn get_by_partition_key_as_vec_with_filter(
        &self,
        partition_key: &str,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let read_access = self.inner.read();
        let mut result = LazyVec::new();
        if let Some(partition) = read_access.items.get(partition_key) {
            for item in partition.values() {
                if filter(item) {
                    result.add(item.clone());
                }
            }
        }

        result.get_result()
    }

    pub fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Option<Arc<TMyNoSqlEntity>> {
        let read_access = self.inner.read();
        read_access
            .items
            .get(partition_key)
            .and_then(|partition| partition.get(row_key))
            .cloned()
    }

    pub fn get_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let read_access = self.inner.read();
        let mut result = LazyVec::new();
        for partition in read_access.items.values() {
            for item in partition.values() {
                result.add(item.clone());
            }
        }

        result.get_result()
    }

    pub fn get_as_vec_with_filter(
        &self,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let read_access = self.inner.read();
        let mut result = LazyVec::new();
        for partition in read_access.items.values() {
            for item in partition.values() {
                if filter(item) {
                    result.add(item.clone());
                }
            }
        }

        result.get_result()
    }

    pub fn has_partition(&self, partition_key: &str) -> bool {
        let read_access = self.inner.read();
        read_access.items.contains_key(partition_key)
    }
}
