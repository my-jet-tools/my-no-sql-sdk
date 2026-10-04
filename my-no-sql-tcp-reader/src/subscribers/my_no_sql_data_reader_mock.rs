use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};

use crate::MyNoSqlDataReaderCallBacks;

use super::{GetEntitiesBuilder, GetEntityBuilder, MyNoSqlDataReader, MyNoSqlDataReaderMockInner};

pub struct MyNoSqlDataReaderMock<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
> {
    pub inner: Arc<MyNoSqlDataReaderMockInner<TMyNoSqlEntity>>,
}

impl<TMyNoSqlEntity> MyNoSqlDataReaderMock<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MyNoSqlDataReaderMockInner::new()),
        }
    }

    /// Writes the rows to the copy. The assigned callbacks get them the way they get the rows of
    /// an `UpdateRows` packet from the tcp reader: one `inserted_or_replaced` call per partition,
    /// made from the events loop of the callbacks - a test has to wait for it.
    ///
    /// The rows are handed over as `LazyMyNoSqlEntity::Deserialized` - the rows of an entity with
    /// lazy deserialization as well, which the tcp reader may hand over as `Raw`.
    ///
    /// The events loop lives in the Tokio runtime `assign_callback` was called in. Once that
    /// runtime is gone there is nobody to make the calls: an `update` or a `delete` which has
    /// rows to report panics, after it has changed the copy.
    pub fn update(&self, items: impl Iterator<Item = Arc<TMyNoSqlEntity>>) {
        self.inner.update(items);
    }
    /// Removes the rows - (partition key, row key) pairs - from the copy. The assigned callbacks
    /// get the ones which were there the way they get the rows of a `DeleteRows` packet from the
    /// tcp reader: one `deleted` call per partition, made from the same events loop.
    pub fn delete(&self, to_delete: impl Iterator<Item = (String, String)>) {
        self.inner.delete(to_delete);
    }
}

#[async_trait::async_trait]
impl<TMyNoSqlEntity> MyNoSqlDataReader<TMyNoSqlEntity> for MyNoSqlDataReaderMock<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
{
     fn get_table_snapshot_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let result = self.inner.get_table_snapshot_as_vec();

        if result.len() == 0 {
            return None;
        }

        Some(result)
    }

     fn get_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        self.inner.get_by_partition_key(partition_key)
    }

     fn get_partition_keys(&self) -> Vec<String> {
        self.inner.get_partition_keys()
    }

     fn get_by_partition_key_as_vec(
        &self,
        partition_key: &str,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        self.inner.get_by_partition_key_as_vec(partition_key)
    }

     fn get_entity(&self, partition_key: &str, row_key: &str) -> Option<Arc<TMyNoSqlEntity>> {
        self.inner.get_entity(partition_key, row_key)
    }

    fn get_entities<'s>(&self, partition_key: &'s str) -> GetEntitiesBuilder<TMyNoSqlEntity> {
        GetEntitiesBuilder::new_mock(partition_key.to_string(), self.inner.clone())
    }

    fn get_entity_with_callback_to_server<'s>(
        &'s self,
        partition_key: &'s str,
        row_key: &'s str,
    ) -> GetEntityBuilder<'s, TMyNoSqlEntity> {
        GetEntityBuilder::new_mock(partition_key, row_key, self.inner.clone())
    }

     fn has_partition(&self, partition_key: &str) -> bool {
        self.inner.has_partition(partition_key)
    }

    async fn wait_until_first_data_arrives(&self) {
        
    }

    fn assign_callback<
        TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    >(
        &self,
        callbacks: Arc<TMyNoSqlDataReaderCallBacks>,
    ) {
        self.inner.assign_callback(callbacks)
    }
}
