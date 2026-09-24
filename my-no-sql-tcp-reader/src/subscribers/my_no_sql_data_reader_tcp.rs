use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use my_json::json_reader::JsonArrayIterator;
use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use my_no_sql_tcp_shared::sync_to_main::SyncToMainNodeHandler;
use rust_extensions::{ApplicationStates, StrOrString};
use serde::de::DeserializeOwned;
use parking_lot::Mutex;

use super::{
    EntityRawData, GetEntitiesBuilder, GetEntityBuilder, LazyMyNoSqlEntity, MyNoSqlDataReader,
    MyNoSqlDataReaderCallBacks, MyNoSqlDataReaderData, UpdateEvent,
};

pub struct MyNoSqlDataReaderInner<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
> {
    data: Mutex<MyNoSqlDataReaderData<TMyNoSqlEntity>>,
    sync_handler: Arc<SyncToMainNodeHandler>,
}

impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static>
    MyNoSqlDataReaderInner<TMyNoSqlEntity>
{
    pub fn get_data(&self) -> &Mutex<MyNoSqlDataReaderData<TMyNoSqlEntity>> {
        &self.data
    }

    pub fn get_sync_handler(&self) -> &Arc<SyncToMainNodeHandler> {
        &self.sync_handler
    }
}

pub struct MyNoSqlDataReaderTcp<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
> {
    inner: Arc<MyNoSqlDataReaderInner<TMyNoSqlEntity>>,
}

impl<TMyNoSqlEntity> MyNoSqlDataReaderTcp<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
{
    pub fn new(
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
        sync_handler: Arc<SyncToMainNodeHandler>,
    ) -> Self {
        Self {
            inner: Arc::new(MyNoSqlDataReaderInner {
                data: Mutex::new(
                    MyNoSqlDataReaderData::new(TMyNoSqlEntity::TABLE_NAME, app_states),
                ),
                sync_handler,
            }),
        }
    }

    pub fn get_table_snapshot(
        &self,
    ) -> Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>> {
        let mut reader = self.inner.data.lock();
        return reader.get_table_snapshot();
    }

    pub fn get_table_snapshot_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let mut reader = self.inner.data.lock();
        reader.get_table_snapshot_as_vec()
    }

    pub fn get_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let mut reader = self.inner.data.lock();
        reader.get_by_partition(partition_key)
    }

    pub fn get_by_partition_key_as_vec(
        &self,
        partition_key: &str,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let mut reader = self.inner.data.lock();
        reader.get_by_partition_as_vec(partition_key)
    }

    pub fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Option<Arc<TMyNoSqlEntity>> {
        let mut reader = self.inner.data.lock();
        reader.get_entity(partition_key, row_key)
    }

    pub fn get_entities<'s>(
        &self,
        partition_key: impl Into<StrOrString<'s>>,
    ) -> GetEntitiesBuilder<TMyNoSqlEntity> {
        GetEntitiesBuilder::new(partition_key.into().to_string(), self.inner.clone())
    }

    pub fn get_entity_with_callback_to_server<'s>(
        &'s self,
        partition_key: &'s str,
        row_key: &'s str,
    ) -> GetEntityBuilder<'s, TMyNoSqlEntity> {
        GetEntityBuilder::new(partition_key, row_key, self.inner.clone())
    }

    pub fn has_partition(&self, partition_key: &str) -> bool {
        let reader = self.inner.data.lock();
        reader.has_partition(partition_key)
    }

    pub fn iter_and_find_entity_inside_partition(
        &self,
        partition_key: &str,
        predicate: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<Arc<TMyNoSqlEntity>> {
        let mut reader = self.inner.data.lock();

        if let Some(entities) = reader.iter_entities(partition_key) {
            for entity in entities {
                let entity = entity.get();

                if predicate(&entity) {
                    return Some(entity.clone());
                }
            }
        }

        None
    }

    pub fn deserialize_array(
        &self,
        data: &[u8],
    ) -> BTreeMap<String, Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>> {
        let json_array_iterator = JsonArrayIterator::new(data);

        if let Err(err) = &json_array_iterator {
            panic!(
                "Table: {}. The whole array of json entities is broken. Err: {:?}",
                TMyNoSqlEntity::TABLE_NAME,
                err
            );
        }

        let json_array_iterator = json_array_iterator.unwrap();
        let mut result = BTreeMap::new();

        while let Some(db_entity) = json_array_iterator.get_next() {
            if let Err(err) = &db_entity {
                panic!(
                    "Table: {}. The whole array of json entities is broken. Err: {:?}",
                    TMyNoSqlEntity::TABLE_NAME,
                    err
                );
            }

            let db_entity_data = db_entity.unwrap();

            let item_to_insert = if TMyNoSqlEntity::LAZY_DESERIALIZATION {
                let data = db_entity_data.as_bytes().to_vec();
                let db_json_entity =
                    my_no_sql_core::db_json_entity::DbJsonEntity::from_slice(&data).unwrap();

                LazyMyNoSqlEntity::Raw(
                    EntityRawData {
                        db_json_entity,
                        data,
                    }
                    .into(),
                )
            } else {
                let result = TMyNoSqlEntity::deserialize_entity(db_entity_data.as_bytes());

                match result {
                    Ok(result) => LazyMyNoSqlEntity::Deserialized(Arc::new(result)),
                    Err(err) => {
                        println!(
                            "Invalid entity to deserialize. Table: {}. Content: {:?}",
                            TMyNoSqlEntity::TABLE_NAME,
                            db_entity_data.as_str()
                        );

                        panic!("Can not lazy deserialize entity. Err: {}", err);
                    }
                }
            };

            let partition_key = item_to_insert.get_partition_key();
            if !result.contains_key(partition_key) {
                result.insert(partition_key.to_string(), Vec::new());
            }

            result.get_mut(partition_key).unwrap().push(item_to_insert);
        }

        result
    }

    pub fn get_enum_case_models_by_partition_key<
        's,
        TResult: MyNoSqlEntity
            + my_no_sql_abstractions::GetMyNoSqlEntitiesByPartitionKey
            + From<Arc<TMyNoSqlEntity>>
            + Sync
            + Send
            + 'static,
    >(
        &self,
    ) -> Option<Vec<TResult>> {
        let entities = self
            .get_by_partition_key_as_vec(TResult::PARTITION_KEY)?;

        let mut result = Vec::with_capacity(entities.len());

        for entity in entities {
            result.push(TResult::from(entity));
        }

        Some(result)
    }

    pub fn get_enum_case_model<
        TResult: MyNoSqlEntity
            + From<Arc<TMyNoSqlEntity>>
            + my_no_sql_abstractions::GetMyNoSqlEntity
            + Sync
            + Send
            + 'static,
    >(
        &self,
    ) -> Option<TResult> {
        let entity = self
            .get_entity(TResult::PARTITION_KEY, TResult::ROW_KEY)?;

        Some(TResult::from(entity))
    }

    pub fn get_partition_keys(&self) -> Vec<String> {
        let write_access = self.inner.data.lock();
        write_access.get_partition_keys()
    }
}

#[async_trait]
impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send> UpdateEvent
    for MyNoSqlDataReaderTcp<TMyNoSqlEntity>
{
    fn init_table(&self, data: Vec<u8>) {
        let data = self.deserialize_array(data.as_slice());

        let mut write_access = self.inner.data.lock();
        write_access.init_table(data);
    }

    fn init_partition(&self, partition_key: &str, data: Vec<u8>) {
        let data = self.deserialize_array(data.as_slice());

        let mut write_access = self.inner.data.lock();
        write_access.init_partition(partition_key, data);
    }

    fn update_rows(&self, data: Vec<u8>) {
        let data = self.deserialize_array(data.as_slice());

        let mut write_access = self.inner.data.lock();
        write_access.update_rows(data);
    }

    fn delete_rows(&self, rows_to_delete: Vec<my_no_sql_tcp_shared::DeleteRowTcpContract>) {
        let mut write_access = self.inner.data.lock();
        write_access.delete_rows(rows_to_delete);
    }
}

#[async_trait::async_trait]
impl<TMyNoSqlEntity> MyNoSqlDataReader<TMyNoSqlEntity> for MyNoSqlDataReaderTcp<TMyNoSqlEntity>
where
    TMyNoSqlEntity:
        MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + DeserializeOwned + 'static,
{
    fn get_partition_keys(&self) -> Vec<String> {
        self.get_partition_keys()
    }
    fn get_table_snapshot_as_vec(&self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        self.get_table_snapshot_as_vec()
    }

    fn get_by_partition_key(
        &self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        self.get_by_partition_key(partition_key)
    }

    fn get_by_partition_key_as_vec(
        &self,
        partition_key: &str,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        self.get_by_partition_key_as_vec(partition_key)
    }

    fn get_entity(&self, partition_key: &str, row_key: &str) -> Option<Arc<TMyNoSqlEntity>> {
        self.get_entity(partition_key, row_key)
    }

    fn get_entities<'s>(&self, partition_key: &'s str) -> GetEntitiesBuilder<TMyNoSqlEntity> {
        self.get_entities(partition_key)
    }

    fn get_entity_with_callback_to_server<'s>(
        &'s self,
        partition_key: &'s str,
        row_key: &'s str,
    ) -> GetEntityBuilder<'s, TMyNoSqlEntity> {
        self.get_entity_with_callback_to_server(partition_key, row_key)
    }

    fn has_partition(&self, partition_key: &str) -> bool {
        self.has_partition(partition_key)
    }

    async fn wait_until_first_data_arrives(&self) {
        loop {
            {
                let reader = self.inner.data.lock();
                if reader.has_entities_at_all() {
                    return;
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn assign_callback<
        TMyNoSqlDataReaderCallBacks: MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> + Send + Sync + 'static,
    >(
        &self,
        callbacks: Arc<TMyNoSqlDataReaderCallBacks>,
    ) {
        let app_states = {
            let reader = self.inner.data.lock();
            reader.get_app_states().clone()
        };
        let pusher =
            super::MyNoSqlDataReaderCallBacksPusher::new(callbacks, app_states);
        let mut write_access = self.inner.data.lock();
        write_access.set_callbacks(Arc::new(pusher));
    }
}
