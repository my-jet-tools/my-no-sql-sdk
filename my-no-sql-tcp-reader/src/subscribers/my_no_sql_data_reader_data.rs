use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use rust_extensions::ApplicationStates;

use crate::DataReaderEntitiesSet;

use super::{LazyMyNoSqlEntity, MyNoSqlDataReaderCallBacksPusher};

pub struct MyNoSqlDataReaderData<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
> {
    entities: DataReaderEntitiesSet<TMyNoSqlEntity>,
    callbacks: Option<Arc<MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>>>,
    app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
}

impl<TMyNoSqlEntity> MyNoSqlDataReaderData<TMyNoSqlEntity>
where
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
{
    pub fn new(
        table_name: &'static str,
        app_states: Arc<dyn ApplicationStates + Send + Sync + 'static>,
    ) -> Self {
        Self {
            entities: DataReaderEntitiesSet::new(table_name),
            callbacks: None,
            app_states,
        }
    }

    pub fn get_app_states(&self) -> &Arc<dyn ApplicationStates + Send + Sync + 'static> {
        &self.app_states
    }

    pub fn set_callbacks(
        &mut self,
        pusher: Arc<MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>>,
    ) {
        self.callbacks = Some(pusher);
    }

    pub fn init_table(
        &mut self,
        data: BTreeMap<String, Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
    ) {
        let init_table_result = self.entities.init_table(data);

        if let Some(callbacks) = self.callbacks.as_ref() {
            super::callback_triggers::trigger_table_difference_sync(
                callbacks.as_ref(),
                init_table_result.table_before,
                init_table_result.table_now,
            );
        }
    }

    pub fn init_partition(
        &mut self,
        partition_key: &str,
        src_entities: BTreeMap<String, Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
    ) {
        let init_partition_result = self.entities.init_partition(partition_key, src_entities);

        if let Some(callbacks) = self.callbacks.as_ref() {
            super::callback_triggers::trigger_partition_difference_sync(
                callbacks.as_ref(),
                partition_key,
                init_partition_result.partition_now,
                init_partition_result.partition_before,
            );
        }
    }

    pub fn update_rows(
        &mut self,
        src_data: BTreeMap<String, Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
    ) {
        self.entities.update_rows(src_data, &self.callbacks);
    }

    pub fn delete_rows(&mut self, rows_to_delete: Vec<my_no_sql_tcp_shared::DeleteRowTcpContract>) {
        self.entities.delete_rows(rows_to_delete, &self.callbacks);
    }

    pub fn get_partition_keys(&self) -> Vec<String> {
        self.entities.get_partition_keys()
    }

    pub fn get_table_snapshot(
        &mut self,
    ) -> Option<BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>>> {
        let entities = self.entities.as_mut()?;
        if entities.len() == 0 {
            return None;
        }

        let mut result: BTreeMap<String, BTreeMap<String, Arc<TMyNoSqlEntity>>> = BTreeMap::new();
        for (partition_key, entities) in entities.iter_mut() {
            let mut to_insert = BTreeMap::new();

            for (row_key, entity) in entities.iter_mut() {
                to_insert.insert(row_key.clone(), entity.get().clone());
            }

            result.insert(partition_key.clone(), to_insert);
        }

        return Some(result);
    }

    pub fn get_table_snapshot_as_vec(&mut self) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_mut()?;

        if entities.len() == 0 {
            return None;
        }

        let mut result = Vec::new();

        for partition in entities.values_mut() {
            for entity in partition.values_mut() {
                result.push(entity.get().clone());
            }
        }

        Some(result)
    }

    pub fn get_entity(
        &mut self,
        partition_key: &str,
        row_key: &str,
    ) -> Option<Arc<TMyNoSqlEntity>> {
        let entities = self.entities.as_mut()?;

        let partition = entities.get_mut(partition_key)?;

        let row = partition.get_mut(row_key)?;

        Some(row.get().clone())
    }

    pub fn get_by_partition(
        &mut self,
        partition_key: &str,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_mut()?;

        let partition = entities.get_mut(partition_key)?;

        let mut result = BTreeMap::new();

        for itm in partition.iter_mut() {
            result.insert(itm.0.clone(), itm.1.get().clone());
        }

        Some(result)
    }

    pub fn get_by_partition_with_filter(
        &mut self,
        partition_key: &str,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<BTreeMap<String, Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_mut()?;

        let partition = entities.get_mut(partition_key)?;

        let mut result = BTreeMap::new();

        for db_row in partition.values_mut() {
            let db_row = db_row.get();
            if filter(&db_row) {
                result.insert(db_row.get_row_key().to_string(), db_row.clone());
            }
        }

        Some(result)
    }

    pub fn iter_entities<'s>(
        &'s mut self,
        partition_key: &str,
    ) -> Option<impl Iterator<Item = &'s mut LazyMyNoSqlEntity<TMyNoSqlEntity>>> {
        let entities = self.entities.as_mut()?;
        let partition = entities.get_mut(partition_key)?;
        Some(partition.values_mut())
    }

    pub fn has_partition(&self, partition_key: &str) -> bool {
        let entities = self.entities.as_ref();

        if entities.is_none() {
            return false;
        }

        let entities = entities.unwrap();

        entities.contains_key(partition_key)
    }

    pub fn get_by_partition_as_vec(
        &mut self,
        partition_key: &str,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_mut()?;

        let partition = entities.get_mut(partition_key)?;

        if partition.len() == 0 {
            return None;
        }

        let mut result = Vec::with_capacity(partition.len());

        for db_row in partition.values_mut() {
            result.push(db_row.get().clone());
        }

        Some(result)
    }

    pub fn get_by_partition_as_vec_with_filter(
        &mut self,
        partition_key: &str,
        filter: impl Fn(&TMyNoSqlEntity) -> bool,
    ) -> Option<Vec<Arc<TMyNoSqlEntity>>> {
        let entities = self.entities.as_mut()?;

        let partition = entities.get_mut(partition_key)?;

        if partition.len() == 0 {
            return None;
        }

        let mut result = Vec::with_capacity(partition.len());

        for db_row in partition.values_mut() {
            let db_row = db_row.get();
            if filter(db_row.as_ref()) {
                result.push(db_row.clone());
            }
        }

        Some(result)
    }

    pub fn has_entities_at_all(&self) -> bool {
        self.entities.is_initialized()
    }
}
