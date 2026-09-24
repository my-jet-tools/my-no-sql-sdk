use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};

use crate::subscribers::{LazyMyNoSqlEntity, MyNoSqlDataReaderCallBacksPusher};

pub struct DataReaderEntitiesSet<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
> {
    entities: Option<BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>>,
    table_name: &'static str,
}

impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static>
    DataReaderEntitiesSet<TMyNoSqlEntity>
{
    pub fn new(table_name: &'static str) -> Self {
        Self {
            entities: None,
            table_name,
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.entities.is_some()
    }

    pub fn as_ref(
        &self,
    ) -> Option<&BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>> {
        self.entities.as_ref()
    }

    pub fn as_mut(
        &mut self,
    ) -> Option<&mut BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>> {
        self.entities.as_mut()
    }

    fn init_and_get_table(
        &mut self,
    ) -> &mut BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>> {
        if self.entities.is_none() {
            println!("MyNoSqlTcpReader table {} is initialized", self.table_name);
            self.entities = Some(BTreeMap::new());
            return self.entities.as_mut().unwrap();
        }

        return self.entities.as_mut().unwrap();
    }

    pub fn init_table<'s>(
        &'s mut self,
        data: BTreeMap<String, Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
    ) -> InitTableResult<'s, TMyNoSqlEntity> {
        let mut new_table: BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>> =
            BTreeMap::new();

        for (partition_key, src_entities_by_partition) in data {
            new_table.insert(partition_key.to_string(), BTreeMap::new());

            let by_partition = new_table.get_mut(partition_key.as_str()).unwrap();

            for entity in src_entities_by_partition {
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }

        let table_before = self.entities.replace(new_table);

        InitTableResult {
            table_now: self.entities.as_ref().unwrap(),
            table_before,
        }
    }

    pub fn init_partition<'s>(
        &'s mut self,
        partition_key: &str,
        src_entities: BTreeMap<String, Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
    ) -> InitPartitionResult<'s, TMyNoSqlEntity> {
        let entities = self.init_and_get_table();

        let mut new_partition = BTreeMap::new();

        let before_partition = entities.remove(partition_key);

        // src_entities comes in grouped by partition key - the rows inside are indexed by their
        // own row key, the same way init_table and update_rows index them.
        for entities in src_entities.into_values() {
            for entity in entities {
                new_partition.insert(entity.get_row_key().to_string(), entity);
            }
        }

        entities.insert(partition_key.to_string(), new_partition);

        InitPartitionResult {
            partition_before: entities.get(partition_key).unwrap(),
            partition_now: before_partition,
        }
    }

    pub fn update_rows(
        &mut self,
        src_data: BTreeMap<String, Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
        callbacks: &Option<Arc<MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>>>,
    ) {
        let entities = self.init_and_get_table();

        for (partition_key, src_entities) in src_data {
            let mut updates = if callbacks.is_some() {
                Some(Vec::new())
            } else {
                None
            };

            if !entities.contains_key(partition_key.as_str()) {
                entities.insert(partition_key.to_string(), BTreeMap::new());
            }

            let by_partition = entities.get_mut(partition_key.as_str()).unwrap();

            for entity in src_entities {
                if let Some(updates) = updates.as_mut() {
                    updates.push(entity.clone());
                }
                by_partition.insert(entity.get_row_key().to_string(), entity);
            }

            if let Some(callbacks) = callbacks {
                if let Some(updates) = updates {
                    if updates.len() > 0 {
                        callbacks.inserted_or_replaced(partition_key.as_str(), updates);
                    }
                }
            }
        }
    }

    pub fn delete_rows(
        &mut self,
        rows_to_delete: Vec<my_no_sql_tcp_shared::DeleteRowTcpContract>,
        callbacks: &Option<Arc<MyNoSqlDataReaderCallBacksPusher<TMyNoSqlEntity>>>,
    ) {
        let mut deleted_rows = if callbacks.is_some() {
            Some(BTreeMap::new())
        } else {
            None
        };

        let entities = self.init_and_get_table();

        for row_to_delete in &rows_to_delete {
            let mut delete_partition = false;
            if let Some(partition) = entities.get_mut(row_to_delete.partition_key.as_str()) {
                // The removed row is the one the callbacks are told about - it is gone from the
                // partition by then, so it has to be kept here rather than read back.
                if let Some(removed) = partition.remove(row_to_delete.row_key.as_str()) {
                    if let Some(deleted_rows) = deleted_rows.as_mut() {
                        deleted_rows
                            .entry(row_to_delete.partition_key.to_string())
                            .or_insert_with(Vec::new)
                            .push(removed);
                    }
                }

                delete_partition = partition.len() == 0;
            }

            if delete_partition {
                entities.remove(row_to_delete.partition_key.as_str());
            }
        }

        if let Some(callbacks) = callbacks.as_ref() {
            if let Some(partitions) = deleted_rows {
                for (partition_key, rows) in partitions {
                    callbacks.deleted(partition_key.as_str(), rows);
                }
            }
        }
    }

    pub fn get_partition_keys(&self) -> Vec<String> {
        match self.entities.as_ref() {
            Some(entities) => entities.keys().cloned().collect(),
            None => Vec::new(),
        }
    }
}

pub struct InitTableResult<
    's,
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
> {
    pub table_now: &'s BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
    pub table_before: Option<BTreeMap<String, BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>>,
}

pub struct InitPartitionResult<
    's,
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
> {
    pub partition_before: &'s BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    pub partition_now: Option<BTreeMap<String, LazyMyNoSqlEntity<TMyNoSqlEntity>>>,
}
