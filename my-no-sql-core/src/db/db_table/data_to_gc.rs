use std::sync::Arc;

use rust_extensions::sorted_vec::{EntityWithStrKey, SortedVecWithStrKey};

use crate::db::{DbRow, PartitionKey};

pub struct DbRowsToExpire {
    pub partition_key: PartitionKey,
    pub rows: Vec<Arc<DbRow>>,
}

impl EntityWithStrKey for DbRowsToExpire {
    fn get_key(&self) -> &str {
        self.partition_key.as_str()
    }
}

pub struct DataToGc {
    pub partitions: SortedVecWithStrKey<PartitionKey>,
    pub db_rows: SortedVecWithStrKey<DbRowsToExpire>,
}

impl DataToGc {
    pub fn new() -> Self {
        Self {
            partitions: SortedVecWithStrKey::new(),
            db_rows: SortedVecWithStrKey::new(),
        }
    }

    pub fn add_partition_to_expire(&mut self, partition_key: PartitionKey) {
        match self
            .partitions
            .insert_or_if_not_exists(partition_key.as_str())
        {
            rust_extensions::sorted_vec::InsertIfNotExists::Insert(entry) => {
                entry.insert(partition_key)
            }
            rust_extensions::sorted_vec::InsertIfNotExists::Exists(_) => {}
        }
    }

    pub fn add_rows_to_expire(&mut self, partition_key: &PartitionKey, rows: Vec<Arc<DbRow>>) {
        if self.partitions.contains(partition_key.as_str()) {
            return;
        }

        match self.db_rows.insert_or_update(partition_key.as_str()) {
            rust_extensions::sorted_vec::InsertOrUpdateEntry::Insert(entry) => {
                entry.insert(DbRowsToExpire {
                    partition_key: partition_key.clone(),
                    rows,
                })
            }
            rust_extensions::sorted_vec::InsertOrUpdateEntry::Update(entry) => {
                entry.item.rows.extend(rows)
            }
        }
    }

    pub fn has_partition_to_gc(&self, partition_key: &str) -> bool {
        self.partitions.contains(partition_key)
    }

    pub fn has_data_to_gc(&self) -> bool {
        self.partitions.len() > 0 || self.db_rows.len() > 0
    }
}
