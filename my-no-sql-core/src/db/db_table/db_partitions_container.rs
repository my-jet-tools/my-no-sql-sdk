#[cfg(feature = "master-node")]
use rust_extensions::date_time::DateTimeAsMicroseconds;
use rust_extensions::sorted_vec::SortedVecWithStrKey;

#[cfg(feature = "master-node")]
use crate::db::PartitionKey;

use crate::db::{DbPartition, PartitionKeyParameter};
#[cfg(feature = "master-node")]
pub struct PartitionToGc {
    pub partition_key: PartitionKey,
    pub last_read_moment: DateTimeAsMicroseconds,
}

pub struct DbPartitionsContainer {
    partitions: SortedVecWithStrKey<DbPartition>,
    #[cfg(feature = "master-node")]
    partitions_to_expire_index:
        crate::ExpirationIndexContainer<super::DbPartitionExpirationIndexOwned>,
}

impl DbPartitionsContainer {
    pub fn new() -> Self {
        Self {
            partitions: SortedVecWithStrKey::new(),
            #[cfg(feature = "master-node")]
            partitions_to_expire_index: crate::ExpirationIndexContainer::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn get_partitions<'s>(&'s self) -> std::slice::Iter<'s, DbPartition> {
        self.partitions.iter()
    }

    pub fn get_partitions_mut<'s>(&'s mut self) -> std::slice::IterMut<'s, DbPartition> {
        self.partitions.iter_mut()
    }
    #[cfg(feature = "master-node")]
    pub fn get_partitions_to_expire(&self, now: DateTimeAsMicroseconds) -> Vec<PartitionKey> {
        self.partitions_to_expire_index
            .get_items_to_expire(now, |itm| itm.partition_key.clone())
    }

    pub fn add_partition_if_not_exists(
        &mut self,
        partition_key: &impl PartitionKeyParameter,
    ) -> &mut DbPartition {
        let index = match self
            .partitions
            .insert_or_if_not_exists(partition_key.as_str())
        {
            rust_extensions::sorted_vec::InsertIfNotExists::Insert(entry) => {
                entry.insert_and_get_index(DbPartition::new(partition_key.to_partition_key()))
            }
            rust_extensions::sorted_vec::InsertIfNotExists::Exists(index) => index,
        };

        self.partitions.get_by_index_mut(index).unwrap()
    }

    pub fn get(&self, partition_key: &str) -> Option<&DbPartition> {
        self.partitions.get(partition_key)
    }

    pub fn get_mut(&mut self, partition_key: &str) -> Option<&mut DbPartition> {
        self.partitions.get_mut(partition_key)
    }

    pub fn has_partition(&self, partition_key: &str) -> bool {
        self.partitions.contains(partition_key)
    }

    pub fn insert(&mut self, db_partition: DbPartition) {
        #[cfg(feature = "master-node")]
        self.partitions_to_expire_index.add(&db_partition);

        let (_, _removed_partition) = self.partitions.insert_or_replace(db_partition);

        #[cfg(feature = "master-node")]
        if let Some(removed_partition) = _removed_partition {
            self.partitions_to_expire_index.remove(&removed_partition);
        }
    }

    pub fn remove(&mut self, partition_key: &str) -> Option<DbPartition> {
        let removed_partition = self.partitions.remove(partition_key);
        #[cfg(feature = "master-node")]
        if let Some(removed_partition) = &removed_partition {
            self.partitions_to_expire_index.remove(removed_partition);
        }

        removed_partition
    }

    pub fn clear(&mut self) -> Option<SortedVecWithStrKey<DbPartition>> {
        if self.partitions.len() == 0 {
            return None;
        }

        let mut result = SortedVecWithStrKey::new();

        std::mem::swap(&mut result, &mut self.partitions);
        #[cfg(feature = "master-node")]
        self.partitions_to_expire_index.clear();

        Some(result)
    }

    #[cfg(feature = "master-node")]
    pub fn get_partitions_to_gc_by_max_amount(
        &self,
        max_partitions_amount: usize,
    ) -> Option<Vec<PartitionToGc>> {
        if self.partitions.len() <= max_partitions_amount {
            return None;
        }

        let mut partitions_to_gc = Vec::new();

        for db_partition in self.partitions.iter() {
            let last_read_access = db_partition.get_last_read_moment();

            match partitions_to_gc.binary_search_by(|itm: &PartitionToGc| {
                itm.last_read_moment
                    .unix_microseconds
                    .cmp(&last_read_access.unix_microseconds)
            }) {
                Ok(_) => {}
                Err(index) => {
                    partitions_to_gc.insert(
                        index,
                        PartitionToGc {
                            partition_key: db_partition.partition_key.clone(),
                            last_read_moment: last_read_access,
                        },
                    );
                }
            }
        }

        while partitions_to_gc.len() > max_partitions_amount {
            partitions_to_gc.pop();
        }

        Some(partitions_to_gc)
    }
}
