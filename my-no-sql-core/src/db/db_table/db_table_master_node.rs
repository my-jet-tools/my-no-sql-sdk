use rust_extensions::{
    date_time::DateTimeAsMicroseconds,
    sorted_vec::{EntityWithStrKey, SortedVecWithStrKey},
};

use crate::db::PartitionKey;

use super::{
    AvgSize, DataToGc, DbPartitionsContainer, DbTableAttributes, DbTableInner, DbTableName,
};

pub struct PartitionLastWriteMoment {
    pub partition_key: PartitionKey,
    pub last_write_moment: DateTimeAsMicroseconds,
}

impl EntityWithStrKey for PartitionLastWriteMoment {
    fn get_key(&self) -> &str {
        self.partition_key.as_str()
    }
}

impl DbTableInner {
    pub fn new(name: DbTableName, attributes: DbTableAttributes) -> Self {
        Self {
            name,
            partitions: DbPartitionsContainer::new(),
            last_write_moment: DateTimeAsMicroseconds::now(),
            attributes,
            avg_size: AvgSize::new(),
        }
    }

    pub fn get_expiration_index_rows_amount(&self) -> usize {
        let mut result = 0;

        for db_partition in self.partitions.get_partitions() {
            result += db_partition.get_expiration_index_rows_amount();
        }

        result
    }

    pub fn get_partitions_last_write_moment(
        &self,
    ) -> SortedVecWithStrKey<PartitionLastWriteMoment> {
        let mut result = SortedVecWithStrKey::new_with_capacity(self.partitions.len());

        for db_partition in self.partitions.get_partitions() {
            result.insert_or_replace(PartitionLastWriteMoment {
                partition_key: db_partition.partition_key.clone(),
                last_write_moment: db_partition.last_write_moment,
            });
        }

        result
    }

    pub fn get_data_to_gc(&self, now: DateTimeAsMicroseconds) -> DataToGc {
        let mut result = DataToGc::new();

        if let Some(max_partitions_amount) = self.attributes.max_partitions_amount {
            if let Some(partitions_to_expire) = self
                .partitions
                .get_partitions_to_gc_by_max_amount(max_partitions_amount)
            {
                for item in partitions_to_expire {
                    result.add_partition_to_expire(item.partition_key);
                }
            }
        }

        for partition_key in self.partitions.get_partitions_to_expire(now) {
            result.add_partition_to_expire(partition_key);
        }

        //Find DbRows to expire
        for db_partition in self.partitions.get_partitions() {
            if result.has_partition_to_gc(db_partition.partition_key.as_str()) {
                continue;
            }

            let rows_to_expire = db_partition.get_rows_to_expire(now);

            if rows_to_expire.len() > 0 {
                result.add_rows_to_expire(&db_partition.partition_key, rows_to_expire);
            }

            //Find DBRows to GC by max amount
            if let Some(max_rows_per_partition) = self.attributes.max_rows_per_partition_amount {
                if let Some(rows_to_gc) = db_partition
                    .rows
                    .get_rows_to_gc_by_max_amount(max_rows_per_partition)
                {
                    result.add_rows_to_expire(&db_partition.partition_key, rows_to_gc);
                }
            }
        }

        result
    }
}

#[cfg(feature = "master-node")]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        db::DbTableInner,
        db_json_entity::{DbJsonEntity, JsonTimeStamp},
    };

    use super::*;

    #[test]
    fn test_insert_record() {
        let mut db_table =
            DbTableInner::new("test-table".into(), DbTableAttributes::create_default());

        let now = JsonTimeStamp::now();

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test"
        }"#;

        let db_row = DbJsonEntity::parse_into_db_row(test_json.as_bytes().into(), &now).unwrap();

        let db_row = Arc::new(db_row);

        db_table.insert_row(&db_row, None);

        assert_eq!(db_table.get_table_size(), db_row.get_content_size());
        assert_eq!(db_table.get_partitions_amount(), 1);
    }

    #[test]
    fn test_insert_and_insert_or_replace() {
        let mut db_table =
            DbTableInner::new("test-table".into(), DbTableAttributes::create_default());

        let now = JsonTimeStamp::now();

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test"
        }"#;

        let db_row = DbJsonEntity::parse_into_db_row(test_json.as_bytes().into(), &now).unwrap();

        let db_row = Arc::new(db_row);

        db_table.insert_row(&db_row, None);

        let test_json = r#"{
            "PartitionKey": "test",
            "RowKey": "test",
            "AAA": "111"
        }"#;

        let db_row2 = DbJsonEntity::parse_into_db_row(test_json.as_bytes().into(), &now).unwrap();

        let db_row2 = Arc::new(db_row2);

        db_table.insert_or_replace_row(db_row2.clone(), None);

        assert_eq!(db_table.get_table_size(), db_row2.get_content_size());
        assert_eq!(db_table.get_partitions_amount(), 1);
    }
}
