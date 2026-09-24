use std::sync::Arc;

use my_no_sql_core::db::{DbRow, PartitionKey, PartitionKeyParameter};
use my_no_sql_core::my_json::json_writer::JsonArrayWriter;

pub struct DbRowsByPartitionsSnapshot {
    pub partitions: Vec<(PartitionKey, Vec<Arc<DbRow>>)>,
}

impl DbRowsByPartitionsSnapshot {
    pub fn new() -> Self {
        Self {
            partitions: Vec::new(),
        }
    }

    pub fn has_elements(&self) -> bool {
        self.partitions.len() > 0
    }

    fn get_or_create_partition(
        &mut self,
        partition_key: impl PartitionKeyParameter,
    ) -> &mut Vec<Arc<DbRow>> {
        let index = self
            .partitions
            .binary_search_by(|itm| itm.0.as_str().cmp(partition_key.as_str()));

        match index {
            Ok(index) => self.partitions.get_mut(index).unwrap().1.as_mut(),
            Err(index) => {
                self.partitions
                    .insert(index, (partition_key.to_partition_key(), Vec::new()));
                self.partitions.get_mut(index).unwrap().1.as_mut()
            }
        }
    }

    pub fn add_row(&mut self, partition_key: impl PartitionKeyParameter, db_row: Arc<DbRow>) {
        self.get_or_create_partition(partition_key).push(db_row);
    }

    pub fn add_rows(
        &mut self,
        partition_key: impl PartitionKeyParameter,
        db_rows: Vec<Arc<DbRow>>,
    ) {
        self.get_or_create_partition(partition_key).extend(db_rows);
    }

    pub fn as_json_array(&self) -> JsonArrayWriter {
        let mut json_array_writer = JsonArrayWriter::new();
        for (_, snapshot) in self.partitions.iter() {
            for db_row in snapshot {
                json_array_writer = json_array_writer.write(db_row.as_ref());
            }
        }

        json_array_writer
    }
}
