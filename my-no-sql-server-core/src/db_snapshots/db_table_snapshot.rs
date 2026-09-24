use my_no_sql_core::db::DbTableInner;
use my_no_sql_core::my_json::json_writer::JsonArrayWriter;
#[cfg(feature = "master-node")]
use rust_extensions::date_time::DateTimeAsMicroseconds;

use super::DbPartitionSnapshot;

pub struct DbTableSnapshot {
    #[cfg(feature = "master-node")]
    pub attr: my_no_sql_core::db::DbTableAttributes,
    #[cfg(feature = "master-node")]
    pub last_write_moment: DateTimeAsMicroseconds,
    pub by_partition: Vec<DbPartitionSnapshot>,
}

impl DbTableSnapshot {
    pub fn new(
        #[cfg(feature = "master-node")] last_write_moment: DateTimeAsMicroseconds,
        db_table: &DbTableInner,
    ) -> Self {
        let mut by_partition = Vec::new();

        for db_partition in db_table.partitions.get_partitions() {
            by_partition.push(db_partition.into());
        }

        Self {
            #[cfg(feature = "master-node")]
            attr: db_table.attributes.clone(),
            #[cfg(feature = "master-node")]
            last_write_moment,
            by_partition,
        }
    }

    pub fn as_json_array(&self) -> JsonArrayWriter {
        let mut json_array_writer = JsonArrayWriter::new();

        for db_partition_snapshot in self.by_partition.iter() {
            for db_row in &db_partition_snapshot.db_rows_snapshot.db_rows {
                json_array_writer = json_array_writer.write(db_row.as_ref());
            }
        }

        json_array_writer
    }
}
