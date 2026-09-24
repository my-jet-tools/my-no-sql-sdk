use my_no_sql_core::db::{DbPartition, PartitionKey};

use super::DbRowsSnapshot;

pub struct DbPartitionSnapshot {
    #[cfg(feature = "master-node")]
    pub last_read_moment: rust_extensions::date_time::DateTimeAsMicroseconds,
    #[cfg(feature = "master-node")]
    pub last_write_moment: rust_extensions::date_time::DateTimeAsMicroseconds,
    pub partition_key: PartitionKey,
    pub db_rows_snapshot: DbRowsSnapshot,
}

#[cfg(feature = "master-node")]
impl DbPartitionSnapshot {
    pub fn has_to_persist(
        &self,
        written_in_blob: rust_extensions::date_time::DateTimeAsMicroseconds,
    ) -> bool {
        written_in_blob.unix_microseconds < self.last_write_moment.unix_microseconds
    }
}

impl Into<DbRowsSnapshot> for &DbPartition {
    fn into(self) -> DbRowsSnapshot {
        DbRowsSnapshot::new_from_snapshot(self.rows.get_all().map(|itm| itm.clone()).collect())
    }
}

impl Into<DbPartitionSnapshot> for &DbPartition {
    fn into(self) -> DbPartitionSnapshot {
        DbPartitionSnapshot {
            #[cfg(feature = "master-node")]
            last_read_moment: self.last_read_moment.as_date_time(),
            #[cfg(feature = "master-node")]
            last_write_moment: self.last_write_moment,
            partition_key: self.partition_key.clone(),
            db_rows_snapshot: self.into(),
        }
    }
}
