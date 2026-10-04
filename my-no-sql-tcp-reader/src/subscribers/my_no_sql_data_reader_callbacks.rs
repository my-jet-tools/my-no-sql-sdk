use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};

use super::LazyMyNoSqlEntity;

/// What a packet from the server did to the local copy of a table.
///
/// One packet makes at most one `inserted_or_replaced` call and at most one `deleted` call per
/// partition, never with an empty list. `UpdateRows` reports the rows written, `DeleteRows` the
/// rows removed. A snapshot - `InitTable` (the first one, every reconnect, a clean of the table)
/// or `InitPartition` (a clean of a partition, a deleted partition) - reports every row the
/// partition holds now, changed or not, and then the rows it held and holds no more.
///
/// The calls are made one at a time from an events loop, after the copy has been updated, so a
/// slow callback delays the next ones.
pub trait MyNoSqlDataReaderCallBacks<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
>
{
    /// The rows written to `partition_key`; for a snapshot packet - every row the partition
    /// holds now.
    fn inserted_or_replaced(
        &self,
        partition_key: &str,
        entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    );
    /// The rows which are not in `partition_key` any more, the way they were before the packet.
    fn deleted(&self, partition_key: &str, entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>);
}


impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static>
    MyNoSqlDataReaderCallBacks<TMyNoSqlEntity> for ()
{
    fn inserted_or_replaced(
        &self,
        _partition_key: &str,
        _entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    ) {
        panic!("This is a dumb implementation")
    }

    fn deleted(
        &self,
        _partition_key: &str,
        _entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    ) {
        panic!("This is a dumb implementation")
    }
}
