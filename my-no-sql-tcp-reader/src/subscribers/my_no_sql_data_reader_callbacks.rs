use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};

use super::LazyMyNoSqlEntity;

pub trait MyNoSqlDataReaderCallBacks<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
>
{
    fn inserted_or_replaced(
        &self,
        partition_key: &str,
        entities: Vec<LazyMyNoSqlEntity<TMyNoSqlEntity>>,
    );
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
