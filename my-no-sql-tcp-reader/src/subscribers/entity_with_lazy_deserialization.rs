use std::{fmt::Debug, sync::Arc};

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use my_no_sql_core::db_json_entity::DbJsonEntity;

pub struct EntityRawData {
    pub db_json_entity: DbJsonEntity,
    pub data: Vec<u8>,
}

pub enum LazyMyNoSqlEntity<
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static,
> {
    Raw(Arc<EntityRawData>),
    Deserialized(Arc<TMyNoSqlEntity>),
}

impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static>
    LazyMyNoSqlEntity<TMyNoSqlEntity>
{
    pub fn get_partition_key(&self) -> &str {
        match self {
            LazyMyNoSqlEntity::Deserialized(entity) => entity.get_partition_key(),
            LazyMyNoSqlEntity::Raw(src) => src.db_json_entity.get_partition_key(&src.data),
        }
    }

    pub fn get_row_key(&self) -> &str {
        match self {
            LazyMyNoSqlEntity::Deserialized(entity) => entity.get_row_key(),
            LazyMyNoSqlEntity::Raw(src) => src.db_json_entity.get_row_key(&src.data),
        }
    }

    pub fn get(&mut self) -> &Arc<TMyNoSqlEntity> {
        match self {
            LazyMyNoSqlEntity::Deserialized(entity) => return entity,
            LazyMyNoSqlEntity::Raw(src) => {
                let entity = TMyNoSqlEntity::deserialize_entity(&src.data).unwrap();
                let entity = Arc::new(entity);
                *self = LazyMyNoSqlEntity::Deserialized(entity.clone());
            }
        }

        match self {
            LazyMyNoSqlEntity::Deserialized(entity) => entity,
            LazyMyNoSqlEntity::Raw(_) => panic!("We should have deserialized it"),
        }
    }

    pub fn clone(&self) -> Self {
        match self {
            LazyMyNoSqlEntity::Deserialized(entity) => {
                LazyMyNoSqlEntity::Deserialized(entity.clone())
            }
            LazyMyNoSqlEntity::Raw(src) => LazyMyNoSqlEntity::Raw(src.clone()),
        }
    }
}

impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Send + Sync + 'static>
    From<TMyNoSqlEntity> for LazyMyNoSqlEntity<TMyNoSqlEntity>
{
    fn from(value: TMyNoSqlEntity) -> Self {
        LazyMyNoSqlEntity::Deserialized(Arc::new(value))
    }
}

impl<TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Debug + Send + Sync + 'static>
    std::fmt::Debug for LazyMyNoSqlEntity<TMyNoSqlEntity>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LazyMyNoSqlEntity::Deserialized(entity) => write!(f, "Deserialized({:?})", entity),
            LazyMyNoSqlEntity::Raw(data) => {
                write!(
                    f,
                    "Raw(PartitionKey: {}, RowKey: {}, Timestamp:{:?}, DataSize: {})",
                    self.get_partition_key(),
                    self.get_row_key(),
                    data.db_json_entity.get_time_stamp(data.data.as_slice()),
                    data.data.len()
                )
            }
        }
    }
}
