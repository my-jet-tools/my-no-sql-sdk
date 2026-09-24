use crate::Timestamp;

pub trait MyNoSqlEntity {
    const TABLE_NAME: &'static str;
    const LAZY_DESERIALIZATION: bool;
    fn get_partition_key(&self) -> &str;
    fn get_row_key(&self) -> &str;
    fn get_time_stamp(&self) -> Timestamp;
}

pub trait MyNoSqlEntitySerializer: Sized {
    fn serialize_entity(&self) -> Vec<u8>;
    fn deserialize_entity(src: &[u8]) -> Result<Self, String>;
}

pub trait GetMyNoSqlEntity {
    const PARTITION_KEY: &'static str;
    const ROW_KEY: &'static str;
}

pub trait GetMyNoSqlEntitiesByPartitionKey {
    const PARTITION_KEY: &'static str;
}
