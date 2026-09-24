use my_no_sql_macros::my_no_sql_entity;
use serde::*;

#[my_no_sql_entity(table_name:"test-table", with_expires:true)]
#[derive(Debug, Serialize, Deserialize)]
pub struct MyEntity {
    pub ts: String,
}

#[my_no_sql_entity(table_name:"sessions-entities", with_expires:true)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionEntity {
    pub trader_id: String,
    pub claims: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use my_no_sql_sdk::{
        abstractions::MyNoSqlEntitySerializer,
        core::rust_extensions::date_time::DateTimeAsMicroseconds,
    };

    use crate::test_same_timestamp::SessionEntity;

    use super::MyEntity;

    #[test]
    fn test() {
        let entity = MyEntity {
            partition_key: "test".to_string(),
            row_key: "test".to_string(),
            time_stamp: Default::default(),
            expires: DateTimeAsMicroseconds::now()
                .add(Duration::from_secs(5))
                .into(),
            ts: "str".to_string(),
        };

        let result = entity.serialize_entity();

        let result = MyEntity::deserialize_entity(&result).unwrap();

        assert_eq!(entity.partition_key.as_str(), result.partition_key.as_str());
        assert_eq!(entity.row_key.as_str(), result.row_key.as_str());
        assert_eq!(entity.time_stamp, result.time_stamp);
        assert_eq!(entity.ts, result.ts);
    }


        #[test]
    fn test_2() {
        let entity = SessionEntity {
            partition_key: "test".to_string(),
            row_key: "test".to_string(),
            time_stamp: Default::default(),
            expires: DateTimeAsMicroseconds::now()
                .add(Duration::from_secs(5))
                .into(),
            trader_id: Default::default(),
            claims: Default::default(),
 
        };

        let result = entity.serialize_entity();

        let result = SessionEntity::deserialize_entity(&result).unwrap();

        assert_eq!(entity.partition_key.as_str(), result.partition_key.as_str());
        assert_eq!(entity.row_key.as_str(), result.row_key.as_str());
        assert_eq!(entity.time_stamp, result.time_stamp);
    }
}
