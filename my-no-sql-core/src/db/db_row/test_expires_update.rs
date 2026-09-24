#[cfg(test)]
mod test {

    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::db_json_entity::{DbJsonEntity, JsonTimeStamp};

    #[test]
    fn test_replace_existing_expires() {
        let test_json = r#"{
            "PartitionKey": "TestPk",
            "RowKey": "TestRk",
            "Expires": "2019-01-01T00:00:00"
        }"#;

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row =
            DbJsonEntity::parse_into_db_row(test_json.as_bytes().into(), &inject_time_stamp)
                .unwrap();

        let new_expires = DateTimeAsMicroseconds::from_str("2020-01-02T01:02:03").unwrap();

        db_row.update_expires(new_expires.into());

        let mut result_json = String::new();

        db_row.write_json(&mut result_json);

        let result_entity = DbJsonEntity::new(result_json.as_bytes().into()).unwrap();

        assert_eq!(
            result_entity.get_partition_key(result_json.as_bytes()),
            "TestPk"
        );
        assert_eq!(result_entity.get_row_key(result_json.as_bytes()), "TestRk");

        assert_eq!(
            result_entity.get_expires(result_json.as_bytes()).unwrap(),
            &new_expires.to_rfc3339()[..19]
        );
    }

    #[test]
    fn test_injecting_expires() {
        let test_json = r#"{
            "PartitionKey": "Pk",
            "RowKey": "Rk"
        }"#;

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row =
            DbJsonEntity::parse_into_db_row(test_json.as_bytes().into(), &inject_time_stamp)
                .unwrap();

        let new_expires = DateTimeAsMicroseconds::from_str("2020-01-02T01:02:03").unwrap();

        db_row.update_expires(new_expires.into());

        let mut result_json = String::new();

        db_row.write_json(&mut result_json);

        let result_entity = DbJsonEntity::new(result_json.as_bytes().into()).unwrap();

        assert_eq!(
            result_entity.get_partition_key(result_json.as_bytes()),
            "Pk"
        );
        assert_eq!(result_entity.get_row_key(result_json.as_bytes()), "Rk");

        assert_eq!(
            result_entity.get_expires(result_json.as_bytes()).unwrap(),
            &new_expires.to_rfc3339()[..19]
        );
    }

    #[test]
    fn test_remove_expiration_time() {
        let test_json =
            r#"{"PartitionKey": "Pk","RowKey": "Rk", "Expires": "2019-01-01T00:00:00"}"#;

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row =
            DbJsonEntity::parse_into_db_row(test_json.as_bytes().into(), &inject_time_stamp)
                .unwrap();

        db_row.update_expires(None);

        let mut result_json = String::new();

        db_row.write_json(&mut result_json);

        println!("Result: {}", result_json);

        let result_entity = DbJsonEntity::new(result_json.as_bytes().into()).unwrap();

        assert_eq!(
            result_entity.get_partition_key(result_json.as_bytes()),
            "Pk"
        );
        assert_eq!(result_entity.get_row_key(result_json.as_bytes()), "Rk");

        assert!(result_entity.get_expires(result_json.as_bytes()).is_none());
    }

    #[test]
    fn test_remove_expiration_time_at_begin() {
        let test_json = r#"{"Expires": "2019-01-01T00:00:00","PartitionKey":"Pk","RowKey":"Rk"}"#;

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row =
            DbJsonEntity::parse_into_db_row(test_json.as_bytes().into(), &inject_time_stamp)
                .unwrap();

        db_row.update_expires(None);

        let mut result_json = String::new();

        db_row.write_json(&mut result_json);

        println!("Result: {}", result_json);

        let result_entity = DbJsonEntity::new(result_json.as_bytes().into()).unwrap();

        assert_eq!(
            result_entity.get_partition_key(result_json.as_bytes()),
            "Pk"
        );
        assert_eq!(result_entity.get_row_key(result_json.as_bytes()), "Rk");

        assert!(result_entity.get_expires(result_json.as_bytes()).is_none());
    }

    #[test]
    fn test_remove_expiration_time_at_begin_and_space_after_expire() {
        let test_json = r#"{"Expires": "2019-01-01T00:00:00",  "PartitionKey":"Pk","RowKey":"Rk"}"#;

        let inject_time_stamp = JsonTimeStamp::now();
        let db_row =
            DbJsonEntity::parse_into_db_row(test_json.as_bytes().into(), &inject_time_stamp)
                .unwrap();

        db_row.update_expires(None);

        let mut result_json = String::new();

        db_row.write_json(&mut result_json);

        println!("Result: {}", result_json);

        let result_entity = DbJsonEntity::new(result_json.as_bytes().into()).unwrap();

        assert_eq!(
            result_entity.get_partition_key(result_json.as_bytes()),
            "Pk"
        );
        assert_eq!(result_entity.get_row_key(result_json.as_bytes()), "Rk");

        assert!(result_entity.get_expires(result_json.as_bytes()).is_none());
    }

    #[test]
    fn test_remove_expiration_time_at_the_middle() {
        let test_json = r#"{"PartitionKey": "Pk",
            "Expires": "2019-01-01T00:00:00",
            "RowKey": "Rk"}"#;

        let inject_time_stamp = JsonTimeStamp::now();

        let db_row =
            DbJsonEntity::parse_into_db_row(test_json.as_bytes().into(), &inject_time_stamp)
                .unwrap();

        db_row.update_expires(None);

        let mut result_json = String::new();

        db_row.write_json(&mut result_json);

        println!("Result: {}. Len: {}", result_json, result_json.len());

        let result_entity = DbJsonEntity::new(result_json.as_bytes().into()).unwrap();

        assert_eq!(
            result_entity.get_partition_key(result_json.as_bytes()),
            "Pk"
        );
        assert_eq!(result_entity.get_row_key(result_json.as_bytes()), "Rk");

        assert!(result_entity.get_expires(result_json.as_bytes()).is_none());
    }
}
