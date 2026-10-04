use my_json::json_reader::JsonFirstLineIterator;
use my_no_sql_abstractions::MyNoSqlEntity;
use serde::{de::DeserializeOwned, Serialize};

use crate::db_json_entity::{DbJsonEntity, JsonStrValue};

pub fn serialize<TMyNoSqlEntity>(entity: &TMyNoSqlEntity) -> Vec<u8>
where
    TMyNoSqlEntity: MyNoSqlEntity + Serialize,
{
    serde_json::to_vec(&entity).unwrap()
}

pub fn deserialize<TMyNoSqlEntity>(data: &[u8]) -> Result<TMyNoSqlEntity, String>
where
    TMyNoSqlEntity: MyNoSqlEntity + DeserializeOwned,
{
    let parse_result: Result<TMyNoSqlEntity, _> = serde_json::from_slice(&data);

    match parse_result {
        Ok(el) => return Ok(el),
        Err(err) => {
   
            let json_first_line_iterator = JsonFirstLineIterator::new(data);
            let db_entity = DbJsonEntity::new(json_first_line_iterator);

            match db_entity {
                Ok(db_entity) => {
                    return Err(format!(
                        "Table: {}. Can not parse entity with PartitionKey: [{}] and RowKey: [{}]. Err: {:?}",
                         TMyNoSqlEntity::TABLE_NAME, db_entity.get_partition_key(data), db_entity.get_row_key(data), err
                    ))
                    ;
                }
                Err(err) => {
                    return Err(format!(
                        "Table: {}. Can not extract partitionKey and rowKey. Looks like entity broken at all. Err: {:?}",
                        TMyNoSqlEntity::TABLE_NAME, err
                    ))
                    
                }
            }
        }
    }
}

pub fn inject_partition_key_and_row_key(
    src: Vec<u8>,
    partition_key: &str,
    row_key: Option<&str>,
) -> Vec<u8> {
    let found_object_index = src.iter().position(|&x| x == b'{');

    if found_object_index.is_none() {
        panic!(
            "Can not find object start while injecting partitionKey:{partition_key} and rowKey:{row_key:?}"
        );
    }

    let found_object_index = found_object_index.unwrap();

    // The keys arrive as values and go into a payload, so they go in as raw json - the one
    // direction, `read_as_raw`, the reader undoes with `read_as_value`.
    let partition_key = JsonStrValue::Unescaped(partition_key);

    let mut to_insert = if let Some(row_key) = row_key {
        format!(
            "\"PartitionKey\":\"{}\",\"RowKey\":\"{}\",",
            partition_key.read_as_raw().as_str(),
            JsonStrValue::Unescaped(row_key).read_as_raw().as_str(),
        )
        .into_bytes()
    } else {
        format!(
            "\"PartitionKey\":\"{}\",",
            partition_key.read_as_raw().as_str(),
        )
        .into_bytes()
    };

    // The comma behind the keys separates them from the members the object already has. An
    // object which has none (`{}`) gets the keys and nothing else - with the comma it would
    // end with `,}`, which is not json.
    let has_no_members = src[found_object_index + 1..]
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        == Some(&b'}');

    if has_no_members {
        to_insert.pop();
    }

    let mut result = Vec::with_capacity(src.len() + to_insert.len());

    result.extend_from_slice(&src[..found_object_index + 1]);

    result.extend_from_slice(to_insert.as_slice());

    result.extend_from_slice(&src[found_object_index + 1..]);

    result
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_injection() {
        let src = r#"{"TimeStamp":"2020-01-01T00:00:00.0000000Z","Value":"Value"}"#;

        let injected =
            super::inject_partition_key_and_row_key(src.as_bytes().to_vec(), "PK", "RK".into());

        let dest = String::from_utf8(injected).unwrap();

        assert_eq!(
            r#"{"PartitionKey":"PK","RowKey":"RK","TimeStamp":"2020-01-01T00:00:00.0000000Z","Value":"Value"}"#,
            dest
        );
    }

    #[test]
    fn test_injection_no_rk() {
        let src = r#"{"TimeStamp":"2020-01-01T00:00:00.0000000Z","Value":"Value"}"#;

        let injected = super::inject_partition_key_and_row_key(src.as_bytes().to_vec(), "PK", None);

        let dest = String::from_utf8(injected).unwrap();

        assert_eq!(
            r#"{"PartitionKey":"PK","TimeStamp":"2020-01-01T00:00:00.0000000Z","Value":"Value"}"#,
            dest
        );
    }

    /// An entity which serializes nothing but its keys - an enum case with no fields of its
    /// own and a default `TimeStamp`, which is left out of the json.
    #[test]
    fn test_injection_into_an_object_with_no_members() {
        let injected = super::inject_partition_key_and_row_key(b"{}".to_vec(), "PK", "RK".into());

        assert_eq!(
            r#"{"PartitionKey":"PK","RowKey":"RK"}"#,
            String::from_utf8(injected).unwrap()
        );

        let injected = super::inject_partition_key_and_row_key(b"{ }".to_vec(), "PK", None);

        assert_eq!(
            r#"{"PartitionKey":"PK" }"#,
            String::from_utf8(injected).unwrap()
        );
    }

    #[derive(serde::Deserialize, Debug)]
    struct TestEntity {
        #[serde(rename = "PartitionKey")]
        partition_key: String,
        #[serde(rename = "RowKey")]
        row_key: String,
    }

    impl my_no_sql_abstractions::MyNoSqlEntity for TestEntity {
        const TABLE_NAME: &'static str = "test";
        const LAZY_DESERIALIZATION: bool = false;

        fn get_partition_key(&self) -> &str {
            &self.partition_key
        }

        fn get_row_key(&self) -> &str {
            &self.row_key
        }

        fn get_time_stamp(&self) -> my_no_sql_abstractions::Timestamp {
            Default::default()
        }
    }

    /// A row serde can not read is reported by its keys, and they are read out of the json
    /// for the message - the way a stored row is read, so a row whose key is not a json string
    /// is named by the key it lies under on the server: `123` lies under `2`. A key which can
    /// not be read even that way - a one-character `5` - used to panic here; now the error
    /// says what is wrong with it.
    #[test]
    fn a_row_whose_key_is_not_a_string_is_an_error_and_not_a_panic() {
        for (json, expected) in [
            (
                r#"{"PartitionKey":5,"RowKey":"Rk"}"#,
                "PartitionKey must be a json string",
            ),
            (
                r#"{"PartitionKey":123,"RowKey":"Rk"}"#,
                "PartitionKey: [2] and RowKey: [Rk]",
            ),
            (
                r#"{"PartitionKey":"Pk","RowKey":true}"#,
                "PartitionKey: [Pk] and RowKey: [ru]",
            ),
        ] {
            let err = super::deserialize::<TestEntity>(json.as_bytes()).unwrap_err();

            assert!(
                err.contains(expected),
                "source: {}. Err: {}",
                json,
                err
            );
        }
    }
}
