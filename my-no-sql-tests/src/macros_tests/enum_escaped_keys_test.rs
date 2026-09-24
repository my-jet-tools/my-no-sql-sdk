//! Which enum case a record is, is decided by comparing its keys against the constants in the
//! code. The record arrives as json, where the very same key has many valid spellings - .NET's
//! `System.Text.Json` escapes every non-ASCII character with its default encoder - so the
//! comparison has to be about the value, not about the characters in the payload.

use my_no_sql_macros::*;
use my_no_sql_sdk::abstractions::MyNoSqlEntitySerializer;
use serde::*;

#[enum_of_my_no_sql_entity(table_name:"test-escaped-keys", generate_unwraps)]
pub enum EscapedKeysEnumEntity {
    Demo(DemoModel),
    Other(OtherModel),
}

#[enum_model(partition_key:"демо", row_key:"строка")]
#[derive(Serialize, Deserialize, Clone)]
pub struct DemoModel {
    pub field1: String,
}

#[enum_model(partition_key:"other")]
#[derive(Serialize, Deserialize, Clone)]
pub struct OtherModel {
    pub field2: String,
}

/// What a .NET client puts on the wire for the very same keys.
const ESCAPED_JSON: &str = concat!(
    "{\"PartitionKey\":\"\\u0434\\u0435\\u043c\\u043e\",",
    "\"RowKey\":\"\\u0441\\u0442\\u0440\\u043e\\u043a\\u0430\",",
    "\"TimeStamp\":\"2020-05-06T07:08:09\",\"field1\":\"value\"}"
);

#[test]
fn a_dotnet_escaped_payload_lands_in_its_case() {
    let entity = EscapedKeysEnumEntity::deserialize_entity(ESCAPED_JSON.as_bytes()).unwrap();

    assert_eq!(entity.unwrap_demo().field1, "value");
}

/// ...and the unescaped spelling of the same keys is the same case.
#[test]
fn the_plain_spelling_lands_in_the_same_case() {
    let json = r#"{"PartitionKey":"демо","RowKey":"строка","TimeStamp":"2020-05-06T07:08:09","field1":"value"}"#;

    let entity = EscapedKeysEnumEntity::deserialize_entity(json.as_bytes()).unwrap();

    assert_eq!(entity.unwrap_demo().field1, "value");
}

/// A record which belongs to no case is still an error - the comparison did not become
/// permissive, it became right.
#[test]
fn a_foreign_key_is_still_an_unknown_case() {
    let json = r#"{"PartitionKey":"демп","RowKey":"строка","TimeStamp":"2020-05-06T07:08:09","field1":"value"}"#;

    let result = EscapedKeysEnumEntity::deserialize_entity(json.as_bytes());

    assert!(result.is_err());
}

#[test]
fn round_trip_of_a_serialized_entity_still_works() {
    let entity = EscapedKeysEnumEntity::Other(OtherModel {
        row_key: "rk".to_string(),
        time_stamp: Default::default(),
        field2: "value2".to_string(),
    });

    let serialized = entity.serialize_entity();

    let dest = EscapedKeysEnumEntity::deserialize_entity(&serialized).unwrap();

    assert_eq!(dest.unwrap_other().field2, "value2");
}
