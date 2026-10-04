//! `deserialize_entity` of an enum entity answers with an `Err` whatever it is given - the way a
//! struct entity does, and in the same words. The writer counts on it: a body which is not a
//! row at all - the html page of a proxy, a plain-text answer - is what it turns into its own
//! error. The message ends up in a log, so it has to name the table the row is in - and only
//! the enum knows it: the model of a case has no table of its own.

use my_no_sql_macros::*;
use my_no_sql_sdk::abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp};
use serde::*;

/// A struct entity of the very same table: what it says about a body is what the enum says.
#[my_no_sql_entity("test-enum-errors")]
#[derive(Serialize, Deserialize, Clone)]
pub struct StructEntity {
    pub field1: String,
    pub field2: i32,
}

#[enum_of_my_no_sql_entity(table_name:"test-enum-errors")]
pub enum EnumEntity {
    Single(SingleModel),
    Many(ManyModel),
    HandMade(HandMadeModel),
}

#[enum_model(partition_key:"single", row_key: "rk")]
#[derive(Serialize, Deserialize, Clone)]
pub struct SingleModel {
    pub field1: String,
    pub field2: i32,
}

#[enum_model(partition_key:"many")]
#[derive(Serialize, Deserialize, Clone)]
pub struct ManyModel {
    pub field1: String,
    pub field2: i32,
}

/// A case which is not made by `#[enum_model]` and reports a row in words of its own.
#[derive(Clone)]
pub struct HandMadeModel;

impl HandMadeModel {
    pub const PARTITION_KEY: &'static str = "hand-made";
    pub const ROW_KEY: Option<&'static str> = Some("rk");
}

impl MyNoSqlEntity for HandMadeModel {
    const TABLE_NAME: &'static str = "";
    const LAZY_DESERIALIZATION: bool = true;

    fn get_partition_key(&self) -> &str {
        Self::PARTITION_KEY
    }

    fn get_row_key(&self) -> &str {
        "rk"
    }

    fn get_time_stamp(&self) -> Timestamp {
        Default::default()
    }
}

impl MyNoSqlEntitySerializer for HandMadeModel {
    fn serialize_entity(&self) -> Vec<u8> {
        b"{}".to_vec()
    }

    fn deserialize_entity(_src: &[u8]) -> Result<Self, String> {
        Err("Nothing here can be read".to_string())
    }
}

fn error_of_the_enum(body: &str) -> String {
    match EnumEntity::deserialize_entity(body.as_bytes()) {
        Ok(_) => panic!("Expected an error. Body: {}", body),
        Err(err) => err,
    }
}

fn error_of_the_struct(body: &str) -> String {
    match StructEntity::deserialize_entity(body.as_bytes()) {
        Ok(_) => panic!("Expected an error. Body: {}", body),
        Err(err) => err,
    }
}

/// Every one of them used to be a panic.
#[test]
fn a_body_which_is_not_an_entity_is_an_error() {
    for body in [
        "",
        "Record not found",
        "<html><body>index</body></html>",
        "[]",
        "{}",
        r#"{"PartitionKey":"single"}"#,
        r#"{"RowKey":"rk"}"#,
        r#"{"PartitionKey":"single","RowKey":"rk","field1":"#,
    ] {
        let err = error_of_the_enum(body);

        assert!(
            err.starts_with("Table: test-enum-errors. Can not extract partitionKey and rowKey."),
            "body: {}, error: {}",
            body,
            err
        );

        assert_eq!(error_of_the_struct(body), err, "body: {}", body);
    }
}

/// The keys say which case the row is, and then the row is not what the model of the case
/// reads. The model has no table of its own, so the message used to begin with `Table: . `.
#[test]
fn a_case_which_does_not_parse_names_the_table_of_the_enum() {
    for (json, expected) in [
        (
            r#"{"PartitionKey":"single","RowKey":"rk","field1":"value","field2":"not a number"}"#,
            "Table: test-enum-errors. Can not parse entity with PartitionKey: [single] and RowKey: [rk]. Err: ",
        ),
        (
            r#"{"PartitionKey":"many","RowKey":"any","field1":"value"}"#,
            "Table: test-enum-errors. Can not parse entity with PartitionKey: [many] and RowKey: [any]. Err: ",
        ),
    ] {
        let err = error_of_the_enum(json);

        assert!(err.starts_with(expected), "source: {}, error: {}", json, err);

        assert_eq!(error_of_the_struct(json), err, "source: {}", json);
    }
}

/// ...and a model which words its errors itself gets the table named as well.
#[test]
fn an_error_in_the_words_of_the_model_names_the_table_too() {
    assert_eq!(
        "Table: test-enum-errors. Nothing here can be read",
        error_of_the_enum(r#"{"PartitionKey":"hand-made","RowKey":"rk"}"#)
    );
}

#[test]
fn a_row_of_no_case_is_still_an_unknown_case() {
    assert_eq!(
        "Table: 'test-enum-errors'. Unknown Enum Case for the record with PartitionKey: other and RowKey: rk",
        error_of_the_enum(r#"{"PartitionKey":"other","RowKey":"rk","field1":"value","field2":5}"#)
    );
}
