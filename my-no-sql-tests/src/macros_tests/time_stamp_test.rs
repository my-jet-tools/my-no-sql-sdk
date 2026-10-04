//! A default (unset) `time_stamp` means "the server stamps its own time", so it does not go on
//! the wire at all - not even as `"TimeStamp":null`. For `Replace` on a server built before
//! my-no-sql-core stopped reading a `null` as a text the two are different things: a missing
//! `TimeStamp` is answered 400, and a `null` one is answered 409 "Record is changed" - about a
//! record nobody touched. A newer server answers 400 for both; a string which is not a date
//! still gets the 409. So both entity macros have to put the field on the wire the same way.

use my_no_sql_macros::*;
use my_no_sql_sdk::abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp};
use my_no_sql_sdk::core::rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::*;

#[my_no_sql_entity("test-time-stamp")]
#[derive(Serialize, Deserialize, Clone)]
pub struct StructEntity {
    pub field1: String,
}

#[enum_of_my_no_sql_entity(table_name:"test-time-stamp-enum", generate_unwraps)]
pub enum EnumEntity {
    Single(SingleModel),
    Many(ManyModel),
    Marker(MarkerModel),
    Optional(OptionalModel),
}

/// The row key is a part of the case - the struct has no `row_key` field.
#[enum_model(partition_key:"single", row_key: "rk")]
#[derive(Serialize, Deserialize, Clone)]
pub struct SingleModel {
    pub field1: String,
}

/// The case is the whole partition - the row key is a field.
#[enum_model(partition_key:"many")]
#[derive(Serialize, Deserialize, Clone)]
pub struct ManyModel {
    pub field1: String,
}

/// A case with no fields of its own.
#[enum_model(partition_key:"marker", row_key: "rk")]
#[derive(Serialize, Deserialize, Clone)]
pub struct MarkerModel {}

/// A case which can have every field of its own skipped.
#[enum_model(partition_key:"optional", row_key: "rk")]
#[derive(Serialize, Deserialize, Clone)]
pub struct OptionalModel {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field1: Option<String>,
}

fn real_time_stamp() -> Timestamp {
    DateTimeAsMicroseconds::from_str("2025-01-01T12:00:00.123456")
        .unwrap()
        .into()
}

fn struct_entity(time_stamp: Timestamp) -> StructEntity {
    StructEntity {
        partition_key: "pk".to_string(),
        row_key: "rk".to_string(),
        time_stamp,
        field1: "value".to_string(),
    }
}

fn single_case(time_stamp: Timestamp) -> EnumEntity {
    EnumEntity::Single(SingleModel {
        time_stamp,
        field1: "value".to_string(),
    })
}

fn many_case(time_stamp: Timestamp) -> EnumEntity {
    EnumEntity::Many(ManyModel {
        row_key: "rk".to_string(),
        time_stamp,
        field1: "value".to_string(),
    })
}

fn as_json<TEntity: MyNoSqlEntitySerializer>(entity: &TEntity) -> String {
    String::from_utf8(entity.serialize_entity()).unwrap()
}

#[test]
fn a_default_time_stamp_of_a_struct_entity_is_left_out() {
    assert_eq!(
        r#"{"PartitionKey":"pk","RowKey":"rk","field1":"value"}"#,
        as_json(&struct_entity(Default::default()))
    );
}

#[test]
fn a_real_time_stamp_of_a_struct_entity_goes_as_time_stamp() {
    assert_eq!(
        r#"{"PartitionKey":"pk","RowKey":"rk","TimeStamp":"2025-01-01T12:00:00.123456","field1":"value"}"#,
        as_json(&struct_entity(real_time_stamp()))
    );
}

#[test]
fn a_struct_entity_with_a_time_stamp_round_trips() {
    let json = r#"{"PartitionKey":"pk","RowKey":"rk","TimeStamp":"2025-01-01T12:00:00.123456","field1":"value"}"#;

    let entity = StructEntity::deserialize_entity(json.as_bytes()).unwrap();

    assert_eq!(real_time_stamp(), entity.time_stamp);
    assert_eq!(json, as_json(&entity));
}

#[test]
fn a_struct_entity_without_a_time_stamp_reads_as_a_default_one() {
    let json = r#"{"PartitionKey":"pk","RowKey":"rk","field1":"value"}"#;

    let entity = StructEntity::deserialize_entity(json.as_bytes()).unwrap();

    assert!(entity.time_stamp.is_default());
    assert_eq!("value", entity.field1);
}

/// What used to go as `"TimeStamp":null`.
#[test]
fn a_default_time_stamp_of_an_enum_case_is_left_out() {
    assert_eq!(
        r#"{"PartitionKey":"single","RowKey":"rk","field1":"value"}"#,
        as_json(&single_case(Default::default()))
    );

    assert_eq!(
        r#"{"PartitionKey":"many","RowKey":"rk","field1":"value"}"#,
        as_json(&many_case(Default::default()))
    );
}

#[test]
fn a_real_time_stamp_of_an_enum_case_goes_as_time_stamp() {
    assert_eq!(
        r#"{"PartitionKey":"single","RowKey":"rk","TimeStamp":"2025-01-01T12:00:00.123456","field1":"value"}"#,
        as_json(&single_case(real_time_stamp()))
    );

    assert_eq!(
        r#"{"PartitionKey":"many","RowKey":"rk","TimeStamp":"2025-01-01T12:00:00.123456","field1":"value"}"#,
        as_json(&many_case(real_time_stamp()))
    );
}

#[test]
fn an_enum_case_with_a_time_stamp_round_trips() {
    for json in [
        r#"{"PartitionKey":"single","RowKey":"rk","TimeStamp":"2025-01-01T12:00:00.123456","field1":"value"}"#,
        r#"{"PartitionKey":"many","RowKey":"rk","TimeStamp":"2025-01-01T12:00:00.123456","field1":"value"}"#,
    ] {
        let entity = EnumEntity::deserialize_entity(json.as_bytes()).unwrap();

        assert_eq!(real_time_stamp(), entity.get_time_stamp());
        assert_eq!(json, as_json(&entity));
    }
}

#[test]
fn an_enum_case_without_a_time_stamp_reads_as_a_default_one() {
    let json = r#"{"PartitionKey":"single","RowKey":"rk","field1":"value"}"#;

    let entity = EnumEntity::deserialize_entity(json.as_bytes()).unwrap();

    assert!(entity.get_time_stamp().is_default());
    assert_eq!("value", entity.unwrap_single().field1);

    let json = r#"{"PartitionKey":"many","RowKey":"rk","field1":"value"}"#;

    let entity = EnumEntity::deserialize_entity(json.as_bytes()).unwrap();

    assert!(entity.get_time_stamp().is_default());
    assert_eq!("value", entity.unwrap_many().field1);
}

/// The keys are injected in front of what the case serializes itself. With the default
/// `TimeStamp` left out a case can have nothing of its own to serialize - the entity is then
/// its keys and nothing else, and it still has to be a json which parses.
#[test]
fn an_enum_case_with_nothing_of_its_own_to_serialize_is_its_keys() {
    let json = as_json(&EnumEntity::Marker(MarkerModel {
        time_stamp: Default::default(),
    }));

    assert_eq!(r#"{"PartitionKey":"marker","RowKey":"rk"}"#, json);

    let entity = EnumEntity::deserialize_entity(json.as_bytes()).unwrap();
    assert!(entity.unwrap_marker().time_stamp.is_default());

    let json = as_json(&EnumEntity::Optional(OptionalModel {
        time_stamp: Default::default(),
        field1: None,
    }));

    assert_eq!(r#"{"PartitionKey":"optional","RowKey":"rk"}"#, json);

    let entity = EnumEntity::deserialize_entity(json.as_bytes()).unwrap();
    assert!(entity.unwrap_optional().field1.is_none());
}

/// ...and whatever a case does serialize goes behind the keys as it is - a string which itself
/// ends with `,}` included.
#[test]
fn an_enum_case_with_something_to_serialize_keeps_it_as_it_is() {
    let json = as_json(&EnumEntity::Marker(MarkerModel {
        time_stamp: real_time_stamp(),
    }));

    assert_eq!(
        r#"{"PartitionKey":"marker","RowKey":"rk","TimeStamp":"2025-01-01T12:00:00.123456"}"#,
        json
    );

    let json = as_json(&EnumEntity::Optional(OptionalModel {
        time_stamp: Default::default(),
        field1: Some("value,}".to_string()),
    }));

    assert_eq!(
        r#"{"PartitionKey":"optional","RowKey":"rk","field1":"value,}"}"#,
        json
    );

    let entity = EnumEntity::deserialize_entity(json.as_bytes()).unwrap();
    assert_eq!(Some("value,}"), entity.unwrap_optional().field1.as_deref());
}
