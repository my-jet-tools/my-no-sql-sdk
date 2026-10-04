//! A default (unset) `expires` means "this row does not expire", and a row which does not
//! expire has no `Expires` - so it does not go on the wire at all, not even as
//! `"Expires":null`. It is the rule the default `time_stamp` follows, see `time_stamp_test.rs`.
//! Reading stays as tolerant as it was: rows which carry the `null` are still out there.

use my_no_sql_macros::*;
use my_no_sql_sdk::abstractions::{MyNoSqlEntitySerializer, Timestamp};
use my_no_sql_sdk::core::rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::*;

#[my_no_sql_entity(table_name:"test-expires", with_expires:true)]
#[derive(Serialize, Deserialize, Clone)]
pub struct ExpiringEntity {
    pub field1: String,
}

fn real_moment() -> Timestamp {
    DateTimeAsMicroseconds::from_str("2025-01-01T12:00:00.123456")
        .unwrap()
        .into()
}

fn expiring_entity(expires: Timestamp) -> ExpiringEntity {
    ExpiringEntity {
        partition_key: "pk".to_string(),
        row_key: "rk".to_string(),
        time_stamp: Default::default(),
        expires,
        field1: "value".to_string(),
    }
}

fn as_json(entity: &ExpiringEntity) -> String {
    String::from_utf8(entity.serialize_entity()).unwrap()
}

/// What used to go as `"Expires":null`.
#[test]
fn a_default_expires_is_left_out() {
    assert_eq!(
        r#"{"PartitionKey":"pk","RowKey":"rk","field1":"value"}"#,
        as_json(&expiring_entity(Default::default()))
    );
}

#[test]
fn a_real_expires_goes_as_expires() {
    assert_eq!(
        r#"{"PartitionKey":"pk","RowKey":"rk","field1":"value","Expires":"2025-01-01T12:00:00.123456"}"#,
        as_json(&expiring_entity(real_moment()))
    );
}

#[test]
fn an_entity_with_expires_round_trips() {
    let json = r#"{"PartitionKey":"pk","RowKey":"rk","field1":"value","Expires":"2025-01-01T12:00:00.123456"}"#;

    let entity = ExpiringEntity::deserialize_entity(json.as_bytes()).unwrap();

    assert_eq!(real_moment(), entity.expires);
    assert_eq!(json, as_json(&entity));
}

/// ...and an entity which has none goes back out the way it came in.
#[test]
fn an_entity_without_expires_round_trips() {
    let json = r#"{"PartitionKey":"pk","RowKey":"rk","field1":"value"}"#;

    let entity = ExpiringEntity::deserialize_entity(json.as_bytes()).unwrap();

    assert!(entity.expires.is_default());
    assert_eq!(json, as_json(&entity));
}

/// A row written before the field was left out - or by a client which spells "not set" as
/// `null` - carries `"Expires":null`, wherever in the row it stands.
#[test]
fn an_expires_of_null_reads_as_a_default_one() {
    for json in [
        r#"{"PartitionKey":"pk","RowKey":"rk","field1":"value","Expires":null}"#,
        r#"{"PartitionKey":"pk","RowKey":"rk","Expires":null,"field1":"value"}"#,
    ] {
        let entity = ExpiringEntity::deserialize_entity(json.as_bytes()).unwrap();

        assert!(entity.expires.is_default(), "source: {}", json);
        assert_eq!("value", entity.field1, "source: {}", json);
    }
}
