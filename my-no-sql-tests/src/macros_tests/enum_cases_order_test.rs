//! Which case a row is, is decided by its keys: a case with a row key is one row, a case without
//! one is the whole partition. The two can share a partition, and then the row of the first one
//! fits both - it has to be read as the case which names it, not as the case which happens to
//! be declared first.

use my_no_sql_macros::*;
use my_no_sql_sdk::abstractions::MyNoSqlEntitySerializer;
use serde::*;

/// The whole partition is declared before the one row of it.
#[enum_of_my_no_sql_entity(table_name:"test-cases-order", generate_unwraps)]
pub enum WideFirstEntity {
    Wide(WideModel),
    Narrow(NarrowModel),
}

/// The same two cases the other way round.
#[enum_of_my_no_sql_entity(table_name:"test-cases-order", generate_unwraps)]
pub enum NarrowFirstEntity {
    Narrow(NarrowModel),
    Wide(WideModel),
}

#[enum_model(partition_key:"same")]
#[derive(Serialize, Deserialize, Clone)]
pub struct WideModel {
    pub field1: String,
}

#[enum_model(partition_key:"same", row_key: "narrow")]
#[derive(Serialize, Deserialize, Clone)]
pub struct NarrowModel {
    pub field1: String,
}

/// The one row of the partition asks for a field the case of the partition does not have.
#[enum_of_my_no_sql_entity(table_name:"test-cases-order")]
pub enum StricterRowEntity {
    Wide(WideModel),
    Stricter(StricterModel),
}

#[enum_model(partition_key:"same", row_key: "stricter")]
#[derive(Serialize, Deserialize, Clone)]
pub struct StricterModel {
    pub field1: String,
    pub field2: i32,
}

/// Cases which can not be told apart at all: the very same keys.
#[enum_of_my_no_sql_entity(table_name:"test-cases-order", generate_unwraps)]
pub enum SameKeysEntity {
    FirstRow(FirstRowModel),
    SecondRow(SecondRowModel),
    FirstPartition(FirstPartitionModel),
    SecondPartition(SecondPartitionModel),
}

#[enum_model(partition_key:"row", row_key: "rk")]
#[derive(Serialize, Deserialize, Clone)]
pub struct FirstRowModel {
    pub field1: String,
}

#[enum_model(partition_key:"row", row_key: "rk")]
#[derive(Serialize, Deserialize, Clone)]
pub struct SecondRowModel {
    pub field1: String,
}

#[enum_model(partition_key:"partition")]
#[derive(Serialize, Deserialize, Clone)]
pub struct FirstPartitionModel {
    pub field1: String,
}

#[enum_model(partition_key:"partition")]
#[derive(Serialize, Deserialize, Clone)]
pub struct SecondPartitionModel {
    pub field1: String,
}

fn narrow() -> NarrowModel {
    NarrowModel {
        time_stamp: Default::default(),
        field1: "narrow".to_string(),
    }
}

fn wide(row_key: &str) -> WideModel {
    WideModel {
        row_key: row_key.to_string(),
        time_stamp: Default::default(),
        field1: "wide".to_string(),
    }
}

/// It used to be read back as `Wide`.
#[test]
fn a_case_with_both_keys_declared_after_its_partition_is_read_as_itself() {
    let serialized = WideFirstEntity::Narrow(narrow()).serialize_entity();

    assert_eq!(
        r#"{"PartitionKey":"same","RowKey":"narrow","field1":"narrow"}"#,
        std::str::from_utf8(&serialized).unwrap()
    );

    let entity = WideFirstEntity::deserialize_entity(&serialized).unwrap();

    assert_eq!("narrow", entity.unwrap_narrow().field1);
}

/// ...while every other row of the partition is still the case of the partition.
#[test]
fn the_other_rows_of_the_partition_are_the_case_of_the_partition() {
    let serialized = WideFirstEntity::Wide(wide("other")).serialize_entity();

    let entity = WideFirstEntity::deserialize_entity(&serialized).unwrap();

    assert_eq!("wide", entity.unwrap_wide().field1);
    assert_eq!("other", entity.unwrap_wide().row_key);
}

/// The keys alone say which case a row is - what it was written as does not. A row written as
/// the case of the partition under the row key of the other case is that other case when it is
/// read. It used to be read back as `Wide`.
#[test]
fn a_row_under_the_keys_of_a_case_is_that_case_whatever_it_was_written_as() {
    let serialized = WideFirstEntity::Wide(wide("narrow")).serialize_entity();

    assert_eq!(
        r#"{"PartitionKey":"same","RowKey":"narrow","field1":"wide"}"#,
        std::str::from_utf8(&serialized).unwrap()
    );

    let entity = WideFirstEntity::deserialize_entity(&serialized).unwrap();

    assert_eq!("wide", entity.unwrap_narrow().field1);
}

/// ...and when it does not read as that case it is an error. It is not handed over to the case
/// of the partition, which would have read it - and did, when it was declared first.
#[test]
fn a_row_under_the_keys_of_a_case_which_does_not_read_as_it_is_an_error() {
    let serialized = StricterRowEntity::Wide(wide("stricter")).serialize_entity();

    match StricterRowEntity::deserialize_entity(&serialized) {
        Ok(_) => panic!("Expected an error"),
        Err(err) => assert!(
            err.starts_with(
                "Table: test-cases-order. Can not parse entity with PartitionKey: [same] and RowKey: [stricter]. Err: "
            ),
            "error: {}",
            err
        ),
    }
}

#[test]
fn the_order_of_declaration_changes_nothing() {
    let serialized = NarrowFirstEntity::Narrow(narrow()).serialize_entity();
    let entity = NarrowFirstEntity::deserialize_entity(&serialized).unwrap();
    assert_eq!("narrow", entity.unwrap_narrow().field1);

    let serialized = NarrowFirstEntity::Wide(wide("other")).serialize_entity();
    let entity = NarrowFirstEntity::deserialize_entity(&serialized).unwrap();
    assert_eq!("other", entity.unwrap_wide().row_key);
}

/// Nothing tells such cases apart, so the one declared first gets the row - as it always did.
#[test]
fn of_the_cases_with_the_very_same_keys_the_first_declared_one_wins() {
    let serialized = SameKeysEntity::SecondRow(SecondRowModel {
        time_stamp: Default::default(),
        field1: "second".to_string(),
    })
    .serialize_entity();

    let entity = SameKeysEntity::deserialize_entity(&serialized).unwrap();
    assert_eq!("second", entity.unwrap_first_row().field1);

    let serialized = SameKeysEntity::SecondPartition(SecondPartitionModel {
        row_key: "rk".to_string(),
        time_stamp: Default::default(),
        field1: "second".to_string(),
    })
    .serialize_entity();

    let entity = SameKeysEntity::deserialize_entity(&serialized).unwrap();
    assert_eq!("second", entity.unwrap_first_partition().field1);
}
