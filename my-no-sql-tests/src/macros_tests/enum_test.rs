use my_no_sql_macros::*;
use my_no_sql_sdk::abstractions::MyNoSqlEntitySerializer;
use serde::*;

#[enum_of_my_no_sql_entity(table_name:"Test", generate_unwraps)]
pub enum MyNoSqlEnumEntityTest {
    Case1(Struct1),
    Case2(Struct2),
}

#[enum_model(partition_key:"pk1", row_key: "rk1")]
#[derive(Serialize, Deserialize, Clone)]
pub struct Struct1 {
    pub field1: String,
    pub field2: i32,
}

#[enum_model(partition_key:"pk2")]
#[derive(Serialize, Deserialize, Clone)]
pub struct Struct2 {
    pub field3: String,
    pub field4: i32,
}

#[test]
fn test_serialize_deserialize_case_1() {
    let src_model = Struct1 {
        time_stamp: Default::default(),
        field1: "test".to_string(),
        field2: 123,
    };
    let entity = MyNoSqlEnumEntityTest::Case1(src_model.clone());

    let vec = entity.serialize_entity();

    let dest = MyNoSqlEnumEntityTest::deserialize_entity(&vec).unwrap();

    let model = dest.unwrap_case1();

    assert_eq!(src_model.field1, model.field1);
    assert_eq!(src_model.field2, model.field2);
}

#[test]
fn test_serialize_deserialize_case_2() {
    let src_model = Struct2 {
        row_key: "rk2".to_string(),
        time_stamp: Default::default(),
        field3: "test3".to_string(),
        field4: 1234,
    };
    let entity = MyNoSqlEnumEntityTest::Case2(src_model.clone());

    let vec = entity.serialize_entity();

    let dest = MyNoSqlEnumEntityTest::deserialize_entity(&vec).unwrap();

    let model = dest.unwrap_case2();

    assert_eq!(src_model.field3, model.field3);
    assert_eq!(src_model.field4, model.field4);
}
