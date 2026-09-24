use my_no_sql_macros::*;

use serde::*;

#[enum_of_my_no_sql_entity(table_name:"Test", generate_unwraps)]
pub enum MyNoSqlEnumEntityTestVer1 {
    Case1(Struct1),
}

#[enum_of_my_no_sql_entity(table_name:"Test", generate_unwraps)]
pub enum MyNoSqlEnumEntityTestVer2 {
    Case1(Struct1),
    Case2(Struct2),
}

#[enum_model(partition_key:"pk1", row_key: "rk1")]
#[derive(Serialize, Deserialize, Clone)]
pub struct Struct1 {
    pub field1: String,
    pub field2: i32,
}

#[enum_model(partition_key:"pk2", row_key: "rk2")]
#[derive(Serialize, Deserialize, Clone)]
pub struct Struct2 {
    pub field3: String,
    pub field4: i32,
}

#[cfg(test)]
mod tests {

    use my_no_sql_sdk::abstractions::MyNoSqlEntitySerializer;

    use super::*;

    #[test]
    fn test() {
        let model_ver2 = MyNoSqlEnumEntityTestVer2::Case2(Struct2 {
            time_stamp: Default::default(),
            field3: "field3".to_string(),
            field4: 4,
        });

        let result = model_ver2.serialize_entity();

        let model_ver1 = MyNoSqlEnumEntityTestVer1::deserialize_entity(result.as_slice());

        assert!(model_ver1.is_err());

        if let Err(err) = model_ver1 {
            println!("{}", err);
        }
    }
}
