//! service-sdk waits for the first snapshot of every reader it hands out, and it does that through
//! `MyNoSqlDataReader::wait_until_first_data_arrives`. An enum entity has no serde `Deserialize` -
//! it is read through `MyNoSqlEntitySerializer` - so the tcp reader has to implement the trait on
//! the very bounds the trait itself asks for, and on nothing more.

use std::{collections::BTreeMap, sync::Arc};

use my_no_sql_sdk::abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer};
use my_no_sql_tcp_reader::{
    MyNoSqlDataReader, MyNoSqlDataReaderTcp, MyNoSqlTcpConnection, MyNoSqlTcpConnectionSettings,
};

use super::enum_test::{MyNoSqlEnumEntityTest, Struct1, Struct2};

fn assert_is_data_reader<TMyNoSqlEntity, TReader>()
where
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
    TReader: MyNoSqlDataReader<TMyNoSqlEntity>,
{
}

/// The bounds are the ones `get_ns_reader` has in service-sdk. It compiles only while the impl for
/// the tcp reader asks for nothing on top of them - one more bound there, and this stops building.
fn tcp_reader_is_a_data_reader<TMyNoSqlEntity>()
where
    TMyNoSqlEntity: MyNoSqlEntity + MyNoSqlEntitySerializer + Sync + Send + 'static,
{
    assert_is_data_reader::<TMyNoSqlEntity, MyNoSqlDataReaderTcp<TMyNoSqlEntity>>();
}

#[test]
fn a_reader_of_an_enum_entity_is_a_data_reader() {
    tcp_reader_is_a_data_reader::<MyNoSqlEnumEntityTest>();
}

struct TestSettings;

#[async_trait::async_trait]
impl MyNoSqlTcpConnectionSettings for TestSettings {
    async fn get_host_port(&self) -> String {
        "127.0.0.1:5125".to_string()
    }
}

/// The reader has its own `get_enum_case_model` and `get_enum_case_models_by_partition_key`, and
/// the trait has methods of the same names - the second one giving a `BTreeMap`, not a `Vec`. With
/// the trait in scope, as it is here, a plain call still has to land on the reader's own method.
#[test]
fn enum_case_calls_land_on_the_readers_own_methods() {
    // never started - nothing gets connected
    let connection = MyNoSqlTcpConnection::new("test-app", Arc::new(TestSettings));
    let reader = connection.get_reader::<MyNoSqlEnumEntityTest>();

    let case_1: Option<Struct1> = reader.get_enum_case_model();
    let case_2: Option<Vec<Struct2>> = reader.get_enum_case_models_by_partition_key();

    // the trait's one is there as well, and it is a different method
    let case_2_by_trait: Option<BTreeMap<String, Struct2>> =
        MyNoSqlDataReader::get_enum_case_models_by_partition_key(reader.as_ref());

    assert!(case_1.is_none());
    assert!(case_2.is_none());
    assert!(case_2_by_trait.is_none());
}
