# How to use


## 1. Make settings structure and implement MyNoSqlTcpConnectionSettings trait.

Basically Connection read host_port each reconnection moment. Trait gives ability to update host_port in realtime without restarting the application. A connection string which can not be parsed does not break that: the error is written to `my_logger`, the connect attempt is skipped, and host_port is read again at the next one.
```rust
pub struct MyNoSqlTcpReaderSettings {}

#[async_trait::async_trait]
impl my_no_sql_tcp_reader::MyNoSqlTcpConnectionSettings for MyNoSqlTcpReaderSettings {
    async fn get_host_port(&self) -> String {
        "localhost:5125".to_string()
    }
}

```

## 2. Create entity
The Serde and [my-no-sql-macros](../my-no-sql-macros) macros libraries are used. The macro expands to `my_no_sql_sdk::…` paths, so the crate which defines the entity has to depend on `my-no-sql-sdk` as well.


Without expiration
```rust
#[my_no_sql_macros::my_no_sql_entity("test")]
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TestEntity {
}

```

With expiration (the macro adds the `expires: Timestamp` field)
```rust
#[my_no_sql_macros::my_no_sql_entity(table_name:"test", with_expires:true)]
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TestEntity {
}
```

## 3. Create connection, extract reader from the connection and start the connection;

```rust
let connection = my_no_sql_tcp_reader::MyNoSqlTcpConnection::new(
    "app_name".to_string(),
    Arc::new(MyNoSqlTcpReaderSettings {}),
);

let reader: Arc<MyNoSqlDataReaderTcp<TestEntity>> = connection.get_reader();
    
connection.start().await;

// The first snapshot arrives after the connection is established - wait for it before reading.
// wait_until_first_data_arrives is a method of the MyNoSqlDataReader trait.
use my_no_sql_tcp_reader::MyNoSqlDataReader;
reader.wait_until_first_data_arrives().await;
```

## 4. Get Records from reader
```rust
let entity = reader.get_entity("partition_key", "row_key");
println!("{:?}", entity);
```

## 5. Get Records from reader and update row last read moment, partition last read moment and row expiration moment
```rust
use my_no_sql_sdk::core::rust_extensions::date_time::DateTimeAsMicroseconds;

// the row expires in a minute
let expires = DateTimeAsMicroseconds::now().add(std::time::Duration::from_secs(60));

let entity = reader
         .get_entity_with_callback_to_server("partition_key", "row_key")
         .set_row_last_read_moment()
         .set_partition_last_read_moment()
         .set_row_expiration_moment(Some(expires))
         .execute();
         
println!("{:?}", entity);
```

The moments are reported to the server in the background, one report at a time. A row gets the expiration moment which
was asked for it last. Rows of a partition which wait for their turn together and whose moments fall into the same second
go in one report, with the latest of those moments - so a row may get a moment which is later than the one asked for by
less than a second, never an earlier one. A read which finds no report on its way is reported at once.
