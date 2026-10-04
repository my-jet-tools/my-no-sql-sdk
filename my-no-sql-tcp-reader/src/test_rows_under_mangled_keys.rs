//! A server may still hold rows which were written before keys were checked: a `PartitionKey`
//! or a `RowKey` which is not a json string, stored under the value with its first and last
//! character cut off (`123` under `2`). The server keeps loading such rows and keeps sending
//! them to its subscribers under that key.
//!
//! A reader with lazy deserialization keeps a row as it came and indexes it by what
//! `DbJsonEntity::from_slice` reads its keys as - so the packet has to be read, and the row has
//! to land under the very key the server has it under: that is the key a delete of the row
//! arrives with. Refusing such a key in `from_slice` would panic here on every snapshot which
//! holds the row.

use std::sync::Arc;

use my_no_sql_abstractions::{MyNoSqlEntity, MyNoSqlEntitySerializer, Timestamp};
use my_no_sql_tcp_shared::{sync_to_main::SyncToMainNodeHandler, DeleteRowTcpContract};

use crate::{subscribers::UpdateEvent, MyNoSqlDataReaderTcp};

#[derive(serde::Serialize, serde::Deserialize)]
struct LazyEntity {
    #[serde(rename = "PartitionKey")]
    partition_key: String,
    #[serde(rename = "RowKey")]
    row_key: String,
}

impl MyNoSqlEntity for LazyEntity {
    const TABLE_NAME: &'static str = "test-table";
    const LAZY_DESERIALIZATION: bool = true;

    fn get_partition_key(&self) -> &str {
        self.partition_key.as_str()
    }

    fn get_row_key(&self) -> &str {
        self.row_key.as_str()
    }

    fn get_time_stamp(&self) -> Timestamp {
        rust_extensions::date_time::DateTimeAsMicroseconds::new(0).into()
    }
}

impl MyNoSqlEntitySerializer for LazyEntity {
    fn serialize_entity(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    fn deserialize_entity(src: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(src).map_err(|err| format!("{}", err))
    }
}

fn reader() -> MyNoSqlDataReaderTcp<LazyEntity> {
    MyNoSqlDataReaderTcp::new(Arc::new(SyncToMainNodeHandler::new(
        my_logger::LOGGER.clone(),
    )))
}

/// The snapshot an old server's data makes: well formed rows next to rows whose keys are a
/// number, `true`, an object - each as it lies on the disk.
const SNAPSHOT: &str = r#"[
    {"PartitionKey":"2","RowKey":"normal-in-2","TimeStamp":"2020-05-06T07:08:09"},
    {"PartitionKey":123,"RowKey":"pk-number","TimeStamp":"2020-05-06T07:08:09"},
    {"PartitionKey":true,"RowKey":"pk-true","TimeStamp":"2020-05-06T07:08:09"},
    {"PartitionKey":{"a":"b"},"RowKey":"pk-object","TimeStamp":"2020-05-06T07:08:09"},
    {"PartitionKey":"mixed","RowKey":4567,"TimeStamp":"2020-05-06T07:08:09"},
    {"PartitionKey":"mixed","RowKey":"r-normal","TimeStamp":"2020-05-06T07:08:09"}
]"#;

/// (partition key, row keys) of what `deserialize_array` makes of a packet.
fn keys_of(reader: &MyNoSqlDataReaderTcp<LazyEntity>, packet: &str) -> Vec<(String, Vec<String>)> {
    reader
        .deserialize_array(packet.as_bytes())
        .into_iter()
        .map(|(partition_key, rows)| {
            let mut row_keys: Vec<String> = rows
                .iter()
                .map(|row| {
                    assert_eq!(partition_key, row.get_partition_key());
                    row.get_row_key().to_string()
                })
                .collect();
            row_keys.sort();
            (partition_key, row_keys)
        })
        .collect()
}

fn keys(partition_key: &str, row_keys: &[&str]) -> (String, Vec<String>) {
    (
        partition_key.to_string(),
        row_keys.iter().map(|row_key| row_key.to_string()).collect(),
    )
}

#[test]
fn a_packet_which_holds_rows_stored_under_mangled_keys_is_read_and_indexed_by_those_keys() {
    assert_eq!(
        keys_of(&reader(), SNAPSHOT),
        vec![
            keys("\"a\":\"b\"", &["pk-object"]),
            keys("2", &["normal-in-2", "pk-number"]),
            keys("mixed", &["56", "r-normal"]),
            keys("ru", &["pk-true"]),
        ]
    );
}

/// The snapshot goes into the copy, the well formed rows next to such rows are served, and a
/// delete of such a row - the server sends it by the key the row lies under - removes it.
#[test]
fn the_reader_holds_the_snapshot_and_follows_a_delete_of_such_a_row() {
    let reader = reader();

    reader.init_table(SNAPSHOT.as_bytes().to_vec());

    assert_eq!(
        reader.get_partition_keys(),
        vec!["\"a\":\"b\"", "2", "mixed", "ru"]
    );

    assert!(reader.has_partition("2"));
    assert!(reader.get_entity("2", "normal-in-2").is_some());
    assert!(reader.get_entity("mixed", "r-normal").is_some());

    reader.delete_rows(vec![
        DeleteRowTcpContract {
            partition_key: "ru".to_string(),
            row_key: "pk-true".to_string(),
        },
        DeleteRowTcpContract {
            partition_key: "mixed".to_string(),
            row_key: "56".to_string(),
        },
    ]);

    assert_eq!(reader.get_partition_keys(), vec!["\"a\":\"b\"", "2", "mixed"]);

    // the partition `mixed` is left with its well formed row only - every row of it can be read
    let mixed = reader.get_by_partition_key("mixed").unwrap();
    assert_eq!(mixed.keys().collect::<Vec<_>>(), vec!["r-normal"]);
}
