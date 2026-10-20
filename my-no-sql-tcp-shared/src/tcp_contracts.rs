use my_tcp_sockets::{
    socket_reader::{ReadingTcpContractFail, SocketReader, SocketReaderInMem},
    TcpSerializerState, TcpWriteBuffer,
};
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{tcp_packets::*, DeleteRowTcpContract, TcpCompression};

/// Version of the `GreetingFromNode` packet this SDK writes:
/// `[GREETING_FROM_NODE][2][node_location][node_version][compression]`.
///
/// - `0` - no compression field;
/// - `1` - a bool which asked for DEFLATE. DEFLATE is gone, so such a node gets its data
///   uncompressed - and reads it;
/// - `2` - the number of a [`TcpCompression`].
const GREETING_FROM_NODE_VERSION: u8 = 2;

#[derive(Debug)]
pub enum MyNoSqlTcpContract {
    Ping,
    Pong,
    Greeting {
        name: String,
    },
    Subscribe {
        table_name: String,
    },
    InitTable {
        table_name: String,
        data: Vec<u8>,
    },
    InitPartition {
        table_name: String,
        partition_key: String,
        data: Vec<u8>,
    },
    UpdateRows {
        table_name: String,
        data: Vec<u8>,
    },
    DeleteRows {
        table_name: String,
        rows: Vec<DeleteRowTcpContract>,
    },
    Error {
        message: String,
    },

    GreetingFromNode {
        node_location: String,
        node_version: String,
        compression: TcpCompression,
    },
    SubscribeAsNode(String),
    Unsubscribe(String),
    TableNotFound(String),
    CompressedPayload(Vec<u8>),
    UpdatePartitionsLastReadTime {
        confirmation_id: i64,
        table_name: String,
        partitions: Vec<String>,
    },
    UpdateRowsLastReadTime {
        confirmation_id: i64,
        table_name: String,
        partition_key: String,
        row_keys: Vec<String>,
    },
    UpdatePartitionsExpirationTime {
        confirmation_id: i64,
        table_name: String,
        partitions: Vec<(String, Option<DateTimeAsMicroseconds>)>,
    },
    UpdateRowsExpirationTime {
        confirmation_id: i64,
        table_name: String,
        partition_key: String,
        row_keys: Vec<String>,
        expiration_time: Option<DateTimeAsMicroseconds>,
    },
    Confirmation {
        confirmation_id: i64,
    },
    /// Fixes the namespace of the connection. Has to be sent right after the Greeting and
    /// before any Subscribe. A server which knows nothing about namespaces fails to read the
    /// packet (`InvalidPacketId`) and breaks the connection - which is exactly what we want,
    /// since falling back to the default namespace silently would read the wrong data.
    SetNamespace {
        namespace: String,
    },
    /// A `Ping` which carries the round trip of the previous Ping → Pong, measured by the
    /// subscriber (a reader or a node). Every keep-alive ping sent after the first Pong of the
    /// connection goes out this way - see [`crate::MyNoSqlReaderTcpSerializer`]. The server
    /// treats it exactly as a `Ping`: answers with `Pong` and keeps the number as the latency of
    /// the connection.
    ///
    /// A server which predates the packet fails to read it (`InvalidPacketId`) and drops the
    /// connection - servers and nodes are upgraded before the subscribers which ping them.
    PingWithLatency {
        micros: u64,
    },
}

impl MyNoSqlTcpContract {
    /// Wraps the packet into a `CompressedPayload` made with `compression` - if the result is
    /// smaller. `TcpCompression::None` gives the packet back as it is.
    #[cfg(feature = "zstd")]
    pub fn compress_if_makes_sense(self, compression: TcpCompression) -> Self {
        if let Self::CompressedPayload(_) = self {
            panic!("You can not get compress payload from compressed payload");
        }

        match compression {
            TcpCompression::None => return self,
            TcpCompression::Zstd => {}
        }

        let mut non_compressed = Vec::new();

        self.serialize(&mut non_compressed);

        let compressed = super::payload_compressor::compress(non_compressed.as_slice()).unwrap();

        if compressed.len() + 10 < non_compressed.as_slice().len() {
            Self::CompressedPayload(compressed)
        } else {
            self
        }
    }

    pub async fn decompress_if_compressed(self) -> Result<Self, ReadingTcpContractFail> {
        if let Self::CompressedPayload(payload) = self {
            // A broken payload breaks the connection instead of panicking the read loop
            let uncompressed_payload = super::payload_compressor::decompress(payload.as_slice())?;

            let mut reader = SocketReaderInMem::new(uncompressed_payload);

            Self::deserialize(&mut reader).await
        } else {
            Ok(self)
        }
    }

    pub async fn deserialize<TSocketReader: SocketReader + Send + Sync + 'static>(
        socket_reader: &mut TSocketReader,
    ) -> Result<Self, ReadingTcpContractFail> {
        let packet_no = socket_reader.read_byte().await?;

        let result = match packet_no {
            PING => Ok(Self::Ping {}),
            PONG => Ok(Self::Pong {}),
            GREETING => {
                let name = crate::common_deserializes::read_pascal_string(socket_reader).await?;
                Ok(Self::Greeting { name })
            }
            SUBSCRIBE => {
                let table_name =
                    crate::common_deserializes::read_pascal_string(socket_reader).await?;
                Ok(Self::Subscribe { table_name })
            }
            INIT_TABLE => {
                let table_name =
                    crate::common_deserializes::read_pascal_string(socket_reader).await?;
                let data = socket_reader.read_byte_array().await?;
                Ok(Self::InitTable { table_name, data })
            }
            INIT_PARTITION => {
                let table_name =
                    crate::common_deserializes::read_pascal_string(socket_reader).await?;
                let partition_key =
                    crate::common_deserializes::read_pascal_string(socket_reader).await?;
                let data = socket_reader.read_byte_array().await?;
                Ok(Self::InitPartition {
                    table_name,
                    partition_key,
                    data,
                })
            }
            UPDATE_ROWS => {
                let table_name =
                    crate::common_deserializes::read_pascal_string(socket_reader).await?;

                let data = socket_reader.read_byte_array().await?;
                Ok(Self::UpdateRows { table_name, data })
            }
            DELETE_ROWS => {
                let table_name =
                    crate::common_deserializes::read_pascal_string(socket_reader).await?;

                let rows_amount = socket_reader.read_i32().await?;

                let mut rows = Vec::new();

                for _ in 0..rows_amount {
                    let row = DeleteRowTcpContract::deserialize(socket_reader).await?;
                    rows.push(row);
                }

                Ok(Self::DeleteRows { table_name, rows })
            }
            ERROR => {
                let packet_version = socket_reader.read_byte().await?;

                if packet_version != 0 {
                    panic!(
                        "Unexpected packet version. Version is: {}, but expected version is 0",
                        packet_version
                    );
                }

                let message = crate::common_deserializes::read_pascal_string(socket_reader).await?;

                panic!("TCP protocol error: {}", message);
            }
            GREETING_FROM_NODE => {
                let packet_version = socket_reader.read_byte().await?;

                let node_location =
                    crate::common_deserializes::read_pascal_string(socket_reader).await?;

                let node_version =
                    crate::common_deserializes::read_pascal_string(socket_reader).await?;

                let compression = match packet_version {
                    0 => TcpCompression::None,
                    1 => {
                        // The node asked for DEFLATE, which is gone - it gets its data uncompressed
                        socket_reader.read_bool().await?;
                        TcpCompression::None
                    }
                    // A number this SDK does not know gives none as well: a node always reads
                    // an uncompressed packet
                    _ => TcpCompression::from_u8(socket_reader.read_byte().await?)
                        .unwrap_or(TcpCompression::None),
                };

                Ok(Self::GreetingFromNode {
                    node_location,
                    node_version,
                    compression,
                })
            }
            SUBSCRIBE_AS_NODE => {
                // Version 0 = we read table_name only
                socket_reader.read_byte().await?;
                let table_name =
                    super::common_deserializes::read_pascal_string(socket_reader).await?;
                Ok(Self::SubscribeAsNode(table_name))
            }
            TABLES_NOT_FOUND => {
                // Version 0 = we read table_name only
                socket_reader.read_byte().await?;
                let table_name =
                    super::common_deserializes::read_pascal_string(socket_reader).await?;
                Ok(Self::TableNotFound(table_name))
            }
            UNSUBSCRIBE => {
                // Version 0 = we read table_name only
                socket_reader.read_byte().await?;
                let table_name =
                    super::common_deserializes::read_pascal_string(socket_reader).await?;
                Ok(Self::Unsubscribe(table_name))
            }
            COMPRESSED_PAYLOAD => {
                let data = socket_reader.read_byte_array().await?;
                Ok(Self::CompressedPayload(data))
            }
            UPDATE_PARTITIONS_LAST_READ_TIME => {
                let _protocol_version = socket_reader.read_byte().await?;
                let confirmation_id = socket_reader.read_i64().await?;
                let table_name =
                    super::common_deserializes::read_pascal_string(socket_reader).await?;

                let partitions =
                    super::common_deserializes::read_list_of_pascal_strings(socket_reader).await?;

                Ok(Self::UpdatePartitionsLastReadTime {
                    confirmation_id,
                    table_name,
                    partitions,
                })
            }
            UPDATE_ROWS_LAST_READ_TIME => {
                let _protocol_version = socket_reader.read_byte().await?;
                let confirmation_id = socket_reader.read_i64().await?;
                let table_name =
                    super::common_deserializes::read_pascal_string(socket_reader).await?;

                let partition_key =
                    super::common_deserializes::read_pascal_string(socket_reader).await?;

                let row_keys =
                    super::common_deserializes::read_list_of_pascal_strings(socket_reader).await?;

                Ok(Self::UpdateRowsLastReadTime {
                    confirmation_id,
                    table_name,
                    partition_key,
                    row_keys,
                })
            }

            UPDATE_PARTITIONS_EXPIRATION_TIME => {
                let _protocol_version = socket_reader.read_byte().await?;
                let confirmation_id = socket_reader.read_i64().await?;
                let table_name =
                    super::common_deserializes::read_pascal_string(socket_reader).await?;

                let amount = socket_reader.read_i32().await? as usize;

                let mut partitions = Vec::with_capacity(amount);

                for _ in 0..amount {
                    let partition_key =
                        super::common_deserializes::read_pascal_string(socket_reader).await?;
                    let expiration_time =
                        super::common_deserializes::read_date_time_opt(socket_reader).await?;

                    partitions.push((partition_key, expiration_time));
                }

                Ok(Self::UpdatePartitionsExpirationTime {
                    confirmation_id,
                    table_name,
                    partitions,
                })
            }

            UPDATE_ROWS_EXPIRATION_TIME => {
                let _protocol_version = socket_reader.read_byte().await?;
                let confirmation_id = socket_reader.read_i64().await?;
                let table_name =
                    super::common_deserializes::read_pascal_string(socket_reader).await?;

                let partition_key =
                    super::common_deserializes::read_pascal_string(socket_reader).await?;

                let row_keys =
                    super::common_deserializes::read_list_of_pascal_strings(socket_reader).await?;

                let expiration_time =
                    super::common_deserializes::read_date_time_opt(socket_reader).await?;

                Ok(Self::UpdateRowsExpirationTime {
                    confirmation_id,
                    table_name,
                    partition_key,
                    row_keys,
                    expiration_time: expiration_time,
                })
            }

            CONFIRMATION => {
                let _protocol_version = socket_reader.read_byte().await?;
                let confirmation_id = socket_reader.read_i64().await?;
                Ok(Self::Confirmation { confirmation_id })
            }
            SET_NAMESPACE => {
                // Version is read and ignored - it is a room for the future extensions
                let _protocol_version = socket_reader.read_byte().await?;
                let namespace = crate::common_deserializes::read_pascal_string(socket_reader).await?;
                Ok(Self::SetNamespace { namespace })
            }
            PING_WITH_LATENCY => {
                let micros = socket_reader.read_u64().await?;
                Ok(Self::PingWithLatency { micros })
            }
            _ => Err(ReadingTcpContractFail::InvalidPacketId(packet_no)),
        };

        return result;
    }

    pub fn serialize(&self, write_buffer: &mut impl TcpWriteBuffer) {
        match self {
            Self::Ping => {
                write_buffer.write_byte(PING);
            }
            Self::Pong => {
                write_buffer.write_byte(PONG);
            }
            Self::Greeting { name } => {
                write_buffer.write_byte(GREETING);
                write_buffer.write_pascal_string(name);
            }
            Self::Subscribe { table_name } => {
                write_buffer.write_byte(SUBSCRIBE);
                write_buffer.write_pascal_string(table_name);
            }
            Self::InitTable { table_name, data } => {
                write_buffer.write_byte(INIT_TABLE);
                write_buffer.write_pascal_string(table_name);
                write_buffer.write_byte_array(data.as_slice());
            }
            Self::InitPartition {
                table_name,
                partition_key,
                data,
            } => {
                write_buffer.write_byte(INIT_PARTITION);
                write_buffer.write_pascal_string(table_name);
                write_buffer.write_pascal_string(partition_key);
                write_buffer.write_byte_array(data.as_slice());
            }
            Self::UpdateRows { table_name, data } => {
                write_buffer.write_byte(UPDATE_ROWS);
                write_buffer.write_pascal_string(table_name);
                write_buffer.write_byte_array(data.as_slice());
            }
            Self::DeleteRows { table_name, rows } => {
                write_buffer.write_byte(DELETE_ROWS);
                write_buffer.write_pascal_string(table_name);
                write_buffer.write_i32(rows.len() as i32);

                for row in rows {
                    row.serialize(write_buffer);
                }
            }
            Self::Error { message } => {
                write_buffer.write_byte(ERROR);
                // Version=0; Means we have one field - message;
                write_buffer.write_byte(0); // Protocol version
                write_buffer.write_pascal_string(message);
            }

            Self::GreetingFromNode {
                node_location,
                node_version,
                compression,
            } => {
                write_buffer.write_byte(GREETING_FROM_NODE);
                write_buffer.write_byte(GREETING_FROM_NODE_VERSION);
                write_buffer.write_pascal_string(node_location);
                write_buffer.write_pascal_string(node_version);
                write_buffer.write_byte(compression.as_u8());
            }

            Self::SubscribeAsNode(table_name) => {
                write_buffer.write_byte(SUBSCRIBE_AS_NODE);
                // Protocol version
                write_buffer.write_byte(0);
                write_buffer.write_pascal_string(table_name.as_str());
            }

            Self::TableNotFound(table_name) => {
                write_buffer.write_byte(TABLES_NOT_FOUND);
                // Protocol version
                write_buffer.write_byte(0);
                write_buffer.write_pascal_string(table_name.as_str());
            }

            Self::Unsubscribe(table_name) => {
                write_buffer.write_byte(UNSUBSCRIBE);
                // Protocol version
                write_buffer.write_byte(0);
                write_buffer.write_pascal_string(table_name.as_str());
            }
            Self::CompressedPayload(payload) => {
                write_buffer.write_byte(COMPRESSED_PAYLOAD);
                write_buffer.write_byte_array(payload.as_slice());
            }
            Self::UpdatePartitionsLastReadTime {
                table_name,
                confirmation_id,
                partitions,
            } => {
                write_buffer.write_byte(UPDATE_PARTITIONS_LAST_READ_TIME);
                write_buffer.write_byte(0); // Protocol version
                write_buffer.write_i64(*confirmation_id);
                write_buffer.write_pascal_string(table_name);
                write_buffer.write_list_of_pascal_strings(partitions);
            }
            Self::UpdateRowsLastReadTime {
                table_name,
                confirmation_id,
                partition_key,
                row_keys,
            } => {
                write_buffer.write_byte(UPDATE_ROWS_LAST_READ_TIME);
                write_buffer.write_byte(0); // Protocol version
                write_buffer.write_i64(*confirmation_id);
                write_buffer.write_pascal_string(table_name);
                write_buffer.write_pascal_string(partition_key);
                write_buffer.write_list_of_pascal_strings(row_keys);
            }

            Self::UpdatePartitionsExpirationTime {
                table_name,
                confirmation_id,
                partitions,
            } => {
                write_buffer.write_byte(UPDATE_PARTITIONS_EXPIRATION_TIME);
                write_buffer.write_byte(0); // Protocol version

                write_buffer.write_i64(*confirmation_id);

                write_buffer.write_pascal_string(table_name.as_str());

                let amount = partitions.len() as i32;
                write_buffer.write_i32(amount);

                for (partition_key, expiration_time) in partitions {
                    write_buffer.write_pascal_string(partition_key.as_str());

                    crate::common_serializers::serialize_date_time_opt(
                        write_buffer,
                        *expiration_time,
                    );
                }
            }

            Self::UpdateRowsExpirationTime {
                table_name,
                confirmation_id,
                partition_key,
                row_keys,
                expiration_time,
            } => {
                write_buffer.write_byte(UPDATE_ROWS_EXPIRATION_TIME);
                write_buffer.write_byte(0); // Protocol version
                write_buffer.write_i64(*confirmation_id);
                write_buffer.write_pascal_string(table_name.as_str());
                write_buffer.write_pascal_string(partition_key.as_str());
                write_buffer.write_list_of_pascal_strings(row_keys);
                crate::common_serializers::serialize_date_time_opt(write_buffer, *expiration_time);
            }

            Self::Confirmation { confirmation_id } => {
                write_buffer.write_byte(CONFIRMATION);
                write_buffer.write_byte(0); // Protocol version
                write_buffer.write_i64(*confirmation_id);
            }

            Self::SetNamespace { namespace } => {
                write_buffer.write_byte(SET_NAMESPACE);
                write_buffer.write_byte(0); // Protocol version
                write_buffer.write_pascal_string(namespace.as_str());
            }

            Self::PingWithLatency { micros } => {
                write_buffer.write_byte(PING_WITH_LATENCY);
                write_buffer.write_u64(*micros);
            }
        }
    }
}

impl my_tcp_sockets::TcpContract for MyNoSqlTcpContract {

    fn is_ping(&self) -> bool {
        match self {
            Self::Ping | Self::PingWithLatency { .. } => true,
            _ => false,
        }
    }

    fn is_pong(&self) -> bool {
        match self {
            Self::Pong => true,
            _ => false,
        }
    }
}

impl TcpSerializerState<MyNoSqlTcpContract> for () {
    fn is_tcp_contract_related_to_metadata(&self, _contract: &MyNoSqlTcpContract) -> bool {
        false
    }

    fn apply_tcp_contract(&mut self, _contract: &MyNoSqlTcpContract) {}
}

#[cfg(test)]
mod test {
    use my_tcp_sockets::socket_reader::SocketReaderInMem;

    use my_tcp_sockets::{TcpContract, TcpWriteBuffer};

    use super::{MyNoSqlTcpContract, GREETING_FROM_NODE_VERSION};
    use crate::tcp_packets::{GREETING_FROM_NODE, PING_WITH_LATENCY, SET_NAMESPACE};
    use crate::TcpCompression;

    #[tokio::test]
    async fn test_set_namespace_serialize_deserialize() {
        let mut buffer = Vec::new();

        MyNoSqlTcpContract::SetNamespace {
            namespace: "alpha".to_string(),
        }
        .serialize(&mut buffer);

        assert_eq!(SET_NAMESPACE, buffer[0]);
        assert_eq!(0, buffer[1]); // Protocol version

        let mut socket_reader = SocketReaderInMem::new(buffer);

        let result = MyNoSqlTcpContract::deserialize(&mut socket_reader)
            .await
            .unwrap();

        match result {
            MyNoSqlTcpContract::SetNamespace { namespace } => assert_eq!("alpha", namespace),
            _ => panic!("Unexpected contract: {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_ping_with_latency_serialize_deserialize() {
        let mut buffer = Vec::new();

        MyNoSqlTcpContract::PingWithLatency { micros: 1_234_567 }.serialize(&mut buffer);

        assert_eq!(PING_WITH_LATENCY, buffer[0]);
        assert_eq!(1 + 8, buffer.len()); // packet id + u64, nothing else

        let mut socket_reader = SocketReaderInMem::new(buffer);

        let result = MyNoSqlTcpContract::deserialize(&mut socket_reader)
            .await
            .unwrap();

        // The socket library routes it as a ping, and so does every server
        assert!(result.is_ping());
        assert!(!result.is_pong());

        match result {
            MyNoSqlTcpContract::PingWithLatency { micros } => assert_eq!(1_234_567, micros),
            _ => panic!("Unexpected contract: {:?}", result),
        }
    }

    /// Reads a `GreetingFromNode` and the `Ping` which follows it - the `Ping` is read only if
    /// the greeting was read up to its last byte.
    async fn read_greeting_then_ping(buffer: Vec<u8>) -> TcpCompression {
        let mut socket_reader = SocketReaderInMem::new(buffer);

        let result = MyNoSqlTcpContract::deserialize(&mut socket_reader)
            .await
            .unwrap();

        let compression = match result {
            MyNoSqlTcpContract::GreetingFromNode {
                node_location,
                node_version,
                compression,
            } => {
                assert_eq!("eu-west", node_location);
                assert_eq!("1.2.3", node_version);
                compression
            }
            _ => panic!("Unexpected contract: {:?}", result),
        };

        let result = MyNoSqlTcpContract::deserialize(&mut socket_reader)
            .await
            .unwrap();

        match result {
            MyNoSqlTcpContract::Ping => {}
            _ => panic!("Unexpected contract: {:?}", result),
        }

        compression
    }

    /// A greeting written by hand, the way a node of another version writes it.
    fn greeting_from_node_of_version(packet_version: u8, tail: Option<u8>) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.write_byte(GREETING_FROM_NODE);
        buffer.write_byte(packet_version);
        buffer.write_pascal_string("eu-west");
        buffer.write_pascal_string("1.2.3");

        if let Some(tail) = tail {
            buffer.write_byte(tail);
        }

        MyNoSqlTcpContract::Ping.serialize(&mut buffer);

        buffer
    }

    #[tokio::test]
    async fn test_greeting_from_node_serialize_deserialize() {
        for compression in [TcpCompression::None, TcpCompression::Zstd] {
            let mut buffer = Vec::new();

            MyNoSqlTcpContract::GreetingFromNode {
                node_location: "eu-west".to_string(),
                node_version: "1.2.3".to_string(),
                compression,
            }
            .serialize(&mut buffer);

            assert_eq!(GREETING_FROM_NODE, buffer[0]);
            assert_eq!(2, GREETING_FROM_NODE_VERSION);
            assert_eq!(GREETING_FROM_NODE_VERSION, buffer[1]);
            assert_eq!(compression.as_u8(), *buffer.last().unwrap());

            MyNoSqlTcpContract::Ping.serialize(&mut buffer);

            assert_eq!(compression, read_greeting_then_ping(buffer).await);
        }
    }

    #[tokio::test]
    async fn test_greeting_from_node_of_version_0_is_read_as_none() {
        let buffer = greeting_from_node_of_version(0, None);
        assert_eq!(TcpCompression::None, read_greeting_then_ping(buffer).await);
    }

    #[tokio::test]
    async fn test_greeting_from_node_of_version_1_with_deflate_is_read_as_none() {
        // A node of version 1 which asked for DEFLATE (`true`) gets its data uncompressed
        let buffer = greeting_from_node_of_version(1, Some(1));
        assert_eq!(TcpCompression::None, read_greeting_then_ping(buffer).await);
    }

    #[tokio::test]
    async fn test_greeting_from_node_with_unknown_compression_is_read_as_none() {
        let buffer = greeting_from_node_of_version(GREETING_FROM_NODE_VERSION, Some(200));
        assert_eq!(TcpCompression::None, read_greeting_then_ping(buffer).await);
    }

    #[cfg(feature = "zstd")]
    fn big_init_table() -> MyNoSqlTcpContract {
        MyNoSqlTcpContract::InitTable {
            table_name: "instruments".to_string(),
            data: br#"[{"PartitionKey":"instruments","RowKey":"EURUSD","Value":15000.0}]"#
                .repeat(1000),
        }
    }

    #[cfg(feature = "zstd")]
    fn assert_is_big_init_table(contract: MyNoSqlTcpContract) {
        let expected = big_init_table();

        match (contract, expected) {
            (
                MyNoSqlTcpContract::InitTable { table_name, data },
                MyNoSqlTcpContract::InitTable {
                    table_name: expected_table_name,
                    data: expected_data,
                },
            ) => {
                assert_eq!(expected_table_name, table_name);
                assert_eq!(expected_data, data);
            }
            (contract, _) => panic!("Unexpected contract: {:?}", contract),
        }
    }

    #[cfg(feature = "zstd")]
    #[tokio::test]
    async fn test_zstd_compressed_payload_round_trip() {
        let compressed = big_init_table().compress_if_makes_sense(TcpCompression::Zstd);

        match &compressed {
            MyNoSqlTcpContract::CompressedPayload(_) => {}
            _ => panic!("Unexpected contract: {:?}", compressed),
        }

        let mut buffer = Vec::new();
        compressed.serialize(&mut buffer);

        let mut socket_reader = SocketReaderInMem::new(buffer);

        let result = MyNoSqlTcpContract::deserialize(&mut socket_reader)
            .await
            .unwrap()
            .decompress_if_compressed()
            .await
            .unwrap();

        assert_is_big_init_table(result);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_compress_with_none_keeps_the_packet() {
        let result = big_init_table().compress_if_makes_sense(TcpCompression::None);
        assert_is_big_init_table(result);
    }

    #[tokio::test]
    async fn test_broken_compressed_payload_is_an_error_not_a_panic() {
        let result = MyNoSqlTcpContract::CompressedPayload(vec![0xff, 0xff, 0xff, 0xff])
            .decompress_if_compressed()
            .await;

        assert!(result.is_err());
    }
}
