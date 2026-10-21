use my_tcp_sockets::{
    socket_reader::{ReadingTcpContractFail, SocketReader},
    TcpSerializerState, TcpWriteBuffer,
};
use rust_extensions::{
    date_time::DateTimeAsMicroseconds, ParseUInt32VariableSizeResult, UInt32VariableSize,
};

use crate::{tcp_packets::*, DeleteRowTcpContract};
#[cfg(feature = "compression")]
use crate::{BatchPacker, BATCH_STORED};

/// Version of the `GreetingFromNode` packet this SDK writes:
/// `[GREETING_FROM_NODE][2][node_location][node_version][compress]`.
///
/// Version `0` has no `compress`, and is read as `0` - no compression. In every other version
/// the byte of the level follows.
const GREETING_FROM_NODE_VERSION: u8 = 2;

/// A `CompressedPayload` is shorter than this: its length goes as a `UInt32VariableSize`,
/// which takes values below 1_073_741_823.
#[cfg(feature = "compression")]
const COMPRESSED_PAYLOAD_LEN_LIMIT: usize = 1_073_741_823;

/// The most the batch of a `CompressedPayload` unpacks to: as much as the length of a
/// `CompressedPayload` can say. In MyNoSql a batch is one packet, and an `InitTable` is the
/// snapshot of a whole table.
#[cfg(feature = "compression")]
pub const MAX_DECOMPRESSED_BATCH_SIZE: usize = 1_073_741_823;

/// The biggest batch `BatchPacker::write_batch` makes of `len` bytes. One which goes as it is
/// is the bytes and a byte. A piece of a stream - the levels 6 to 9 - can be bigger than that:
/// the bytes go into the stream whether they pack or not, at 3 bytes for every 128 KiB of what
/// does not, and the packer gives a piece the room of `compress_bound` of them and 64 bytes
/// for the flush, after the kind and the size.
#[cfg(feature = "compression")]
fn max_batch_len(len: usize) -> usize {
    1 + 4 + zstd::zstd_safe::compress_bound(len) + 64
}

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
        /// The compression level the server is to pack what it sends to the node with: a number
        /// of `CompressionLevel`, as it is in the settings of the node. The server checks it
        /// with `CompressionLevel::new`.
        compress: u8,
    },
    SubscribeAsNode(String),
    Unsubscribe(String),
    TableNotFound(String),
    /// A packet of the server to a node, packed: a batch of `BatchPacker` -
    /// `[kind][u32 RAW_LEN][packed]` - made by `pack` and unpacked by
    /// [`crate::MyNoSqlReaderTcpSerializer`].
    ///
    /// On the wire: `[COMPRESSED_PAYLOAD][length as UInt32VariableSize][batch]` - 1 byte of
    /// length up to 63, 2 bytes up to 16 383, 3 bytes up to ~4 MB, 4 bytes up to ~1 GB.
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
    /// The packet as it is to be sent: `Some(CompressedPayload)` when the packer packed it,
    /// `None` when it goes as it is - too small, or packing saves nothing.
    ///
    /// `packer` is the one of the connection. At the levels 6 to 9 a packet is packed with the
    /// packets before it, and the node unpacks them in the order they were packed: every
    /// `Some` is sent, and in the order it was made.
    #[cfg(feature = "compression")]
    pub fn pack(&self, packer: &mut BatchPacker) -> Option<Self> {
        if let Self::CompressedPayload(_) = self {
            return None;
        }

        let mut bytes = Vec::new();
        self.serialize(&mut bytes);

        // Before the packer sees the packet: at the levels 6 to 9 it is in the stream once it
        // is packed, and a batch which is not sent would leave the stream of the node behind
        // the one of the server.
        if max_batch_len(bytes.len()) >= COMPRESSED_PAYLOAD_LEN_LIMIT {
            return None;
        }

        let mut wire = Vec::new();
        packer.write_batch(&bytes, &mut wire);

        // Not packed: the packet goes as it is, and is no part of the stream.
        if wire[0] == BATCH_STORED {
            return None;
        }

        Some(Self::CompressedPayload(wire))
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

                // Version 0 has no level: no compression
                let compress = match packet_version {
                    0 => 0,
                    _ => socket_reader.read_byte().await?,
                };

                Ok(Self::GreetingFromNode {
                    node_location,
                    node_version,
                    compress,
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
                let mut len_buf = [0u8; 4];
                len_buf[0] = socket_reader.read_byte().await?;

                // The first byte tells how many bytes the length takes
                let len = match UInt32VariableSize::from_slice(&len_buf[..1]) {
                    ParseUInt32VariableSizeResult::Ok { value, .. } => value.get_value(),
                    ParseUInt32VariableSizeResult::NotEnoughDataInBuffer(size) => {
                        socket_reader.read_buf(&mut len_buf[1..size]).await?;
                        UInt32VariableSize::from_slice(&len_buf[..size])
                            .unwrap()
                            .get_value()
                    }
                };

                let mut data = vec![0u8; len as usize];
                socket_reader.read_buf(&mut data).await?;
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
                compress,
            } => {
                write_buffer.write_byte(GREETING_FROM_NODE);
                write_buffer.write_byte(GREETING_FROM_NODE_VERSION);
                write_buffer.write_pascal_string(node_location);
                write_buffer.write_pascal_string(node_version);
                write_buffer.write_byte(*compress);
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
                let mut len = Vec::with_capacity(4);
                UInt32VariableSize::new(payload.len() as u32).serialize(&mut len);
                write_buffer.write_slice(&len);
                write_buffer.write_slice(payload);
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
    use crate::tcp_packets::{
        COMPRESSED_PAYLOAD, GREETING_FROM_NODE, PING_WITH_LATENCY, SET_NAMESPACE,
    };
    use crate::CompressionLevel;
    #[cfg(feature = "compression")]
    use crate::{BatchPacker, BatchUnpacker, Packing};

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
    async fn read_greeting_then_ping(buffer: Vec<u8>) -> u8 {
        let mut socket_reader = SocketReaderInMem::new(buffer);

        let result = MyNoSqlTcpContract::deserialize(&mut socket_reader)
            .await
            .unwrap();

        let compress = match result {
            MyNoSqlTcpContract::GreetingFromNode {
                node_location,
                node_version,
                compress,
            } => {
                assert_eq!("eu-west", node_location);
                assert_eq!("1.2.3", node_version);
                compress
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

        compress
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
        assert_eq!(9, CompressionLevel::MAX);

        for compress in 0..=CompressionLevel::MAX {
            let mut buffer = Vec::new();

            MyNoSqlTcpContract::GreetingFromNode {
                node_location: "eu-west".to_string(),
                node_version: "1.2.3".to_string(),
                compress,
            }
            .serialize(&mut buffer);

            assert_eq!(GREETING_FROM_NODE, buffer[0]);
            assert_eq!(2, GREETING_FROM_NODE_VERSION);
            assert_eq!(GREETING_FROM_NODE_VERSION, buffer[1]);
            assert_eq!(compress, *buffer.last().unwrap());

            MyNoSqlTcpContract::Ping.serialize(&mut buffer);

            let read = read_greeting_then_ping(buffer).await;
            assert_eq!(compress, read);
            assert_eq!(compress, CompressionLevel::new(read).unwrap().as_u8());
        }
    }

    #[tokio::test]
    async fn test_greeting_from_node_of_version_0_is_read_as_no_compression() {
        let buffer = greeting_from_node_of_version(0, None);
        assert_eq!(0, read_greeting_then_ping(buffer).await);
    }

    /// The level is read as it is: the server refuses one it has no row for with
    /// `CompressionLevel::new`.
    #[tokio::test]
    async fn test_greeting_from_node_with_a_level_with_no_row_is_read_as_it_is() {
        let buffer = greeting_from_node_of_version(GREETING_FROM_NODE_VERSION, Some(200));
        assert_eq!(200, read_greeting_then_ping(buffer).await);
        assert!(CompressionLevel::new(200).is_err());
    }

    #[tokio::test]
    async fn test_compressed_payload_length_is_a_uint32_variable_size() {
        // (payload length, bytes of the length) on the borders of UInt32VariableSize
        for (payload_len, len_size) in [
            (0, 1),
            (63, 1),
            (64, 2),
            (16_383, 2),
            (16_384, 3),
            (4_194_302, 3),
            (4_194_303, 4),
        ] {
            let payload: Vec<u8> = (0..payload_len).map(|i| i as u8).collect();

            let mut buffer = Vec::new();

            MyNoSqlTcpContract::CompressedPayload(payload.clone()).serialize(&mut buffer);

            assert_eq!(COMPRESSED_PAYLOAD, buffer[0]);
            assert_eq!(1 + len_size + payload_len, buffer.len(), "payload of {}", payload_len);

            // The Ping is read only if the packet was read up to its last byte
            MyNoSqlTcpContract::Ping.serialize(&mut buffer);

            let mut socket_reader = SocketReaderInMem::new(buffer);

            let result = MyNoSqlTcpContract::deserialize(&mut socket_reader)
                .await
                .unwrap();

            match result {
                MyNoSqlTcpContract::CompressedPayload(data) => {
                    assert!(payload == data, "payload of {}", payload_len)
                }
                _ => panic!("Unexpected contract: {:?}", result),
            }

            let result = MyNoSqlTcpContract::deserialize(&mut socket_reader)
                .await
                .unwrap();

            match result {
                MyNoSqlTcpContract::Ping => {}
                _ => panic!("Unexpected contract: {:?}", result),
            }
        }
    }

    #[cfg(feature = "compression")]
    fn big_init_table() -> MyNoSqlTcpContract {
        MyNoSqlTcpContract::InitTable {
            table_name: "instruments".to_string(),
            data: br#"[{"PartitionKey":"instruments","RowKey":"EURUSD","Value":15000.0}]"#
                .repeat(1000),
        }
    }

    #[cfg(feature = "compression")]
    fn serialized(contract: &MyNoSqlTcpContract) -> Vec<u8> {
        let mut buffer = Vec::new();
        contract.serialize(&mut buffer);
        buffer
    }

    /// Bytes nothing can make smaller.
    #[cfg(feature = "compression")]
    fn noise(size: usize) -> Vec<u8> {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        (0..size)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    #[cfg(feature = "compression")]
    fn is_stream(level: CompressionLevel) -> bool {
        matches!(level.packing(), Some(Packing::ZstdStream { .. }))
    }

    #[cfg(feature = "compression")]
    #[test]
    fn test_pack_packs_with_the_level_of_the_packer() {
        let packet = big_init_table();
        let bytes = serialized(&packet);

        for level in CompressionLevel::all() {
            match packet.pack(&mut BatchPacker::new(level)) {
                None => assert!(!level.is_on(), "level {level}"),
                Some(MyNoSqlTcpContract::CompressedPayload(batch)) => {
                    assert!(level.is_on(), "level {level}");
                    assert!(batch.len() < bytes.len() / 10, "level {level}");
                    assert_eq!(BatchUnpacker::new().read_batch(&batch).unwrap(), bytes);
                }
                Some(other) => panic!("level {level}: {other:?}"),
            }
        }
    }

    #[cfg(feature = "compression")]
    #[test]
    fn test_pack_leaves_what_does_not_pack_as_it_is() {
        let noise_table = MyNoSqlTcpContract::InitTable {
            table_name: "files".to_string(),
            data: noise(100_000),
        };

        for level in CompressionLevel::all() {
            let mut packer = BatchPacker::new(level);

            // Too small to pack
            assert!(MyNoSqlTcpContract::Ping.pack(&mut packer).is_none());

            // Packed already
            let packed = MyNoSqlTcpContract::CompressedPayload(vec![b'a'; 10_000]);
            assert!(packed.pack(&mut packer).is_none(), "level {level}");

            // Nothing to save. A stream takes it whatever it makes of it: it is what the next
            // packets are packed with.
            assert_eq!(
                is_stream(level),
                noise_table.pack(&mut packer).is_some(),
                "level {level}"
            );
        }
    }

    /// At the levels 6 to 9 what does not pack goes into the stream, and the batch is bigger
    /// than the packet and a byte: `pack` checks the length of the packet against
    /// `max_batch_len`.
    #[cfg(feature = "compression")]
    #[test]
    fn test_a_batch_is_never_bigger_than_max_batch_len() {
        let packet = noise(3 * 1024 * 1024);

        for level in CompressionLevel::all() {
            let mut wire = Vec::new();
            BatchPacker::new(level).write_batch(&packet, &mut wire);

            assert!(
                wire.len() <= super::max_batch_len(packet.len()),
                "level {level}"
            );

            if is_stream(level) {
                assert!(wire.len() > packet.len() + 1, "level {level}");
            }
        }
    }
}
