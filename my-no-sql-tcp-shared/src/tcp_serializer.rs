use std::{io, time::Duration};

use my_tcp_sockets::{
    socket_reader::{ReadingTcpContractFail, SocketReader},
    TcpSerializerFactory, TcpSocketSerializer, TcpWriteBuffer,
};

#[cfg(feature = "compression")]
use crate::BatchUnpacker;
use crate::MyNoSqlTcpContract;

pub struct MyNoSqlReaderTcpSerializer {
    /// Unpacks the `CompressedPayload`s of the server. Made with the serializer, which is one
    /// for a connection: at the levels 6 to 9 the stream lives as long as the connection, and a
    /// new connection starts a new one.
    #[cfg(feature = "compression")]
    unpacker: BatchUnpacker,
}

impl MyNoSqlReaderTcpSerializer {
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "compression")]
            unpacker: BatchUnpacker::new(),
        }
    }

    /// The packet the batch of a `CompressedPayload` carries. A batch which does not unpack, or
    /// a packet in it which can not be read, breaks the connection: the node connects again,
    /// and the new connection starts a new stream.
    #[cfg(feature = "compression")]
    async fn unpack(&mut self, batch: &[u8]) -> Result<MyNoSqlTcpContract, ReadingTcpContractFail> {
        let packet = self
            .unpacker
            .read_batch(batch)
            .map_err(invalid_data)?
            .to_vec();

        self.unpacker.release_buffer();

        let mut reader = crate::packet_reader::PacketReader::new(packet);

        let contract = MyNoSqlTcpContract::deserialize(&mut reader)
            .await
            .map_err(|err| {
                invalid_data(format!(
                    "CompressedPayload: the packet in it can not be read: {err:?}"
                ))
            })?;

        if !reader.is_read_to_end() {
            return Err(invalid_data(
                "CompressedPayload: there are bytes after the packet in it",
            ));
        }

        if let MyNoSqlTcpContract::CompressedPayload(_) = contract {
            return Err(invalid_data(
                "CompressedPayload: there is a CompressedPayload in it",
            ));
        }

        Ok(contract)
    }

    #[cfg(not(feature = "compression"))]
    async fn unpack(
        &mut self,
        _batch: &[u8],
    ) -> Result<MyNoSqlTcpContract, ReadingTcpContractFail> {
        Err(invalid_data(
            "Got a CompressedPayload, but my-no-sql-tcp-shared is built without the `compression` feature",
        ))
    }
}

impl Default for MyNoSqlReaderTcpSerializer {
    fn default() -> Self {
        Self::new()
    }
}

fn invalid_data(message: impl Into<String>) -> ReadingTcpContractFail {
    ReadingTcpContractFail::IoError(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}

#[async_trait::async_trait]
impl TcpSocketSerializer<MyNoSqlTcpContract, ()> for MyNoSqlReaderTcpSerializer {
    fn serialize(&self, out: &mut impl TcpWriteBuffer, contract: &MyNoSqlTcpContract, _: &()) {
        contract.serialize(out)
    }

    /// `latency` is the round trip of the previous Ping → Pong, measured by the socket library;
    /// `None` until the first Pong of the connection. It rides along in the ping so the server
    /// can show the latency of the connection.
    fn get_ping(&self, latency: Option<Duration>) -> MyNoSqlTcpContract {
        match latency {
            Some(latency) => MyNoSqlTcpContract::PingWithLatency {
                micros: latency.as_micros() as u64,
            },
            None => MyNoSqlTcpContract::Ping,
        }
    }

    async fn deserialize<TSocketReader: Send + Sync + 'static + SocketReader>(
        &mut self,
        socket_reader: &mut TSocketReader,
        _: &(),
    ) -> Result<MyNoSqlTcpContract, ReadingTcpContractFail> {
        // The server may send any packet wrapped into `CompressedPayload`; unwrap it here so
        // the rest of the reader only ever sees real contracts.
        match MyNoSqlTcpContract::deserialize(socket_reader).await? {
            MyNoSqlTcpContract::CompressedPayload(batch) => self.unpack(&batch).await,
            contract => Ok(contract),
        }
    }
}

pub struct MyNoSqlTcpSerializerFactory;

#[async_trait::async_trait]
impl TcpSerializerFactory<MyNoSqlTcpContract, MyNoSqlReaderTcpSerializer, ()>
    for MyNoSqlTcpSerializerFactory
{
    async fn create_serializer(&self) -> MyNoSqlReaderTcpSerializer {
        MyNoSqlReaderTcpSerializer::new()
    }
    async fn create_serializer_state(&self) -> () {
        ()
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use my_tcp_sockets::{
        socket_reader::{ReadingTcpContractFail, SocketReaderInMem},
        TcpSocketSerializer,
    };

    use super::MyNoSqlReaderTcpSerializer;
    use crate::MyNoSqlTcpContract;

    fn serialized(contract: &MyNoSqlTcpContract) -> Vec<u8> {
        let mut buffer = Vec::new();
        contract.serialize(&mut buffer);
        buffer
    }

    fn assert_is_invalid_data(result: Result<MyNoSqlTcpContract, ReadingTcpContractFail>) {
        match result {
            Err(ReadingTcpContractFail::IoError(err)) => {
                assert_eq!(io::ErrorKind::InvalidData, err.kind(), "{err}")
            }
            Err(err) => panic!("Unexpected error: {err:?}"),
            Ok(contract) => panic!("Unexpected contract: {contract:?}"),
        }
    }

    #[tokio::test]
    async fn a_packet_which_is_not_packed_is_read_as_it_is() {
        let mut wire = serialized(&MyNoSqlTcpContract::Pong);
        wire.extend(serialized(&MyNoSqlTcpContract::UpdateRows {
            table_name: "instruments".to_string(),
            data: b"[]".to_vec(),
        }));

        let mut serializer = MyNoSqlReaderTcpSerializer::new();
        let mut reader = SocketReaderInMem::new(wire);

        match serializer.deserialize(&mut reader, &()).await.unwrap() {
            MyNoSqlTcpContract::Pong => {}
            contract => panic!("Unexpected contract: {contract:?}"),
        }

        match serializer.deserialize(&mut reader, &()).await.unwrap() {
            MyNoSqlTcpContract::UpdateRows { table_name, data } => {
                assert_eq!("instruments", table_name);
                assert_eq!(b"[]".to_vec(), data);
            }
            contract => panic!("Unexpected contract: {contract:?}"),
        }
    }

    #[cfg(not(feature = "compression"))]
    #[tokio::test]
    async fn a_compressed_payload_is_an_error_without_the_compression_feature() {
        let wire = serialized(&MyNoSqlTcpContract::CompressedPayload(vec![
            0,
            crate::tcp_packets::PING,
        ]));

        let mut reader = SocketReaderInMem::new(wire);

        assert_is_invalid_data(
            MyNoSqlReaderTcpSerializer::new()
                .deserialize(&mut reader, &())
                .await,
        );
    }

    #[cfg(feature = "compression")]
    mod compression {
        use my_tcp_sockets::TcpSocketSerializer;

        use super::{assert_is_invalid_data, serialized};
        use crate::{
            packet_reader::PacketReader, tcp_packets::*, BatchPacker, CompressionLevel,
            DeleteRowTcpContract, MyNoSqlReaderTcpSerializer, MyNoSqlTcpContract, Packing,
            BATCH_LZ4, BATCH_STORED, BATCH_ZSTD, BATCH_ZSTD_STREAM,
        };

        fn rows(from: usize, count: usize) -> Vec<u8> {
            let rows: Vec<String> = (from..from + count)
                .map(|no| {
                    format!(
                        r#"{{"PartitionKey":"pk-{}","RowKey":"rk-{no}","TimeStamp":"2026-10-21T12:{:02}:{:02}","Bid":1.0{},"Ask":1.0{}}}"#,
                        no % 17,
                        no / 60 % 60,
                        no % 60,
                        no.wrapping_mul(2_654_435_761) % 977,
                        no % 983
                    )
                })
                .collect();

            format!("[{}]", rows.join(",")).into_bytes()
        }

        /// What a server sends a node over a connection: snapshots of tables, megabytes each,
        /// and hundreds of small packets between them - updates, deletes, pongs - some of them
        /// too small to be packed.
        fn packets_of_a_connection() -> Vec<MyNoSqlTcpContract> {
            (0..500usize)
                .map(|no| match no % 100 {
                    0 => MyNoSqlTcpContract::InitTable {
                        table_name: format!("table-{}", no / 100),
                        // 2 to 7 MB: up to more than the window of a stream
                        data: rows(no, 20_000 + no / 100 * 12_000),
                    },
                    1..=4 => MyNoSqlTcpContract::InitPartition {
                        table_name: "table-0".to_string(),
                        partition_key: format!("pk-{no}"),
                        data: rows(no, 50 + no * 3),
                    },
                    5..=9 => MyNoSqlTcpContract::Pong,
                    10..=14 => MyNoSqlTcpContract::Confirmation {
                        confirmation_id: no as i64,
                    },
                    no_in_100 if no_in_100 % 3 == 0 => MyNoSqlTcpContract::DeleteRows {
                        table_name: "t".to_string(),
                        rows: (0..1 + no % 4)
                            .map(|row| DeleteRowTcpContract {
                                partition_key: format!("{}", no % 7),
                                row_key: format!("{row}"),
                            })
                            .collect(),
                    },
                    _ => MyNoSqlTcpContract::UpdateRows {
                        table_name: format!("table-{}", no % 3),
                        data: rows(no, 1 + no % 5),
                    },
                })
                .collect()
        }

        fn is_stream(level: CompressionLevel) -> bool {
            matches!(level.packing(), Some(Packing::ZstdStream { .. }))
        }

        /// Under this size a packet is not packed at `level`.
        fn min_size_to_pack(level: CompressionLevel) -> usize {
            match level.packing() {
                None => usize::MAX,
                Some(Packing::Lz4 { .. }) => 64,
                Some(Packing::Zstd { .. }) => 128,
                Some(Packing::ZstdStream { .. }) => 32,
            }
        }

        /// The packets as the server sends them at `level`: through one packer, the one of the
        /// connection, and every one which `pack` leaves as it is goes as it is.
        fn wire_of(
            packets: &[MyNoSqlTcpContract],
            sizes: &[usize],
            level: CompressionLevel,
        ) -> (Vec<u8>, usize) {
            let mut packer = BatchPacker::new(level);
            let mut wire = Vec::new();
            let mut packed = 0;

            for (packet, size) in packets.iter().zip(sizes) {
                match packet.pack(&mut packer) {
                    Some(compressed) => {
                        assert!(*size >= min_size_to_pack(level), "level {level}: {size}");
                        packed += 1;
                        compressed.serialize(&mut wire);
                    }
                    None => {
                        // A stream takes every packet it is not too small for
                        if is_stream(level) {
                            assert!(*size < min_size_to_pack(level), "level {level}: {size}");
                        }
                        packet.serialize(&mut wire);
                    }
                }
            }

            (wire, packed)
        }

        #[tokio::test]
        async fn a_node_reads_what_the_server_sends_at_every_level() {
            let packets = packets_of_a_connection();
            let expected: Vec<Vec<u8>> = packets.iter().map(serialized).collect();

            let sizes: Vec<usize> = expected.iter().map(Vec::len).collect();

            let total: usize = sizes.iter().sum();
            assert!(total > 20 * 1024 * 1024, "{total}");

            let small = sizes.iter().filter(|size| **size < 32).count();
            assert!(small > 100, "{small}");

            for level in CompressionLevel::all() {
                let (wire, packed) = wire_of(&packets, &sizes, level);

                if level.is_on() {
                    assert!(
                        wire.len() < total / 2,
                        "level {level}: {} of {total}",
                        wire.len()
                    );
                    assert!(
                        packed > 0 && packed + small <= packets.len(),
                        "level {level}"
                    );
                } else {
                    assert_eq!(total, wire.len());
                    assert_eq!(0, packed);
                }

                if is_stream(level) {
                    // Pieces of the stream with packets which are no part of it between them
                    assert!(packed > 250, "level {level}: {packed}");
                }

                let mut serializer = MyNoSqlReaderTcpSerializer::new();
                let mut reader = PacketReader::new(wire);

                for (no, expected) in expected.iter().enumerate() {
                    let contract = serializer.deserialize(&mut reader, &()).await.unwrap();
                    assert!(
                        serialized(&contract) == *expected,
                        "level {level}, packet {no}: {contract:?}"
                    );
                }

                assert!(reader.is_read_to_end(), "level {level}");
            }
        }

        async fn read_compressed_payload(batch: Vec<u8>) {
            let wire = serialized(&MyNoSqlTcpContract::CompressedPayload(batch));
            let mut reader = PacketReader::new(wire);

            assert_is_invalid_data(
                MyNoSqlReaderTcpSerializer::new()
                    .deserialize(&mut reader, &())
                    .await,
            );
        }

        /// A batch of the kind which goes as it is, with `packet` in it.
        fn stored(packet: &[u8]) -> Vec<u8> {
            let mut batch = vec![BATCH_STORED];
            batch.extend_from_slice(packet);
            batch
        }

        #[tokio::test]
        async fn a_broken_compressed_payload_is_an_error_not_a_panic() {
            let packet = MyNoSqlTcpContract::UpdateRows {
                table_name: "instruments".to_string(),
                data: rows(0, 100),
            };

            for level in CompressionLevel::all().filter(|level| level.is_on()) {
                let Some(MyNoSqlTcpContract::CompressedPayload(batch)) =
                    packet.pack(&mut BatchPacker::new(level))
                else {
                    panic!("level {level}: not packed");
                };

                // Cut short, with something after it, of another kind
                read_compressed_payload(batch[..batch.len() - 3].to_vec()).await;

                let mut longer = batch.clone();
                longer.extend_from_slice(b"more");
                read_compressed_payload(longer).await;

                // (A Zstandard frame is the beginning of a stream as well: it is not tried as
                // a piece of one.)
                let others: &[u8] = match batch[0] {
                    BATCH_LZ4 => &[BATCH_ZSTD, BATCH_ZSTD_STREAM, 4],
                    BATCH_ZSTD => &[BATCH_LZ4, 4],
                    _ => &[BATCH_LZ4, BATCH_ZSTD, 4],
                };

                for kind in others {
                    let mut mislabeled = batch.clone();
                    mislabeled[0] = *kind;
                    read_compressed_payload(mislabeled).await;
                }
            }

            // Nothing in it, garbage
            read_compressed_payload(Vec::new()).await;
            read_compressed_payload(vec![0xff, 0xff, 0xff, 0xff]).await;
        }

        /// A piece of a stream is read only after the pieces before it: by the serializer of
        /// the connection, which read them.
        #[tokio::test]
        async fn a_piece_of_a_stream_is_an_error_on_a_connection_which_did_not_read_the_ones_before_it(
        ) {
            let mut packer = BatchPacker::new(CompressionLevel::new(7).unwrap());

            let pieces: Vec<Vec<u8>> = (0..2)
                .map(|no| {
                    let packet = MyNoSqlTcpContract::UpdateRows {
                        table_name: "instruments".to_string(),
                        data: rows(no, 50),
                    };

                    match packet.pack(&mut packer) {
                        Some(MyNoSqlTcpContract::CompressedPayload(batch)) => batch,
                        other => panic!("{other:?}"),
                    }
                })
                .collect();

            read_compressed_payload(pieces[1].clone()).await;
        }

        #[tokio::test]
        async fn a_packet_in_a_compressed_payload_which_can_not_be_read_is_an_error_not_a_panic() {
            // Cut short: a table name of 5 bytes, 1 of them there
            read_compressed_payload(stored(&[INIT_TABLE, 5, b'a'])).await;

            // A byte array which says it is longer than the packet, and one of -1 bytes
            for len in [1_000_000i32, -1] {
                let mut packet = vec![UPDATE_ROWS, 1, b't'];
                packet.extend_from_slice(&len.to_le_bytes());
                packet.extend_from_slice(b"[]");
                read_compressed_payload(stored(&packet)).await;
            }

            // No packet with this id
            read_compressed_payload(stored(&[200])).await;

            // Nothing at all
            read_compressed_payload(stored(&[])).await;

            // Bytes after the packet
            read_compressed_payload(stored(&[PONG, PONG])).await;

            // A CompressedPayload in a CompressedPayload
            let inner = serialized(&MyNoSqlTcpContract::CompressedPayload(stored(&[PONG])));
            read_compressed_payload(stored(&inner)).await;
        }

        #[tokio::test]
        async fn a_compressed_payload_with_a_packet_which_goes_as_it_is_is_read() {
            let mut serializer = MyNoSqlReaderTcpSerializer::new();

            let wire = serialized(&MyNoSqlTcpContract::CompressedPayload(stored(&[PONG])));
            let mut reader = PacketReader::new(wire);

            match serializer.deserialize(&mut reader, &()).await.unwrap() {
                MyNoSqlTcpContract::Pong => {}
                contract => panic!("Unexpected contract: {contract:?}"),
            }
        }
    }
}
