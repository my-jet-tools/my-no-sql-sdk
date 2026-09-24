use std::time::Duration;

use my_tcp_sockets::{
    socket_reader::{ReadingTcpContractFail, SocketReader},
    TcpSerializerFactory, TcpSocketSerializer, TcpWriteBuffer,
};

use crate::MyNoSqlTcpContract;

pub struct MyNoSqlReaderTcpSerializer;

impl MyNoSqlReaderTcpSerializer {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MyNoSqlReaderTcpSerializer {
    fn default() -> Self {
        Self::new()
    }
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
        MyNoSqlTcpContract::deserialize(socket_reader)
            .await?
            .decompress_if_compressed()
            .await
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
