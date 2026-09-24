use my_tcp_sockets::{
    socket_reader::{ReadingTcpContractFail, SocketReader},
    TcpWriteBuffer,
};

#[derive(Debug)]
pub struct DeleteRowTcpContract {
    pub partition_key: String,
    pub row_key: String,
}

impl DeleteRowTcpContract {
    pub async fn deserialize<TSocketReader: SocketReader>(
        socket_reader: &mut TSocketReader,
    ) -> Result<Self, ReadingTcpContractFail> {
        let partition_key = crate::common_deserializes::read_pascal_string(socket_reader).await?;
        let row_key = crate::common_deserializes::read_pascal_string(socket_reader).await?;

        let result = Self {
            partition_key,
            row_key,
        };

        Ok(result)
    }

    pub fn serialize(&self, write_buffer: &mut impl TcpWriteBuffer) {
        write_buffer.write_pascal_string(self.partition_key.as_str());
        write_buffer.write_pascal_string(self.row_key.as_str());
    }
}
