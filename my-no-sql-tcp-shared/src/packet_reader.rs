use std::io;

use my_tcp_sockets::socket_reader::{ReadBuffer, ReadingTcpContractFail, SocketReader};

/// Reads the packet a `CompressedPayload` unpacks to: a whole packet in memory. Unlike
/// `SocketReaderInMem`, a packet which is cut short or says more than it has is an error and
/// not a panic - the bytes come from the network - and a read does not move the rest of the
/// packet.
pub struct PacketReader {
    packet: Vec<u8>,
    pos: usize,
}

impl PacketReader {
    pub fn new(packet: Vec<u8>) -> Self {
        Self { packet, pos: 0 }
    }

    /// Every byte of the packet is read.
    pub fn is_read_to_end(&self) -> bool {
        self.pos == self.packet.len()
    }

    fn take(&mut self, len: usize) -> Result<&[u8], ReadingTcpContractFail> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|end| *end <= self.packet.len())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "the packet is cut short: {len} bytes to read at {} of {}",
                        self.pos,
                        self.packet.len()
                    ),
                )
            })?;

        let bytes = &self.packet[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }
}

#[async_trait::async_trait]
impl SocketReader for PacketReader {
    async fn read_byte(&mut self) -> Result<u8, ReadingTcpContractFail> {
        Ok(self.take(1)?[0])
    }

    async fn read_buf(&mut self, buf: &mut [u8]) -> Result<(), ReadingTcpContractFail> {
        buf.copy_from_slice(self.take(buf.len())?);
        Ok(())
    }

    /// The default takes the memory for the array before it reads it - as much as the length
    /// says, whatever it says.
    async fn read_byte_array(&mut self) -> Result<Vec<u8>, ReadingTcpContractFail> {
        let len = self.read_i32().await?;

        let len = usize::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("a byte array of {len} bytes"),
            )
        })?;

        Ok(self.take(len)?.to_vec())
    }

    /// No packet of MyNoSql ends with a marker.
    async fn read_until_end_marker<'s>(
        &mut self,
        _read_buffer: &'s mut ReadBuffer,
        _end_marker: &[u8],
    ) -> Result<&'s [u8], ReadingTcpContractFail> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "a MyNoSql packet has no end marker",
        )
        .into())
    }
}

#[cfg(test)]
mod tests {
    use my_tcp_sockets::socket_reader::SocketReader;

    use super::PacketReader;

    #[tokio::test]
    async fn reads_what_there_is_and_no_more() {
        let mut reader = PacketReader::new(vec![1, 2, 3, 0, 0, 7]);

        assert_eq!(1, reader.read_byte().await.unwrap());
        assert_eq!(2 + (3 << 8), reader.read_i32().await.unwrap());
        assert!(!reader.is_read_to_end());

        assert!(reader.read_i32().await.is_err());
        assert_eq!(7, reader.read_byte().await.unwrap());
        assert!(reader.is_read_to_end());

        assert!(reader.read_byte().await.is_err());
    }

    #[tokio::test]
    async fn a_byte_array_longer_than_the_packet_is_an_error() {
        for len in [4i32, i32::MAX, -1] {
            let mut packet = len.to_le_bytes().to_vec();
            packet.extend_from_slice(&[1, 2, 3]);

            assert!(PacketReader::new(packet).read_byte_array().await.is_err());
        }

        let mut packet = 3i32.to_le_bytes().to_vec();
        packet.extend_from_slice(&[1, 2, 3]);
        let mut reader = PacketReader::new(packet);
        assert_eq!(vec![1, 2, 3], reader.read_byte_array().await.unwrap());
        assert!(reader.is_read_to_end());
    }
}
