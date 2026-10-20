//! Payload compression for the MyNoSql TCP protocol.
//!
//! Only the server → node link is compressed. A node names the codec in its
//! `GreetingFromNode` ([`crate::TcpCompression`]), and the server sends every packet of that
//! connection as a `CompressedPayload`: one whole serialized [`crate::MyNoSqlTcpContract`]
//! compressed as a single zstd frame. Ordinary readers do not declare compression and never
//! get a `CompressedPayload`.
//!
//! The codec and the level - zstd, level 3 - are the ones of the my-reverse-proxy gateway
//! handshake, where they were measured for CPU and ratio.
//!
//! zstd is a C library, so the codec sits behind the `zstd` feature of this crate. The SDK
//! enables it for the infrastructure roles only - `master-node` and `read-node`; a service
//! built with `data-reader` / `data-writer` does not need a C toolchain. Built without the
//! feature, [`decompress`] answers every payload with an error, and the connection breaks.

#[cfg(feature = "zstd")]
const ZSTD_LEVEL: i32 = 3;

/// Compresses a serialized packet into a zstd frame.
#[cfg(feature = "zstd")]
pub fn compress(payload: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    zstd::stream::encode_all(payload, ZSTD_LEVEL)
}

/// Decompresses a payload produced by [`compress`].
#[cfg(feature = "zstd")]
pub fn decompress(payload: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    zstd::stream::decode_all(payload)
}

#[cfg(not(feature = "zstd"))]
pub fn decompress(_payload: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Got a CompressedPayload, but my-no-sql-tcp-shared is built without the `zstd` feature",
    ))
}

#[cfg(all(test, feature = "zstd"))]
mod tests {
    #[test]
    fn round_trip() {
        let payload =
            br#"[{"PartitionKey":"instruments","RowKey":"EURUSD","Value":15000.0}]"#.repeat(50);

        let compressed = super::compress(&payload).unwrap();

        assert!(compressed.len() < payload.len());
        assert_eq!(super::decompress(&compressed).unwrap(), payload);
    }

    #[test]
    fn round_trip_empty() {
        let compressed = super::compress(&[]).unwrap();
        assert!(super::decompress(&compressed).unwrap().is_empty());
    }

    #[test]
    fn broken_payload_is_an_error_not_a_panic() {
        assert!(super::decompress(&[0xff, 0xff, 0xff, 0xff]).is_err());
    }
}
