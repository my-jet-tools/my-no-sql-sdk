//! Payload compression for the MyNoSql TCP protocol.
//!
//! A whole serialized [`crate::MyNoSqlTcpContract`] is compressed as one **raw DEFLATE**
//! stream (RFC 1951 — no zlib/gzip header, no trailer) and carried in the
//! `CompressedPayload` packet. Both sides of the wire must agree on this, so the server
//! and every reader have to be built from the same version of this crate.
//!
//! Historically this was a ZIP container holding a single entry named `"d"`. The
//! compressed bytes are the same DEFLATE stream ZIP stored inside, minus the container:
//! the local header + central directory cost ~100 bytes per packet, which on small
//! updates made the "compressed" payload *larger* than the original. Raw DEFLATE keeps
//! the compression ratio identical on big snapshots and drops that fixed overhead.

use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};
use std::io::{Read, Write};

/// DEFLATE level used for the wire payload. 6 is what the previous ZIP-based
/// implementation used, and the best ratio/throughput trade-off for JSON row payloads:
/// on a 3.9 MB snapshot level 9 buys 0.35 percentage points of ratio for 2.2x the CPU.
const DEFLATE_LEVEL: u32 = 6;

/// Compresses a serialized packet into a raw DEFLATE stream.
pub fn compress(payload: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    // JSON rows compress to roughly an eighth; a quarter is a safe starting capacity.
    let mut encoder = DeflateEncoder::new(
        Vec::with_capacity(payload.len() / 4 + 64),
        Compression::new(DEFLATE_LEVEL),
    );

    encoder.write_all(payload)?;

    encoder.finish()
}

/// Inflates a payload produced by [`compress`].
pub fn decompress(payload: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoder = DeflateDecoder::new(payload);

    let mut result = Vec::with_capacity(payload.len() * 4);

    decoder.read_to_end(&mut result)?;

    Ok(result)
}

#[cfg(test)]
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
