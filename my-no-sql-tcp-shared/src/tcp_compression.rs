/// Compression the server sends a node's data with. The node declares it in its
/// `GreetingFromNode` as a number, and the server wraps every packet it sends to that
/// connection into a `CompressedPayload` made with it - see `payload_compressor`.
///
/// The numbers are the ones the my-reverse-proxy gateway handshake uses: `0` none, `1` zstd.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpCompression {
    None,
    /// zstd, level 3.
    Zstd,
}

impl TcpCompression {
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zstd => 1,
        }
    }

    /// `None` for a number this SDK does not know.
    pub fn from_u8(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::None),
            1 => Some(Self::Zstd),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zstd => "zstd",
        }
    }
}
