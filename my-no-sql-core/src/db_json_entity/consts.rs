pub const PARTITION_KEY: &str = "PartitionKey";
pub const ROW_KEY: &str = "RowKey";

/// Longest PartitionKey which is accepted, in bytes of the logical (unescaped) key - the
/// json spelling of it may well be longer.
pub const MAX_PARTITION_KEY_LEN: usize = 255;

pub const TIME_STAMP: &str = "TimeStamp";
pub const TIME_STAMP_LOWER_CASE: &str = "timestamp";
pub const EXPIRES: &str = "Expires";
