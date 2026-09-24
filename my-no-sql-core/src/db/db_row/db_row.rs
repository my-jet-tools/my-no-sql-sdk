use std::sync::Arc;

use my_json::json_writer::JsonValueWriter;
#[cfg(feature = "master-node")]
use rust_extensions::date_time::AtomicDateTimeAsMicroseconds;
#[cfg(feature = "master-node")]
use rust_extensions::date_time::DateTimeAsMicroseconds;
use rust_extensions::sorted_vec::EntityWithStrKey;

use crate::db::PartitionKeyParameter;
use crate::db_json_entity::DbJsonEntity;

use super::RowKeyParameter;

/// A row of a table.
///
/// It comes in two shapes:
/// - [`DbRow::Plain`] — the raw JSON bytes are kept as-is (the historical layout);
///   keys/timestamp/expires are byte offsets into `raw` and reads are zero-copy.
/// - [`DbRow::Compressed`] — the JSON body is kept DEFLATE-compressed in memory
///   (master-node only). Keys and metadata are kept uncompressed so indexing/GC
///   never decompress; only emitting the row (`write_json`/`content_bytes`)
///   decompresses.
pub enum DbRow {
    Plain(DbRowPlain),
    #[cfg(feature = "master-node")]
    Compressed(DbRowCompressed),
}

/// DEFLATE level used for per-row compression. 6 is the ratio/speed sweet spot for a
/// single JSON row: on a 170-byte row it stores 84% where level 1 stores 96%, and past
/// it level 9 buys ~1 more percentage point for no useful gain. Decompression speed —
/// the hot path, since every read inflates — does not depend on the level.
#[cfg(feature = "master-node")]
const DEFLATE_COMPRESSION_LEVEL: u32 = 6;

/// Compresses a row body into a raw DEFLATE stream. In-memory only: the compressed form
/// never reaches disk or the wire (both go through the decompressed bytes), so the
/// algorithm can be changed without any compatibility concern.
#[cfg(feature = "master-node")]
fn compress_row(raw: &[u8]) -> Vec<u8> {
    use std::io::Write;

    let mut encoder = flate2::write::DeflateEncoder::new(
        Vec::with_capacity(raw.len() / 2 + 32),
        flate2::Compression::new(DEFLATE_COMPRESSION_LEVEL),
    );

    encoder
        .write_all(raw)
        .expect("deflate compress of a db row failed");

    encoder
        .finish()
        .expect("deflate compress of a db row failed")
}

/// Inflates a row body produced by [`compress_row`]. `content_len` is the exact
/// decompressed size, kept alongside the compressed bytes, so the buffer is allocated
/// once and never grows.
#[cfg(feature = "master-node")]
fn decompress_row(compressed: &[u8], content_len: usize) -> Vec<u8> {
    use std::io::Read;

    let mut raw = Vec::with_capacity(content_len);

    flate2::read::DeflateDecoder::new(compressed)
        .read_to_end(&mut raw)
        .expect("deflate decompress of a db row failed");

    raw
}

/// Historical, uncompressed row layout: keys/timestamp/expires are positions into `raw`.
pub struct DbRowPlain {
    partition_key: crate::db_json_entity::KeyValueContentPosition,
    row_key: crate::db_json_entity::KeyValueContentPosition,
    /// The logical PartitionKey, kept next to `raw` only when the raw JSON carries escape
    /// sequences and the slice between the quotes therefore is not the key itself. `None` -
    /// nothing to unescape, the key is borrowed straight out of `raw`.
    partition_key_unescaped: Option<Box<str>>,
    /// The logical RowKey - see `partition_key_unescaped`.
    row_key_unescaped: Option<Box<str>>,
    raw: Vec<u8>,
    #[cfg(feature = "master-node")]
    expires_value: AtomicDateTimeAsMicroseconds,
    #[cfg(feature = "master-node")]
    expires: Option<crate::db_json_entity::JsonKeyValuePosition>,
    #[cfg(feature = "master-node")]
    time_stamp: crate::db_json_entity::KeyValueContentPosition,
    #[cfg(feature = "master-node")]
    time_stamp_value: DateTimeAsMicroseconds,
    #[cfg(feature = "master-node")]
    last_read_access: AtomicDateTimeAsMicroseconds,
}

impl DbRowPlain {
    pub fn new(mut db_json_entity: DbJsonEntity, raw: Vec<u8>) -> Self {
        #[cfg(feature = "debug_db_row")]
        println!(
            "Created DbRow: PK:{}. RK:{}. Expires{:?}",
            db_json_entity.get_partition_key(raw.as_slice()),
            db_json_entity.get_row_key(raw.as_slice()),
            db_json_entity.expires
        );

        // The keys were unescaped once, while parsing - move them over instead of doing it again.
        let partition_key_unescaped = db_json_entity.partition_key_unescaped.take();
        let row_key_unescaped = db_json_entity.row_key_unescaped.take();

        #[cfg(feature = "master-node")]
        let time_stamp = db_json_entity.time_stamp.unwrap();
        #[cfg(feature = "master-node")]
        let time_stamp_value =
            my_no_sql_abstractions::parse_time_stamp(time_stamp.value.get_str_value(&raw))
                .unwrap_or_else(DateTimeAsMicroseconds::now);

        Self {
            raw,
            partition_key: db_json_entity.partition_key.value,
            row_key: db_json_entity.row_key.value,
            partition_key_unescaped,
            row_key_unescaped,
            #[cfg(feature = "master-node")]
            time_stamp: time_stamp.value,
            #[cfg(feature = "master-node")]
            time_stamp_value,
            #[cfg(feature = "master-node")]
            expires_value: if let Some(expires_value) = db_json_entity.expires_value {
                AtomicDateTimeAsMicroseconds::new(expires_value.unix_microseconds)
            } else {
                AtomicDateTimeAsMicroseconds::new(0)
            },
            #[cfg(feature = "master-node")]
            expires: db_json_entity.expires,
            #[cfg(feature = "master-node")]
            last_read_access: AtomicDateTimeAsMicroseconds::new(time_stamp_value.unix_microseconds),
        }
    }

    pub fn get_partition_key(&self) -> &str {
        match self.partition_key_unescaped.as_ref() {
            Some(partition_key) => partition_key,
            None => self.partition_key.get_str_value(&self.raw),
        }
    }

    pub fn get_row_key(&self) -> &str {
        match self.row_key_unescaped.as_ref() {
            Some(row_key) => row_key,
            None => self.row_key.get_str_value(&self.raw),
        }
    }

    #[cfg(feature = "master-node")]
    pub fn get_time_stamp(&self) -> &str {
        self.time_stamp.get_str_value(&self.raw)
    }

    #[cfg(feature = "master-node")]
    pub fn get_time_stamp_as_date_time(&self) -> DateTimeAsMicroseconds {
        self.time_stamp_value
    }

    #[cfg(feature = "master-node")]
    pub fn update_expires(
        &self,
        expires: Option<DateTimeAsMicroseconds>,
    ) -> Option<DateTimeAsMicroseconds> {
        let old_value = self.get_expires();

        if let Some(expires) = expires {
            self.expires_value.update(expires);
        } else {
            self.expires_value.update(DateTimeAsMicroseconds::new(0));
        }

        old_value
    }

    #[cfg(feature = "master-node")]
    pub fn get_expires(&self) -> Option<DateTimeAsMicroseconds> {
        let result = self.expires_value.as_date_time();

        if result.unix_microseconds == 0 {
            None
        } else {
            Some(result)
        }
    }

    #[cfg(feature = "master-node")]
    pub fn write_json(&self, out: &mut String) {
        write_json_raw(&self.raw, &self.expires, self.get_expires(), out);
    }

    #[cfg(not(feature = "master-node"))]
    pub fn write_json(&self, out: &mut String) {
        let str = unsafe { std::str::from_utf8_unchecked(&self.raw) };
        out.push_str(str);
    }
}

/// Compressed row layout (master-node only). The JSON body is DEFLATE-compressed;
/// keys are kept as owned strings (so indexing never decompresses); the
/// timestamp/expires positions remain valid against the *decompressed* bytes.
#[cfg(feature = "master-node")]
pub struct DbRowCompressed {
    partition_key_str: String,
    row_key_str: String,
    partition_key: crate::db_json_entity::KeyValueContentPosition,
    row_key: crate::db_json_entity::KeyValueContentPosition,
    compressed: Vec<u8>,
    content_len: usize,
    expires_value: AtomicDateTimeAsMicroseconds,
    expires: Option<crate::db_json_entity::JsonKeyValuePosition>,
    time_stamp: crate::db_json_entity::KeyValueContentPosition,
    time_stamp_value: DateTimeAsMicroseconds,
    last_read_access: AtomicDateTimeAsMicroseconds,
}

#[cfg(feature = "master-node")]
impl DbRowCompressed {
    fn from_plain(plain: &DbRowPlain) -> Self {
        Self {
            partition_key_str: plain.get_partition_key().to_string(),
            row_key_str: plain.get_row_key().to_string(),
            partition_key: plain.partition_key.clone(),
            row_key: plain.row_key.clone(),
            compressed: compress_row(&plain.raw),
            content_len: plain.raw.len(),
            expires_value: AtomicDateTimeAsMicroseconds::new(
                plain.expires_value.as_date_time().unix_microseconds,
            ),
            expires: plain.expires.clone(),
            time_stamp: plain.time_stamp.clone(),
            time_stamp_value: plain.time_stamp_value,
            last_read_access: AtomicDateTimeAsMicroseconds::new(
                plain.last_read_access.as_date_time().unix_microseconds,
            ),
        }
    }

    fn into_plain(&self) -> DbRowPlain {
        let raw = self.decompress();

        // Derived from the very same bytes the positions point into, so the plain row keeps
        // addressing itself by the same logical keys the compressed one did.
        let partition_key_unescaped = self.partition_key.unescape_str_value(raw.as_slice());
        let row_key_unescaped = self.row_key.unescape_str_value(raw.as_slice());

        DbRowPlain {
            raw,
            partition_key: self.partition_key.clone(),
            row_key: self.row_key.clone(),
            partition_key_unescaped,
            row_key_unescaped,
            expires_value: AtomicDateTimeAsMicroseconds::new(
                self.expires_value.as_date_time().unix_microseconds,
            ),
            expires: self.expires.clone(),
            time_stamp: self.time_stamp.clone(),
            time_stamp_value: self.time_stamp_value,
            last_read_access: AtomicDateTimeAsMicroseconds::new(
                self.last_read_access.as_date_time().unix_microseconds,
            ),
        }
    }

    fn get_time_stamp_as_date_time(&self) -> DateTimeAsMicroseconds {
        self.time_stamp_value
    }

    fn decompress(&self) -> Vec<u8> {
        decompress_row(&self.compressed, self.content_len)
    }

    fn get_expires(&self) -> Option<DateTimeAsMicroseconds> {
        let result = self.expires_value.as_date_time();

        if result.unix_microseconds == 0 {
            None
        } else {
            Some(result)
        }
    }

    fn update_expires(
        &self,
        expires: Option<DateTimeAsMicroseconds>,
    ) -> Option<DateTimeAsMicroseconds> {
        let old_value = self.get_expires();

        if let Some(expires) = expires {
            self.expires_value.update(expires);
        } else {
            self.expires_value.update(DateTimeAsMicroseconds::new(0));
        }

        old_value
    }

    fn write_json(&self, out: &mut String) {
        let raw = self.decompress();
        write_json_raw(&raw, &self.expires, self.get_expires(), out);
    }
}

impl DbRow {
    pub fn new(db_json_entity: DbJsonEntity, raw: Vec<u8>) -> Self {
        DbRow::Plain(DbRowPlain::new(db_json_entity, raw))
    }

    pub fn get_partition_key(&self) -> &str {
        match self {
            DbRow::Plain(r) => r.get_partition_key(),
            #[cfg(feature = "master-node")]
            DbRow::Compressed(r) => r.partition_key_str.as_str(),
        }
    }

    pub fn get_row_key(&self) -> &str {
        match self {
            DbRow::Plain(r) => r.get_row_key(),
            #[cfg(feature = "master-node")]
            DbRow::Compressed(r) => r.row_key_str.as_str(),
        }
    }

    /// Physical stored bytes. For a compressed row these are the **compressed**
    /// bytes — use [`DbRow::content_bytes`] / [`DbRow::get_content_size`] when you
    /// need the logical JSON.
    pub fn get_src_as_slice(&self) -> &[u8] {
        match self {
            DbRow::Plain(r) => r.raw.as_slice(),
            #[cfg(feature = "master-node")]
            DbRow::Compressed(r) => r.compressed.as_slice(),
        }
    }

    /// The logical JSON bytes of the row (decompressing if needed). Does not inject
    /// the runtime `expires` value — this is the stored/persisted form.
    pub fn content_bytes(&self) -> std::borrow::Cow<'_, [u8]> {
        match self {
            DbRow::Plain(r) => std::borrow::Cow::Borrowed(r.raw.as_slice()),
            #[cfg(feature = "master-node")]
            DbRow::Compressed(r) => std::borrow::Cow::Owned(r.decompress()),
        }
    }

    /// Logical (uncompressed) content length, O(1). Used for table-size accounting.
    pub fn get_content_size(&self) -> usize {
        match self {
            DbRow::Plain(r) => r.raw.len(),
            #[cfg(feature = "master-node")]
            DbRow::Compressed(r) => r.content_len,
        }
    }

    pub fn write_json(&self, out: &mut String) {
        match self {
            DbRow::Plain(r) => r.write_json(out),
            #[cfg(feature = "master-node")]
            DbRow::Compressed(r) => r.write_json(out),
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut result = String::new();
        self.write_json(&mut result);
        result.into_bytes()
    }
}

#[cfg(feature = "master-node")]
impl DbRow {
    pub fn is_compressed(&self) -> bool {
        matches!(self, DbRow::Compressed(_))
    }

    /// Returns a compressed copy of the row. If it is already compressed the same
    /// `Arc` is returned untouched.
    pub fn compress_arc(db_row: Arc<DbRow>) -> Arc<DbRow> {
        match db_row.as_ref() {
            DbRow::Plain(plain) => Arc::new(DbRow::Compressed(DbRowCompressed::from_plain(plain))),
            DbRow::Compressed(_) => db_row,
        }
    }

    /// Returns a plain (uncompressed) copy of the row. If it is already plain the
    /// same `Arc` is returned untouched.
    pub fn decompress_arc(db_row: Arc<DbRow>) -> Arc<DbRow> {
        match db_row.as_ref() {
            DbRow::Compressed(compressed) => Arc::new(DbRow::Plain(compressed.into_plain())),
            DbRow::Plain(_) => db_row,
        }
    }

    /// O(1) access to the row's timestamp as a `DateTimeAsMicroseconds`. Works for
    /// both plain and compressed rows without decompressing.
    pub fn get_time_stamp_as_date_time(&self) -> DateTimeAsMicroseconds {
        match self {
            DbRow::Plain(r) => r.get_time_stamp_as_date_time(),
            DbRow::Compressed(r) => r.get_time_stamp_as_date_time(),
        }
    }

    pub fn update_last_read_access(&self, value: DateTimeAsMicroseconds) {
        match self {
            DbRow::Plain(r) => r.last_read_access.update(value),
            DbRow::Compressed(r) => r.last_read_access.update(value),
        }
    }

    pub fn get_last_read_access(&self) -> DateTimeAsMicroseconds {
        match self {
            DbRow::Plain(r) => r.last_read_access.as_date_time(),
            DbRow::Compressed(r) => r.last_read_access.as_date_time(),
        }
    }

    pub fn update_expires(
        &self,
        expires: Option<DateTimeAsMicroseconds>,
    ) -> Option<DateTimeAsMicroseconds> {
        match self {
            DbRow::Plain(r) => r.update_expires(expires),
            DbRow::Compressed(r) => r.update_expires(expires),
        }
    }

    pub fn get_expires(&self) -> Option<DateTimeAsMicroseconds> {
        match self {
            DbRow::Plain(r) => r.get_expires(),
            DbRow::Compressed(r) => r.get_expires(),
        }
    }
}

impl EntityWithStrKey for DbRow {
    fn get_key(&self) -> &str {
        self.get_row_key()
    }
}

impl PartitionKeyParameter for Arc<DbRow> {
    fn as_str(&self) -> &str {
        self.get_partition_key()
    }

    fn into_partition_key(self) -> crate::db::PartitionKey {
        self.get_partition_key().into()
    }

    fn to_partition_key(&self) -> crate::db::PartitionKey {
        self.get_partition_key().into()
    }
}

impl RowKeyParameter for Arc<DbRow> {
    fn as_str(&self) -> &str {
        self.get_row_key()
    }
}

#[cfg(feature = "master-node")]
fn write_json_raw(
    raw: &[u8],
    expires: &Option<crate::db_json_entity::JsonKeyValuePosition>,
    expires_value: Option<DateTimeAsMicroseconds>,
    out: &mut String,
) {
    if expires_value.is_none() {
        if let Some(expires) = expires {
            if let Some(before_separator) = find_json_separator_before(raw, expires.key.start - 1) {
                unsafe {
                    out.push_str(std::str::from_utf8_unchecked(&raw[..before_separator]));
                    out.push_str(std::str::from_utf8_unchecked(&raw[expires.value.end..]));
                }
                return;
            }

            if let Some(after_separator) = find_json_separator_after(raw, expires.value.end) {
                unsafe {
                    out.push_str(std::str::from_utf8_unchecked(&raw[..expires.key.start]));
                    out.push_str(std::str::from_utf8_unchecked(&raw[after_separator..]));
                }
                return;
            }

            unsafe {
                out.push_str(std::str::from_utf8_unchecked(&raw[..expires.key.start]));
                out.push_str(std::str::from_utf8_unchecked(&raw[expires.value.end..]));
            }
        } else {
            unsafe {
                out.push_str(std::str::from_utf8_unchecked(raw));
            }
        }

        return;
    }

    let expires_value = expires_value.unwrap();

    unsafe {
        if let Some(expires) = expires {
            out.push_str(std::str::from_utf8_unchecked(&raw[..expires.key.start]));
            inject_expires(out, expires_value);
            out.push_str(std::str::from_utf8_unchecked(&raw[expires.value.end..]));
        } else {
            let end_of_json = crate::db_json_entity::get_the_end_of_the_json(raw);
            out.push_str(std::str::from_utf8_unchecked(&raw[..end_of_json]));
            out.push(',');
            inject_expires(out, expires_value);
            out.push_str(std::str::from_utf8_unchecked(&raw[end_of_json..]));
        }
    }
}

#[cfg(feature = "master-node")]
fn inject_expires(out: &mut String, expires_value: DateTimeAsMicroseconds) {
    out.push('"');
    out.push_str(crate::db_json_entity::consts::EXPIRES);
    out.push_str("\":\"");
    out.push_str(&expires_value.to_rfc3339()[..19]);
    out.push('"');
}
#[cfg(feature = "master-node")]
fn find_json_separator_before(src: &[u8], pos: usize) -> Option<usize> {
    let mut i = pos;
    while i > 0 {
        let b = src[i];

        if b <= 32 {
            i -= 1;
            continue;
        }

        if b == b',' {
            return Some(i);
        }

        break;
    }

    None
}
#[cfg(feature = "master-node")]
fn find_json_separator_after(src: &[u8], pos: usize) -> Option<usize> {
    let mut i = pos;
    while i < src.len() {
        let b = src[i];

        if b <= 32 {
            i += 1;
            continue;
        }

        if b == b',' {
            return Some(i + 1);
        }

        break;
    }

    None
}

impl JsonValueWriter for &'_ DbRow {
    const IS_ARRAY: bool = false;

    fn write(&self, dest: &mut String) {
        self.write_json(dest)
    }
}

#[cfg(feature = "master-node")]
impl crate::ExpirationIndex<Arc<DbRow>> for Arc<DbRow> {
    fn get_id_as_str(&self) -> &str {
        self.get_row_key()
    }

    fn to_owned(&self) -> Arc<DbRow> {
        self.clone()
    }

    fn get_expiration_moment(&self) -> Option<rust_extensions::date_time::DateTimeAsMicroseconds> {
        self.get_expires()
    }
}

#[cfg(feature = "debug_db_row")]
impl Drop for DbRow {
    fn drop(&mut self) {
        println!(
            "Dropped DbRow: PK:{}. RK:{}",
            self.get_partition_key(),
            self.get_row_key(),
        );
    }
}

#[cfg(feature = "master-node")]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::db::DbRow;
    use crate::db_json_entity::{DbJsonEntity, JsonTimeStamp};

    fn create_plain_row(json: &str) -> Arc<DbRow> {
        let time_stamp = JsonTimeStamp::now();
        Arc::new(DbJsonEntity::parse_into_db_row(json.as_bytes().into(), &time_stamp).unwrap())
    }

    #[test]
    fn compressed_renders_identically_without_expires() {
        let json = r#"{"PartitionKey":"my-partition","RowKey":"my-row","Value":"hello world"}"#;

        let plain = create_plain_row(json);
        assert!(!plain.is_compressed());

        let compressed = DbRow::compress_arc(plain.clone());
        assert!(compressed.is_compressed());

        assert_eq!(plain.to_vec(), compressed.to_vec());

        let mut plain_json = String::new();
        plain.write_json(&mut plain_json);
        let mut compressed_json = String::new();
        compressed.write_json(&mut compressed_json);
        assert_eq!(plain_json, compressed_json);
    }

    #[test]
    fn compressed_renders_identically_with_expires() {
        let json = r#"{"PartitionKey":"my-partition","RowKey":"my-row","Expires":"2099-01-01T00:00:00","Value":"hello world"}"#;

        let plain = create_plain_row(json);
        let compressed = DbRow::compress_arc(plain.clone());

        // Exercises the expires-injection branch of write_json (the Expires field is
        // stored as a position and re-emitted from the runtime atomic).
        assert_eq!(plain.to_vec(), compressed.to_vec());
        assert_eq!(plain.get_expires(), compressed.get_expires());
        assert!(plain.get_expires().is_some());
    }

    #[test]
    fn keys_and_content_size_are_correct_on_a_compressed_row() {
        let json = r#"{"PartitionKey":"my-partition","RowKey":"my-row","Value":"hello world"}"#;

        let plain = create_plain_row(json);
        let compressed = DbRow::compress_arc(plain.clone());

        assert_eq!(compressed.get_partition_key(), "my-partition");
        assert_eq!(compressed.get_row_key(), "my-row");

        // get_content_size is the *logical* (decompressed) length, so it must match
        // the plain row even though the physical bytes differ.
        assert_eq!(compressed.get_content_size(), plain.get_content_size());
        assert_eq!(
            compressed.content_bytes().as_ref(),
            plain.content_bytes().as_ref()
        );
    }

    #[test]
    fn decompress_arc_round_trips_back_to_plain() {
        let json = r#"{"PartitionKey":"my-partition","RowKey":"my-row","Expires":"2099-01-01T00:00:00","Value":"hello world"}"#;

        let plain = create_plain_row(json);
        let original = plain.to_vec();

        let compressed = DbRow::compress_arc(plain.clone());
        let back_to_plain = DbRow::decompress_arc(compressed);

        assert!(!back_to_plain.is_compressed());
        assert_eq!(back_to_plain.to_vec(), original);
    }

    #[test]
    fn update_expires_is_preserved_across_compression() {
        use rust_extensions::date_time::DateTimeAsMicroseconds;

        let json = r#"{"PartitionKey":"my-partition","RowKey":"my-row","Value":"hello world"}"#;

        let plain = create_plain_row(json);
        let new_expires = DateTimeAsMicroseconds::from_str("2099-06-01T00:00:00").unwrap();
        plain.update_expires(Some(new_expires));

        let compressed = DbRow::compress_arc(plain.clone());
        assert_eq!(compressed.get_expires(), plain.get_expires());

        let back_to_plain = DbRow::decompress_arc(compressed);
        assert_eq!(back_to_plain.get_expires(), plain.get_expires());
    }
}
