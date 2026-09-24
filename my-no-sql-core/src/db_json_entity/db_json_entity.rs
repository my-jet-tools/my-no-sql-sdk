use crate::db::DbRow;

use my_json::json_reader::JsonArrayIterator;

use rust_extensions::date_time::DateTimeAsMicroseconds;

use std::sync::Arc;

use super::DbEntityParseFail;
use super::DbJsonEntityWithContent;
use super::JsonStrValue;
use super::DbRowContentCompiler;
use super::JsonKeyValuePosition;
use super::JsonTimeStamp;
use super::KeyValueContentPosition;
use my_json::json_reader::JsonFirstLineIterator;

pub struct DbJsonEntity {
    pub partition_key: JsonKeyValuePosition,
    pub row_key: JsonKeyValuePosition,
    pub time_stamp: Option<JsonKeyValuePosition>,
    pub expires: Option<JsonKeyValuePosition>,
    pub expires_value: Option<DateTimeAsMicroseconds>,
    /// The logical PartitionKey - filled in only when the raw JSON carries escape sequences
    /// (`"a\\b"`, `"д"`, ...) and the value between the quotes therefore is not the key
    /// itself. `None` - the raw slice is the key and is borrowed as is.
    ///
    /// Resolved once, here, so that every consumer (this entity's accessors, [`DbRow`], the
    /// readers' index) addresses a row by the same, logical, key.
    pub(crate) partition_key_unescaped: Option<Box<str>>,
    /// The logical RowKey - see [`Self::partition_key_unescaped`].
    pub(crate) row_key_unescaped: Option<Box<str>>,
}

impl DbJsonEntity {
    pub fn from_slice(src: &[u8]) -> Result<Self, DbEntityParseFail> {
        Self::new(JsonFirstLineIterator::new(src))
    }
    pub fn new(json_first_line_reader: JsonFirstLineIterator) -> Result<Self, DbEntityParseFail> {
        let mut partition_key = None;
        let mut row_key = None;
        let mut expires = None;
        let mut time_stamp = None;

        let mut expires_value = None;

        while let Some(line) = json_first_line_reader.get_next() {
            let (name_ref, value_ref) = line?;

            let name = name_ref.as_unescaped_str()?;
            match name {
                super::consts::PARTITION_KEY => {
                    if value_ref.as_str().is_none() {
                        return Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull);
                    }

                    partition_key =
                        Some(JsonKeyValuePosition::new(&name_ref.data, &value_ref.data));
                }

                super::consts::ROW_KEY => {
                    if value_ref.as_str().is_none() {
                        return Err(DbEntityParseFail::FieldRowKeyCanNotBeNull);
                    }
                    row_key = Some(JsonKeyValuePosition::new(&name_ref.data, &value_ref.data));
                }
                super::consts::EXPIRES => {
                    expires_value = value_ref.as_date_time();
                    expires = Some(JsonKeyValuePosition::new(&name_ref.data, &value_ref.data));
                }
                super::consts::TIME_STAMP => {
                    time_stamp = Some(JsonKeyValuePosition::new(&name_ref.data, &value_ref.data));
                }
                _ => {
                    if rust_extensions::str_utils::compare_strings_case_insensitive(
                        name,
                        super::consts::TIME_STAMP_LOWER_CASE,
                    ) {
                        time_stamp =
                            Some(JsonKeyValuePosition::new(&name_ref.data, &value_ref.data));
                    }
                }
            }
        }

        if partition_key.is_none() {
            return Err(DbEntityParseFail::FieldPartitionKeyIsRequired);
        }

        if row_key.is_none() {
            return Err(DbEntityParseFail::FieldRowKeyIsRequired);
        }

        let raw = json_first_line_reader.as_slice();

        let partition_key = partition_key.unwrap();
        let row_key = row_key.unwrap();

        let result = Self {
            partition_key_unescaped: partition_key.value.unescape_str_value(raw),
            row_key_unescaped: row_key.value.unescape_str_value(raw),
            partition_key,
            row_key,
            expires,
            time_stamp,
            expires_value,
        };

        Ok(result)
    }

    pub fn parse<'s>(
        raw: &'s [u8],
        time_stamp_to_inject: &'s JsonTimeStamp,
    ) -> Result<DbJsonEntityWithContent<'s>, DbEntityParseFail> {
        let entity = Self::new(JsonFirstLineIterator::new(raw))?;

        return Ok(DbJsonEntityWithContent::new(
            raw,
            time_stamp_to_inject,
            entity,
        ));
    }

    pub fn parse_into_db_row(
        json_first_line_reader: JsonFirstLineIterator,
        now: &JsonTimeStamp,
    ) -> Result<DbRow, DbEntityParseFail> {
        let mut partition_key = None;
        let mut row_key = None;
        let mut expires = None;
        let mut time_stamp = None;
        let mut expires_value = None;

        let mut raw = DbRowContentCompiler::new(json_first_line_reader.as_slice().len());

        while let Some(line) = json_first_line_reader.get_next() {
            let (name_ref, value_ref) = line?;

            let name = name_ref.as_unescaped_str().unwrap();
            match name {
                super::consts::PARTITION_KEY => {
                    partition_key = Some(raw.append(&name_ref, &value_ref));
                }

                super::consts::ROW_KEY => {
                    row_key = Some(raw.append(&name_ref, &value_ref));
                    time_stamp = raw
                        .append_str_value(super::consts::TIME_STAMP, now.as_str())
                        .into();
                }
                super::consts::EXPIRES => {
                    expires_value = value_ref.as_date_time();
                    expires = Some(raw.append(&name_ref, &value_ref));
                }
                super::consts::TIME_STAMP => {}
                _ => {
                    if rust_extensions::str_utils::compare_strings_case_insensitive(
                        name,
                        super::consts::TIME_STAMP_LOWER_CASE,
                    ) {
                    } else {
                        raw.append(&name_ref, &value_ref);
                    }
                }
            }
        }

        let content = raw.into_vec();

        if partition_key.is_none() {
            return Err(DbEntityParseFail::FieldPartitionKeyIsRequired);
        }

        let partition_key = partition_key.unwrap();

        if partition_key.value.is_null(content.as_slice()) {
            return Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull);
        }

        if row_key.is_none() {
            return Err(DbEntityParseFail::FieldRowKeyIsRequired);
        }

        let row_key = row_key.unwrap();

        if row_key.value.is_null(content.as_slice()) {
            return Err(DbEntityParseFail::FieldRowKeyCanNotBeNull);
        }

        let partition_key_unescaped = partition_key.value.unescape_str_value(content.as_slice());

        // The limit is on the key, not on the json spelling of it: `д` is one character of
        // a key and six of the payload.
        let partition_key_len = match partition_key_unescaped.as_ref() {
            Some(partition_key) => partition_key.len(),
            None => partition_key.value.get_str_value(content.as_slice()).len(),
        };

        if partition_key_len > super::consts::MAX_PARTITION_KEY_LEN {
            return Err(DbEntityParseFail::PartitionKeyIsTooLong);
        }

        let db_json_entity = Self {
            partition_key_unescaped,
            row_key_unescaped: row_key.value.unescape_str_value(content.as_slice()),
            partition_key,
            row_key,
            expires,
            time_stamp,
            expires_value,
        };

        let result = DbRow::new(db_json_entity, content);

        Ok(result)
    }

    /// Same as [`Self::parse_into_db_row`], but the entity's own `TimeStamp`
    /// (case-insensitive) is kept instead of being overwritten by the server clock.
    ///
    /// The timestamp is injected in the same position as `parse_into_db_row` (right
    /// after `RowKey`), so the resulting `raw` layout is unchanged. Read the value back
    /// with [`crate::db::DbRow::get_time_stamp_as_date_time`].
    ///
    /// Unlike `parse_into_db_row`, the client's timestamp is mandatory here: if the
    /// entity has no `TimeStamp`, or its value does not parse as an ISO date-time, this
    /// returns [`DbEntityParseFail::FieldTimeStampIsRequired`] naming that entity's
    /// partition/row key. Server-`now` substitution is `parse_into_db_row`'s job, not
    /// this one's.
    pub fn parse_into_db_row_and_keep_date_time(
        json_first_line_reader: JsonFirstLineIterator,
    ) -> Result<DbRow, DbEntityParseFail> {
        // Pre-pass over the same slice to read the entity's own TimeStamp.
        let time_stamp = {
            let slice = json_first_line_reader.as_slice();
            let entity = Self::new(JsonFirstLineIterator::new(slice))?;

            match entity.get_time_stamp(slice) {
                Some(value) if DateTimeAsMicroseconds::parse_iso_string(value).is_some() => {
                    JsonTimeStamp::parse_or_now(value)
                }
                _ => {
                    return Err(DbEntityParseFail::FieldTimeStampIsRequired {
                        partition_key: entity.get_partition_key(slice).to_string(),
                        row_key: entity.get_row_key(slice).to_string(),
                    });
                }
            }
        };

        Self::parse_into_db_row(json_first_line_reader, &time_stamp)
    }

    /// Same as [`Self::parse_grouped_by_partition_key`], but each row keeps its own
    /// `TimeStamp` (see [`Self::parse_into_db_row_and_keep_date_time`]). Iterates the
    /// array in document order and fails on the first entity whose `TimeStamp` is
    /// missing or unparseable, carrying that entity's partition/row key.
    pub fn parse_grouped_by_partition_key_and_keep_date_time(
        src: &[u8],
    ) -> Result<Vec<(String, Vec<Arc<DbRow>>)>, DbEntityParseFail> {
        let mut result = Vec::new();

        let json_array_iterator = JsonArrayIterator::new(src)?;

        while let Some(json) = json_array_iterator.get_next() {
            let json = json?;
            let db_row = DbJsonEntity::parse_into_db_row_and_keep_date_time(
                json.unwrap_as_object().unwrap(),
            )?;

            let partition_key = db_row.get_partition_key();

            match result.binary_search_by(|itm: &(String, Vec<Arc<DbRow>>)| {
                itm.0.as_str().cmp(partition_key)
            }) {
                Ok(index) => {
                    result[index].1.push(Arc::new(db_row));
                }
                Err(index) => {
                    result.insert(index, (partition_key.to_string(), vec![Arc::new(db_row)]));
                }
            }
        }

        Ok(result)
    }

    /// The logical PartitionKey - JSON escapes are already resolved, so this is the value a
    /// point request (`?partitionKey=...`) addresses the row by.
    pub fn get_partition_key<'s>(&'s self, raw: &'s [u8]) -> &'s str {
        match self.partition_key_unescaped.as_ref() {
            Some(partition_key) => partition_key,
            None => self.partition_key.value.get_str_value(raw),
        }
    }

    /// The logical RowKey - see [`Self::get_partition_key`].
    pub fn get_row_key<'s>(&'s self, raw: &'s [u8]) -> &'s str {
        match self.row_key_unescaped.as_ref() {
            Some(row_key) => row_key,
            None => self.row_key.value.get_str_value(raw),
        }
    }

    /// The PartitionKey as a [`JsonStrValue`] - for a caller which only needs to answer a
    /// question about it (`eq_with_str` / `cmp_with_str`) and not to build it.
    pub fn partition_key_value<'s>(&'s self, raw: &'s [u8]) -> JsonStrValue<'s> {
        match self.partition_key_unescaped.as_ref() {
            Some(partition_key) => JsonStrValue::Unescaped(partition_key),
            None => self.partition_key.value.get_json_value(raw),
        }
    }

    /// The RowKey as a [`JsonStrValue`] - see [`Self::partition_key_value`].
    pub fn row_key_value<'s>(&'s self, raw: &'s [u8]) -> JsonStrValue<'s> {
        match self.row_key_unescaped.as_ref() {
            Some(row_key) => JsonStrValue::Unescaped(row_key),
            None => self.row_key.value.get_json_value(raw),
        }
    }

    pub fn get_expires<'s>(&self, raw: &'s [u8]) -> Option<&'s str> {
        if let Some(expires) = &self.expires {
            return Some(expires.value.get_str_value(raw));
        }

        None
    }

    pub fn get_time_stamp<'s>(&self, raw: &'s [u8]) -> Option<&'s str> {
        if let Some(time_stamp) = &self.time_stamp {
            return Some(time_stamp.value.get_str_value(raw));
        }
        None
    }

    pub fn restore_into_db_row(raw: Vec<u8>) -> Result<DbRow, DbEntityParseFail> {
        let json_first_line_reader = JsonFirstLineIterator::new(raw.as_slice());
        let db_row = Self::new(json_first_line_reader)?;
        let result = DbRow::new(db_row, raw);
        Ok(result)
    }

    pub fn parse_as_vec(
        src: &[u8],
        inject_time_stamp: &JsonTimeStamp,
    ) -> Result<Vec<Arc<DbRow>>, DbEntityParseFail> {
        let mut result = Vec::new();

        let json_array_iterator = JsonArrayIterator::new(src)?;

        while let Some(json) = json_array_iterator.get_next() {
            let json = json?;
            let db_row = DbJsonEntity::parse_into_db_row(
                json.unwrap_as_object().unwrap(),
                inject_time_stamp,
            )?;
            result.push(Arc::new(db_row));
        }
        return Ok(result);
    }

    pub fn restore_as_vec(src: &[u8]) -> Result<Vec<Arc<DbRow>>, DbEntityParseFail> {
        let mut result = Vec::new();

        let json_array_iterator = JsonArrayIterator::new(src)?;

        while let Some(json) = json_array_iterator.get_next() {
            let json = json?;
            let db_entity = DbJsonEntity::restore_into_db_row(json.as_bytes().to_vec())?;
            result.push(Arc::new(db_entity));
        }
        return Ok(result);
    }

    pub fn parse_grouped_by_partition_key<'s>(
        src: &'s [u8],
        inject_time_stamp: &JsonTimeStamp,
    ) -> Result<Vec<(String, Vec<Arc<DbRow>>)>, DbEntityParseFail> {
        let mut result = Vec::new();

        let json_array_iterator = JsonArrayIterator::new(src)?;

        while let Some(json) = json_array_iterator.get_next() {
            let json = json?;
            let db_row = DbJsonEntity::parse_into_db_row(
                json.unwrap_as_object().unwrap(),
                inject_time_stamp,
            )?;

            let partition_key = db_row.get_partition_key();

            match result.binary_search_by(|itm: &(String, Vec<Arc<DbRow>>)| {
                itm.0.as_str().cmp(partition_key)
            }) {
                Ok(index) => {
                    result[index].1.push(Arc::new(db_row));
                }
                Err(index) => {
                    result.insert(index, (partition_key.to_string(), vec![Arc::new(db_row)]));
                }
            }
        }

        Ok(result)
    }

    pub fn restore_grouped_by_partition_key(
        src: &[u8],
    ) -> Result<Vec<(String, Vec<Arc<DbRow>>)>, DbEntityParseFail> {
        let mut result = Vec::new();

        let json_array_iterator = JsonArrayIterator::new(src)?;

        while let Some(json) = json_array_iterator.get_next() {
            let json = json?;
            let db_row = DbJsonEntity::restore_into_db_row(json.as_bytes().to_vec())?;

            let partition_key = db_row.get_partition_key();

            match result.binary_search_by(|itm: &(String, Vec<Arc<DbRow>>)| {
                itm.0.as_str().cmp(partition_key)
            }) {
                Ok(index) => {
                    result[index].1.push(Arc::new(db_row));
                }
                Err(index) => {
                    result.insert(index, (partition_key.to_string(), vec![Arc::new(db_row)]));
                }
            }
        }

        return Ok(result);
    }

    pub fn replace_timestamp_value(&mut self, raw: &mut Vec<u8>, json_time_stamp: &JsonTimeStamp) {
        let timestamp_value = format!("{dq}{val}{dq}", dq = '"', val = json_time_stamp.as_str());

        let timestamp_value = timestamp_value.as_bytes();

        let ts_as_bytes = super::consts::TIME_STAMP.as_bytes();

        let time_stamp_position = self.time_stamp.as_ref().unwrap();

        for i in 0..ts_as_bytes.len() {
            raw[time_stamp_position.key.start + 1 + i] = ts_as_bytes[i];
        }

        let content_timestamp_len = time_stamp_position.value.len();

        if content_timestamp_len < timestamp_value.len() {
            replace_timestamp(raw, time_stamp_position, json_time_stamp);
            return;
        }

        let mut no = 0;
        for i in time_stamp_position.value.start..time_stamp_position.value.end {
            if no < timestamp_value.len() {
                raw[i] = timestamp_value[no];
            } else {
                raw[i] = b' ';
            }

            no += 1;
        }
    }

    pub fn inject_at_the_end_of_json(&mut self, raw: &mut Vec<u8>, time_stamp: &JsonTimeStamp) {
        let end_of_json = get_the_end_of_the_json(raw);

        raw.truncate(end_of_json);

        raw.push(b',');
        self.time_stamp = inject_time_stamp_key_value(raw, time_stamp).into();
        raw.push(b'}');
    }
}

fn replace_timestamp(
    raw: &mut Vec<u8>,
    time_stamp_position: &JsonKeyValuePosition,
    time_stamp: &JsonTimeStamp,
) {
    let temp_buffer_len = raw.len() - time_stamp_position.value.end;
    let mut temp_buffer = Vec::with_capacity(temp_buffer_len);

    temp_buffer.extend_from_slice(raw.as_slice()[time_stamp_position.value.end..].as_ref());

    raw.truncate(time_stamp_position.key.start);

    inject_time_stamp_key_value(raw, time_stamp);

    raw.extend_from_slice(temp_buffer.as_slice());
}

fn inject_time_stamp_key_value(
    raw: &mut Vec<u8>,
    time_stamp: &JsonTimeStamp,
) -> JsonKeyValuePosition {
    let mut key = KeyValueContentPosition {
        start: raw.len(),
        end: 0,
    };

    raw.push(b'"');
    raw.extend_from_slice(super::consts::TIME_STAMP.as_bytes());
    raw.push(b'"');

    key.end = raw.len();

    raw.push(b':');

    let mut value = KeyValueContentPosition {
        start: raw.len(),
        end: 0,
    };

    raw.push(b'"');
    raw.extend_from_slice(time_stamp.as_slice());
    raw.push(b'"');

    value.end = raw.len();

    JsonKeyValuePosition { key, value }
}

pub fn get_the_end_of_the_json(data: &[u8]) -> usize {
    for i in (0..data.len()).rev() {
        if data[i] == my_json::consts::CLOSE_BRACKET {
            return i;
        }
    }

    panic!("Invalid Json. Can not find the end of json");
}

#[cfg(test)]
mod tests {

    use my_json::json_reader::{AsJsonSlice, JsonFirstLineIterator};
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::db_json_entity::{DbEntityParseFail, JsonTimeStamp};

    use super::DbJsonEntity;

    #[test]
    pub fn test_partition_key_and_row_key_and_time_stamp_are_ok() {
        let src_json = r#"{"TwoFaMethods": {},
        "PartitionKey": "ff95cdae9f7e4f1a847f6b83ad68b495",
        "RowKey": "6c09c7f0e44d4ef79cfdd4252ebd54ab",
        "TimeStamp": "2022-03-17T09:28:27.5923",
        "Expires": "2022-03-17T13:28:29.6537478Z"
      }"#;

        let json_first_line_reader = JsonFirstLineIterator::new(src_json.as_bytes());

        let json_time = JsonTimeStamp::now();

        let entity = DbJsonEntity::parse_into_db_row(json_first_line_reader, &json_time).unwrap();

        let json_first_line_reader: JsonFirstLineIterator = entity.get_src_as_slice().into();

        let dest_entity =
            DbJsonEntity::parse_into_db_row(json_first_line_reader, &json_time).unwrap();

        assert_eq!(
            "ff95cdae9f7e4f1a847f6b83ad68b495",
            dest_entity.get_partition_key()
        );

        assert_eq!(
            "6c09c7f0e44d4ef79cfdd4252ebd54ab",
            dest_entity.get_row_key()
        );
    }

    #[test]
    pub fn parse_expires_with_z() {
        let src_json = r#"{"TwoFaMethods": {},
            "PartitionKey": "ff95cdae9f7e4f1a847f6b83ad68b495",
            "RowKey": "6c09c7f0e44d4ef79cfdd4252ebd54ab",
            "TimeStamp": "2022-03-17T09:28:27.5923",
            "Expires": "2022-03-17T13:28:29.6537478Z"
          }"#;

        let json_first_line_reader = JsonFirstLineIterator::new(src_json.as_bytes());

        let entity = DbJsonEntity::new(json_first_line_reader).unwrap();

        let expires = entity.expires_value.as_ref().unwrap();

        assert_eq!("2022-03-17T13:28:29.653747", &expires.to_rfc3339()[..26]);

        let expires_value_position = entity.expires.unwrap();

        let expires_key =
            &src_json.as_bytes()[expires_value_position.key.start..expires_value_position.key.end];

        assert_eq!("\"Expires\"", std::str::from_utf8(expires_key).unwrap());

        let expires_value = &src_json.as_bytes()
            [expires_value_position.value.start..expires_value_position.value.end];

        assert_eq!(
            "\"2022-03-17T13:28:29.6537478Z\"",
            std::str::from_utf8(expires_value).unwrap()
        );
    }

    fn parse_with_partition_key_of(
        partition_key: &str,
    ) -> Result<crate::db::DbRow, DbEntityParseFail> {
        let json = format!(r#"{{"PartitionKey":"{}","RowKey":"Rk"}}"#, partition_key);

        DbJsonEntity::parse_into_db_row(json.as_bytes().into(), &JsonTimeStamp::now())
    }

    fn assert_partition_key_is_too_long(partition_key: &str) {
        match parse_with_partition_key_of(partition_key) {
            Err(DbEntityParseFail::PartitionKeyIsTooLong) => {}
            Err(err) => panic!("Expected PartitionKeyIsTooLong, got {:?}", err),
            Ok(_) => panic!("Expected PartitionKeyIsTooLong, the row was accepted"),
        }
    }

    #[test]
    fn partition_key_at_the_limit_is_accepted() {
        let partition_key = "p".repeat(super::super::consts::MAX_PARTITION_KEY_LEN);

        let db_row = parse_with_partition_key_of(partition_key.as_str()).unwrap();

        assert_eq!(db_row.get_partition_key(), partition_key);
    }

    #[test]
    fn partition_key_over_the_limit_is_rejected() {
        assert_partition_key_is_too_long(
            "p".repeat(super::super::consts::MAX_PARTITION_KEY_LEN + 1)
                .as_str(),
        );
    }

    /// The limit counts the key, not its json spelling: `д` is six characters of payload
    /// and two bytes of key.
    #[test]
    fn escaped_partition_key_is_measured_after_unescaping() {
        // 300 characters of json, 100 bytes of key - accepted
        let db_row = parse_with_partition_key_of("\\u0434".repeat(50).as_str()).unwrap();
        assert_eq!(db_row.get_partition_key(), "д".repeat(50));

        // ...and a key which really is too long stays rejected, however it is spelled
        assert_partition_key_is_too_long("\\u0434".repeat(200).as_str());
    }

    #[test]
    pub fn parse_with_partition_key_is_null() {
        let src_json = r#"{"TwoFaMethods": {},
            "PartitionKey": null,
            "RowKey": "test",
            "TimeStamp": "2022-03-17T09:28:27.5923",
            "Expires": "2022-03-17T13:28:29.6537478Z"
          }"#;

        let json_first_line_reader = JsonFirstLineIterator::new(src_json.as_bytes());

        let result = DbJsonEntity::new(json_first_line_reader);

        if let Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull) = result {
        } else {
            panic!("Should not be here")
        }
    }
    #[test]
    pub fn parse_some_case_from_real_life() {
        let src_json = r#"{"value":{"is_enabled":true,"fee_percent":5.0,"min_balance_usd":100.0,"fee_period_days":30,"inactivity_period_days":90},"PartitionKey":"*","RowKey":"*"}"#;

        let time_stamp = JsonTimeStamp::now();

        let json_first_line_reader = JsonFirstLineIterator::new(src_json.as_bytes());
        let db_row = DbJsonEntity::parse_into_db_row(json_first_line_reader, &time_stamp).unwrap();

        println!(
            "{:?}",
            std::str::from_utf8(db_row.get_src_as_slice()).unwrap()
        );
    }

    #[test]
    fn test_timestamp_injection_at_the_end_of_json() {
        let json_ts = JsonTimeStamp::from_date_time(
            DateTimeAsMicroseconds::parse_iso_string("2022-01-01T12:01:02.123456").unwrap(),
        );

        let mut json = r#"{"PartitionKey":"PK", "RowKey":"RK"}     "#.as_bytes().to_vec();

        let json_first_line_reader = JsonFirstLineIterator::new(json.as_slice());

        let mut db_json_entity = DbJsonEntity::new(json_first_line_reader).unwrap();

        db_json_entity.inject_at_the_end_of_json(&mut json, &json_ts);

        assert_eq!(db_json_entity.get_partition_key(&json), "PK");
        assert_eq!(db_json_entity.get_row_key(&json), "RK");

        assert_eq!(
            db_json_entity.get_time_stamp(&json).unwrap(),
            json_ts.as_str()
        );

        assert_eq!(
            std::str::from_utf8(json.as_slice()).unwrap(),
            format!(
                r#"{{"PartitionKey":"PK", "RowKey":"RK","TimeStamp":"{}"}}"#,
                json_ts.as_str()
            )
        );
    }

    #[test]
    fn test_replace_null_to_timestamp_and_change_timestamp_which_has_less_size() {
        let json_ts = JsonTimeStamp::from_date_time(
            DateTimeAsMicroseconds::parse_iso_string("2022-01-01T12:01:02.123456").unwrap(),
        );

        let json = r#"{"PartitionKey":"Pk", "RowKey":"Rk", "timestamp":null}"#;

        let json_first_line_reader = JsonFirstLineIterator::new(json.as_slice());

        let db_row = DbJsonEntity::parse_into_db_row(json_first_line_reader, &json_ts).unwrap();

        assert_eq!(db_row.get_partition_key(), "Pk",);
        assert_eq!(db_row.get_row_key(), "Rk",);
    }

    #[test]
    fn test_replace_null_to_timestamp_and_change_timestamp_which_has_bigger_size() {
        let json_ts = JsonTimeStamp::from_date_time(
            DateTimeAsMicroseconds::parse_iso_string("2022-01-01T12:01:02.123456").unwrap(),
        );

        let json = r#"{"PartitionKey":"Pk", "RowKey":"Rk", "timestamp":"12345678901234567890123456789012345678901234567890"}"#;

        let json_first_line_reader = JsonFirstLineIterator::new(json.as_bytes());

        let db_json_entity =
            DbJsonEntity::parse_into_db_row(json_first_line_reader, &json_ts).unwrap();

        assert_eq!(db_json_entity.get_partition_key(), "Pk",);
        assert_eq!(db_json_entity.get_row_key(), "Rk",);

        assert_eq!(db_json_entity.get_row_key(), "Rk",);
    }

    #[test]
    fn test_we_have_timestamp_before_partition_key() {
        let test_json = r#"{
            "Timestamp":"",
            "PartitionKey": "Pk",
            "Expires": "2019-01-01T00:00:00",
            "RowKey": "Rk"}"#;

        let inject_time_stamp = JsonTimeStamp::now();

        let json_first_line_reader = JsonFirstLineIterator::new(test_json.as_bytes());

        let db_row =
            DbJsonEntity::parse_into_db_row(json_first_line_reader, &inject_time_stamp).unwrap();

        assert_eq!(db_row.get_partition_key(), "Pk");
        assert_eq!(db_row.get_row_key(), "Rk");

        #[cfg(feature = "master-node")]
        assert_eq!(
            db_row.get_expires().unwrap().unix_microseconds,
            DateTimeAsMicroseconds::from_str("2019-01-01T00:00:00")
                .unwrap()
                .unix_microseconds
        );
    }

    #[test]
    fn keep_date_time_timestamp_before_row_key() {
        let json = r#"{"TimeStamp":"2020-05-06T07:08:09","PartitionKey":"Pk","RowKey":"Rk"}"#;

        let db_row =
            DbJsonEntity::parse_into_db_row_and_keep_date_time(json.as_bytes().into()).unwrap();

        assert_eq!(db_row.get_partition_key(), "Pk");
        assert_eq!(db_row.get_row_key(), "Rk");

        // The injected raw must carry the entity's own timestamp, in canonical form.
        let reparsed = DbJsonEntity::new(db_row.get_src_as_slice().into()).unwrap();
        assert_eq!(
            reparsed.get_time_stamp(db_row.get_src_as_slice()).unwrap(),
            "2020-05-06T07:08:09"
        );

        #[cfg(feature = "master-node")]
        {
            let expected = DateTimeAsMicroseconds::parse_iso_string("2020-05-06T07:08:09").unwrap();
            assert_eq!(
                db_row.get_time_stamp_as_date_time().unix_microseconds,
                expected.unix_microseconds
            );
        }
    }

    #[test]
    fn keep_date_time_timestamp_after_row_key() {
        let json = r#"{"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":"2020-05-06T07:08:09"}"#;

        let db_row =
            DbJsonEntity::parse_into_db_row_and_keep_date_time(json.as_bytes().into()).unwrap();

        assert_eq!(db_row.get_partition_key(), "Pk");
        assert_eq!(db_row.get_row_key(), "Rk");

        let reparsed = DbJsonEntity::new(db_row.get_src_as_slice().into()).unwrap();
        assert_eq!(
            reparsed.get_time_stamp(db_row.get_src_as_slice()).unwrap(),
            "2020-05-06T07:08:09"
        );
    }

    #[test]
    fn keep_date_time_lower_case_timestamp() {
        let json = r#"{"PartitionKey":"Pk","RowKey":"Rk","timestamp":"2020-05-06T07:08:09"}"#;

        let db_row =
            DbJsonEntity::parse_into_db_row_and_keep_date_time(json.as_bytes().into()).unwrap();

        let reparsed = DbJsonEntity::new(db_row.get_src_as_slice().into()).unwrap();
        assert_eq!(
            reparsed.get_time_stamp(db_row.get_src_as_slice()).unwrap(),
            "2020-05-06T07:08:09"
        );
    }

    /// The client's version is what `InsertOrReplaceIfNew` compares, so the injected
    /// value has to keep every microsecond of it - it used to be cut to 4 digits.
    #[test]
    fn keep_date_time_preserves_microseconds() {
        for (src, expected) in [
            ("2020-05-06T07:08:09.540412", "2020-05-06T07:08:09.540412"),
            ("2020-05-06T07:08:09.999999", "2020-05-06T07:08:09.999999"),
            ("2020-05-06T07:08:09.540400", "2020-05-06T07:08:09.5404"),
            ("2020-05-06T07:08:09.5404", "2020-05-06T07:08:09.5404"),
            ("2020-05-06T07:08:09.540412Z", "2020-05-06T07:08:09.540412"),
        ] {
            let json = format!(
                r#"{{"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":"{}"}}"#,
                src
            );

            let db_row =
                DbJsonEntity::parse_into_db_row_and_keep_date_time(json.as_bytes().into()).unwrap();

            let reparsed = DbJsonEntity::new(db_row.get_src_as_slice().into()).unwrap();

            assert_eq!(
                expected,
                reparsed.get_time_stamp(db_row.get_src_as_slice()).unwrap(),
                "source: {}",
                src
            );

            #[cfg(feature = "master-node")]
            assert_eq!(
                DateTimeAsMicroseconds::from_str(src)
                    .unwrap()
                    .unix_microseconds,
                db_row.get_time_stamp_as_date_time().unix_microseconds,
                "source: {}",
                src
            );
        }
    }

    #[test]
    fn keep_date_time_no_timestamp_field_is_error() {
        let json = r#"{"PartitionKey":"Pk","RowKey":"Rk"}"#;

        let result = DbJsonEntity::parse_into_db_row_and_keep_date_time(json.as_bytes().into());

        match result {
            Err(DbEntityParseFail::FieldTimeStampIsRequired {
                partition_key,
                row_key,
            }) => {
                assert_eq!(partition_key, "Pk");
                assert_eq!(row_key, "Rk");
            }
            _ => panic!("Expected FieldTimeStampIsRequired"),
        }
    }

    #[test]
    fn keep_date_time_garbage_timestamp_is_error() {
        let json = r#"{"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":"not-a-date"}"#;

        let result = DbJsonEntity::parse_into_db_row_and_keep_date_time(json.as_bytes().into());

        match result {
            Err(DbEntityParseFail::FieldTimeStampIsRequired {
                partition_key,
                row_key,
            }) => {
                assert_eq!(partition_key, "Pk");
                assert_eq!(row_key, "Rk");
            }
            _ => panic!("Expected FieldTimeStampIsRequired"),
        }
    }

    #[test]
    fn keep_date_time_grouped_fails_on_second_entity_without_timestamp() {
        let json = r#"[
            {"PartitionKey":"Pk1","RowKey":"Rk1","TimeStamp":"2020-05-06T07:08:09"},
            {"PartitionKey":"Pk2","RowKey":"Rk2"}
        ]"#;

        let result =
            DbJsonEntity::parse_grouped_by_partition_key_and_keep_date_time(json.as_bytes());

        match result {
            Err(DbEntityParseFail::FieldTimeStampIsRequired {
                partition_key,
                row_key,
            }) => {
                assert_eq!(partition_key, "Pk2");
                assert_eq!(row_key, "Rk2");
            }
            _ => panic!("Expected FieldTimeStampIsRequired"),
        }
    }

    #[cfg(feature = "master-node")]
    #[test]
    fn keep_date_time_round_trips_after_compression() {
        use crate::db::DbRow;

        let json = r#"{"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":"2020-05-06T07:08:09"}"#;

        let db_row =
            DbJsonEntity::parse_into_db_row_and_keep_date_time(json.as_bytes().into()).unwrap();

        let expected = DateTimeAsMicroseconds::parse_iso_string("2020-05-06T07:08:09").unwrap();

        let compressed = DbRow::compress_arc(std::sync::Arc::new(db_row));
        assert!(compressed.is_compressed());

        assert_eq!(
            compressed.get_time_stamp_as_date_time().unix_microseconds,
            expected.unix_microseconds
        );
    }
}
