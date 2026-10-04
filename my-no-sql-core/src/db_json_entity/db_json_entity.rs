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
use my_json::json_reader::JsonParseError;

pub struct DbJsonEntity {
    pub partition_key: JsonKeyValuePosition,
    pub row_key: JsonKeyValuePosition,
    pub time_stamp: Option<JsonKeyValuePosition>,
    pub expires: Option<JsonKeyValuePosition>,
    pub expires_value: Option<DateTimeAsMicroseconds>,
    /// The logical PartitionKey - filled in only when the raw JSON carries escape sequences
    /// (`"a\\b"`, `"\u0434"`, ...) and the value between the quotes therefore is not the key
    /// itself. `None` - the raw slice is the key and is borrowed as is.
    ///
    /// Resolved once, here, so that every consumer (this entity's accessors, [`DbRow`], the
    /// readers' index) addresses a row by the same, logical, key.
    pub(crate) partition_key_unescaped: Option<Box<str>>,
    /// The logical RowKey - see [`Self::partition_key_unescaped`].
    pub(crate) row_key_unescaped: Option<Box<str>>,
}

impl DbJsonEntity {
    /// [`Self::new`] over a slice.
    pub fn from_slice(src: &[u8]) -> Result<Self, DbEntityParseFail> {
        Self::new(JsonFirstLineIterator::new(src))
    }

    /// Reads an entity which already is a row somewhere: one the server sends to a reader, one
    /// which is loaded from a disk or from another node (the `restore_*` functions go through
    /// here). So it asks of the keys only that they can be read, and a row which loaded
    /// yesterday loads today: a key which is not a json string - it could be written before
    /// keys were checked - is still read the way such a row was stored, as the value with its
    /// first and last character cut off (`123` is the key `2`). Only what can not be cut that
    /// way - a value of a single character, bytes which are not utf-8 - is refused.
    ///
    /// An entity a client writes is read by [`Self::parse`] and by
    /// [`Self::parse_into_db_row`], which take a json string for a key and nothing else.
    ///
    /// Of several fields with the same name the last one counts, as it does in
    /// [`Self::parse_into_db_row`]: what was accepted when it was written is read back the
    /// same way.
    ///
    /// The keys are looked at once the whole entity has been read. So an entity with more
    /// than one thing wrong with it may be refused for another reason than it used to be,
    /// when a `null` key was refused the moment it was met: a json error behind a `null` key
    /// is the json error, of two `null` keys the PartitionKey is the one which is named, and
    /// a key which is not utf-8 is `.. must be a json string` - it used to be taken for a
    /// `null`.
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
                    partition_key =
                        Some(JsonKeyValuePosition::new(&name_ref.data, &value_ref.data));
                }

                super::consts::ROW_KEY => {
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

        let raw = json_first_line_reader.as_slice();

        // A `null` is refused in front of a key which is missing - the order the two refusals
        // have always had for an entity with one thing wrong with it.
        if let Some(partition_key) = partition_key.as_ref() {
            if partition_key.value.is_null(raw) {
                return Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull);
            }
        }

        if let Some(row_key) = row_key.as_ref() {
            if row_key.value.is_null(raw) {
                return Err(DbEntityParseFail::FieldRowKeyCanNotBeNull);
            }
        }

        if partition_key.is_none() {
            return Err(DbEntityParseFail::FieldPartitionKeyIsRequired);
        }

        if row_key.is_none() {
            return Err(DbEntityParseFail::FieldRowKeyIsRequired);
        }

        let partition_key = partition_key.unwrap();
        let row_key = row_key.unwrap();

        check_stored_key(super::consts::PARTITION_KEY, &partition_key.value, raw)?;
        check_stored_key(super::consts::ROW_KEY, &row_key.value, raw)?;

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

    /// [`Self::new`] for an entity a client writes: its keys have to be json strings - see
    /// [`check_key_is_a_string`].
    fn new_to_write(raw: &[u8]) -> Result<Self, DbEntityParseFail> {
        let entity = Self::new(JsonFirstLineIterator::new(raw))?;

        check_key_is_a_string(super::consts::PARTITION_KEY, &entity.partition_key.value, raw)?;
        check_key_is_a_string(super::consts::ROW_KEY, &entity.row_key.value, raw)?;

        Ok(entity)
    }

    pub fn parse<'s>(
        raw: &'s [u8],
        time_stamp_to_inject: &'s JsonTimeStamp,
    ) -> Result<DbJsonEntityWithContent<'s>, DbEntityParseFail> {
        let entity = Self::new_to_write(raw)?;

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

            let name = name_ref.as_unescaped_str()?;
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

        check_key_is_a_string(
            super::consts::PARTITION_KEY,
            &partition_key.value,
            content.as_slice(),
        )?;

        if row_key.is_none() {
            return Err(DbEntityParseFail::FieldRowKeyIsRequired);
        }

        let row_key = row_key.unwrap();

        if row_key.value.is_null(content.as_slice()) {
            return Err(DbEntityParseFail::FieldRowKeyCanNotBeNull);
        }

        check_key_is_a_string(super::consts::ROW_KEY, &row_key.value, content.as_slice())?;

        // The row is handed out as a `String` - `DbRow::write_json` does not check it again -
        // and its readers parse it as utf-8. So an entity a client writes has to be utf-8 as a
        // whole, not in its keys only. A row which already is a row is not asked: see `new`.
        if std::str::from_utf8(content.as_slice()).is_err() {
            return Err(entity_is_not_utf8());
        }

        let partition_key_unescaped = partition_key.value.unescape_str_value(content.as_slice());

        // The limit is on the key, not on the json spelling of it: `\u0434` is one character of
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
    /// after `RowKey`), so the resulting `raw` layout is unchanged. With the `master-node`
    /// feature the value is read back with `DbRow::get_time_stamp_as_date_time`.
    ///
    /// Unlike `parse_into_db_row`, the client's timestamp is mandatory here: if the
    /// entity has no `TimeStamp`, or its value is not a json string which parses as an ISO
    /// date-time (`DateTimeAsMicroseconds::parse_iso_string`), this returns
    /// [`DbEntityParseFail::FieldTimeStampIsRequired`] naming that entity's partition/row
    /// key. The value is parsed once, and the moment that check reads is the one the row is
    /// stamped with - so whatever passes the check is what is stored, in the canonical
    /// spelling: `2020-05-06T07:08Z` is kept as `2020-05-06T07:08:00`. Server-`now`
    /// substitution is `parse_into_db_row`'s job, never this one's.
    pub fn parse_into_db_row_and_keep_date_time(
        json_first_line_reader: JsonFirstLineIterator,
    ) -> Result<DbRow, DbEntityParseFail> {
        // Pre-pass over the same slice to read the entity's own TimeStamp.
        let time_stamp = {
            let slice = json_first_line_reader.as_slice();
            let entity = Self::new_to_write(slice)?;

            // The value is read once: the stamp is built from the very moment the check
            // accepts. A second reading of the text, by another parser, could disagree with
            // the first one - and what it failed to read would turn into the server clock.
            match entity.read_time_stamp_as_date_time(slice) {
                Some(date_time) => JsonTimeStamp::from_date_time(date_time),
                None => {
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
            let db_row =
                DbJsonEntity::parse_into_db_row_and_keep_date_time(json.unwrap_as_object()?)?;

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
    /// question about it (`eq_with_str`) and not to build it.
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

    /// The `Expires` as the entity spells it. `None` - there is no such field, or its value is
    /// not a json string: a `null` and a number have no quotes to read it between.
    pub fn get_expires<'s>(&self, raw: &'s [u8]) -> Option<&'s str> {
        if let Some(expires) = &self.expires {
            return expires.value.try_get_str_value(raw);
        }

        None
    }

    /// The `TimeStamp` as the entity spells it - see [`Self::get_expires`]. A `TimeStamp`
    /// which is there but is not a json string reads the same as one which is missing.
    pub fn get_time_stamp<'s>(&self, raw: &'s [u8]) -> Option<&'s str> {
        if let Some(time_stamp) = &self.time_stamp {
            return time_stamp.value.try_get_str_value(raw);
        }
        None
    }

    /// The entity's own `TimeStamp` as a moment, read by the ISO check
    /// (`DateTimeAsMicroseconds::parse_iso_string`). `None` - there is no such field, its
    /// value is not a json string, or the string is not an ISO date-time.
    fn read_time_stamp_as_date_time(&self, raw: &[u8]) -> Option<DateTimeAsMicroseconds> {
        DateTimeAsMicroseconds::parse_iso_string(self.get_time_stamp(raw)?)
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
            let db_row =
                DbJsonEntity::parse_into_db_row(json.unwrap_as_object()?, inject_time_stamp)?;
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
            let db_row =
                DbJsonEntity::parse_into_db_row(json.unwrap_as_object()?, inject_time_stamp)?;

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

/// What is asked of the key of an entity a client writes: it is read out of a json string and
/// out of nothing else. A number, `true`, an object have no quotes to read the key between -
/// taking them for granted made the key `2` out of `123` and a panic out of `5`. Bytes between
/// the quotes which are not utf-8 are not a json string either.
///
/// It is refused as a json parse error because that is the one refusal which carries a text:
/// the server hands the text over to the client as it is, so the answer names the key and
/// says what is wrong with it. `null` is not handled here - it has a refusal of its own.
fn check_key_is_a_string(
    name: &str,
    value: &KeyValueContentPosition,
    raw: &[u8],
) -> Result<(), DbEntityParseFail> {
    if value.try_get_str_value(raw).is_some() {
        return Ok(());
    }

    Err(key_is_not_a_string(name))
}

/// What is asked of the key of a row which already is a row - see [`DbJsonEntity::new`]: that
/// it can be read at all.
///
/// A key which is not a json string could be written before keys were checked, and such a row
/// was stored under the value with its first and last character cut off - `123` under `2` -
/// which is the partition it lies in. It is still read that way: refusing it here would have
/// the server refuse the whole partition at start, and a reader - every snapshot which holds
/// the row. Only what can not be cut that way is refused: a value of a single character -
/// it used to panic - and bytes which are not utf-8, which used to be refused as a `null`
/// key. A new row never gets such a key - that is what [`check_key_is_a_string`] is for.
fn check_stored_key(
    name: &str,
    value: &KeyValueContentPosition,
    raw: &[u8],
) -> Result<(), DbEntityParseFail> {
    if value.try_get_str_value(raw).is_some() {
        return Ok(());
    }

    // What `KeyValueContentPosition::get_str_value` is going to take for the key.
    let between_the_ends = value
        .end
        .checked_sub(1)
        .and_then(|end| raw.get(value.start + 1..end));

    match between_the_ends {
        Some(between_the_ends) if std::str::from_utf8(between_the_ends).is_ok() => Ok(()),
        _ => Err(key_is_not_a_string(name)),
    }
}

fn key_is_not_a_string(name: &str) -> DbEntityParseFail {
    DbEntityParseFail::JsonParseError(JsonParseError::new(format!(
        "{} must be a json string",
        name
    )))
}

/// The refusal of an entity a client writes which carries bytes which are not utf-8 outside
/// its keys - a json parse error for the same reason [`check_key_is_a_string`] gives one.
fn entity_is_not_utf8() -> DbEntityParseFail {
    DbEntityParseFail::JsonParseError(JsonParseError::new(
        "The entity must be utf-8".to_string(),
    ))
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

    /// The limit counts the key, not its json spelling: `\u0434` is six characters of payload
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

    fn parse_and_keep_time_stamp_of(
        time_stamp: &str,
    ) -> Result<crate::db::DbRow, DbEntityParseFail> {
        let json = format!(
            r#"{{"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":"{}"}}"#,
            time_stamp
        );

        DbJsonEntity::parse_into_db_row_and_keep_date_time(json.as_bytes().into())
    }

    /// The `TimeStamp` a row ended up with, as its json spells it.
    fn stored_time_stamp(db_row: &crate::db::DbRow) -> String {
        let reparsed = DbJsonEntity::new(db_row.get_src_as_slice().into()).unwrap();

        reparsed
            .get_time_stamp(db_row.get_src_as_slice())
            .unwrap()
            .to_string()
    }

    fn assert_time_stamp_is_required(
        result: Result<crate::db::DbRow, DbEntityParseFail>,
        src: &str,
    ) {
        match result {
            Err(DbEntityParseFail::FieldTimeStampIsRequired {
                partition_key,
                row_key,
            }) => {
                assert_eq!(partition_key, "Pk", "source: {}", src);
                assert_eq!(row_key, "Rk", "source: {}", src);
            }
            Err(err) => panic!(
                "Expected FieldTimeStampIsRequired, got {:?}. Source: {}",
                err, src
            ),
            Ok(db_row) => panic!(
                "Expected FieldTimeStampIsRequired, the row got TimeStamp {}. Source: {}",
                stored_time_stamp(&db_row),
                src
            ),
        }
    }

    /// What the ISO check accepts is what the row is stamped with. For the check a value with
    /// no full seconds is `hh:mm`, whatever one or two characters follow the minutes;
    /// `JsonTimeStamp::parse_or_now` - which used to build the stamp from the same text -
    /// reads it only when none do (16 characters). With a zone (17) or with cut off seconds
    /// (18) the row was accepted and got the server clock instead of the version the client
    /// sent.
    #[test]
    fn keep_date_time_stores_the_moment_the_check_accepted() {
        for (src, expected) in [
            ("2020-05-06T07:08", "2020-05-06T07:08:00"),
            ("2020-05-06T07:08Z", "2020-05-06T07:08:00"),
            ("2020-05-06T07:08:0", "2020-05-06T07:08:00"),
        ] {
            let db_row = parse_and_keep_time_stamp_of(src).unwrap();

            assert_eq!(expected, stored_time_stamp(&db_row), "source: {}", src);

            #[cfg(feature = "master-node")]
            assert_eq!(
                DateTimeAsMicroseconds::parse_iso_string(src)
                    .unwrap()
                    .unix_microseconds,
                db_row.get_time_stamp_as_date_time().unix_microseconds,
                "source: {}",
                src
            );
        }
    }

    /// A value the ISO check can not read is refused exactly as a missing one is - also when
    /// some other reader of a `TimeStamp` would make a moment out of it (a unix number, a
    /// compact `yyyymmddhhmmss`). Nothing is left to be replaced by the server clock.
    #[test]
    fn keep_date_time_refuses_what_the_check_can_not_read() {
        for src in [
            "",
            "2020-05-06T07",
            "2020-13-06T07:08:09",
            "1588748889",
            "20200506070809",
        ] {
            assert_time_stamp_is_required(parse_and_keep_time_stamp_of(src), src);
        }
    }

    /// Only a json string holds a `TimeStamp`. Any other token used to be read with its
    /// first and last character cut off, as if they were the quotes: `920200050159` passed
    /// the check as `2020-05-15` and a one-character token (`5`) panicked.
    #[test]
    fn keep_date_time_refuses_a_time_stamp_which_is_not_a_string() {
        for token in [
            // what a default `Timestamp` is serialized as when the field is not skipped
            "null",
            "5",
            "1588748889000000",
            "920200050159",
            "true",
            "{}",
        ] {
            let json = format!(
                r#"{{"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":{}}}"#,
                token
            );

            assert_time_stamp_is_required(
                DbJsonEntity::parse_into_db_row_and_keep_date_time(json.as_bytes().into()),
                token,
            );
        }
    }

    /// ...and a string which is not utf-8 is not a date-time either - it used to panic.
    #[test]
    fn keep_date_time_refuses_a_time_stamp_which_is_not_utf8() {
        let mut json = br#"{"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":""#.to_vec();
        json.extend_from_slice(b"\xff\xfe2020-05-06T07:08:09\"}");

        assert_time_stamp_is_required(
            DbJsonEntity::parse_into_db_row_and_keep_date_time(json.as_slice().into()),
            "not utf-8",
        );
    }

    /// The bulk entry point reads every row the same way.
    #[test]
    fn keep_date_time_grouped_keeps_each_row_its_own_time_stamp() {
        let json = r#"[
            {"PartitionKey":"Pk1","RowKey":"Rk1","TimeStamp":"2020-05-06T07:08Z"},
            {"PartitionKey":"Pk2","RowKey":"Rk2","TimeStamp":"2021-06-07T08:09:10.123456"}
        ]"#;

        let result =
            DbJsonEntity::parse_grouped_by_partition_key_and_keep_date_time(json.as_bytes())
                .unwrap();

        assert_eq!(result.len(), 2);

        assert_eq!(result[0].0, "Pk1");
        assert_eq!(stored_time_stamp(&result[0].1[0]), "2020-05-06T07:08:00");

        assert_eq!(result[1].0, "Pk2");
        assert_eq!(
            stored_time_stamp(&result[1].1[0]),
            "2021-06-07T08:09:10.123456"
        );
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

    /// Json values which are not strings. Where a string is expected - a key, a `TimeStamp`,
    /// an `Expires` - such a value used to be read with its first and last character cut off,
    /// as if they were the quotes: `123` was `2`, `true` was `ru`, `{}` was the empty string,
    /// and a one-character `5` panicked.
    const NOT_STRINGS: [&str; 10] = [
        "5",
        "123",
        "4567",
        "-1.5",
        "true",
        "false",
        "{}",
        r#"{"a":"b"}"#,
        "[]",
        r#"["a"]"#,
    ];

    fn assert_key_must_be_a_string<T>(result: Result<T, DbEntityParseFail>, key: &str, src: &str) {
        match result {
            Err(DbEntityParseFail::JsonParseError(err)) => assert_eq!(
                format!("{} must be a json string", key),
                err.to_string(),
                "source: {}",
                src
            ),
            Err(err) => panic!(
                "Expected {} to be refused as not a string, got {:?}. Source: {}",
                key, err, src
            ),
            Ok(_) => panic!(
                "Expected {} to be refused as not a string, the entity was accepted. Source: {}",
                key, src
            ),
        }
    }

    /// Hands `parse` an entity whose PartitionKey, and then one whose RowKey, is each of
    /// [`NOT_STRINGS`]. Whichever way an entity a client writes is read, it is refused and the
    /// refusal names the key - no row is made under whatever such a value could be cut into.
    ///
    /// The `TimeStamp` is there for the readers which need one: the key is the only thing
    /// wrong with the entity.
    fn assert_keys_must_be_strings<T>(parse: impl Fn(&[u8]) -> Result<T, DbEntityParseFail>) {
        for value in NOT_STRINGS {
            let json = format!(
                r#"{{"PartitionKey":{},"RowKey":"Rk","TimeStamp":"2020-05-06T07:08:09"}}"#,
                value
            );

            assert_key_must_be_a_string(parse(json.as_bytes()), "PartitionKey", &json);

            let json = format!(
                r#"{{"PartitionKey":"Pk","RowKey":{},"TimeStamp":"2020-05-06T07:08:09"}}"#,
                value
            );

            assert_key_must_be_a_string(parse(json.as_bytes()), "RowKey", &json);
        }
    }

    /// The same entities as rows which already are rows. A key which is not a string could be
    /// written before keys were checked, and the row lies under the value with its first and
    /// last character cut off. `read` - it gives back the keys of the row - still has to read
    /// it under that very key, so that a table which loaded yesterday loads today. Only a
    /// value which can not be cut that way is refused: it used to panic.
    fn assert_stored_keys_are_read_as_they_were_stored(
        read: impl Fn(&[u8]) -> Result<(String, String), DbEntityParseFail>,
    ) {
        for value in NOT_STRINGS {
            let json = format!(
                r#"{{"PartitionKey":{},"RowKey":"Rk","TimeStamp":"2020-05-06T07:08:09"}}"#,
                value
            );

            let result = read(json.as_bytes());

            if value.len() < 2 {
                assert_key_must_be_a_string(result, "PartitionKey", &json);
            } else {
                assert_eq!(
                    (value[1..value.len() - 1].to_string(), "Rk".to_string()),
                    result.unwrap(),
                    "source: {}",
                    json
                );
            }

            let json = format!(
                r#"{{"PartitionKey":"Pk","RowKey":{},"TimeStamp":"2020-05-06T07:08:09"}}"#,
                value
            );

            let result = read(json.as_bytes());

            if value.len() < 2 {
                assert_key_must_be_a_string(result, "RowKey", &json);
            } else {
                assert_eq!(
                    ("Pk".to_string(), value[1..value.len() - 1].to_string()),
                    result.unwrap(),
                    "source: {}",
                    json
                );
            }
        }
    }

    fn keys_of(db_row: &crate::db::DbRow) -> (String, String) {
        (
            db_row.get_partition_key().to_string(),
            db_row.get_row_key().to_string(),
        )
    }

    #[test]
    fn new_reads_a_stored_row_under_the_key_it_was_stored_under() {
        assert_stored_keys_are_read_as_they_were_stored(|json| {
            let entity = DbJsonEntity::new(JsonFirstLineIterator::new(json))?;

            Ok((
                entity.get_partition_key(json).to_string(),
                entity.get_row_key(json).to_string(),
            ))
        });
    }

    #[test]
    fn from_slice_reads_a_stored_row_under_the_key_it_was_stored_under() {
        assert_stored_keys_are_read_as_they_were_stored(|json| {
            let entity = DbJsonEntity::from_slice(json)?;

            Ok((
                entity.partition_key_value(json).to_string(),
                entity.row_key_value(json).to_string(),
            ))
        });
    }

    #[test]
    fn parse_refuses_a_key_which_is_not_a_string() {
        let time_stamp = JsonTimeStamp::now();

        assert_keys_must_be_strings(|json| DbJsonEntity::parse(json, &time_stamp).map(|_| ()));
    }

    #[test]
    fn parse_into_db_row_refuses_a_key_which_is_not_a_string() {
        let time_stamp = JsonTimeStamp::now();

        assert_keys_must_be_strings(|json| {
            DbJsonEntity::parse_into_db_row(json.into(), &time_stamp)
        });
    }

    #[test]
    fn parse_into_db_row_and_keep_date_time_refuses_a_key_which_is_not_a_string() {
        assert_keys_must_be_strings(|json| {
            DbJsonEntity::parse_into_db_row_and_keep_date_time(json.into())
        });
    }

    #[test]
    fn restore_into_db_row_reads_a_stored_row_under_the_key_it_was_stored_under() {
        assert_stored_keys_are_read_as_they_were_stored(|json| {
            let db_row = DbJsonEntity::restore_into_db_row(json.to_vec())?;

            Ok(keys_of(&db_row))
        });
    }

    /// The entity as the second one of an array, behind one which is fine - an array reader
    /// has to get as far as the broken one and stop there.
    fn as_the_second_of_an_array(json: &[u8]) -> Vec<u8> {
        let mut result =
            br#"[{"PartitionKey":"Pk0","RowKey":"Rk0","TimeStamp":"2020-05-06T07:08:09"},"#
                .to_vec();

        result.extend_from_slice(json);
        result.push(b']');

        result
    }

    #[test]
    fn parse_as_vec_refuses_a_key_which_is_not_a_string() {
        let time_stamp = JsonTimeStamp::now();

        assert_keys_must_be_strings(|json| {
            DbJsonEntity::parse_as_vec(as_the_second_of_an_array(json).as_slice(), &time_stamp)
        });
    }

    #[test]
    fn restore_as_vec_reads_a_stored_row_under_the_key_it_was_stored_under() {
        assert_stored_keys_are_read_as_they_were_stored(|json| {
            let db_rows =
                DbJsonEntity::restore_as_vec(as_the_second_of_an_array(json).as_slice())?;

            assert_eq!(2, db_rows.len());
            assert_eq!(("Pk0".to_string(), "Rk0".to_string()), keys_of(&db_rows[0]));

            Ok(keys_of(&db_rows[1]))
        });
    }

    #[test]
    fn parse_grouped_by_partition_key_refuses_a_key_which_is_not_a_string() {
        let time_stamp = JsonTimeStamp::now();

        assert_keys_must_be_strings(|json| {
            DbJsonEntity::parse_grouped_by_partition_key(
                as_the_second_of_an_array(json).as_slice(),
                &time_stamp,
            )
        });
    }

    #[test]
    fn restore_grouped_by_partition_key_reads_a_stored_row_under_the_key_it_was_stored_under() {
        assert_stored_keys_are_read_as_they_were_stored(|json| {
            let partitions = DbJsonEntity::restore_grouped_by_partition_key(
                as_the_second_of_an_array(json).as_slice(),
            )?;

            // The partitions come back sorted by their keys - the row in question is the one
            // which is not the row in front of it in the array
            let mut db_rows: Vec<_> = partitions
                .iter()
                .flat_map(|(partition_key, db_rows)| {
                    db_rows.iter().map(move |db_row| {
                        assert_eq!(partition_key.as_str(), db_row.get_partition_key());
                        keys_of(db_row)
                    })
                })
                .filter(|keys| *keys != ("Pk0".to_string(), "Rk0".to_string()))
                .collect();

            assert_eq!(1, db_rows.len());

            Ok(db_rows.remove(0))
        });
    }

    #[test]
    fn parse_grouped_by_partition_key_and_keep_date_time_refuses_a_key_which_is_not_a_string() {
        assert_keys_must_be_strings(|json| {
            DbJsonEntity::parse_grouped_by_partition_key_and_keep_date_time(
                as_the_second_of_an_array(json).as_slice(),
            )
        });
    }

    /// ...wherever the key stands in the entity and whatever stands around it: the last field,
    /// right in front of the closing bracket; spaces and line breaks around the value; the
    /// other spellings the json reader takes for a literal and for a number. And the key a row
    /// is addressed by is the last one of its name - a string in front of it does not make it
    /// one.
    #[test]
    fn a_key_which_is_not_a_string_is_refused_wherever_it_stands() {
        let time_stamp = JsonTimeStamp::now();

        for (json, key) in [
            (r#"{"RowKey":"Rk","PartitionKey":5}"#, "PartitionKey"),
            (r#"{"PartitionKey":"Pk","RowKey":5}"#, "RowKey"),
            (
                "{ \"PartitionKey\" : 5 , \"RowKey\" : \"Rk\" }",
                "PartitionKey",
            ),
            (
                "{\n\t\"PartitionKey\"\t:\t123\n,\n\t\"RowKey\"\t:\t\"Rk\"\n}",
                "PartitionKey",
            ),
            (r#"{"PartitionKey":"Pk","RowKey":TRUE}"#, "RowKey"),
            (r#"{"PartitionKey":1e5,"RowKey":"Rk"}"#, "PartitionKey"),
            (
                r#"{"PartitionKey":"Pk","PartitionKey":123,"RowKey":"Rk"}"#,
                "PartitionKey",
            ),
            (
                r#"{"PartitionKey":"Pk","RowKey":"Rk","RowKey":4567}"#,
                "RowKey",
            ),
        ] {
            assert_key_must_be_a_string(
                DbJsonEntity::parse(json.as_bytes(), &time_stamp).map(|_| ()),
                key,
                json,
            );

            assert_key_must_be_a_string(
                DbJsonEntity::parse_into_db_row(json.as_bytes().into(), &time_stamp),
                key,
                json,
            );
        }
    }

    /// Of several fields with the name of a key the last one is the key - to the reader of an
    /// entity a client writes and to the reader of a stored row alike. The second one used to
    /// refuse a `null` wherever it stood, so a row which was accepted and stored with a `null`
    /// in front of its key could not be loaded again - and took its partition with it.
    #[test]
    fn of_the_fields_with_the_name_of_a_key_the_last_one_counts_to_every_reader() {
        let time_stamp = JsonTimeStamp::now();

        for json in [
            r#"{"PartitionKey":null,"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":"2020-05-06T07:08:09"}"#,
            r#"{"PartitionKey":"Pk","RowKey":null,"RowKey":"Rk","TimeStamp":"2020-05-06T07:08:09"}"#,
            r#"{"PartitionKey":123,"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":"2020-05-06T07:08:09"}"#,
        ] {
            let expected = ("Pk".to_string(), "Rk".to_string());

            let entity = DbJsonEntity::parse(json.as_bytes(), &time_stamp).unwrap();
            assert_eq!("Pk", entity.get_partition_key(), "source: {}", json);
            assert_eq!("Rk", entity.get_row_key(), "source: {}", json);

            let db_row =
                DbJsonEntity::parse_into_db_row(json.as_bytes().into(), &time_stamp).unwrap();
            assert_eq!(expected, keys_of(&db_row), "source: {}", json);

            // ...and the row is loaded again the way it was stored - every field of it is kept
            let stored = db_row.get_src_as_slice().to_vec();
            let db_row = DbJsonEntity::restore_into_db_row(stored).unwrap();
            assert_eq!(expected, keys_of(&db_row), "source: {}", json);
        }

        for (json, is_partition_key) in [
            (r#"{"PartitionKey":"Pk","PartitionKey":null,"RowKey":"Rk"}"#, true),
            (r#"{"PartitionKey":"Pk","RowKey":"Rk","RowKey":null}"#, false),
        ] {
            let is_refused_as_null = |result: Result<(), DbEntityParseFail>| match result {
                Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull) => is_partition_key,
                Err(DbEntityParseFail::FieldRowKeyCanNotBeNull) => !is_partition_key,
                _ => false,
            };

            assert!(
                is_refused_as_null(
                    DbJsonEntity::parse_into_db_row(json.as_bytes().into(), &time_stamp)
                        .map(|_| ())
                ),
                "source: {}",
                json
            );

            assert!(
                is_refused_as_null(DbJsonEntity::from_slice(json.as_bytes()).map(|_| ())),
                "source: {}",
                json
            );
        }
    }

    /// The name of a field which is not utf-8 is a parse failure. `parse_into_db_row` used to
    /// panic on it, where `new` refused it.
    #[test]
    fn a_field_name_which_is_not_utf8_is_refused() {
        let time_stamp = JsonTimeStamp::now();

        let json = &b"{\"PartitionKey\":\"Pk\",\"RowKey\":\"Rk\",\"\xffabc\":1}"[..];

        assert!(matches!(
            DbJsonEntity::parse_into_db_row(json.into(), &time_stamp),
            Err(DbEntityParseFail::JsonParseError(_))
        ));

        assert!(matches!(
            DbJsonEntity::from_slice(json),
            Err(DbEntityParseFail::JsonParseError(_))
        ));

        assert!(matches!(
            DbJsonEntity::parse_as_vec(as_the_second_of_an_array(json).as_slice(), &time_stamp),
            Err(DbEntityParseFail::JsonParseError(_))
        ));
    }

    /// The keys are not the only place for bytes which are not utf-8. A row is handed out as
    /// a `String` and parsed by its readers as utf-8, so an entity a client writes is refused
    /// when any value of it is not utf-8 - it used to be stored, and `DbRow::write_json`
    /// made a `String` which is not utf-8 out of it. A row which already is a row is still
    /// read.
    #[test]
    fn a_written_entity_which_is_not_utf8_is_refused() {
        let time_stamp = JsonTimeStamp::now();

        let payloads: [&[u8]; 3] = [
            b"{\"PartitionKey\":\"Pk\",\"RowKey\":\"Rk\",\"TimeStamp\":\"2020-05-06T07:08:09\",\"Data\":\"\x80\"}",
            b"{\"Data\":{\"a\":[\"\xffabc\"]},\"PartitionKey\":\"Pk\",\"RowKey\":\"Rk\",\"TimeStamp\":\"2020-05-06T07:08:09\"}",
            b"{\"PartitionKey\":\"Pk\",\"RowKey\":\"Rk\",\"TimeStamp\":\"2020-05-06T07:08:09\",\"Expires\":\"\xffabc\"}",
        ];

        for json in payloads {
            let results = [
                DbJsonEntity::parse_into_db_row(json.into(), &time_stamp).map(|_| ()),
                DbJsonEntity::parse_into_db_row_and_keep_date_time(json.into()).map(|_| ()),
                DbJsonEntity::parse_as_vec(as_the_second_of_an_array(json).as_slice(), &time_stamp)
                    .map(|_| ()),
                DbJsonEntity::parse(json, &time_stamp)
                    .and_then(|entity| entity.into_db_row())
                    .map(|_| ()),
            ];

            for result in results {
                match result {
                    Err(DbEntityParseFail::JsonParseError(err)) => {
                        assert_eq!("The entity must be utf-8", err.to_string())
                    }
                    other => panic!(
                        "Expected the entity to be refused as not utf-8, got {:?}",
                        other
                    ),
                }
            }

            // ...and a row which was stored before that is still read, under its keys
            let db_row = DbJsonEntity::restore_into_db_row(json.to_vec()).unwrap();
            assert_eq!(("Pk".to_string(), "Rk".to_string()), keys_of(&db_row));
        }

        // The `TimeStamp` a client sends is not a part of the row - it is replaced - so it
        // does not make the row one which is not utf-8.
        let json = &b"{\"PartitionKey\":\"Pk\",\"RowKey\":\"Rk\",\"TimeStamp\":\"\x80\"}"[..];

        let db_row = DbJsonEntity::parse_into_db_row(json.into(), &time_stamp).unwrap();

        let mut out = String::new();
        db_row.write_json(&mut out);
        assert!(std::str::from_utf8(out.as_bytes()).is_ok());
    }

    /// An element of the array which is not an object - a number, a string, a `null` - is a
    /// parse failure. The readers of what a client writes used to panic on it, where the
    /// `restore_*` ones refused it.
    #[test]
    fn an_element_of_the_array_which_is_not_an_object_is_refused() {
        let time_stamp = JsonTimeStamp::now();

        for element in ["5", "null", r#""a""#, "true", "[]"] {
            let json = as_the_second_of_an_array(element.as_bytes());
            let json = json.as_slice();

            assert!(
                matches!(
                    DbJsonEntity::parse_as_vec(json, &time_stamp),
                    Err(DbEntityParseFail::JsonParseError(_))
                ),
                "source: {}",
                element
            );

            assert!(
                matches!(
                    DbJsonEntity::parse_grouped_by_partition_key(json, &time_stamp),
                    Err(DbEntityParseFail::JsonParseError(_))
                ),
                "source: {}",
                element
            );

            assert!(
                matches!(
                    DbJsonEntity::parse_grouped_by_partition_key_and_keep_date_time(json),
                    Err(DbEntityParseFail::JsonParseError(_))
                ),
                "source: {}",
                element
            );

            assert!(
                DbJsonEntity::restore_as_vec(json).is_err(),
                "source: {}",
                element
            );
        }
    }

    /// A `null` key is refused as a `null` however it is spelled - the json reader takes
    /// `NULL` and `Null` for one as well. `parse_into_db_row` used to know the lower case
    /// spelling only, and made the keys `UL` and `ul` out of the other two.
    #[test]
    fn a_key_which_is_null_is_refused_however_null_is_spelled() {
        let time_stamp = JsonTimeStamp::now();

        for null in ["null", "NULL", "Null"] {
            let json = format!(r#"{{"PartitionKey":{},"RowKey":"Rk"}}"#, null);

            assert!(
                matches!(
                    DbJsonEntity::from_slice(json.as_bytes()),
                    Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull)
                ),
                "source: {}",
                json
            );

            assert!(
                matches!(
                    DbJsonEntity::parse_into_db_row(json.as_bytes().into(), &time_stamp),
                    Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull)
                ),
                "source: {}",
                json
            );

            let json = format!(r#"{{"PartitionKey":"Pk","RowKey":{}}}"#, null);

            assert!(
                matches!(
                    DbJsonEntity::from_slice(json.as_bytes()),
                    Err(DbEntityParseFail::FieldRowKeyCanNotBeNull)
                ),
                "source: {}",
                json
            );

            assert!(
                matches!(
                    DbJsonEntity::parse_into_db_row(json.as_bytes().into(), &time_stamp),
                    Err(DbEntityParseFail::FieldRowKeyCanNotBeNull)
                ),
                "source: {}",
                json
            );
        }
    }

    /// ...and a key which is not utf-8 is not a json string either. `parse_into_db_row` used
    /// to panic on it, `new` took it for a `null`.
    #[test]
    fn a_key_which_is_not_utf8_is_refused() {
        let time_stamp = JsonTimeStamp::now();

        for (json, key) in [
            (
                &b"{\"PartitionKey\":\"\xffabc\",\"RowKey\":\"Rk\"}"[..],
                "PartitionKey",
            ),
            (
                &b"{\"PartitionKey\":\"Pk\",\"RowKey\":\"\xffabc\"}"[..],
                "RowKey",
            ),
        ] {
            assert_key_must_be_a_string(DbJsonEntity::from_slice(json), key, "not utf-8");

            assert_key_must_be_a_string(
                DbJsonEntity::parse_into_db_row(json.into(), &time_stamp),
                key,
                "not utf-8",
            );
        }
    }

    /// Only a json string holds a `TimeStamp` or an `Expires`. Whatever else stands there is
    /// not read as one - `null` used to come back as `ul`, `123` as `2`, and `5` panicked.
    #[test]
    fn time_stamp_and_expires_which_are_not_strings_are_not_read() {
        let time_stamp_to_inject = JsonTimeStamp::now();

        for value in ["null"].into_iter().chain(NOT_STRINGS) {
            let json = format!(
                r#"{{"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":{},"Expires":{}}}"#,
                value, value
            );

            let json = json.as_bytes();

            let entity = DbJsonEntity::from_slice(json).unwrap();

            assert_eq!(None, entity.get_time_stamp(json), "source: {}", value);
            assert_eq!(None, entity.get_expires(json), "source: {}", value);

            // ...and the same through `DbJsonEntityWithContent` - a `Replace` on the server
            // asks it for the version the client has read the row at
            let entity = DbJsonEntity::parse(json, &time_stamp_to_inject).unwrap();

            assert_eq!(None, entity.get_time_stamp(), "source: {}", value);
            assert_eq!(None, entity.get_expires(), "source: {}", value);
        }
    }

    /// ...and neither is a string which is not utf-8 - it used to panic.
    #[test]
    fn time_stamp_and_expires_which_are_not_utf8_are_not_read() {
        let time_stamp_to_inject = JsonTimeStamp::now();

        let mut json = br#"{"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":""#.to_vec();
        json.extend_from_slice(b"\xffabc\",\"Expires\":\"\xffabc\"}");

        let json = json.as_slice();

        let entity = DbJsonEntity::from_slice(json).unwrap();

        assert_eq!(None, entity.get_time_stamp(json));
        assert_eq!(None, entity.get_expires(json));

        let entity = DbJsonEntity::parse(json, &time_stamp_to_inject).unwrap();

        assert_eq!(None, entity.get_time_stamp());
        assert_eq!(None, entity.get_expires());
    }

    /// A string is read as it is spelled, the quotes are not a part of it - and an empty one
    /// is still a string.
    #[test]
    fn time_stamp_and_expires_which_are_strings_are_read_as_they_are_spelled() {
        let time_stamp_to_inject = JsonTimeStamp::now();

        for (time_stamp, expires) in [
            ("2020-05-06T07:08:09", "2021-06-07T08:09:10.123456Z"),
            ("not-a-date", "not-a-date"),
            ("", ""),
        ] {
            let json = format!(
                r#"{{"PartitionKey":"Pk","RowKey":"Rk","TimeStamp":"{}","Expires":"{}"}}"#,
                time_stamp, expires
            );

            let json = json.as_bytes();

            let entity = DbJsonEntity::from_slice(json).unwrap();

            assert_eq!(Some(time_stamp), entity.get_time_stamp(json));
            assert_eq!(Some(expires), entity.get_expires(json));

            let entity = DbJsonEntity::parse(json, &time_stamp_to_inject).unwrap();

            assert_eq!(Some(time_stamp), entity.get_time_stamp());
            assert_eq!(Some(expires), entity.get_expires());
        }
    }

    /// An entity whose key is not a string is refused for its key also when it has no
    /// `TimeStamp` to keep. The pre-pass of the keeping readers is a reader of what a client
    /// writes: read the way a stored row is, the entity would be refused for its `TimeStamp`
    /// and named by the key its value can be cut into - `123` by `2`.
    #[test]
    fn keep_date_time_refuses_a_key_which_is_not_a_string_in_front_of_its_time_stamp() {
        for (json, key) in [
            (r#"{"PartitionKey":123,"RowKey":"Rk"}"#, "PartitionKey"),
            (
                r#"{"PartitionKey":"Pk","RowKey":true,"TimeStamp":null}"#,
                "RowKey",
            ),
            (
                r#"{"PartitionKey":{"a":"b"},"RowKey":"Rk","TimeStamp":"not-a-date"}"#,
                "PartitionKey",
            ),
        ] {
            assert_key_must_be_a_string(
                DbJsonEntity::parse_into_db_row_and_keep_date_time(json.as_bytes().into()),
                key,
                json,
            );

            assert_key_must_be_a_string(
                DbJsonEntity::parse_grouped_by_partition_key_and_keep_date_time(
                    as_the_second_of_an_array(json.as_bytes()).as_slice(),
                ),
                key,
                json,
            );
        }
    }

    /// Of an entity with a `null` key and without the other key, the `null` is what is
    /// refused - the reader of a stored row used to refuse it the moment it met it. Looking
    /// at the keys only after the whole entity has been read must not turn that into "the
    /// other key is required".
    #[test]
    fn a_null_key_is_refused_in_front_of_the_other_key_which_is_missing() {
        let time_stamp = JsonTimeStamp::now();

        let json = &br#"{"RowKey":null,"TimeStamp":"2020-05-06T07:08:09"}"#[..];

        assert!(matches!(
            DbJsonEntity::from_slice(json),
            Err(DbEntityParseFail::FieldRowKeyCanNotBeNull)
        ));
        assert!(matches!(
            DbJsonEntity::restore_into_db_row(json.to_vec()),
            Err(DbEntityParseFail::FieldRowKeyCanNotBeNull)
        ));
        assert!(matches!(
            DbJsonEntity::parse(json, &time_stamp),
            Err(DbEntityParseFail::FieldRowKeyCanNotBeNull)
        ));

        let json = &br#"{"PartitionKey":null,"TimeStamp":"2020-05-06T07:08:09"}"#[..];

        assert!(matches!(
            DbJsonEntity::from_slice(json),
            Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull)
        ));
        assert!(matches!(
            DbJsonEntity::restore_into_db_row(json.to_vec()),
            Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull)
        ));
        assert!(matches!(
            DbJsonEntity::parse(json, &time_stamp),
            Err(DbEntityParseFail::FieldPartitionKeyCanNotBeNull)
        ));
    }
}
