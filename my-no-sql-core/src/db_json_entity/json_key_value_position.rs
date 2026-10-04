use my_json::json_reader::{JsonContentOffset, JsonValue};

use super::JsonStrValue;

#[derive(Debug, Clone)]
pub struct KeyValueContentPosition {
    pub start: usize,
    pub end: usize,
}

impl KeyValueContentPosition {
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn get_value<'s>(&self, raw: &'s [u8]) -> &'s str {
        std::str::from_utf8(&raw[self.start..self.end]).unwrap()
    }

    /// The content between the quotes exactly as it is stored - JSON escape sequences are **not**
    /// resolved, so a value the client sent as `"demo\\DIRNG"` comes back with both backslashes.
    ///
    /// Anything used as a key has to go through [`Self::unescape_str_value`] instead: the
    /// logical key is what a point request addresses.
    ///
    /// The first and the last byte are cut off as if they were the quotes, whatever they are,
    /// and a value shorter than two bytes - or one whose rest is not utf-8 - panics. So this
    /// is for two kinds of values only. A value which is known to be a json string: the key
    /// of an entity a client writes is one, `DbJsonEntity::parse` and `parse_into_db_row`
    /// see to it. And the key of a row which already is a row: `DbJsonEntity::new` lets a
    /// stored key which is not a json string through on purpose, once it has checked that
    /// the key survives the cut - such a row lies under the cut value (`123` under `2`).
    /// A value which may be anything the json allows goes through
    /// [`Self::try_get_str_value`].
    pub fn get_str_value<'s>(&self, raw: &'s [u8]) -> &'s str {
        std::str::from_utf8(&raw[self.start + 1..self.end - 1]).unwrap()
    }

    /// [`Self::get_str_value`] for a value which is not known to be a json string. `None` - it
    /// is not one (`null`, a number, `true`, an object, an array), or what is between the
    /// quotes is not utf-8.
    ///
    /// Cutting the first and the last character off such a value, as if they were the quotes,
    /// reads `123` as `2` and `null` as `ul` - and panics on a one-character `5`.
    pub fn try_get_str_value<'s>(&self, raw: &'s [u8]) -> Option<&'s str> {
        let content = raw[self.start..self.end]
            .strip_prefix(b"\"")?
            .strip_suffix(b"\"")?;

        std::str::from_utf8(content).ok()
    }

    /// The value as a [`JsonStrValue`] - still raw, so the caller picks the form it needs:
    /// [`JsonStrValue::eq_with_str`] to answer a question about it,
    /// [`JsonStrValue::read_as_value`] to actually build it.
    pub fn get_json_value<'s>(&self, raw: &'s [u8]) -> JsonStrValue<'s> {
        JsonStrValue::RawAsStr(self.get_str_value(raw))
    }

    /// The value materialized - but **only** when the raw form is not already it: `None`
    /// means the raw slice can be borrowed as the value and nothing has to be copied.
    ///
    /// For an index which needs the value as a plain `&str` on every lookup this is the one
    /// place to pay for it; everything which only compares should use
    /// [`Self::get_json_value`] instead.
    pub fn unescape_str_value(&self, raw: &[u8]) -> Option<Box<str>> {
        let value = self.get_json_value(raw);

        if !value.has_escapes() {
            return None;
        }

        Some(value.read_as_value().into_string().into_boxed_str())
    }

    /// `null` in any spelling the json reader takes for one - `NULL` and `Null` as well. The
    /// bytes are compared as they are: a value which is not utf-8 is just not a `null`.
    pub fn is_null(&self, raw: &[u8]) -> bool {
        my_json::json_utils::is_null(&raw[self.start..self.end])
    }
}

#[derive(Debug, Clone)]
pub struct JsonKeyValuePosition {
    pub key: KeyValueContentPosition,
    pub value: KeyValueContentPosition,
}

impl JsonKeyValuePosition {
    pub fn new(name: &JsonContentOffset, value: &JsonValue) -> Self {
        Self {
            key: KeyValueContentPosition {
                start: name.start,
                end: name.end,
            },

            value: KeyValueContentPosition {
                start: value.start,
                end: value.end,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::KeyValueContentPosition;

    /// The position of a value which is the whole of `raw`.
    fn position_of(raw: &[u8]) -> KeyValueContentPosition {
        KeyValueContentPosition {
            start: 0,
            end: raw.len(),
        }
    }

    #[test]
    fn try_get_str_value_reads_what_is_between_the_quotes() {
        let cases: [(&[u8], &str); 7] = [
            (br#""Pk""#, "Pk"),
            (br#""""#, ""),
            (br#""5""#, "5"),
            (br#""null""#, "null"),
            // as it is stored - the escape sequences are not resolved
            (br#""a\"b""#, r#"a\"b"#),
            (br#""a\\""#, r#"a\\"#),
            ("\"демо\"".as_bytes(), "демо"),
        ];

        for (raw, expected) in cases {
            assert_eq!(
                Some(expected),
                position_of(raw).try_get_str_value(raw),
                "source: {}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    /// A value which is not a json string has no text to give out. A pair of quotes is two
    /// quotes: a lone one opens a string and does not close it.
    #[test]
    fn try_get_str_value_is_none_for_what_is_not_a_json_string() {
        let cases: [&[u8]; 14] = [
            b"5",
            b"123",
            b"-1.5",
            b"null",
            b"true",
            b"{}",
            br#"{"a":"b"}"#,
            b"[]",
            br#"["a"]"#,
            b"",
            b"\"",
            br#""abc"#,
            br#"abc""#,
            // quoted, but what is between the quotes is not utf-8
            b"\"\xffabc\"",
        ];

        for raw in cases {
            assert_eq!(
                None,
                position_of(raw).try_get_str_value(raw),
                "source: {}",
                String::from_utf8_lossy(raw)
            );
        }
    }

    /// The value is read at its position, out of the payload it stands in.
    #[test]
    fn try_get_str_value_reads_the_value_at_its_position() {
        let raw = br#"{"PartitionKey":"Pk","V":5}"#;

        let partition_key = KeyValueContentPosition { start: 16, end: 20 };
        let v = KeyValueContentPosition { start: 25, end: 26 };

        assert_eq!(Some("Pk"), partition_key.try_get_str_value(raw));
        assert_eq!(None, v.try_get_str_value(raw));
    }

    /// A `null` is one in every spelling the json reader takes for it, and nothing else is: not
    /// a string which spells it, not a token which only begins like it, not bytes which are
    /// not utf-8 - reading those as a text used to panic.
    #[test]
    fn is_null_knows_every_spelling_of_a_null_and_nothing_else() {
        let nulls: [&[u8]; 4] = [b"null", b"NULL", b"Null", b"nUlL"];

        for raw in nulls {
            assert!(
                position_of(raw).is_null(raw),
                "source: {}",
                String::from_utf8_lossy(raw)
            );
        }

        let not_nulls: [&[u8]; 7] = [
            br#""null""#,
            b"nul",
            b"nulll",
            b"5",
            b"",
            b"n\xffll",
            b"\xff\xfe\xfd\xfc",
        ];

        for raw in not_nulls {
            assert!(
                !position_of(raw).is_null(raw),
                "source: {}",
                String::from_utf8_lossy(raw)
            );
        }
    }
}
