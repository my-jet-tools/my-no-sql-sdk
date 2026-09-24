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
    pub fn get_str_value<'s>(&self, raw: &'s [u8]) -> &'s str {
        std::str::from_utf8(&raw[self.start + 1..self.end - 1]).unwrap()
    }

    /// The value as a [`JsonStrValue`] - still raw, so the caller picks the form it needs:
    /// [`JsonStrValue::eq_with_str`] / [`JsonStrValue::cmp_with_str`] to answer a question
    /// about it, [`JsonStrValue::read_as_value`] to actually build it.
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

    pub fn is_null(&self, raw: &[u8]) -> bool {
        self.get_value(raw) == "null"
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
