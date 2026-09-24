use rust_extensions::StrOrString;

/// A json string value, held in whichever form it already is - so that nothing is converted
/// until somebody actually needs the other form.
///
/// The two forms are not interchangeable and mixing them up is what makes a row unreachable:
///
/// * **raw** - the characters between the quotes of the payload, escape sequences unresolved.
///   `demo\DIRNG` travels as `demo\\DIRNG`, `д` may travel as `д`. One value has many
///   valid raw spellings, so raw is what you store and transmit, never what you compare or
///   index by.
/// * **value** - the string those characters stand for. This is what the caller passed as
///   `partitionKey=` / `rowKey=`, what an index is keyed by, and what comparisons mean.
///
/// Use [`Self::eq_with_str`] to answer a question about the value without materializing it -
/// it walks the escapes on the fly and never allocates.
#[derive(Debug)]
pub enum JsonStrValue<'s> {
    /// Raw json content, borrowed out of the payload.
    RawAsStr(&'s str),
    /// The value itself - there is nothing to resolve.
    Unescaped(&'s str),
}

impl<'s> JsonStrValue<'s> {
    /// The raw json form - what goes between the quotes of a payload. Borrowed for a value
    /// which is already raw; a value which is not gets escaped, and even then it allocates
    /// only when it really contains something to escape.
    pub fn read_as_raw(&self) -> StrOrString<'_> {
        match self {
            Self::RawAsStr(raw) => StrOrString::create_as_str(raw),
            Self::Unescaped(value) => my_json::json_string_value::escape_json_string_value(value),
        }
    }

    /// The value the json stands for - what a key is addressed by. Borrowed when there is
    /// nothing to resolve (which is every ordinary key), owned only when there is.
    pub fn read_as_value(&self) -> StrOrString<'_> {
        match self {
            Self::RawAsStr(raw) => my_json::json_string_value::de_escape_json_string_value(raw),
            Self::Unescaped(value) => StrOrString::create_as_str(value),
        }
    }

    /// `true` when this is the value `other` spells - resolving escapes as it walks, without
    /// building the value first.
    pub fn eq_with_str(&self, other: &str) -> bool {
        match self {
            Self::Unescaped(value) => *value == other,
            Self::RawAsStr(raw) => raw_eq_with_str(raw, other),
        }
    }

    /// Whether the raw form carries any escape sequence at all. A key which has none is the
    /// same string in both forms and can be borrowed as it is.
    pub fn has_escapes(&self) -> bool {
        match self {
            Self::RawAsStr(raw) => has_escapes(raw),
            Self::Unescaped(_) => false,
        }
    }
}

impl<'s> std::fmt::Display for JsonStrValue<'s> {
    /// Writes the **value** - what the reader of a log line or an error message is looking
    /// for - straight into the formatter, without building it first.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unescaped(value) => f.write_str(value),
            Self::RawAsStr(raw) => write_as_value(raw, f),
        }
    }
}

fn write_as_value(raw: &str, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if !has_escapes(raw) {
        return f.write_str(raw);
    }

    for c in DeEscapedChars::new(raw) {
        std::fmt::Write::write_char(f, c)?;
    }

    Ok(())
}

fn has_escapes(raw: &str) -> bool {
    raw.as_bytes().contains(&b'\\')
}

fn raw_eq_with_str(raw: &str, other: &str) -> bool {
    if !has_escapes(raw) {
        return raw == other;
    }

    let mut expected = other.chars();

    for c in DeEscapedChars::new(raw) {
        if expected.next() != Some(c) {
            return false;
        }
    }

    expected.next().is_none()
}

/// The characters of a raw json string, with the escape sequences resolved as they are read.
///
/// Deliberately mirrors `my_json::json_string_value::de_escape_json_string_value` - including
/// what it does with the broken input a hostile client can send - so that `eq_with_str` and
/// `read_as_value` can never disagree about the same bytes.
struct DeEscapedChars<'s> {
    chars: std::iter::Peekable<std::str::Chars<'s>>,
    /// A resolved escape can be two characters long (an unknown escape is kept as the
    /// backslash plus the character which followed it); the second one waits here.
    pending: Option<char>,
}

impl<'s> DeEscapedChars<'s> {
    fn new(raw: &'s str) -> Self {
        Self {
            chars: raw.chars().peekable(),
            pending: None,
        }
    }

    /// Reads exactly 4 hex digits, consuming whatever it managed to read - the same
    /// non-restoring behaviour the my-json de-escaper has.
    fn parse_unicode_escape(chars: &mut std::iter::Peekable<std::str::Chars>) -> Option<u32> {
        let mut code: u32 = 0;

        for _ in 0..4 {
            let digit = chars.next()?.to_digit(16)?;
            code = code * 16 + digit;
        }

        Some(code)
    }
}

impl<'s> Iterator for DeEscapedChars<'s> {
    type Item = char;

    fn next(&mut self) -> Option<char> {
        if let Some(pending) = self.pending.take() {
            return Some(pending);
        }

        let c = self.chars.next()?;

        if c != '\\' {
            return Some(c);
        }

        match self.chars.next() {
            Some('"') => Some('"'),
            Some('\\') => Some('\\'),
            Some('/') => Some('/'),
            Some('b') => Some('\x08'),
            Some('f') => Some('\x0C'),
            Some('n') => Some('\x0A'),
            Some('r') => Some('\x0D'),
            Some('t') => Some('\x09'),
            Some('u') => match Self::parse_unicode_escape(&mut self.chars) {
                Some(code) if (0xD800..=0xDBFF).contains(&code) => {
                    // A high surrogate pairs up with the low one which follows it
                    let mut lookahead = self.chars.clone();

                    let combined = if lookahead.next() == Some('\\') && lookahead.next() == Some('u')
                    {
                        match Self::parse_unicode_escape(&mut lookahead) {
                            Some(low) if (0xDC00..=0xDFFF).contains(&low) => {
                                Some(0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00))
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };

                    match combined {
                        Some(code_point) => {
                            self.chars = lookahead;
                            Some(char::from_u32(code_point).unwrap_or('\u{FFFD}'))
                        }
                        // an unpaired high surrogate is not a character
                        None => Some('\u{FFFD}'),
                    }
                }
                // ...and neither is a lone low one
                Some(code) if (0xDC00..=0xDFFF).contains(&code) => Some('\u{FFFD}'),
                Some(code) => Some(char::from_u32(code).unwrap_or('\u{FFFD}')),
                // a truncated \uXXXX stays in the text as it is
                None => {
                    self.pending = Some('u');
                    Some('\\')
                }
            },
            // an unknown escape stays in the text as it is
            Some(other) => {
                self.pending = Some(other);
                Some('\\')
            }
            // a trailing backslash is just a backslash
            None => Some('\\'),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JsonStrValue;

    /// `(raw json content, the value it stands for)`
    const CASES: [(&str, &str); 12] = [
        ("plain-key", "plain-key"),
        ("", ""),
        (r#"a\\b"#, "a\\b"),
        (r#"a\"b"#, "a\"b"),
        (r#"a\/b"#, "a/b"),
        (r#"a\nb"#, "a\nb"),
        (r#"a\tb"#, "a\tb"),
        (r#"a\rb"#, "a\rb"),
        ("\\u0434\\u0435\\u043c\\u043e", "демо"),
        ("\\ud83d\\ude00", "😀"),
        (r#"demo\\DIRNGUSDENM50D"#, "demo\\DIRNGUSDENM50D"),
        ("\\\\", "\\"),
    ];

    #[test]
    fn raw_reads_back_as_the_value_it_stands_for() {
        for (raw, value) in CASES {
            assert_eq!(
                JsonStrValue::RawAsStr(raw).read_as_value().as_str(),
                value,
                "raw: {}",
                raw
            );
        }
    }

    #[test]
    fn a_value_escapes_back_into_json() {
        for (_, value) in CASES {
            let as_json = JsonStrValue::Unescaped(value);
            let raw = as_json.read_as_raw().into_string();

            // whatever spelling the escaper picks, it has to stand for the same value
            assert_eq!(
                JsonStrValue::RawAsStr(raw.as_str()).read_as_value().as_str(),
                value,
                "value: {:?} escaped as {:?}",
                value,
                raw
            );
        }
    }

    /// The point of the type: comparing never has to build the value.
    #[test]
    fn eq_with_str_agrees_with_read_as_value() {
        for (raw, value) in CASES {
            let json_value = JsonStrValue::RawAsStr(raw);

            assert!(json_value.eq_with_str(value), "raw: {}", raw);

            // ...and it says no to everything else, including the raw spelling itself
            for (_, other) in CASES {
                assert_eq!(
                    json_value.eq_with_str(other),
                    other == value,
                    "raw: {} vs {}",
                    raw,
                    other
                );
            }
        }
    }

    /// A key in a log line or an error message is the value, not its json spelling.
    #[test]
    fn display_writes_the_value() {
        for (raw, value) in CASES {
            assert_eq!(JsonStrValue::RawAsStr(raw).to_string(), value, "raw: {}", raw);
            assert_eq!(JsonStrValue::Unescaped(value).to_string(), value);
        }
    }

    #[test]
    fn eq_with_str_is_not_fooled_by_a_prefix() {
        let json_value = JsonStrValue::RawAsStr(r#"a\\b"#);

        assert!(!json_value.eq_with_str("a\\"));
        assert!(!json_value.eq_with_str("a\\bc"));
        assert!(!json_value.eq_with_str(r#"a\\b"#));
    }

    #[test]
    fn malformed_escaping_reads_the_same_both_ways() {
        for raw in [
            r#"a\"#,       // trailing backslash
            r#"a\qb"#,     // unknown escape
            r#"a\u12"#,    // truncated \uXXXX
            r#"a\uzzzz"#,  // not hex
            "\\ud83d",     // unpaired high surrogate
            "\\ude00",     // lone low surrogate
            "\\ud83dabc",  // high surrogate followed by plain text
            "\\ud83d\\n",  // high surrogate followed by another escape
        ] {
            let json_value = JsonStrValue::RawAsStr(raw);
            let value = json_value.read_as_value();

            assert!(
                json_value.eq_with_str(value.as_str()),
                "raw: {} read as {:?}",
                raw,
                value.as_str()
            );
        }
    }
}
