use my_no_sql_abstractions::{format_time_stamp, parse_time_stamp};
use rust_extensions::date_time::DateTimeAsMicroseconds;

/// A `TimeStamp` ready to be injected into a row's json, together with the moment it
/// stands for. `str_value` is always the canonical spelling
/// (`2026-08-09T16:44:39.5404`), whichever constructor produced it.
pub struct JsonTimeStamp {
    str_value: String,
    pub date_time: DateTimeAsMicroseconds,
}

impl JsonTimeStamp {
    pub fn now() -> Self {
        Self::from_date_time(DateTimeAsMicroseconds::now())
    }

    pub fn from_date_time(date_time: DateTimeAsMicroseconds) -> Self {
        Self {
            str_value: format_time_stamp(date_time),
            date_time,
        }
    }

    /// Reads a client-supplied `TimeStamp`, falling back to the server clock if it does
    /// not parse. Whatever the client sent, the value is stored in the canonical form -
    /// comparison is numeric anyway, and nothing outside can push a stray spelling into
    /// the row.
    pub fn parse_or_now(src: &str) -> Self {
        match parse_time_stamp(src) {
            Some(date_time) => Self::from_date_time(date_time),
            None => Self::now(),
        }
    }

    pub fn as_str(&self) -> &str {
        self.str_value.as_str()
    }

    pub fn as_slice(&self) -> &[u8] {
        self.str_value.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use my_no_sql_abstractions::TIME_STAMP_STR_MAX_LEN;
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use super::JsonTimeStamp;

    #[test]
    fn test_parse_dt() {
        let ts = JsonTimeStamp::parse_or_now("2020-01-01T00:00:00.123");

        assert_eq!("2020-01-01T00:00:00.123", ts.as_str());
    }

    #[test]
    fn test_parse_dt_2() {
        let ts = JsonTimeStamp::parse_or_now("2020-01-01T00:00:00.1234");

        assert_eq!("2020-01-01T00:00:00.1234", ts.as_str());
    }

    #[test]
    fn test_parse_dt_3() {
        let ts = JsonTimeStamp::parse_or_now("2020-01-01T00:00:00");

        assert_eq!("2020-01-01T00:00:00", ts.as_str());
    }

    /// A client's spelling is normalized, not echoed back: the trailing zeros it may
    /// have sent are trimmed, and a truncated value keeps the moment it names.
    #[test]
    fn parse_or_now_normalizes_the_client_spelling() {
        for (src, expected) in [
            ("2020-01-01T00:00:00.000000", "2020-01-01T00:00:00"),
            ("2020-01-01T00:00:00.500000", "2020-01-01T00:00:00.5"),
            ("2020-01-01T00:00:00.123400", "2020-01-01T00:00:00.1234"),
            ("2020-01-01T00:00:00.123456Z", "2020-01-01T00:00:00.123456"),
            ("2020-01-01T00:00:00.1234+00:00", "2020-01-01T00:00:00.1234"),
        ] {
            assert_eq!(
                expected,
                JsonTimeStamp::parse_or_now(src).as_str(),
                "source: {}",
                src
            );
        }
    }

    #[test]
    fn parse_or_now_keeps_the_moment_it_was_given() {
        for src in [
            "2020-01-01T00:00:00.123",
            "2020-01-01T00:00:00.1234",
            "2020-01-01T00:00:00.123456",
            "2020-01-01T00:00:00.123456Z",
            "2020-01-01T00:00:00.123456+00:00",
        ] {
            let expected = DateTimeAsMicroseconds::from_str(src).unwrap();
            let ts = JsonTimeStamp::parse_or_now(src);

            assert_eq!(
                expected.unix_microseconds, ts.date_time.unix_microseconds,
                "source: {}",
                src
            );
        }
    }

    #[test]
    fn parse_or_now_falls_back_to_now_on_garbage() {
        let before = DateTimeAsMicroseconds::now();
        let ts = JsonTimeStamp::parse_or_now("not-a-date");

        assert!(ts.date_time.unix_microseconds >= before.unix_microseconds);
        assert!(ts.as_str().len() <= TIME_STAMP_STR_MAX_LEN);
    }

    /// Regression: the value used to be cut by a hand-written scan, which both lost
    /// precision and could leave a `+00` zone tail behind.
    #[test]
    fn every_constructor_gives_a_canonical_value() {
        let mut values = vec![
            JsonTimeStamp::now(),
            JsonTimeStamp::parse_or_now("2026-08-09T16:44:39.540412"),
            JsonTimeStamp::parse_or_now("not-a-date"),
        ];

        // The spellings that used to end up broken: a fraction with trailing zeros, and
        // one whose 5th digit is significant.
        for micros in [0, 500000, 540000, 540400, 540412, 999999] {
            let mut date_time =
                DateTimeAsMicroseconds::from_str("2026-08-09T16:44:39.000000").unwrap();
            date_time.unix_microseconds += micros;
            values.push(JsonTimeStamp::from_date_time(date_time));
        }

        for ts in values {
            let as_str = ts.as_str();

            assert!(as_str.len() <= TIME_STAMP_STR_MAX_LEN, "value: {}", as_str);
            // A fraction that is present never ends in a zero (a value ending in `0`
            // without a dot is just a second like `:30`).
            assert!(
                !as_str.contains('.') || !as_str.ends_with('0'),
                "value: {}",
                as_str
            );
            assert!(!as_str.contains('+'), "value: {}", as_str);
            assert!(!as_str.contains('Z'), "value: {}", as_str);
            assert_eq!(as_str.as_bytes(), ts.as_slice(), "value: {}", as_str);

            // What is written has to read back as the moment it was built from.
            assert_eq!(
                ts.date_time.unix_microseconds,
                DateTimeAsMicroseconds::from_str(as_str)
                    .unwrap()
                    .unix_microseconds,
                "value: {}",
                as_str
            );
        }
    }

    /// Two rows written within the same 100 µs used to collapse onto one timestamp,
    /// because the value was truncated to 4 fractional digits.
    #[test]
    fn sub_100_microseconds_apart_stay_distinct() {
        let first = DateTimeAsMicroseconds::from_str("2026-08-09T16:44:39.540400").unwrap();
        let second = DateTimeAsMicroseconds::new(first.unix_microseconds + 12);

        assert_ne!(
            JsonTimeStamp::from_date_time(first).as_str(),
            JsonTimeStamp::from_date_time(second).as_str()
        );
    }
}
