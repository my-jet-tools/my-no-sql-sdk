use std::fmt::{Debug, Display};

use rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::{Deserialize, Deserializer};

use crate::{format_time_stamp, parse_time_stamp};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn to_date_time(&self) -> DateTimeAsMicroseconds {
        DateTimeAsMicroseconds::new(self.0)
    }
    pub fn is_default(&self) -> bool {
        self.0 == 0
    }

    pub fn to_i64(&self) -> i64 {
        self.0
    }
}

impl Into<Timestamp> for DateTimeAsMicroseconds {
    fn into(self) -> Timestamp {
        Timestamp(self.unix_microseconds)
    }
}

impl Into<DateTimeAsMicroseconds> for Timestamp {
    fn into(self) -> DateTimeAsMicroseconds {
        DateTimeAsMicroseconds::new(self.0)
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            return f.write_str("null");
        }

        let timestamp = format_time_stamp(self.to_date_time());
        f.write_str(timestamp.as_str())
    }
}

impl Debug for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            f.debug_tuple("Timestamp").field(&"null").finish()
        } else {
            let timestamp = format_time_stamp(self.to_date_time());
            f.debug_tuple("Timestamp").field(&timestamp).finish()
        }
    }
}

impl serde::Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if self.0 == 0 {
            return serializer.serialize_none();
        }

        serializer.serialize_str(format_time_stamp(self.to_date_time()).as_str())
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer);

        if s.is_err() {
            return Ok(Timestamp(0));
        }

        let s = s.unwrap();

        // An unparsable timestamp reads as the default one - the same answer the branch
        // above gives when the field is not a string at all. It must not take the whole
        // deserialization down with it.
        match parse_time_stamp(s.as_str()) {
            Some(datetime) => Ok(Timestamp(datetime.unix_microseconds)),
            None => Ok(Timestamp(0)),
        }
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self(0)
    }
}

impl Into<Timestamp> for i64 {
    fn into(self) -> Timestamp {
        Timestamp(self)
    }
}

impl Into<Timestamp> for u64 {
    fn into(self) -> Timestamp {
        Timestamp(self as i64)
    }
}

pub fn skip_timestamp_serializing(timestamp: &Timestamp) -> bool {
    timestamp.is_default()
}

#[cfg(test)]
mod test {
    use rust_extensions::date_time::{DateTimeAsMicroseconds, DateTimeStruct};
    use serde::{Deserialize, Serialize};

    use super::Timestamp;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct MyType {
        pub my_field: i32,
        #[serde(skip_serializing_if = "super::skip_timestamp_serializing")]
        pub timestamp: Timestamp,
    }

    #[test]
    fn test_serialization() {
        use rust_extensions::date_time::DateTimeAsMicroseconds;

        let my_type = MyType {
            my_field: 15,
            timestamp: DateTimeAsMicroseconds::from_str("2025-01-01T12:00:00.123456")
                .unwrap()
                .into(),
        };

        println!("{:?}", my_type);

        let serialized = serde_json::to_string(&my_type).unwrap();

        println!("Serialized: {}", serialized);

        let result_type: MyType = serde_json::from_str(serialized.as_str()).unwrap();

        assert_eq!(my_type.my_field, result_type.my_field);
        assert_eq!(my_type.timestamp.0, result_type.timestamp.0);
    }

    #[test]
    fn test_serialization_none() {
        use rust_extensions::date_time::DateTimeAsMicroseconds;

        let my_type = MyType {
            my_field: 15,
            timestamp: DateTimeAsMicroseconds::new(0).into(),
        };

        println!("{:?}", my_type);

        let serialized = serde_json::to_string(&my_type).unwrap();

        println!("Serialized: {}", serialized);

        let result_type: MyType = serde_json::from_str(serialized.as_str()).unwrap();

        assert_eq!(my_type.my_field, result_type.my_field);
        assert_eq!(my_type.timestamp.0, result_type.timestamp.0);
    }

    /// The wire form is the canonical one: microsecond precision, trailing zeros of
    /// the fraction trimmed, no zone suffix.
    #[test]
    fn test_serialized_value_is_canonical() {
        let cases = [
            ("2025-01-01T12:00:00", "2025-01-01T12:00:00"),
            ("2025-01-01T12:00:00.5", "2025-01-01T12:00:00.5"),
            ("2025-01-01T12:00:00.123", "2025-01-01T12:00:00.123"),
            ("2025-01-01T12:00:00.1234", "2025-01-01T12:00:00.1234"),
            ("2025-01-01T12:00:00.123400", "2025-01-01T12:00:00.1234"),
            ("2025-01-01T12:00:00.123456", "2025-01-01T12:00:00.123456"),
        ];

        for (src, expected) in cases {
            let timestamp: Timestamp = DateTimeAsMicroseconds::from_str(src).unwrap().into();

            assert_eq!(
                format!("\"{}\"", expected),
                serde_json::to_string(&timestamp).unwrap(),
                "source: {}",
                src
            );

            assert_eq!(expected, timestamp.to_string().as_str(), "source: {}", src);
        }
    }

    #[test]
    fn test_deserialization_of_a_broken_timestamp_does_not_panic() {
        let result: MyType =
            serde_json::from_str(r#"{"my_field":15,"timestamp":"not-a-date"}"#).unwrap();

        assert_eq!(15, result.my_field);
        assert!(result.timestamp.is_default());
    }

    /// A value written before the canonical form existed carries a truncated fraction.
    #[test]
    fn test_deserialization_of_a_legacy_timestamp() {
        let result: MyType =
            serde_json::from_str(r#"{"my_field":15,"timestamp":"2026-08-09T16:44:39.5404"}"#)
                .unwrap();

        let expected = DateTimeAsMicroseconds::from_str("2026-08-09T16:44:39.540400").unwrap();

        assert_eq!(expected.unix_microseconds, result.timestamp.to_i64());
    }

    #[test]
    fn test_from_real_example() {
        let time_stamp = DateTimeAsMicroseconds::from_str("2024-11-29T14:59:15.6145").unwrap();

        let dt_struct: DateTimeStruct = time_stamp.into();

        assert_eq!(dt_struct.year, 2024);
        assert_eq!(dt_struct.month, 11);
        assert_eq!(dt_struct.day, 29);

        assert_eq!(dt_struct.time.hour, 14);
        assert_eq!(dt_struct.time.min, 59);
        assert_eq!(dt_struct.time.sec, 15);
        assert_eq!(dt_struct.time.micros, 614500);
    }
}
