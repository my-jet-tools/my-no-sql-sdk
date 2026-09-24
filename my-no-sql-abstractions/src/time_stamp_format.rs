use rust_extensions::date_time::DateTimeAsMicroseconds;

/// Longest a canonical `TimeStamp` can get: `2026-08-09T16:44:39.540412`, all six
/// fractional digits significant. Shorter values are the norm - the fraction is
/// trimmed of its trailing zeros, and drops out entirely on a whole second.
pub const TIME_STAMP_STR_MAX_LEN: usize = 26;

/// Canonical `TimeStamp` spelling: microsecond precision, trailing zeros of the
/// fraction trimmed, no zone suffix (the value is always UTC).
///
/// ```text
/// 540412 µs -> 2026-08-09T16:44:39.540412
/// 540400 µs -> 2026-08-09T16:44:39.5404
/// 500000 µs -> 2026-08-09T16:44:39.5
///      0 µs -> 2026-08-09T16:44:39
/// ```
///
/// Trimming is lossless - `.5404` and `.540400` are the same instant - and keeps the
/// value as short as the moment allows, which is what every row on disk and in memory
/// carries. Lexicographic order still matches chronological order: the fraction
/// compares as a decimal prefix, and a missing one sorts before any present one.
///
/// Writing is strict — every `TimeStamp` this SDK produces has this exact shape.
/// Reading is tolerant, see [`parse_time_stamp`].
pub fn format_time_stamp(dt: DateTimeAsMicroseconds) -> String {
    let rfc3339 = dt.to_rfc3339_utc();
    trim_to_canonical(rfc3339.as_str()).to_string()
}

/// [`format_time_stamp`] appending into an existing buffer.
pub fn push_time_stamp(dt: DateTimeAsMicroseconds, dest: &mut String) {
    let rfc3339 = dt.to_rfc3339_utc();
    dest.push_str(trim_to_canonical(rfc3339.as_str()));
}

/// `to_rfc3339_utc()` gives `2026-08-09T16:44:39.540400Z` - a fixed 6-digit fraction
/// with a `Z` suffix. The canonical form is that string without the suffix and without
/// the fraction's trailing zeros.
fn trim_to_canonical(rfc3339: &str) -> &str {
    let src = rfc3339.strip_suffix('Z').unwrap_or(rfc3339);

    let dot_position = match src.rfind('.') {
        Some(dot_position) => dot_position,
        None => return src,
    };

    // Trimming stops at the dot at the latest, so it can never eat into the seconds.
    let trimmed = src.trim_end_matches('0');

    if trimmed.len() == dot_position + 1 {
        // The whole fraction was zeros - the dot goes with them.
        &src[..dot_position]
    } else {
        trimmed
    }
}

/// Reads any spelling a `TimeStamp` can arrive in - 0..9 fractional digits, with `Z`,
/// with `+00:00` or with no suffix at all - so rows written before the canonical form
/// existed keep working. Only the writing side is strict.
pub fn parse_time_stamp(src: &str) -> Option<DateTimeAsMicroseconds> {
    DateTimeAsMicroseconds::from_str(src)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(src: &str) -> DateTimeAsMicroseconds {
        DateTimeAsMicroseconds::from_str(src).unwrap()
    }

    #[test]
    fn format_trims_the_trailing_zeros_of_the_fraction() {
        let cases = [
            // A whole second loses the fraction altogether, dot included.
            ("2026-08-09T16:44:39", "2026-08-09T16:44:39"),
            ("2026-08-09T16:44:39.000000", "2026-08-09T16:44:39"),
            ("2026-08-09T16:44:39.5", "2026-08-09T16:44:39.5"),
            ("2026-08-09T16:44:39.500000", "2026-08-09T16:44:39.5"),
            ("2026-08-09T16:44:39.54", "2026-08-09T16:44:39.54"),
            ("2026-08-09T16:44:39.5404", "2026-08-09T16:44:39.5404"),
            ("2026-08-09T16:44:39.540400", "2026-08-09T16:44:39.5404"),
            // Only the trailing zeros go - a significant digit behind them stays,
            // and so does every leading zero of the fraction.
            ("2026-08-09T16:44:39.540412", "2026-08-09T16:44:39.540412"),
            ("2026-08-09T16:44:39.540010", "2026-08-09T16:44:39.54001"),
            ("2026-08-09T16:44:39.000001", "2026-08-09T16:44:39.000001"),
            ("2026-08-09T16:44:39.000100", "2026-08-09T16:44:39.0001"),
            ("2026-08-09T16:44:39.999999", "2026-08-09T16:44:39.999999"),
        ];

        for (src, expected) in cases {
            let result = format_time_stamp(dt(src));
            assert_eq!(expected, result.as_str(), "source: {}", src);
            assert!(
                result.len() <= TIME_STAMP_STR_MAX_LEN,
                "source: {}, result: {}",
                src,
                result
            );
        }
    }

    /// Trimming is only allowed to shorten the text, never to move the moment.
    #[test]
    fn format_never_loses_a_microsecond() {
        let base = dt("2026-08-09T16:44:39").unix_microseconds;

        for micros in 0..1000i64 {
            for step in [1i64, 271, 999] {
                let value = DateTimeAsMicroseconds::new(base + micros * step);
                let formatted = format_time_stamp(value);

                assert_eq!(
                    value.unix_microseconds,
                    parse_time_stamp(formatted.as_str())
                        .unwrap()
                        .unix_microseconds,
                    "formatted: {}",
                    formatted
                );
            }
        }
    }

    /// Shorter values must not reorder against longer ones - the whole point of a
    /// canonical spelling is that sorting the text sorts the moments.
    #[test]
    fn lexicographic_order_matches_chronological_order() {
        let base = dt("2026-08-09T16:44:39").unix_microseconds;

        let mut previous: Option<String> = None;

        for micros in [
            0i64, 1, 100, 1000, 100000, 500000, 540000, 540400, 540412, 999999,
        ] {
            let formatted = format_time_stamp(DateTimeAsMicroseconds::new(base + micros));

            if let Some(previous) = previous {
                assert!(
                    previous.as_str() < formatted.as_str(),
                    "{} must sort before {}",
                    previous,
                    formatted
                );
            }

            previous = Some(formatted);
        }
    }

    #[test]
    fn format_has_no_zone_suffix() {
        let result = format_time_stamp(dt("2026-08-09T16:44:39.540000"));

        assert!(!result.contains('+'), "unexpected zone suffix: {}", result);
        assert!(!result.contains('Z'), "unexpected zone suffix: {}", result);
    }

    #[test]
    fn push_appends_to_the_buffer() {
        let mut dest = String::from("TimeStamp:");
        push_time_stamp(dt("2026-08-09T16:44:39.540400"), &mut dest);

        assert_eq!("TimeStamp:2026-08-09T16:44:39.5404", dest.as_str());
    }

    #[test]
    fn parse_of_format_is_the_same_moment() {
        let cases = [
            "2026-08-09T16:44:39",
            "2026-08-09T16:44:39.000001",
            "2026-08-09T16:44:39.540400",
            "2026-08-09T16:44:39.999999",
            "1970-01-01T00:00:00",
            "2099-12-31T23:59:59.999999",
        ];

        for src in cases {
            let src = dt(src);
            let result = parse_time_stamp(format_time_stamp(src).as_str()).unwrap();

            assert_eq!(src.unix_microseconds, result.unix_microseconds);
        }
    }

    /// Whatever a client sends - any amount of fractional digits, any zone spelling -
    /// has to land on the same moment.
    #[test]
    fn parse_accepts_every_spelling_of_the_same_moment() {
        let expected = dt("2026-08-09T16:44:39.540000").unix_microseconds;

        let cases = [
            "2026-08-09T16:44:39.54",
            "2026-08-09T16:44:39.540",
            "2026-08-09T16:44:39.5400",
            "2026-08-09T16:44:39.540000",
            "2026-08-09T16:44:39.5400000",
            "2026-08-09T16:44:39.540Z",
            "2026-08-09T16:44:39.540000Z",
            "2026-08-09T16:44:39.540+00:00",
            "2026-08-09T16:44:39.540000+00:00",
        ];

        for src in cases {
            assert_eq!(
                expected,
                parse_time_stamp(src).unwrap().unix_microseconds,
                "source: {}",
                src
            );
        }
    }

    #[test]
    fn parse_accepts_a_value_with_no_fraction_at_all() {
        let expected = dt("2026-08-09T16:44:39").unix_microseconds;

        for src in [
            "2026-08-09T16:44:39",
            "2026-08-09T16:44:39Z",
            "2026-08-09T16:44:39+00:00",
            "2026-08-09T16:44:39.000000",
        ] {
            assert_eq!(
                expected,
                parse_time_stamp(src).unwrap().unix_microseconds,
                "source: {}",
                src
            );
        }
    }

    /// Rows written before the canonical form existed carry a truncated 4-digit
    /// fraction. They must keep reading as the moment they were written with.
    #[test]
    fn parse_of_a_legacy_truncated_value_matches_its_canonical_form() {
        let legacy = parse_time_stamp("2026-08-09T16:44:39.5404").unwrap();
        let canonical = parse_time_stamp("2026-08-09T16:44:39.540400").unwrap();

        assert_eq!(canonical.unix_microseconds, legacy.unix_microseconds);
    }

    #[test]
    fn parse_of_garbage_is_none() {
        for src in ["", "not-a-date", "hello world"] {
            assert!(parse_time_stamp(src).is_none(), "source: {}", src);
        }
    }
}
