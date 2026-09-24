//! A key is addressed by its **logical** value - the string the client meant - while the JSON it
//! arrived in keeps whatever escaping the client's serializer produced. These tests pin that
//! down: parsing resolves the escapes once, and everything derived from the row (its accessors,
//! the partition index, a row restored from disk, a compressed row) agrees on the same key.

use std::sync::Arc;

use crate::db::{DbPartition, DbRow};
use crate::db_json_entity::{DbJsonEntity, JsonTimeStamp};

/// `(what the JSON carries between the quotes, what the key logically is)`
///
/// Every entry is valid JSON for exactly one logical key - which is the whole point: the client
/// has no way to send `demo\DIRNGUSDENM50D` other than escaping it.
const ESCAPED_KEYS: [(&str, &str); 8] = [
    (r#"a\\b"#, "a\\b"),
    (r#"a\"b"#, "a\"b"),
    (r#"a\nb"#, "a\nb"),
    (r#"a\tb"#, "a\tb"),
    (r#"a\/b"#, "a/b"),
    // .NET's System.Text.Json escapes every non-ASCII char with its default encoder
    ("\\u0434\\u0435\\u043c\\u043e", "демо"),
    // a surrogate pair - one code point spelled as two escapes
    ("\\ud83d\\ude00", "😀"),
    // the case from the field report
    (r#"demo\\DIRNGUSDENM50D"#, "demo\\DIRNGUSDENM50D"),
];

fn parse(json: &str) -> DbRow {
    let time_stamp = JsonTimeStamp::now();
    DbJsonEntity::parse_into_db_row(json.as_bytes().into(), &time_stamp).unwrap()
}

fn parse_with_row_key(escaped_row_key: &str) -> DbRow {
    parse(&format!(
        r#"{{"PartitionKey":"pk","RowKey":"{}","Value":"v"}}"#,
        escaped_row_key
    ))
}

fn parse_with_partition_key(escaped_partition_key: &str) -> DbRow {
    parse(&format!(
        r#"{{"PartitionKey":"{}","RowKey":"rk","Value":"v"}}"#,
        escaped_partition_key
    ))
}

#[test]
fn row_key_is_addressed_by_its_logical_value() {
    for (escaped, logical) in ESCAPED_KEYS {
        let db_row = parse_with_row_key(escaped);

        assert_eq!(db_row.get_row_key(), logical, "source: {}", escaped);

        // ...and the stored json still carries the escaped form byte for byte: the fix changes
        // the key which is derived from the payload, never the payload itself.
        let raw = std::str::from_utf8(db_row.get_src_as_slice()).unwrap();
        assert!(
            raw.contains(&format!(r#""RowKey":"{}""#, escaped)),
            "raw json got rewritten: {}",
            raw
        );
    }
}

#[test]
fn partition_key_is_addressed_by_its_logical_value() {
    for (escaped, logical) in ESCAPED_KEYS {
        let db_row = parse_with_partition_key(escaped);

        assert_eq!(db_row.get_partition_key(), logical, "source: {}", escaped);

        let raw = std::str::from_utf8(db_row.get_src_as_slice()).unwrap();
        assert!(
            raw.contains(&format!(r#""PartitionKey":"{}""#, escaped)),
            "raw json got rewritten: {}",
            raw
        );
    }
}

/// The ordinary key - no escapes - has to stay a borrow into `raw`: unescaping every key would
/// put an allocation per row on the parse path.
#[test]
fn a_key_without_escapes_is_borrowed_from_the_raw_json() {
    let db_row = parse(r#"{"PartitionKey":"pk","RowKey":"rk","Value":"v"}"#);

    let raw = db_row.get_src_as_slice().as_ptr_range();

    assert!(raw.contains(&db_row.get_partition_key().as_ptr()));
    assert!(raw.contains(&db_row.get_row_key().as_ptr()));
}

/// ...while the key which does carry escapes can not be borrowed - it is a different string
/// than the bytes on the wire.
#[test]
fn a_key_with_escapes_is_owned() {
    let db_row = parse_with_row_key(r#"a\\b"#);

    let raw = db_row.get_src_as_slice().as_ptr_range();

    assert!(raw.contains(&db_row.get_partition_key().as_ptr()));
    assert!(!raw.contains(&db_row.get_row_key().as_ptr()));
}

/// Two json spellings of one logical key - `a\\b` and the same backslash written as `\` -
/// so the second write replaces the first one instead of creating a twin row.
#[test]
fn two_json_spellings_of_one_key_are_a_single_row() {
    let mut partition = DbPartition::new("pk".to_string());

    partition.insert_or_replace_row(Arc::new(parse(
        "{\"PartitionKey\":\"pk\",\"RowKey\":\"a\\\\b\",\"Value\":\"first\"}",
    )));

    partition.insert_or_replace_row(Arc::new(parse(
        "{\"PartitionKey\":\"pk\",\"RowKey\":\"a\\u005Cb\",\"Value\":\"second\"}",
    )));

    assert_eq!(partition.rows_count(), 1);

    let db_row = partition.get_row("a\\b").expect("row is addressed by its logical key");
    assert!(std::str::from_utf8(db_row.get_src_as_slice())
        .unwrap()
        .contains("second"));

    assert!(partition.remove_row("a\\b").is_some());
    assert_eq!(partition.rows_count(), 0);
}

/// A row is looked up by the logical key in the partition index, for every escaping flavour.
#[test]
fn every_escaped_key_is_found_in_the_partition_index() {
    for (escaped, logical) in ESCAPED_KEYS {
        let mut partition = DbPartition::new("pk".to_string());
        partition.insert_or_replace_row(Arc::new(parse_with_row_key(escaped)));

        assert!(
            partition.get_row(logical).is_some(),
            "not found by the logical key: {}",
            escaped
        );

        // the escaped spelling is NOT the key - that is exactly what used to be indexed
        if escaped != logical {
            assert!(
                partition.get_row(escaped).is_none(),
                "still indexed by the raw json form: {}",
                escaped
            );
        }
    }
}

/// Persist and load back: `restore_into_db_row` goes through the same `DbRow::new`, so a row
/// written before the fix becomes addressable by its logical key on the next restart, with no
/// data migration.
#[test]
fn a_restored_row_keeps_the_logical_key() {
    for (escaped, logical) in ESCAPED_KEYS {
        let db_row = parse_with_row_key(escaped);
        let persisted = db_row.to_vec();

        let restored = DbJsonEntity::restore_into_db_row(persisted.clone()).unwrap();

        assert_eq!(restored.get_row_key(), logical, "source: {}", escaped);
        assert_eq!(restored.get_partition_key(), "pk");
        assert_eq!(restored.to_vec(), persisted);
    }
}

/// The write path (`insert` / `replace` on the server) addresses the existing row through
/// `DbJsonEntityWithContent` - it has to agree with what the row itself reports.
#[test]
fn db_json_entity_with_content_reports_logical_keys() {
    let time_stamp = JsonTimeStamp::now();

    for (escaped, logical) in ESCAPED_KEYS {
        let json = format!(
            r#"{{"PartitionKey":"{}","RowKey":"{}","Value":"v"}}"#,
            escaped, escaped
        );

        let entity = DbJsonEntity::parse(json.as_bytes(), &time_stamp).unwrap();

        assert_eq!(entity.get_partition_key(), logical, "source: {}", escaped);
        assert_eq!(entity.get_row_key(), logical, "source: {}", escaped);

        let db_row = entity.into_db_row().unwrap();
        assert_eq!(db_row.get_partition_key(), logical);
        assert_eq!(db_row.get_row_key(), logical);
    }
}

/// The keys are also what the parsed entity reports against its own slice - the tcp reader
/// indexes a lazily deserialized row through exactly this accessor.
#[test]
fn parsed_entity_reports_logical_keys_against_its_slice() {
    for (escaped, logical) in ESCAPED_KEYS {
        let json = format!(
            r#"{{"PartitionKey":"{}","RowKey":"{}","Value":"v"}}"#,
            escaped, escaped
        );

        let entity = DbJsonEntity::from_slice(json.as_bytes()).unwrap();

        assert_eq!(
            entity.get_partition_key(json.as_bytes()),
            logical,
            "source: {}",
            escaped
        );
        assert_eq!(
            entity.get_row_key(json.as_bytes()),
            logical,
            "source: {}",
            escaped
        );
    }
}

#[cfg(feature = "master-node")]
#[test]
fn compression_round_trip_keeps_the_logical_key() {
    for (escaped, logical) in ESCAPED_KEYS {
        let plain = Arc::new(parse_with_row_key(escaped));

        let compressed = DbRow::compress_arc(plain.clone());
        assert_eq!(compressed.get_row_key(), logical, "source: {}", escaped);
        assert_eq!(compressed.get_partition_key(), "pk");

        let back_to_plain = DbRow::decompress_arc(compressed);
        assert_eq!(back_to_plain.get_row_key(), logical, "source: {}", escaped);
        assert_eq!(back_to_plain.to_vec(), plain.to_vec());
    }
}
