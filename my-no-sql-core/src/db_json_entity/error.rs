use my_json::json_reader::JsonParseError;

#[derive(Debug)]
pub enum DbEntityParseFail {
    FieldPartitionKeyIsRequired,
    FieldRowKeyIsRequired,
    FieldPartitionKeyCanNotBeNull,
    FieldRowKeyCanNotBeNull,
    JsonParseError(JsonParseError),
    PartitionKeyIsTooLong,
    FieldTimeStampIsRequired {
        partition_key: String,
        row_key: String,
    },
}

impl From<JsonParseError> for DbEntityParseFail {
    fn from(src: JsonParseError) -> Self {
        Self::JsonParseError(src)
    }
}
