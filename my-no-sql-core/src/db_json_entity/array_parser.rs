pub use consts::*;

use super::{consts, json_utils::FoundResult, JsonParseError};

pub struct JsonArrayIterator<'t> {
    data: &'t [u8],
    pos: usize,
}

impl<'t> JsonArrayIterator<'t> {
    pub fn new(data: &'t [u8]) -> Self {
        let result = Self { data, pos: 0 };

        result
    }

    fn init(&mut self) -> Result<(), JsonParseError> {
        if self.pos > 0 {
            return Ok(());
        }

        let result = super::json_utils::next_token_must_be(self.data, 0, consts::OPEN_ARRAY);

        match result {
            FoundResult::Ok(pos) => {
                self.pos = pos;
                return Ok(());
            }
            FoundResult::EndOfJson => {
                return Err(JsonParseError::new(format!(
                    "Can not find start of the array token"
                )));
            }
            FoundResult::InvalidTokenFound { found_token, pos } => {
                return Err(JsonParseError::new(format!(
                    "We were looking start of array token but found '{}' at position {}",
                    found_token as char, pos
                )));
            }
        }
    }
}

impl<'t> Iterator for JsonArrayIterator<'t> {
    type Item = Result<&'t [u8], JsonParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.init();

        if let Err(err) = result {
            return Some(Err(err));
        }

        let mut start_pos = self.pos + 1;
        loop {
            let found_token_result =
                super::json_utils::next_token_must_be(self.data, start_pos, consts::OPEN_BRACKET);

            match found_token_result {
                FoundResult::Ok(start_pos) => {
                    let result = super::json_utils::read_json_object(self.data, start_pos);

                    match result {
                        Ok(pos) => {
                            self.pos = pos;
                            return Some(Ok(&self.data[start_pos..pos + 1]));
                        }
                        Err(err) => {
                            return Some(Err(err));
                        }
                    }
                }
                FoundResult::InvalidTokenFound { found_token, pos } => {
                    if found_token == consts::CLOSE_ARRAY {
                        return None;
                    } else if found_token == consts::COMMA {
                        start_pos = pos + 1;
                    } else {
                        return Some(Err(
                            JsonParseError::new(format!("Can not find open object token. We start searching at {} but found token '{}' at the pos {}", start_pos,found_token as char, pos))));
                    }
                }
                FoundResult::EndOfJson => {
                    return None;
                }
            }
        }
    }
}

pub trait ArrayToJsonObjectsSplitter<'t> {
    fn split_array_json_to_objects(self) -> JsonArrayIterator<'t>;
}

impl<'t> ArrayToJsonObjectsSplitter<'t> for &'t [u8] {
    fn split_array_json_to_objects(self) -> JsonArrayIterator<'t> {
        return JsonArrayIterator::new(self);
    }
}

#[cfg(test)]
mod tests {

    use std::{fs::File, io::Read};

    use crate::db_json_entity::{DbJsonEntity, JsonTimeStamp};

    use super::*;

    #[test]
    pub fn test_basic_json_array_split() {
        let json = r###"[{"id":1},{"id":2},{"id":3}]"###;

        println!("{}", json);

        let mut i = 0;
        for sub_json in json.as_bytes().split_array_json_to_objects() {
            let sub_json = sub_json.unwrap();
            i += 1;
            println!("{}", i);
            println!("{}", std::str::from_utf8(sub_json).unwrap());

            assert_eq!(
                format!("{{\"id\":{}}}", i),
                std::str::from_utf8(sub_json).unwrap()
            );
        }
    }

    #[test]
    pub fn test_basic_json_array_split_case_2() {
        let json = r###"[{"id":1} , {"id":2} , {"id":3}]"###;

        println!("{}", json);

        let mut i = 0;
        for sub_json in json.as_bytes().split_array_json_to_objects() {
            let sub_json = sub_json.unwrap();
            i += 1;

            assert_eq!(
                format!("{{\"id\":{}}}", i),
                std::str::from_utf8(sub_json).unwrap()
            );
        }
    }

    #[test]
    pub fn test_basic_json_array_split_case_3() {
        let json = r###"[{"id":1}, {"id":2} ,{"id":3}]"###;

        println!("{}", json);

        let mut i = 0;
        for sub_json in json.as_bytes().split_array_json_to_objects() {
            let sub_json = sub_json.unwrap();
            i += 1;

            assert_eq!(
                format!("{{\"id\":{}}}", i),
                std::str::from_utf8(sub_json).unwrap()
            );
        }
    }
    #[test]
    pub fn parse_empty_array() {
        let json = r###"[]"###;

        let mut i = 0;
        for sub_json in json.as_bytes().split_array_json_to_objects() {
            let sub_json = sub_json.unwrap();

            println!("{}", sub_json.len());
            i += 1;
        }

        assert_eq!(0, i);
    }

    #[test]
    pub fn parse_some_reallife_data() {
        println!("{:?}", std::env::current_dir().unwrap());
        let mut src_file = File::open("./test/test-case.json").unwrap();

        let mut line = Vec::new();
        src_file.read_to_end(&mut line).unwrap();

        for db_entity_json in line.as_slice().split_array_json_to_objects() {
            let db_entity_json = db_entity_json.unwrap();

            let db_json = DbJsonEntity::parse(db_entity_json).unwrap();

            let json_timestamp = JsonTimeStamp::now();
            db_json.to_db_row(&json_timestamp);
        }
    }

    #[test]
    pub fn parse_some_real_life_data_case_2() {
        println!("{:?}", std::env::current_dir().unwrap());
        let mut src_file = File::open("./test/test_case_2.json").unwrap();

        let mut line = Vec::new();
        src_file.read_to_end(&mut line).unwrap();

        for db_entity_json in line.as_slice().split_array_json_to_objects() {
            let db_entity_json = db_entity_json.unwrap();

            let db_json = DbJsonEntity::parse(db_entity_json).unwrap();

            let json_timestamp = JsonTimeStamp::now();
            db_json.to_db_row(&json_timestamp);
        }
    }
}
