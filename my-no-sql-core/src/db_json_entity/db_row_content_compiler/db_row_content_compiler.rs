use my_json::json_reader::{JsonFieldNameRef, JsonValueRef};

use crate::db_json_entity::{JsonKeyValuePosition, KeyValueContentPosition};

use super::{content_compiler::ContentCompiler, SMALL_COMPILER_MAX_SIZE};

pub struct DbRowContentCompiler {
    first_line: bool,
    pub content: ContentCompiler,
}

impl DbRowContentCompiler {
    pub fn new(expected_size: usize) -> Self {
        if expected_size <= SMALL_COMPILER_MAX_SIZE {
            return Self {
                content: ContentCompiler::new(),
                first_line: false,
            };
        }

        let mut content = Vec::new();

        content.shrink_to(expected_size + 32);

        Self {
            content: ContentCompiler::Big(content),
            first_line: false,
        }
    }

    fn append_first_line(&mut self) {
        if self.first_line {
            self.content.push(b',');
        } else {
            self.content.push(b'{');
            self.first_line = true;
        }
    }

    pub fn append(
        &mut self,
        name: &JsonFieldNameRef,
        the_value: &JsonValueRef,
    ) -> JsonKeyValuePosition {
        self.append_first_line();
        let mut key = KeyValueContentPosition {
            start: self.content.len(),
            end: 0,
        };

        self.content.extend_from_slice(name.as_slice());

        key.end = self.content.len();

        self.content.push(b':');

        let mut value = KeyValueContentPosition {
            start: self.content.len(),
            end: 0,
        };
        self.content.extend_from_slice(the_value.as_slice());

        value.end = self.content.len();

        JsonKeyValuePosition { key, value }
    }

    pub fn append_str_value(&mut self, name: &str, value: &str) -> JsonKeyValuePosition {
        self.append_first_line();
        let mut key = KeyValueContentPosition {
            start: self.content.len(),
            end: 0,
        };

        self.content.push(b'"');
        self.content.extend_from_slice(name.as_bytes());
        self.content.push(b'"');
        key.end = self.content.len();

        self.content.push(b':');

        let mut value_pos = KeyValueContentPosition {
            start: self.content.len(),
            end: 0,
        };
        self.content.push(b'"');
        self.content.extend_from_slice(value.as_bytes());
        self.content.push(b'"');
        key.end = self.content.len();

        value_pos.end = self.content.len();

        JsonKeyValuePosition {
            key,
            value: value_pos,
        }
    }

    pub fn into_vec(mut self) -> Vec<u8> {
        self.content.push(b'}');
        self.content.into_vec()
    }
}
