pub const SMALL_COMPILER_MAX_SIZE: usize = 4096;
pub struct SmallContentCompiler {
    content: [u8; SMALL_COMPILER_MAX_SIZE],
    len: usize,
}

impl SmallContentCompiler {
    pub fn new() -> Self {
        Self {
            content: [0; SMALL_COMPILER_MAX_SIZE],
            len: 0,
        }
    }

    pub fn push(&mut self, value: u8) -> bool {
        if self.len == SMALL_COMPILER_MAX_SIZE {
            return false;
        }
        self.content[self.len] = value;
        self.len += 1;
        true
    }

    pub fn extent_from_slice(&mut self, src: &[u8]) -> bool {
        if self.len + src.len() > SMALL_COMPILER_MAX_SIZE {
            return false;
        }
        self.content[self.len..self.len + src.len()].copy_from_slice(src);
        self.len += src.len();
        true
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.content[0..self.len].to_vec()
    }
}
