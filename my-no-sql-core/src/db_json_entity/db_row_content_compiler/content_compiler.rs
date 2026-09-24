use super::SmallContentCompiler;

pub enum ContentCompiler {
    Small(SmallContentCompiler),
    Big(Vec<u8>),
}

impl ContentCompiler {
    pub fn new() -> Self {
        Self::Small(SmallContentCompiler::new())
    }

    fn unwrap_as_big(&mut self) -> &mut Vec<u8> {
        match self {
            Self::Small(_) => {
                panic!("unwrap_as_big")
            }
            Self::Big(vec) => return vec,
        }
    }

    fn promote_to_big(&mut self) -> &mut Vec<u8> {
        match self {
            Self::Small(small) => {
                *self = Self::Big(small.to_vec());
                self.unwrap_as_big()
            }
            Self::Big(vec) => vec,
        }
    }

    pub fn push(&mut self, value: u8) {
        match self {
            Self::Small(small) => {
                if !small.push(value) {
                    self.promote_to_big().push(value);
                }
            }
            Self::Big(big) => {
                big.push(value);
            }
        }
    }

    pub fn extend_from_slice(&mut self, value: &[u8]) {
        match self {
            Self::Small(small) => {
                if !small.extent_from_slice(value) {
                    self.promote_to_big().extend_from_slice(value);
                }
            }
            Self::Big(big) => {
                big.extend_from_slice(value);
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Small(small) => small.len(),
            Self::Big(big) => big.len(),
        }
    }

    pub fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Small(small) => small.to_vec(),
            Self::Big(mut big) => {
                big.shrink_to_fit();
                big
            }
        }
    }
}
