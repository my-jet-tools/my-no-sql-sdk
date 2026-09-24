pub trait RowKeyParameter {
    fn as_str(&self) -> &str;
}

impl RowKeyParameter for String {
    fn as_str(&self) -> &str {
        self.as_str()
    }
}
