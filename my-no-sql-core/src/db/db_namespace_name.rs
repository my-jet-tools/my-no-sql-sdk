use std::{fmt::Display, sync::Arc};

use my_no_sql_abstractions::DEFAULT_NAMESPACE;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DbNamespaceName(Arc<String>);

impl DbNamespaceName {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    pub fn is_default(&self) -> bool {
        self.as_str() == DEFAULT_NAMESPACE
    }
}

impl Default for DbNamespaceName {
    fn default() -> Self {
        Self(Arc::new(DEFAULT_NAMESPACE.to_string()))
    }
}

impl Display for DbNamespaceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for DbNamespaceName {
    fn from(src: String) -> Self {
        Self(Arc::new(src))
    }
}

impl<'s> From<&'s str> for DbNamespaceName {
    fn from(src: &'s str) -> Self {
        Self(Arc::new(src.to_string()))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_default_is_a_default_namespace() {
        let namespace: DbNamespaceName = Default::default();

        assert_eq!(DEFAULT_NAMESPACE, namespace.as_str());
        assert_eq!(true, namespace.is_default());
    }

    #[test]
    fn test_from_str_and_from_string() {
        let from_str: DbNamespaceName = "alpha".into();
        let from_string: DbNamespaceName = "alpha".to_string().into();

        assert_eq!("alpha", from_str.as_str());
        assert_eq!(from_str, from_string);
        assert_eq!(false, from_str.is_default());
    }
}
