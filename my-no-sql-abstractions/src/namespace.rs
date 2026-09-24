use std::fmt::Display;

/// Namespace every table and row lives in when no namespace is configured explicitly.
pub const DEFAULT_NAMESPACE: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    NamespaceNameValidationError(String),
}

impl ValidationError {
    pub fn as_str(&self) -> &str {
        match self {
            Self::NamespaceNameValidationError(reason) => reason.as_str(),
        }
    }
}

impl Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::error::Error for ValidationError {}

/// Namespace name follows the same rules as a table name - only `[a-z0-9-]` are allowed, the
/// name neither starts nor ends with `-` and it has no two `-` in a row. The only difference
/// is the minimal length - a namespace can be as short as a single symbol.
///
/// Upper case is an error and is never silently lower-cased: a namespace is auto-created by
/// the server, so a typo has to fail instead of bringing a garbage namespace to life.
pub fn validate_namespace_name(name: &str) -> Result<(), ValidationError> {
    if name.len() < 1 {
        return Err(ValidationError::NamespaceNameValidationError(
            "Namespace name must contain at least 1 symbol".to_string(),
        ));
    }

    if name.len() > 63 {
        return Err(ValidationError::NamespaceNameValidationError(
            "Namespace name must contain 1-63 symbols".to_string(),
        ));
    }

    let as_bytes = name.as_bytes();

    let mut prev_char: Option<char> = None;

    for (i, s) in as_bytes.iter().enumerate() {
        let c = *s as char;

        if i == 0 {
            if c == '-' {
                return Err(ValidationError::NamespaceNameValidationError(format!(
                    "Namespace can not be started from '-' symbol",
                )));
            }
        }

        if i == as_bytes.len() - 1 {
            if c == '-' {
                return Err(ValidationError::NamespaceNameValidationError(format!(
                    "Namespace can not be ended with '-' symbol",
                )));
            }
        }

        if !symbol_is_allowed(c) {
            return Err(ValidationError::NamespaceNameValidationError(format!(
                "Symbol {} is not allowed which stays at position {}",
                c, i
            )));
        }

        if c == '-' {
            if let Some(prev_char) = prev_char {
                if prev_char == '-' {
                    return Err(ValidationError::NamespaceNameValidationError(format!(
                        "Two following '-' symbols are not allowed. Check please position {}",
                        i
                    )));
                }
            }
        }

        prev_char = Some(c);
    }

    Ok(())
}

fn symbol_is_allowed(c: char) -> bool {
    c == '-' || is_digit(c) || is_lower_case_latin_letter(c)
}

fn is_digit(c: char) -> bool {
    return c >= '0' && c <= '9';
}

fn is_lower_case_latin_letter(c: char) -> bool {
    return c >= 'a' && c <= 'z';
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_default_namespace_is_a_valid_name() {
        assert_eq!(true, validate_namespace_name(DEFAULT_NAMESPACE).is_ok());
    }

    #[test]
    fn test_lower_cases_digits_and_dashes_are_ok() {
        assert_eq!(true, validate_namespace_name("my-namespace-5").is_ok());
    }

    #[test]
    fn test_single_symbol_is_ok() {
        assert_eq!(true, validate_namespace_name("a").is_ok());
    }

    #[test]
    fn test_63_symbols_is_ok_and_64_is_not() {
        let name_63 = "a".repeat(63);
        let name_64 = "a".repeat(64);

        assert_eq!(true, validate_namespace_name(name_63.as_str()).is_ok());
        assert_eq!(false, validate_namespace_name(name_64.as_str()).is_ok());
    }

    #[test]
    fn test_empty_name_is_not_ok() {
        assert_eq!(false, validate_namespace_name("").is_ok());
    }

    #[test]
    fn test_upper_case_is_not_ok() {
        assert_eq!(false, validate_namespace_name("Alpha").is_ok());
    }

    #[test]
    fn test_two_dashes_are_not_ok() {
        assert_eq!(false, validate_namespace_name("my--namespace").is_ok());
    }

    #[test]
    fn test_started_with_dash_is_not_ok() {
        assert_eq!(false, validate_namespace_name("-my-namespace").is_ok());
    }

    #[test]
    fn test_ended_with_dash_is_not_ok() {
        assert_eq!(false, validate_namespace_name("my-namespace-").is_ok());
    }

    #[test]
    fn test_not_allowed_symbols() {
        assert_eq!(false, validate_namespace_name("my_namespace").is_ok());
        assert_eq!(false, validate_namespace_name("my.namespace").is_ok());
        assert_eq!(false, validate_namespace_name("my namespace").is_ok());
        assert_eq!(false, validate_namespace_name("тест").is_ok());
    }
}
