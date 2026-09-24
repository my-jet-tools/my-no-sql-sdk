use std::fmt::Display;

use crate::{validate_namespace_name, ValidationError, DEFAULT_NAMESPACE};

const HOST_KEY: &str = "host";
const NAMESPACE_KEY: &str = "ns";

/// Parsed MyNoSqlServer connection string - the same shape for the reader and for the writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionString {
    pub host: String,
    pub namespace: Option<String>,
}

impl ConnectionString {
    /// Namespace all the tables and rows of this connection live in.
    pub fn get_namespace(&self) -> &str {
        match self.namespace.as_ref() {
            Some(namespace) => namespace.as_str(),
            None => DEFAULT_NAMESPACE,
        }
    }

    /// Namespace which has to be transferred to the server - or `None` when there is nothing
    /// to transfer.
    ///
    /// The default namespace is never sent: it is what the server uses anyway, and staying
    /// silent about it keeps the servers which know nothing about namespaces working.
    pub fn get_namespace_to_send(&self) -> Option<&str> {
        let namespace = self.namespace.as_ref()?;

        if namespace == DEFAULT_NAMESPACE {
            return None;
        }

        Some(namespace.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionStringError {
    HostIsNotSpecified,
    HostIsEmpty,
    NotAKeyValuePair(String),
    UnknownKey(String),
    DuplicatedKey(String),
    InvalidNamespaceName(ValidationError),
}

impl Display for ConnectionStringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HostIsNotSpecified => {
                write!(f, "Connection string must contain '{}' key", HOST_KEY)
            }
            Self::HostIsEmpty => write!(f, "Value of '{}' key can not be empty", HOST_KEY),
            Self::NotAKeyValuePair(token) => write!(
                f,
                "Connection string element '{}' is not a 'key=value' pair",
                token
            ),
            Self::UnknownKey(key) => write!(f, "Connection string key '{}' is unknown", key),
            Self::DuplicatedKey(key) => {
                write!(f, "Connection string key '{}' is specified twice", key)
            }
            Self::InvalidNamespaceName(err) => write!(f, "Invalid namespace name. {}", err),
        }
    }
}

impl std::error::Error for ConnectionStringError {}

/// Parses a connection string of both formats:
///
/// * starts with `host=` - the new format: `;` separated `key=value` pairs, e.g.
///   `host=http://localhost:5123;ns=alpha`. Spaces around `;` and `=` are trimmed, keys are
///   case insensitive, `host` is mandatory, `ns` is optional and an unknown key is an error -
///   it is better not to start at all than to silently read the wrong namespace;
/// * anything else - the legacy format: the whole string is the host and the namespace is the
///   default one (`http://xxx`, `10.0.0.1:5123`).
pub fn parse_connection_string(src: &str) -> Result<ConnectionString, ConnectionStringError> {
    if !is_new_format(src) {
        return Ok(ConnectionString {
            host: src.to_string(),
            namespace: None,
        });
    }

    let mut host = None;
    let mut namespace = None;

    for token in src.split(';') {
        let token = token.trim();

        if token.is_empty() {
            continue;
        }

        let (key, value) = match token.split_once('=') {
            Some(result) => result,
            None => {
                return Err(ConnectionStringError::NotAKeyValuePair(token.to_string()));
            }
        };

        let key = key.trim().to_lowercase();
        let value = value.trim();

        match key.as_str() {
            HOST_KEY => {
                if host.is_some() {
                    return Err(ConnectionStringError::DuplicatedKey(HOST_KEY.to_string()));
                }

                if value.is_empty() {
                    return Err(ConnectionStringError::HostIsEmpty);
                }

                host = Some(value.to_string());
            }
            NAMESPACE_KEY => {
                if namespace.is_some() {
                    return Err(ConnectionStringError::DuplicatedKey(
                        NAMESPACE_KEY.to_string(),
                    ));
                }

                if let Err(err) = validate_namespace_name(value) {
                    return Err(ConnectionStringError::InvalidNamespaceName(err));
                }

                namespace = Some(value.to_string());
            }
            _ => return Err(ConnectionStringError::UnknownKey(key)),
        }
    }

    match host {
        Some(host) => Ok(ConnectionString { host, namespace }),
        None => Err(ConnectionStringError::HostIsNotSpecified),
    }
}

/// New format is detected by the very first `key=value` pair having the `host` key - everything
/// else stays the legacy host as it always was.
fn is_new_format(src: &str) -> bool {
    let first_token = match src.split(';').next() {
        Some(token) => token,
        None => return false,
    };

    match first_token.split_once('=') {
        Some((key, _)) => key.trim().eq_ignore_ascii_case(HOST_KEY),
        None => false,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_legacy_http_connection_string() {
        let result = parse_connection_string("http://localhost:5123").unwrap();

        assert_eq!("http://localhost:5123", result.host);
        assert_eq!(None, result.namespace);
        assert_eq!(DEFAULT_NAMESPACE, result.get_namespace());
        assert_eq!(None, result.get_namespace_to_send());
    }

    #[test]
    fn test_legacy_host_port_connection_string() {
        let result = parse_connection_string("10.0.0.1:5123").unwrap();

        assert_eq!("10.0.0.1:5123", result.host);
        assert_eq!(None, result.namespace);
    }

    #[test]
    fn test_legacy_connection_string_with_query_param_is_not_confused_with_the_new_format() {
        let result = parse_connection_string("http://localhost:5123?param=value").unwrap();

        assert_eq!("http://localhost:5123?param=value", result.host);
        assert_eq!(None, result.namespace);
    }

    #[test]
    fn test_new_format_with_namespace() {
        let result = parse_connection_string("host=http://localhost:5123;ns=alpha").unwrap();

        assert_eq!("http://localhost:5123", result.host);
        assert_eq!(Some("alpha".to_string()), result.namespace);
        assert_eq!("alpha", result.get_namespace());
        assert_eq!(Some("alpha"), result.get_namespace_to_send());
    }

    #[test]
    fn test_new_format_with_host_port_and_namespace() {
        let result = parse_connection_string("host=10.0.0.1:5123;ns=alpha").unwrap();

        assert_eq!("10.0.0.1:5123", result.host);
        assert_eq!(Some("alpha".to_string()), result.namespace);
    }

    #[test]
    fn test_new_format_without_namespace() {
        let result = parse_connection_string("host=http://localhost:5123").unwrap();

        assert_eq!("http://localhost:5123", result.host);
        assert_eq!(None, result.namespace);
        assert_eq!(DEFAULT_NAMESPACE, result.get_namespace());
        assert_eq!(None, result.get_namespace_to_send());
    }

    #[test]
    fn test_default_namespace_is_never_sent_to_the_server() {
        let result = parse_connection_string("host=http://localhost:5123;ns=default").unwrap();

        assert_eq!(Some("default".to_string()), result.namespace);
        assert_eq!(DEFAULT_NAMESPACE, result.get_namespace());
        assert_eq!(None, result.get_namespace_to_send());
    }

    #[test]
    fn test_key_is_case_insensitive() {
        let result = parse_connection_string("HOST=http://localhost:5123;NS=alpha").unwrap();

        assert_eq!("http://localhost:5123", result.host);
        assert_eq!(Some("alpha".to_string()), result.namespace);
    }

    #[test]
    fn test_spaces_are_trimmed() {
        let result =
            parse_connection_string(" host = http://localhost:5123 ; ns = alpha ; ").unwrap();

        assert_eq!("http://localhost:5123", result.host);
        assert_eq!(Some("alpha".to_string()), result.namespace);
    }

    #[test]
    fn test_unknown_key_is_an_error() {
        let result = parse_connection_string("host=http://localhost:5123;namespace=alpha");

        assert_eq!(
            Err(ConnectionStringError::UnknownKey("namespace".to_string())),
            result
        );
    }

    #[test]
    fn test_invalid_namespace_name_is_an_error() {
        let result = parse_connection_string("host=http://localhost:5123;ns=Alpha");

        assert_eq!(true, result.is_err());

        if let Err(err) = result {
            match err {
                ConnectionStringError::InvalidNamespaceName(err) => println!("{}", err),
                _ => panic!("Unexpected error: {:?}", err),
            }
        }
    }

    #[test]
    fn test_empty_namespace_is_an_error() {
        let result = parse_connection_string("host=http://localhost:5123;ns=");

        assert_eq!(true, result.is_err());
    }

    #[test]
    fn test_empty_host_is_an_error() {
        let result = parse_connection_string("host=;ns=alpha");

        assert_eq!(Err(ConnectionStringError::HostIsEmpty), result);
    }

    #[test]
    fn test_duplicated_key_is_an_error() {
        let result = parse_connection_string("host=http://localhost:5123;host=http://localhost:1");

        assert_eq!(
            Err(ConnectionStringError::DuplicatedKey("host".to_string())),
            result
        );
    }

    #[test]
    fn test_element_which_is_not_a_key_value_pair_is_an_error() {
        let result = parse_connection_string("host=http://localhost:5123;alpha");

        assert_eq!(
            Err(ConnectionStringError::NotAKeyValuePair("alpha".to_string())),
            result
        );
    }

    #[test]
    fn test_string_which_does_not_start_from_host_key_is_a_legacy_one() {
        // The format is detected by the leading `host=` only - everything else is the host
        // as it always was.
        let result = parse_connection_string("ns=alpha").unwrap();

        assert_eq!("ns=alpha", result.host);
        assert_eq!(None, result.namespace);
    }
}
