use std::{str::Utf8Error, string::FromUtf8Error};

use flurl::FlUrlError;

#[derive(Debug)]
pub enum DataWriterError {
    TableAlreadyExists(String),
    TableNotFound(String),
    RecordAlreadyExists(String),
    /// The table exists but the addressed row does not — mirrors `RecordAlreadyExists`
    /// for the opposite case. Distinct from `TableNotFound` (whole table missing).
    RecordNotFound(String),
    RecordIsChanged(String),
    RequiredEntityFieldIsMissing(String),
    ServerCouldNotParseJson(String),
    /// Connection string returned by the settings can not be parsed - it is better not to
    /// work at all than to work with a wrong host or namespace.
    InvalidConnectionString(String),
    FromUtf8Error(FromUtf8Error),
    Utf8Error(Utf8Error),
    Error(String),
    FlUrlError(FlUrlError),
    HyperError(flurl::hyper::Error),
    JsonParseError(my_json::json_reader::JsonParseError),
}

impl From<my_no_sql_abstractions::ConnectionStringError> for DataWriterError {
    fn from(src: my_no_sql_abstractions::ConnectionStringError) -> Self {
        Self::InvalidConnectionString(src.to_string())
    }
}

impl From<flurl::hyper::Error> for DataWriterError {
    fn from(src: flurl::hyper::Error) -> Self {
        Self::HyperError(src)
    }
}

impl From<my_json::json_reader::JsonParseError> for DataWriterError {
    fn from(src: my_json::json_reader::JsonParseError) -> Self {
        Self::JsonParseError(src)
    }
}

impl From<FromUtf8Error> for DataWriterError {
    fn from(src: FromUtf8Error) -> Self {
        Self::FromUtf8Error(src)
    }
}

impl From<Utf8Error> for DataWriterError {
    fn from(src: Utf8Error) -> Self {
        Self::Utf8Error(src)
    }
}

impl From<FlUrlError> for DataWriterError {
    fn from(src: FlUrlError) -> Self {
        Self::FlUrlError(src)
    }
}
