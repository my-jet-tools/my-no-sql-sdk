pub extern crate my_no_sql_core as core;

#[cfg(feature = "data-writer")]
pub extern crate my_no_sql_data_writer as data_writer;

#[cfg(feature = "macros")]
pub extern crate my_no_sql_macros as macros;
#[cfg(feature = "macros")]
pub extern crate rust_extensions as rust_extensions;

pub extern crate my_no_sql_abstractions as abstractions;

// Namespace primitives are shared between the SDK and the server - the server takes them
// from here.
pub use my_no_sql_abstractions::{
    parse_connection_string, validate_namespace_name, ConnectionString, ConnectionStringError,
    ValidationError, DEFAULT_NAMESPACE,
};
pub use my_no_sql_core::db::DbNamespaceName;

#[cfg(feature = "data-reader")]
pub extern crate my_no_sql_tcp_reader as reader;

#[cfg(feature = "tcp-contracts")]
pub extern crate my_no_sql_tcp_shared as tcp_contracts;

#[cfg(any(feature = "master-node", feature = "read-node"))]
pub extern crate my_no_sql_server_core as server;
