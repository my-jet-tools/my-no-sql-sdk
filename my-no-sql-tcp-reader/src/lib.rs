mod data_reader_entities_set;
mod my_no_sql_tcp_connection;
#[cfg(test)]
mod test_delete_rows_callbacks;
#[cfg(test)]
mod test_escaped_keys;
#[cfg(test)]
mod test_init_callbacks;
#[cfg(test)]
mod test_rows_under_mangled_keys;
// The mock is there with the `mocks` feature only, and so are its tests - they run with
// `cargo test --features mocks`.
#[cfg(all(test, feature = "mocks"))]
mod test_mock_callbacks;
#[cfg(all(test, feature = "mocks"))]
mod test_mock_reads;
mod settings;
mod subscribers;
mod tcp_events;
pub use data_reader_entities_set::*;

pub use my_no_sql_tcp_connection::MyNoSqlTcpConnection;
pub use settings::*;
// GetEntitiesBuilder and GetEntityBuilder are what get_entities(..) and
// get_entity_with_callback_to_server(..) return. They are exported so that a builder can be named
// outside of the crate: kept in a variable or a field with an explicit type, returned from a
// function, written in an impl of MyNoSqlDataReader.
pub use subscribers::{
    GetEntitiesBuilder, GetEntityBuilder, LazyMyNoSqlEntity, MyNoSqlDataReader,
    MyNoSqlDataReaderCallBacks, MyNoSqlDataReaderData, MyNoSqlDataReaderTcp,
};

#[cfg(feature = "mocks")]
pub use subscribers::MyNoSqlDataReaderMock;
