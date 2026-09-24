use std::sync::Arc;

use super::DataReaderTcpConnection;

pub enum SyncToMainNodeEvent {
    Connected(Arc<DataReaderTcpConnection>),
    Disconnected(Arc<DataReaderTcpConnection>),
    PingToDeliver,
    Delivered(i64),
}
