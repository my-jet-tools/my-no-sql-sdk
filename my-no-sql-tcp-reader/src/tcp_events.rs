use std::sync::Arc;

use arc_swap::ArcSwapOption;
use my_no_sql_tcp_shared::{
    sync_to_main::SyncToMainNodeHandler, MyNoSqlReaderTcpSerializer, MyNoSqlTcpContract,
};
use my_tcp_sockets::{tcp_connection::TcpSocketConnection, SocketEventCallback};

use crate::subscribers::Subscribers;

pub type MyNoSqlTcpConnection =
    TcpSocketConnection<MyNoSqlTcpContract, MyNoSqlReaderTcpSerializer, ()>;


#[derive(Clone)]
pub struct TcpEvents {
    app_name: Arc<String>,
    /// Namespace resolved from the connection string on the last connect attempt. `None` means
    /// the default namespace - nothing is sent to the server in that case.
    namespace: Arc<ArcSwapOption<String>>,
    pub subscribers: Subscribers,
    pub sync_handler: Arc<SyncToMainNodeHandler>,
}

impl TcpEvents {
    pub fn new(
        app_name: String,
        sync_handler: Arc<SyncToMainNodeHandler>,
        namespace: Arc<ArcSwapOption<String>>,
    ) -> Self {
        Self {
            app_name: Arc::new(app_name),
            namespace,
            subscribers: Subscribers::new(),
            sync_handler,
        }
    }
}

#[async_trait::async_trait]
impl SocketEventCallback<MyNoSqlTcpContract, MyNoSqlReaderTcpSerializer, ()> for TcpEvents {
    async fn connected(&mut self, connection: Arc<MyNoSqlTcpConnection>) {
        let contract = MyNoSqlTcpContract::Greeting {
            name: self.app_name.to_string(),
        };

        connection.send(&contract);

        // Namespace has to be fixed by the server before the very first subscription - hence it
        // goes right after the Greeting and before any Subscribe. Default namespace is not sent
        // at all: it is what the server uses anyway.
        if let Some(namespace) = self.namespace.load_full() {
            let contract = MyNoSqlTcpContract::SetNamespace {
                namespace: namespace.to_string(),
            };

            connection.send(&contract);
        }

        for table in self.subscribers.get_tables_to_subscribe().iter() {
            let contract = MyNoSqlTcpContract::Subscribe {
                table_name: table.to_string(),
            };

            connection.send(&contract);
        }

        self.sync_handler
            .tcp_events_pusher_new_connection_established(connection);
    }

    async fn disconnected(&mut self, connection: Arc<MyNoSqlTcpConnection>) {
        self.sync_handler
            .tcp_events_pusher_connection_disconnected(connection);
    }

    async fn payload(&mut self, _connection: &Arc<MyNoSqlTcpConnection>, contract: MyNoSqlTcpContract) {
        match contract {
            // Pings go out through the serializer's `get_ping` - the round trip the library
            // measures on the Pong rides along in the next ping as `PingWithLatency`.
            MyNoSqlTcpContract::Ping | MyNoSqlTcpContract::PingWithLatency { .. } => {}
            MyNoSqlTcpContract::Pong => {}
            MyNoSqlTcpContract::Greeting { name: _ } => {}
            MyNoSqlTcpContract::Subscribe { table_name: _ } => {}
            MyNoSqlTcpContract::SetNamespace { namespace: _ } => {}
            MyNoSqlTcpContract::InitTable { table_name, data } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()) {
                    update_event.as_ref().init_table(data);
                }
            }
            MyNoSqlTcpContract::InitPartition {
                table_name,
                partition_key,
                data,
            } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()) {
                    update_event
                        .as_ref()
                        .init_partition(partition_key.as_str(), data);
                }
            }
            MyNoSqlTcpContract::UpdateRows { table_name, data } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()) {
                    update_event.as_ref().update_rows(data);
                }
            }
            MyNoSqlTcpContract::DeleteRows { table_name, rows } => {
                if let Some(update_event) = self.subscribers.get(table_name.as_str()) {
                    update_event.as_ref().delete_rows(rows);
                }
            }
            MyNoSqlTcpContract::Error { message } => {
                panic!("Server error: {}", message);
            }
            MyNoSqlTcpContract::GreetingFromNode {
                node_location: _,
                node_version: _,
                compress: _,
            } => {}
            MyNoSqlTcpContract::SubscribeAsNode(_) => {}
            MyNoSqlTcpContract::Unsubscribe(_) => {}
            MyNoSqlTcpContract::TableNotFound(_) => {}
            MyNoSqlTcpContract::CompressedPayload(_) => {
                // Unreachable: the serializer inflates compressed packets before dispatch.
                panic!("Got a CompressedPayload which was not decompressed by the serializer");
            }
            MyNoSqlTcpContract::Confirmation { confirmation_id } => self
                .sync_handler
                .tcp_events_pusher_got_confirmation(confirmation_id),
            MyNoSqlTcpContract::UpdatePartitionsLastReadTime {
                confirmation_id: _,
                table_name: _,
                partitions: _,
            } => {}
            MyNoSqlTcpContract::UpdateRowsLastReadTime {
                confirmation_id: _,
                table_name: _,
                partition_key: _,
                row_keys: _,
            } => {}
            MyNoSqlTcpContract::UpdatePartitionsExpirationTime {
                confirmation_id: _,
                table_name: _,
                partitions: _,
            } => {}
            MyNoSqlTcpContract::UpdateRowsExpirationTime {
                confirmation_id: _,
                table_name: _,
                partition_key: _,
                row_keys: _,
                expiration_time: _,
            } => {}
        }
    }
}
