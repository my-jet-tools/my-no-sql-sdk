use parking_lot::Mutex;
use rust_extensions::events_loop::{EventsLoopPublisher, EventsLoopTick, RepeatIteration};

use crate::sync_to_main::DeliverToMainNodeEvent;

use super::{SyncToMainNodeEvent, SyncToMainNodeQueue};

pub struct SyncToMainNodeHandlerInner {
    pub queues: Mutex<SyncToMainNodeQueue>,
    pub events_publisher: EventsLoopPublisher<SyncToMainNodeEvent>,
}

impl SyncToMainNodeHandlerInner {
    pub fn new(events_publisher: EventsLoopPublisher<SyncToMainNodeEvent>) -> Self {
        Self {
            queues: Mutex::new(SyncToMainNodeQueue::new()),
            events_publisher,
        }
    }
}

#[async_trait::async_trait]
impl EventsLoopTick<SyncToMainNodeEvent> for SyncToMainNodeHandlerInner {
    async fn started(&self) {}
    async fn tick(&self, event: SyncToMainNodeEvent) -> RepeatIteration<SyncToMainNodeEvent> {
        match event {
            SyncToMainNodeEvent::Connected(connection) => {
                let mut queues = self.queues.lock();
                queues.new_connection(connection);
                to_main_node_pusher(&mut queues, None);
            }
            SyncToMainNodeEvent::Disconnected(_) => {
                let mut queues = self.queues.lock();
                queues.disconnected();
            }
            SyncToMainNodeEvent::PingToDeliver => {
                let mut queues = self.queues.lock();
                to_main_node_pusher(&mut queues, None);
            }
            SyncToMainNodeEvent::Delivered(confirmation_id) => {
                let mut queues = self.queues.lock();
                to_main_node_pusher(&mut queues, Some(confirmation_id));
            }
        }

        RepeatIteration::No
    }
    async fn finished(&self) {}
}

pub fn to_main_node_pusher(
    queues: &mut SyncToMainNodeQueue,
    delivered_confirmation_id: Option<i64>,
) {
    use crate::MyNoSqlTcpContract;
    let next_event = queues.get_next_event_to_deliver(delivered_confirmation_id);

    if next_event.is_none() {
        return;
    }

    let (connection, next_event) = next_event.unwrap();

    match next_event {
        DeliverToMainNodeEvent::UpdatePartitionsExpiration {
            event,
            confirmation_id,
        } => {
            let mut partitions = Vec::with_capacity(event.partitions.len());

            for (partition, expiration_time) in event.partitions {
                partitions.push((partition, expiration_time));
            }

            connection
                .send(&MyNoSqlTcpContract::UpdatePartitionsExpirationTime {
                    confirmation_id,
                    table_name: event.table_name,
                    partitions,
                });
        }
        DeliverToMainNodeEvent::UpdatePartitionsLastReadTime {
            event,
            confirmation_id,
        } => {
            let mut partitions = Vec::with_capacity(event.partitions.len());

            for (partition, _) in event.partitions {
                partitions.push(partition);
            }

            connection
                .send(&MyNoSqlTcpContract::UpdatePartitionsLastReadTime {
                    confirmation_id,
                    table_name: event.table_name,
                    partitions,
                });
        }
        DeliverToMainNodeEvent::UpdateRowsExpirationTime {
            event,
            confirmation_id,
        } => {
            let mut row_keys = Vec::with_capacity(event.row_keys.len());

            for (row_key, _) in event.row_keys {
                row_keys.push(row_key);
            }

            connection
                .send(&MyNoSqlTcpContract::UpdateRowsExpirationTime {
                    confirmation_id,
                    table_name: event.table_name,
                    partition_key: event.partition_key,
                    row_keys,
                    expiration_time: event.expiration_time,
                });
        }
        DeliverToMainNodeEvent::UpdateRowsLastReadTime {
            event,
            confirmation_id,
        } => {
            connection
                .send(&MyNoSqlTcpContract::UpdateRowsLastReadTime {
                    confirmation_id,
                    table_name: event.table_name,
                    partition_key: event.partition_key,
                    row_keys: event.row_keys,
                });
        }
    }
}
