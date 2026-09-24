use std::sync::Arc;

use super::{
    DataReaderTcpConnection, UpdatePartitionExpirationEvent, UpdatePartitionsExpirationTimeQueue,
    UpdatePartitionsLastReadTimeEvent, UpdatePartitionsLastReadTimeQueue,
    UpdateRowsExpirationTimeEvent, UpdateRowsExpirationTimeQueue, UpdateRowsLastReadTimeEvent,
    UpdateRowsLastReadTimeQueue,
};

#[derive(Debug, Clone)]
pub enum DeliverToMainNodeEvent {
    UpdatePartitionsExpiration {
        event: UpdatePartitionExpirationEvent,
        confirmation_id: i64,
    },
    UpdatePartitionsLastReadTime {
        event: UpdatePartitionsLastReadTimeEvent,
        confirmation_id: i64,
    },
    UpdateRowsExpirationTime {
        event: UpdateRowsExpirationTimeEvent,
        confirmation_id: i64,
    },
    UpdateRowsLastReadTime {
        event: UpdateRowsLastReadTimeEvent,
        confirmation_id: i64,
    },
}

impl DeliverToMainNodeEvent {
    pub fn get_confirmation_id(&self) -> i64 {
        match self {
            DeliverToMainNodeEvent::UpdatePartitionsExpiration {
                event: _,
                confirmation_id,
            } => *confirmation_id,
            DeliverToMainNodeEvent::UpdatePartitionsLastReadTime {
                event: _,
                confirmation_id,
            } => *confirmation_id,
            DeliverToMainNodeEvent::UpdateRowsExpirationTime {
                event: _,
                confirmation_id,
            } => *confirmation_id,
            DeliverToMainNodeEvent::UpdateRowsLastReadTime {
                event: _,
                confirmation_id,
            } => *confirmation_id,
        }
    }
}

pub struct SyncToMainNodeQueue {
    pub confirmation_id: i64,
    pub update_partition_expiration_time_update: UpdatePartitionsExpirationTimeQueue,
    pub update_partitions_last_read_time_queue: UpdatePartitionsLastReadTimeQueue,

    pub update_rows_expiration_time_queue: UpdateRowsExpirationTimeQueue,
    pub update_rows_last_read_time_queue: UpdateRowsLastReadTimeQueue,
    pub on_delivery: Option<DeliverToMainNodeEvent>,
    pub connection: Option<Arc<DataReaderTcpConnection>>,
}

impl SyncToMainNodeQueue {
    pub fn new() -> Self {
        Self {
            confirmation_id: 0,
            update_partition_expiration_time_update: UpdatePartitionsExpirationTimeQueue::new(),
            update_rows_expiration_time_queue: UpdateRowsExpirationTimeQueue::new(),
            update_rows_last_read_time_queue: UpdateRowsLastReadTimeQueue::new(),
            update_partitions_last_read_time_queue: UpdatePartitionsLastReadTimeQueue::new(),
            on_delivery: None,
            connection: None,
        }
    }

    pub fn new_connection(&mut self, connection: Arc<DataReaderTcpConnection>) {
        self.connection = Some(connection);
    }

    fn get_confirmation_id(&mut self) -> i64 {
        self.confirmation_id += 1;
        self.confirmation_id
    }

    fn confirm_delivery(&mut self, delivery_id: i64) {
        let on_delivery = self.on_delivery.take();

        match on_delivery {
            Some(event) => {
                let on_delivery_confirmation_id = event.get_confirmation_id();

                if on_delivery_confirmation_id != delivery_id {
                    println!("Somehow we are waiting confirmation for delivery with id {}, but we go confirmation id {} which is not the same  as the one we are waiting for. This is a bug.", on_delivery_confirmation_id, delivery_id);
                }
            }
            None => {
                println!(
                    "Somehow we got confirmation for delivery, but there is no delivery in progress"
                );
            }
        }
    }

    pub fn get_next_event_to_deliver(
        &mut self,
        delivery_id: Option<i64>,
    ) -> Option<(Arc<DataReaderTcpConnection>, DeliverToMainNodeEvent)> {
        if let Some(delivery_id) = delivery_id {
            self.confirm_delivery(delivery_id);
        }

        if self.on_delivery.is_some() {
            return None;
        }

        if self.connection.is_none() {
            return None;
        }

        if let Some(event) = self.update_partition_expiration_time_update.dequeue() {
            let confirmation_id = self.get_confirmation_id();
            let result = DeliverToMainNodeEvent::UpdatePartitionsExpiration {
                event,
                confirmation_id,
            };

            self.on_delivery = Some(result.clone());
            return Some((self.connection.as_ref().unwrap().clone(), result));
        }

        if let Some(event) = self.update_partitions_last_read_time_queue.dequeue() {
            let confirmation_id = self.get_confirmation_id();
            let result = DeliverToMainNodeEvent::UpdatePartitionsLastReadTime {
                event,
                confirmation_id,
            };

            self.on_delivery = Some(result.clone());
            return Some((self.connection.as_ref().unwrap().clone(), result));
        }

        if let Some(event) = self.update_rows_expiration_time_queue.dequeue() {
            let confirmation_id = self.get_confirmation_id();
            let result = DeliverToMainNodeEvent::UpdateRowsExpirationTime {
                event,
                confirmation_id,
            };

            self.on_delivery = Some(result.clone());
            return Some((self.connection.as_ref().unwrap().clone(), result));
        }

        if let Some(event) = self.update_rows_last_read_time_queue.dequeue() {
            let confirmation_id = self.get_confirmation_id();
            let result = DeliverToMainNodeEvent::UpdateRowsLastReadTime {
                event,
                confirmation_id,
            };

            self.on_delivery = Some(result.clone());
            return Some((self.connection.as_ref().unwrap().clone(), result));
        }

        None
    }

    pub fn disconnected(&mut self) {
        self.connection = None;

        let event_on_delivery = self.on_delivery.take();

        if event_on_delivery.is_none() {
            return;
        }

        let event_on_delivery = event_on_delivery.unwrap();

        match event_on_delivery {
            DeliverToMainNodeEvent::UpdatePartitionsExpiration {
                event,
                confirmation_id: _,
            } => {
                self.update_partition_expiration_time_update
                    .return_event(event);
            }
            DeliverToMainNodeEvent::UpdatePartitionsLastReadTime {
                event,
                confirmation_id: _,
            } => {
                self.update_partitions_last_read_time_queue
                    .return_event(event);
            }
            DeliverToMainNodeEvent::UpdateRowsExpirationTime {
                event,
                confirmation_id: _,
            } => {
                self.update_rows_expiration_time_queue.return_event(event);
            }
            DeliverToMainNodeEvent::UpdateRowsLastReadTime {
                event,
                confirmation_id: _,
            } => {
                self.update_rows_last_read_time_queue.return_event(event);
            }
        }
    }
}
