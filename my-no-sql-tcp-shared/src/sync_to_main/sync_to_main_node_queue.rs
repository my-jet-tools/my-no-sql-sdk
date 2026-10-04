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

const QUEUES_AMOUNT: usize = 4;

pub struct SyncToMainNodeQueue {
    pub confirmation_id: i64,
    pub update_partition_expiration_time_update: UpdatePartitionsExpirationTimeQueue,
    pub update_partitions_last_read_time_queue: UpdatePartitionsLastReadTimeQueue,

    pub update_rows_expiration_time_queue: UpdateRowsExpirationTimeQueue,
    pub update_rows_last_read_time_queue: UpdateRowsLastReadTimeQueue,
    pub on_delivery: Option<DeliverToMainNodeEvent>,
    pub connection: Option<Arc<DataReaderTcpConnection>>,
    /// Number of the queue which is asked first for the next event - the queues take turns.
    queue_to_ask_first: usize,
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
            queue_to_ask_first: 0,
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

        // The queues take turns - each is asked once, starting with the one after the queue
        // which gave the previous event. Asked in the same order every time, a queue which is
        // filled all the time would not let the ones behind it deliver anything.
        for _ in 0..QUEUES_AMOUNT {
            let queue_no = self.queue_to_ask_first;
            self.queue_to_ask_first = (queue_no + 1) % QUEUES_AMOUNT;

            match queue_no {
                0 => {
                    if let Some(event) = self.update_partition_expiration_time_update.dequeue() {
                        let confirmation_id = self.get_confirmation_id();
                        let result = DeliverToMainNodeEvent::UpdatePartitionsExpiration {
                            event,
                            confirmation_id,
                        };

                        self.on_delivery = Some(result.clone());
                        return Some((self.connection.as_ref().unwrap().clone(), result));
                    }
                }
                1 => {
                    if let Some(event) = self.update_partitions_last_read_time_queue.dequeue() {
                        let confirmation_id = self.get_confirmation_id();
                        let result = DeliverToMainNodeEvent::UpdatePartitionsLastReadTime {
                            event,
                            confirmation_id,
                        };

                        self.on_delivery = Some(result.clone());
                        return Some((self.connection.as_ref().unwrap().clone(), result));
                    }
                }
                2 => {
                    if let Some(event) = self.update_rows_expiration_time_queue.dequeue() {
                        let confirmation_id = self.get_confirmation_id();
                        let result = DeliverToMainNodeEvent::UpdateRowsExpirationTime {
                            event,
                            confirmation_id,
                        };

                        self.on_delivery = Some(result.clone());
                        return Some((self.connection.as_ref().unwrap().clone(), result));
                    }
                }
                _ => {
                    if let Some(event) = self.update_rows_last_read_time_queue.dequeue() {
                        let confirmation_id = self.get_confirmation_id();
                        let result = DeliverToMainNodeEvent::UpdateRowsLastReadTime {
                            event,
                            confirmation_id,
                        };

                        self.on_delivery = Some(result.clone());
                        return Some((self.connection.as_ref().unwrap().clone(), result));
                    }
                }
            }
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use my_tcp_sockets::ThreadsStatistics;
    use rust_extensions::Logger;

    use crate::MyNoSqlReaderTcpSerializer;

    use super::{DataReaderTcpConnection, DeliverToMainNodeEvent, SyncToMainNodeQueue};

    const PARTITIONS_EXPIRATION: &str = "partitions expiration";
    const PARTITIONS_LAST_READ_TIME: &str = "partitions last read time";
    const ROWS_EXPIRATION: &str = "rows expiration";
    const ROWS_LAST_READ_TIME: &str = "rows last read time";

    struct TestLogger;

    impl Logger for TestLogger {
        fn write_info(&self, _: String, _: String, _: Option<HashMap<String, String>>) {}
        fn write_warning(&self, _: String, _: String, _: Option<HashMap<String, String>>) {}
        fn write_error(&self, _: String, _: String, _: Option<HashMap<String, String>>) {}
        fn write_fatal_error(&self, _: String, _: String, _: Option<HashMap<String, String>>) {}
        fn write_debug_info(&self, _: String, _: String, _: Option<HashMap<String, String>>) {}
    }

    /// A connection with no socket behind it - the queues send nothing themselves, they only
    /// hand the connection out together with the event.
    async fn connection() -> Arc<DataReaderTcpConnection> {
        let connection = DataReaderTcpConnection::new(
            Arc::new("test".to_string()),
            None,
            1,
            None,
            Arc::new(TestLogger),
            1024 * 1024,
            Duration::from_secs(3),
            Duration::from_secs(3),
            Arc::new(ThreadsStatistics::default()),
            MyNoSqlReaderTcpSerializer::new(),
            (),
        )
        .await;

        Arc::new(connection)
    }

    fn queue_of(event: &DeliverToMainNodeEvent) -> &'static str {
        match event {
            DeliverToMainNodeEvent::UpdatePartitionsExpiration { .. } => PARTITIONS_EXPIRATION,
            DeliverToMainNodeEvent::UpdatePartitionsLastReadTime { .. } => {
                PARTITIONS_LAST_READ_TIME
            }
            DeliverToMainNodeEvent::UpdateRowsExpirationTime { .. } => ROWS_EXPIRATION,
            DeliverToMainNodeEvent::UpdateRowsLastReadTime { .. } => ROWS_LAST_READ_TIME,
        }
    }

    /// Gives every queue something to deliver for the table.
    fn fill_queues(queues: &mut SyncToMainNodeQueue, table_name: &str) {
        queues
            .update_partition_expiration_time_update
            .add(table_name, "pk", None);
        queues
            .update_partitions_last_read_time_queue
            .add_partition(table_name, "pk");
        queues
            .update_rows_expiration_time_queue
            .add(table_name, "pk", ["rk"].into_iter(), None);
        queues
            .update_rows_last_read_time_queue
            .add(table_name, "pk", ["rk"].into_iter());
    }

    /// The main node confirms the event which is on delivery, if there is one - and the next
    /// event goes out.
    fn next_event(queues: &mut SyncToMainNodeQueue) -> Option<DeliverToMainNodeEvent> {
        let delivered = queues
            .on_delivery
            .as_ref()
            .map(|itm| itm.get_confirmation_id());

        let (_, event) = queues.get_next_event_to_deliver(delivered)?;

        Some(event)
    }

    #[tokio::test]
    async fn queues_which_are_filled_all_the_time_take_turns() {
        let mut queues = SyncToMainNodeQueue::new();
        queues.new_connection(connection().await);

        let mut delivered = Vec::new();

        for _ in 0..12 {
            // Reads keep coming - by every delivery each queue has something again
            fill_queues(&mut queues, "table");

            delivered.push(queue_of(&next_event(&mut queues).unwrap()));

            // An event is on delivery - a ping gives nothing and does not move the turn
            assert!(queues.get_next_event_to_deliver(None).is_none());
        }

        // No queue waits for more than three deliveries
        assert_eq!(
            [
                PARTITIONS_EXPIRATION,
                PARTITIONS_LAST_READ_TIME,
                ROWS_EXPIRATION,
                ROWS_LAST_READ_TIME
            ]
            .repeat(3),
            delivered
        );
    }

    #[tokio::test]
    async fn partitions_which_are_updated_all_the_time_do_not_keep_the_rows_waiting() {
        let mut queues = SyncToMainNodeQueue::new();
        queues.new_connection(connection().await);

        queues
            .update_rows_expiration_time_queue
            .add("table", "pk", ["rk"].into_iter(), None);
        queues
            .update_rows_last_read_time_queue
            .add("table", "pk", ["rk"].into_iter());

        let mut delivered = Vec::new();

        for _ in 0..8 {
            // By every delivery both partition queues have something again
            queues
                .update_partition_expiration_time_update
                .add("table", "pk", None);
            queues
                .update_partitions_last_read_time_queue
                .add_partition("table", "pk");

            delivered.push(queue_of(&next_event(&mut queues).unwrap()));
        }

        // The rows leave as their turn comes - after that their queues are empty and are
        // passed over
        assert_eq!(
            vec![
                PARTITIONS_EXPIRATION,
                PARTITIONS_LAST_READ_TIME,
                ROWS_EXPIRATION,
                ROWS_LAST_READ_TIME,
                PARTITIONS_EXPIRATION,
                PARTITIONS_LAST_READ_TIME,
                PARTITIONS_EXPIRATION,
                PARTITIONS_LAST_READ_TIME,
            ],
            delivered
        );
    }

    #[tokio::test]
    async fn turn_goes_on_from_the_queue_which_gave_the_previous_event() {
        let mut queues = SyncToMainNodeQueue::new();
        queues.new_connection(connection().await);

        queues
            .update_partitions_last_read_time_queue
            .add_partition("table", "pk");

        let event = next_event(&mut queues).unwrap();
        assert_eq!(PARTITIONS_LAST_READ_TIME, queue_of(&event));

        // Everything is delivered
        assert!(next_event(&mut queues).is_none());

        queues
            .update_partition_expiration_time_update
            .add("table", "pk", None);
        queues
            .update_partitions_last_read_time_queue
            .add_partition("table", "pk");
        queues
            .update_rows_last_read_time_queue
            .add("table", "pk", ["rk"].into_iter());

        // The turn does not start over - the queues behind the one which gave the previous
        // event are asked first, and the rows expiration queue is passed over as it is empty
        let event = next_event(&mut queues).unwrap();
        assert_eq!(ROWS_LAST_READ_TIME, queue_of(&event));

        let event = next_event(&mut queues).unwrap();
        assert_eq!(PARTITIONS_EXPIRATION, queue_of(&event));

        let event = next_event(&mut queues).unwrap();
        assert_eq!(PARTITIONS_LAST_READ_TIME, queue_of(&event));

        assert!(next_event(&mut queues).is_none());
    }

    #[tokio::test]
    async fn event_of_a_lost_delivery_goes_back_to_its_queue_and_waits_for_its_turn() {
        let connection = connection().await;

        let mut queues = SyncToMainNodeQueue::new();
        queues.new_connection(connection.clone());

        fill_queues(&mut queues, "table");

        // Every time an event is on delivery the connection gets lost - nothing is confirmed
        let mut lost = Vec::new();

        for _ in 0..4 {
            let (_, event) = queues.get_next_event_to_deliver(None).unwrap();
            lost.push(queue_of(&event));

            queues.disconnected();
            queues.new_connection(connection.clone());
        }

        // A lost delivery takes the turn of its queue as a confirmed one does - so an event
        // which can not be delivered does not keep the other queues waiting
        assert_eq!(
            vec![
                PARTITIONS_EXPIRATION,
                PARTITIONS_LAST_READ_TIME,
                ROWS_EXPIRATION,
                ROWS_LAST_READ_TIME,
            ],
            lost
        );

        // Each event is back in the queue it was taken from - they are delivered now, once
        let mut delivered = Vec::new();

        while let Some(event) = next_event(&mut queues) {
            delivered.push(queue_of(&event));
        }

        assert_eq!(lost, delivered);
    }

    #[tokio::test]
    async fn one_event_is_on_delivery_at_a_time() {
        let mut queues = SyncToMainNodeQueue::new();

        queues
            .update_rows_last_read_time_queue
            .add("table1", "pk", ["rk"].into_iter());
        queues
            .update_rows_last_read_time_queue
            .add("table2", "pk", ["rk"].into_iter());

        // No connection to the main node - nothing leaves the queues
        assert!(queues.get_next_event_to_deliver(None).is_none());

        let connection = connection().await;
        queues.new_connection(connection.clone());

        let (to_send_with, event) = queues.get_next_event_to_deliver(None).unwrap();
        assert!(Arc::ptr_eq(&connection, &to_send_with));
        assert_eq!(1, event.get_confirmation_id());

        // Not confirmed yet - the second event waits
        assert!(queues.get_next_event_to_deliver(None).is_none());

        let (_, event) = queues.get_next_event_to_deliver(Some(1)).unwrap();
        assert_eq!(2, event.get_confirmation_id());

        // The connection is lost before the confirmation - the event is back in its queue and
        // stays there until there is a connection again
        queues.disconnected();
        assert!(queues.get_next_event_to_deliver(None).is_none());

        queues.new_connection(connection);

        match queues.get_next_event_to_deliver(None).unwrap() {
            (
                _,
                DeliverToMainNodeEvent::UpdateRowsLastReadTime {
                    event,
                    confirmation_id,
                },
            ) => {
                assert_eq!("table2", event.table_name);
                assert_eq!(3, confirmation_id);
            }
            (_, event) => panic!("Unexpected event: {:?}", event),
        }

        assert!(queues.get_next_event_to_deliver(Some(3)).is_none());
    }
}
