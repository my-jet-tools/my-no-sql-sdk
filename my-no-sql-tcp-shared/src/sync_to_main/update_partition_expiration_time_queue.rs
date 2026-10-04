use rust_extensions::auto_shrink::VecDequeAutoShrink;
use rust_extensions::date_time::DateTimeAsMicroseconds;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct UpdatePartitionExpirationEvent {
    pub table_name: String,
    pub partitions: BTreeMap<String, Option<DateTimeAsMicroseconds>>,
}

/// Expiration moments of partitions which wait to be delivered to the main node, one event per
/// table.
///
/// [`Self::add`] writes into the event queued for the table - the moment set last wins - and
/// queues a new one only when there is none; [`Self::return_event`] merges as well. So the
/// queue never holds two events for the same table, and never hands out an older moment of a
/// partition after a newer one.
pub struct UpdatePartitionsExpirationTimeQueue {
    queue: VecDequeAutoShrink<UpdatePartitionExpirationEvent>,
}

impl UpdatePartitionsExpirationTimeQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDequeAutoShrink::new(32),
        }
    }

    pub fn add(
        &mut self,
        table_name: &str,
        partition_key: &str,
        date_time: Option<DateTimeAsMicroseconds>,
    ) {
        if let Some(item) = self
            .queue
            .iter_mut()
            .find(|itm| itm.table_name == table_name)
        {
            item.partitions.insert(partition_key.to_string(), date_time);
            return;
        }

        let mut partitions = BTreeMap::new();
        partitions.insert(partition_key.to_string(), date_time);

        self.queue.push_back(UpdatePartitionExpirationEvent {
            table_name: table_name.to_string(),
            partitions,
        });
    }

    /// Puts back the event which was on delivery when the connection to the main node got
    /// lost, so it is delivered again.
    ///
    /// Whatever is queued for the table by now was added after the returned event had been
    /// taken for delivery, so the returned event is merged into it and a partition which is in
    /// both keeps the queued, newer moment. Queued as an event of its own, the returned one
    /// would be delivered last and bring the older moment back.
    pub fn return_event(&mut self, event: UpdatePartitionExpirationEvent) {
        if let Some(item) = self
            .queue
            .iter_mut()
            .find(|itm| itm.table_name == event.table_name)
        {
            for (partition_key, date_time) in event.partitions {
                item.partitions.entry(partition_key).or_insert(date_time);
            }
            return;
        }

        self.queue.push_back(event);
    }

    pub fn dequeue(&mut self) -> Option<UpdatePartitionExpirationEvent> {
        self.queue.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use super::{UpdatePartitionExpirationEvent, UpdatePartitionsExpirationTimeQueue};

    fn moment(unix_microseconds: i64) -> Option<DateTimeAsMicroseconds> {
        Some(DateTimeAsMicroseconds::new(unix_microseconds))
    }

    fn partitions(
        event: &UpdatePartitionExpirationEvent,
    ) -> Vec<(&str, Option<DateTimeAsMicroseconds>)> {
        event
            .partitions
            .iter()
            .map(|(partition_key, expires)| (partition_key.as_str(), *expires))
            .collect()
    }

    #[test]
    fn add_of_the_same_partition_does_not_grow_the_queue_and_keeps_the_last_moment() {
        let mut queue = UpdatePartitionsExpirationTimeQueue::new();

        for unix_microseconds in 1..=100 {
            queue.add("table", "pk", moment(unix_microseconds));
        }

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!(vec![("pk", moment(100))], partitions(&event));

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn add_merges_the_partitions_of_a_table_into_one_event() {
        let mut queue = UpdatePartitionsExpirationTimeQueue::new();

        queue.add("table", "pk2", moment(2));
        queue.add("table", "pk1", moment(1));
        // None removes the expiration - it is a value to deliver as any other
        queue.add("table", "pk3", None);

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!(
            vec![("pk1", moment(1)), ("pk2", moment(2)), ("pk3", None)],
            partitions(&event)
        );

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn every_table_has_an_event_of_its_own() {
        let mut queue = UpdatePartitionsExpirationTimeQueue::new();

        queue.add("table1", "pk1", moment(1));
        queue.add("table2", "pk1", moment(2));
        queue.add("table1", "pk2", moment(3));

        // Events leave the queue in the order their tables came in
        let event = queue.dequeue().unwrap();
        assert_eq!("table1", event.table_name);
        assert_eq!(
            vec![("pk1", moment(1)), ("pk2", moment(3))],
            partitions(&event)
        );

        let event = queue.dequeue().unwrap();
        assert_eq!("table2", event.table_name);
        assert_eq!(vec![("pk1", moment(2))], partitions(&event));

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn returned_event_is_dequeued_again() {
        let mut queue = UpdatePartitionsExpirationTimeQueue::new();

        queue.add("table", "pk1", moment(1));
        queue.add("table", "pk2", None);

        // Taken for delivery - and the connection is lost before the confirmation
        let on_delivery = queue.dequeue().unwrap();
        assert!(queue.dequeue().is_none());

        queue.return_event(on_delivery);

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!(vec![("pk1", moment(1)), ("pk2", None)], partitions(&event));

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn returned_event_does_not_override_what_was_queued_after_it() {
        let mut queue = UpdatePartitionsExpirationTimeQueue::new();

        queue.add("table", "pk1", moment(1));
        queue.add("table", "pk2", moment(1));
        queue.add("table", "pk3", moment(1));
        let on_delivery = queue.dequeue().unwrap();

        // Set again while the event is on delivery - these are the moments the main node has
        // to end up with
        queue.add("table", "pk1", moment(2));
        queue.add("table", "pk3", None);

        queue.return_event(on_delivery);

        // And once more after the return - still the same single event
        queue.add("table", "pk4", moment(3));

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!(
            vec![
                ("pk1", moment(2)),
                ("pk2", moment(1)),
                ("pk3", None),
                ("pk4", moment(3))
            ],
            partitions(&event)
        );

        // Nothing is left to be delivered after it and to bring the older moments back
        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn returned_event_is_not_merged_into_the_event_of_another_table() {
        let mut queue = UpdatePartitionsExpirationTimeQueue::new();

        queue.add("table1", "pk", moment(1));
        let on_delivery = queue.dequeue().unwrap();

        queue.add("table2", "pk", moment(2));
        queue.return_event(on_delivery);

        // Nothing is queued for table1 - the returned event is queued as it is
        let event = queue.dequeue().unwrap();
        assert_eq!("table2", event.table_name);
        assert_eq!(vec![("pk", moment(2))], partitions(&event));

        let event = queue.dequeue().unwrap();
        assert_eq!("table1", event.table_name);
        assert_eq!(vec![("pk", moment(1))], partitions(&event));

        assert!(queue.dequeue().is_none());
    }
}
