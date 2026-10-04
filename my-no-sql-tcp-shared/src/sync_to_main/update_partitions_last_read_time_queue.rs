use rust_extensions::auto_shrink::VecDequeAutoShrink;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct UpdatePartitionsLastReadTimeEvent {
    pub table_name: String,
    pub partitions: BTreeMap<String, ()>,
}

/// Partitions which were read and wait to be reported to the main node, grouped by table.
///
/// [`Self::add`] and [`Self::add_partition`] merge into an event which is already queued for
/// the table and queue a new one only when there is none - so reading the same partitions over
/// and over does not grow the queue, whether the connection to the main node is up or down.
pub struct UpdatePartitionsLastReadTimeQueue {
    queue: VecDequeAutoShrink<UpdatePartitionsLastReadTimeEvent>,
}

impl UpdatePartitionsLastReadTimeQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDequeAutoShrink::new(32),
        }
    }

    pub fn add<'s, TPartitions: Iterator<Item = &'s String>>(
        &mut self,
        table_name: &str,
        partition_keys: TPartitions,
    ) {
        let mut partition_keys = partition_keys.peekable();

        // Nothing to deliver - nothing to queue
        if partition_keys.peek().is_none() {
            return;
        }

        if let Some(item) = self
            .queue
            .iter_mut()
            .find(|itm| itm.table_name == table_name)
        {
            for partition_key in partition_keys {
                item.partitions.insert(partition_key.to_string(), ());
            }
            return;
        }

        let mut partitions = BTreeMap::new();
        for partition_key in partition_keys {
            partitions.insert(partition_key.to_string(), ());
        }

        self.queue.push_back(UpdatePartitionsLastReadTimeEvent {
            table_name: table_name.to_string(),
            partitions,
        });
    }

    pub fn add_partition(&mut self, table_name: &str, partition_key: &str) {
        if let Some(item) = self
            .queue
            .iter_mut()
            .find(|itm| itm.table_name == table_name)
        {
            item.partitions.insert(partition_key.to_string(), ());
            return;
        }

        let mut partitions = BTreeMap::new();

        partitions.insert(partition_key.to_string(), ());

        self.queue.push_back(UpdatePartitionsLastReadTimeEvent {
            table_name: table_name.to_string(),
            partitions,
        });
    }

    pub fn return_event(&mut self, event: UpdatePartitionsLastReadTimeEvent) {
        self.queue.push_back(event);
    }

    pub fn dequeue(&mut self) -> Option<UpdatePartitionsLastReadTimeEvent> {
        self.queue.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::{UpdatePartitionsLastReadTimeEvent, UpdatePartitionsLastReadTimeQueue};

    fn partitions(event: &UpdatePartitionsLastReadTimeEvent) -> Vec<&str> {
        event.partitions.keys().map(|itm| itm.as_str()).collect()
    }

    #[test]
    fn add_partition_of_the_same_partition_does_not_grow_the_queue() {
        let mut queue = UpdatePartitionsLastReadTimeQueue::new();

        for _ in 0..100 {
            queue.add_partition("table", "pk");
        }

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!(vec!["pk"], partitions(&event));

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn add_partition_merges_the_partitions_of_a_table_into_one_event() {
        let mut queue = UpdatePartitionsLastReadTimeQueue::new();

        queue.add_partition("table", "pk2");
        queue.add_partition("table", "pk1");
        queue.add_partition("table", "pk2");

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!(vec!["pk1", "pk2"], partitions(&event));

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn add_and_add_partition_merge_into_the_same_event() {
        let mut queue = UpdatePartitionsLastReadTimeQueue::new();

        let partition_keys = ["pk1".to_string(), "pk2".to_string()];

        queue.add("table", partition_keys.iter());
        queue.add_partition("table", "pk3");
        queue.add("table", partition_keys.iter());
        queue.add_partition("table", "pk1");

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!(vec!["pk1", "pk2", "pk3"], partitions(&event));

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn add_without_partitions_queues_nothing() {
        let mut queue = UpdatePartitionsLastReadTimeQueue::new();

        queue.add("table", std::iter::empty());

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn every_table_has_an_event_of_its_own() {
        let mut queue = UpdatePartitionsLastReadTimeQueue::new();

        queue.add_partition("table1", "pk1");
        queue.add_partition("table2", "pk1");
        queue.add_partition("table1", "pk2");

        // Events leave the queue in the order their tables came in
        let event = queue.dequeue().unwrap();
        assert_eq!("table1", event.table_name);
        assert_eq!(vec!["pk1", "pk2"], partitions(&event));

        let event = queue.dequeue().unwrap();
        assert_eq!("table2", event.table_name);
        assert_eq!(vec!["pk1"], partitions(&event));

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn returned_event_is_dequeued_again() {
        let mut queue = UpdatePartitionsLastReadTimeQueue::new();

        queue.add_partition("table", "pk1");
        queue.add_partition("table", "pk2");

        // Taken for delivery - and the connection is lost before the confirmation
        let on_delivery = queue.dequeue().unwrap();
        assert!(queue.dequeue().is_none());

        queue.return_event(on_delivery);

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!(vec!["pk1", "pk2"], partitions(&event));

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn reads_after_an_event_is_returned_do_not_grow_the_queue() {
        let mut queue = UpdatePartitionsLastReadTimeQueue::new();

        queue.add_partition("table", "pk1");
        let on_delivery = queue.dequeue().unwrap();

        // Read while the event is on delivery: nothing is queued for the table, so it is a
        // new event - the returned one is queued behind it.
        queue.add_partition("table", "pk2");
        queue.return_event(on_delivery);

        for _ in 0..100 {
            queue.add_partition("table", "pk1");
            queue.add_partition("table", "pk2");
        }

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!(vec!["pk1", "pk2"], partitions(&event));

        let returned = queue.dequeue().unwrap();
        assert_eq!("table", returned.table_name);
        assert_eq!(vec!["pk1"], partitions(&returned));

        assert!(queue.dequeue().is_none());
    }
}
