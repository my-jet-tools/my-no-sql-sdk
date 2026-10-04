use std::collections::VecDeque;
#[derive(Debug, Clone)]
pub struct UpdateRowsLastReadTimeEvent {
    pub table_name: String,
    pub partition_key: String,
    pub row_keys: Vec<String>,
}

impl UpdateRowsLastReadTimeEvent {
    /// Adds the row key unless it is already there. `row_keys` are kept sorted - the binary
    /// search relies on it - so every row key has to get in through this method.
    pub fn insert_row_key(&mut self, row_key: &str) {
        let index = self
            .row_keys
            .binary_search_by(|itm| itm.as_str().cmp(row_key));

        if let Err(index) = index {
            self.row_keys.insert(index, row_key.to_string());
        }
    }
}

/// Rows which were read and wait to be reported to the main node, grouped by table and
/// partition.
///
/// [`Self::add`] merges into an event which is already queued for the partition and queues a
/// new one only when there is none. Either way the row keys of an event stay sorted and unique -
/// so reading the same rows over and over does not grow the queue, whether the connection to
/// the main node is up or down.
pub struct UpdateRowsLastReadTimeQueue {
    queue: VecDeque<UpdateRowsLastReadTimeEvent>,
}

impl UpdateRowsLastReadTimeQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    pub fn add<'s, TRowKeys: Iterator<Item = &'s str>>(
        &mut self,
        table_name: &str,
        partition_key: &str,
        row_keys: TRowKeys,
    ) {
        let mut row_keys = row_keys.peekable();

        // Nothing to deliver - nothing to queue
        if row_keys.peek().is_none() {
            return;
        }

        if let Some(item) = self
            .queue
            .iter_mut()
            .find(|itm| itm.table_name == table_name && itm.partition_key == partition_key)
        {
            for row_key in row_keys {
                item.insert_row_key(row_key);
            }
            return;
        }

        let mut item = UpdateRowsLastReadTimeEvent {
            table_name: table_name.to_string(),
            partition_key: partition_key.to_string(),
            row_keys: Vec::new(),
        };

        for row_key in row_keys {
            item.insert_row_key(row_key);
        }

        self.queue.push_back(item);
    }

    pub fn return_event(&mut self, event: UpdateRowsLastReadTimeEvent) {
        self.queue.push_back(event);
    }

    pub fn dequeue(&mut self) -> Option<UpdateRowsLastReadTimeEvent> {
        self.queue.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::{UpdateRowsLastReadTimeEvent, UpdateRowsLastReadTimeQueue};

    #[test]
    fn insert_row_key_keeps_row_keys_sorted_and_unique() {
        let mut event = UpdateRowsLastReadTimeEvent {
            table_name: "table".to_string(),
            partition_key: "pk".to_string(),
            row_keys: Vec::new(),
        };

        for row_key in ["rk2", "rk3", "rk1", "rk2", "rk1", "rk0"] {
            event.insert_row_key(row_key);
        }

        assert_eq!(event.row_keys, ["rk0", "rk1", "rk2", "rk3"]);
    }

    #[test]
    fn add_of_the_same_row_does_not_grow_the_event() {
        let mut queue = UpdateRowsLastReadTimeQueue::new();

        for _ in 0..100 {
            queue.add("table", "pk", ["rk"].into_iter());
        }

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(event.row_keys, ["rk"]);

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn add_merges_the_rows_of_a_partition_into_one_event_sorted_and_unique() {
        let mut queue = UpdateRowsLastReadTimeQueue::new();

        queue.add("table", "pk", ["rk3", "rk1"].into_iter());
        queue.add("table", "pk", ["rk2", "rk3", "rk0"].into_iter());
        queue.add("table", "pk", ["rk1"].into_iter());

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(event.row_keys, ["rk0", "rk1", "rk2", "rk3"]);

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn add_without_rows_queues_nothing() {
        let mut queue = UpdateRowsLastReadTimeQueue::new();

        queue.add("table", "pk", std::iter::empty());

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn every_partition_has_an_event_of_its_own() {
        let mut queue = UpdateRowsLastReadTimeQueue::new();

        queue.add("table1", "pk1", ["rk1"].into_iter());
        queue.add("table1", "pk2", ["rk1"].into_iter());
        queue.add("table2", "pk1", ["rk1"].into_iter());
        queue.add("table1", "pk1", ["rk2"].into_iter());

        // Events leave the queue in the order their partitions came in
        let event = queue.dequeue().unwrap();
        assert_eq!("table1", event.table_name);
        assert_eq!("pk1", event.partition_key);
        assert_eq!(event.row_keys, ["rk1", "rk2"]);

        let event = queue.dequeue().unwrap();
        assert_eq!("table1", event.table_name);
        assert_eq!("pk2", event.partition_key);
        assert_eq!(event.row_keys, ["rk1"]);

        let event = queue.dequeue().unwrap();
        assert_eq!("table2", event.table_name);
        assert_eq!("pk1", event.partition_key);
        assert_eq!(event.row_keys, ["rk1"]);

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn returned_event_is_dequeued_again() {
        let mut queue = UpdateRowsLastReadTimeQueue::new();

        queue.add("table", "pk", ["rk1", "rk2"].into_iter());

        // Taken for delivery - and the connection is lost before the confirmation
        let on_delivery = queue.dequeue().unwrap();
        assert!(queue.dequeue().is_none());

        queue.return_event(on_delivery);

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(event.row_keys, ["rk1", "rk2"]);

        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn reads_after_an_event_is_returned_do_not_grow_it() {
        let mut queue = UpdateRowsLastReadTimeQueue::new();

        queue.add("table", "pk", ["rk1", "rk2"].into_iter());
        let on_delivery = queue.dequeue().unwrap();
        queue.return_event(on_delivery);

        // The returned event is the one queued for the partition now - the reads which
        // follow are merged into it the same way.
        for _ in 0..100 {
            queue.add("table", "pk", ["rk2", "rk0"].into_iter());
        }

        let event = queue.dequeue().unwrap();
        assert_eq!("table", event.table_name);
        assert_eq!("pk", event.partition_key);
        assert_eq!(event.row_keys, ["rk0", "rk1", "rk2"]);

        assert!(queue.dequeue().is_none());
    }
}
