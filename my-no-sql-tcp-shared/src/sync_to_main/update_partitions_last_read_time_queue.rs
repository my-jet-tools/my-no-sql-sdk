use rust_extensions::auto_shrink::VecDequeAutoShrink;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct UpdatePartitionsLastReadTimeEvent {
    pub table_name: String,
    pub partitions: BTreeMap<String, ()>,
}

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
