use std::collections::BTreeMap;

use rust_extensions::auto_shrink::VecDequeAutoShrink;
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(Debug, Clone)]
pub struct UpdateRowsExpirationTimeEvent {
    pub table_name: String,
    pub partition_key: String,
    pub row_keys: BTreeMap<String, ()>,
    pub expiration_time: Option<DateTimeAsMicroseconds>,
}

pub struct UpdateRowsExpirationTimeQueue {
    queue: VecDequeAutoShrink<UpdateRowsExpirationTimeEvent>,
}

impl UpdateRowsExpirationTimeQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDequeAutoShrink::new(32),
        }
    }

    pub fn add<'s, TRowKeys: Iterator<Item = &'s str>>(
        &mut self,
        table_name: &str,
        partition_key: &str,
        row_keys: TRowKeys,
        date_time: Option<DateTimeAsMicroseconds>,
    ) {
        if let Some(item) = self
            .queue
            .iter_mut()
            .find(|itm| itm.table_name == table_name && itm.partition_key == partition_key)
        {
            for row_key in row_keys {
                item.row_keys.insert(row_key.to_string(), ());
            }

            return;
        }

        let item = UpdateRowsExpirationTimeEvent {
            table_name: table_name.to_string(),
            partition_key: partition_key.to_string(),
            row_keys: row_keys.map(|k| (k.to_string(), ())).collect(),
            expiration_time: date_time,
        };

        self.queue.push_back(item);
    }

    pub fn return_event(&mut self, event: UpdateRowsExpirationTimeEvent) {
        self.queue.push_back(event);
    }
    pub fn dequeue(&mut self) -> Option<UpdateRowsExpirationTimeEvent> {
        self.queue.pop_front()
    }
}
