use std::collections::VecDeque;
#[derive(Debug, Clone)]
pub struct UpdateRowsLastReadTimeEvent {
    pub table_name: String,
    pub partition_key: String,
    pub row_keys: Vec<String>,
}

impl UpdateRowsLastReadTimeEvent {
    pub fn insert_row_key(&mut self, row_key: &str) {
        let index = self
            .row_keys
            .binary_search_by(|itm| itm.as_str().cmp(row_key));

        if let Err(index) = index {
            self.row_keys.insert(index, row_key.to_string());
        }
    }
}

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
        if let Some(item) = self
            .queue
            .iter_mut()
            .find(|itm| itm.table_name == table_name && itm.partition_key == partition_key)
        {
            for row_key in row_keys {
                item.row_keys.push(row_key.to_string());
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
