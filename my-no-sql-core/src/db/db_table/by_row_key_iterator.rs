use std::sync::Arc;

use crate::db::DbPartition;

pub struct ByRowKeyIterator<'a> {
    partitions_iterator: std::slice::Iter<'a, DbPartition>,
    row_key: &'a str,
    skip: Option<usize>,
    limit: Option<usize>,
    no: usize,
    yielded: usize,
}

impl<'a> ByRowKeyIterator<'a> {
    pub fn new(
        partitions_iterator: std::slice::Iter<'a, DbPartition>,
        row_key: &'a str,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            row_key,
            partitions_iterator,
            skip,
            limit,
            no: 0,
            yielded: 0,
        }
    }
}

impl<'a> Iterator for ByRowKeyIterator<'a> {
    type Item = (&'a DbPartition, &'a Arc<crate::db::DbRow>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(limit) = self.limit {
            if self.yielded >= limit {
                return None;
            }
        }

        loop {
            let next_partition = self.partitions_iterator.next()?;

            let db_row = next_partition.get_row(self.row_key);

            if let Some(db_row) = db_row {
                self.no += 1;
                if let Some(skip) = self.skip {
                    if self.no <= skip {
                        continue;
                    }
                }

                self.yielded += 1;
                return Some((next_partition, db_row));
            }
        }
    }
}
