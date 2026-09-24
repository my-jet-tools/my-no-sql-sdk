use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(Debug, Clone)]
pub struct DbTableAttributes {
    pub persist: bool,
    pub max_partitions_amount: Option<usize>,
    pub max_rows_per_partition_amount: Option<usize>,
    /// When true, the rows of this table are kept in memory DEFLATE-compressed
    /// (transparently decompressed on read). Opt-in per table.
    pub compressed: bool,
    pub created: DateTimeAsMicroseconds,
}

impl DbTableAttributes {
    pub fn create_default() -> Self {
        Self {
            created: DateTimeAsMicroseconds::now(),
            persist: true,
            max_partitions_amount: None,
            max_rows_per_partition_amount: None,
            compressed: false,
        }
    }
}

impl Default for DbTableAttributes {
    fn default() -> Self {
        Self::create_default()
    }
}

impl DbTableAttributes {
    pub fn new(
        persist: bool,
        max_partitions_amount: Option<usize>,
        max_rows_per_partition_amount: Option<usize>,
        compressed: bool,
        created: DateTimeAsMicroseconds,
    ) -> Self {
        Self {
            persist,
            created,
            max_partitions_amount,
            max_rows_per_partition_amount,
            compressed,
        }
    }

    pub fn update(
        &mut self,
        persist_table: bool,
        max_partitions_amount: Option<usize>,
        max_rows_per_partition_amount: Option<usize>,
    ) -> bool {
        let mut result = false;

        if self.persist != persist_table {
            self.persist = persist_table;
            result = true;
        }

        if self.max_partitions_amount != max_partitions_amount {
            self.max_partitions_amount = max_partitions_amount;
            result = true;
        }

        if self.max_rows_per_partition_amount != max_rows_per_partition_amount {
            self.max_rows_per_partition_amount = max_rows_per_partition_amount;
            result = true;
        }

        return result;
    }

    /// Toggles the in-memory compression flag. Returns true if the value changed
    /// (the caller is then responsible for re-encoding the already stored rows).
    pub fn set_compressed(&mut self, compressed: bool) -> bool {
        if self.compressed == compressed {
            return false;
        }

        self.compressed = compressed;
        true
    }
}
