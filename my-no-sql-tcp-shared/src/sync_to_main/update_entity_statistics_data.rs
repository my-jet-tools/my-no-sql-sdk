use rust_extensions::date_time::DateTimeAsMicroseconds;

pub struct UpdateEntityStatisticsData {
    pub partition_last_read_moment: bool,
    pub row_last_read_moment: bool,
    pub partition_expiration_moment: Option<Option<DateTimeAsMicroseconds>>,
    pub row_expiration_moment: Option<Option<DateTimeAsMicroseconds>>,
}

impl UpdateEntityStatisticsData {
    pub fn has_data_to_update(&self) -> bool {
        self.partition_last_read_moment
            || self.row_last_read_moment
            || self.partition_expiration_moment.is_some()
            || self.row_expiration_moment.is_some()
    }
}

impl Default for UpdateEntityStatisticsData {
    fn default() -> Self {
        Self {
            partition_last_read_moment: false,
            row_last_read_moment: false,
            partition_expiration_moment: None,
            row_expiration_moment: None,
        }
    }
}
