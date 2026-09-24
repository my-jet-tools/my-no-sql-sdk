use flurl::FlUrl;
use rust_extensions::date_time::DateTimeAsMicroseconds;

pub struct UpdateReadStatistics {
    pub update_partition_read_access: bool,
    pub update_row_read_access: bool,
    pub update_partition_expiration_moment: Option<Option<DateTimeAsMicroseconds>>,
    pub update_rows_expiration_moment: Option<Option<DateTimeAsMicroseconds>>,
}

impl UpdateReadStatistics {
    pub fn fill_fields(&self, mut fl_url_request: FlUrl) -> FlUrl {
        if self.update_partition_read_access {
            fl_url_request = fl_url_request.with_header("updatePartitionLastReadTime", "true");
        }

        if self.update_row_read_access {
            fl_url_request = fl_url_request.with_header("updateRowsLastReadTime", "true");
        }

        if let Some(update_partition_expiration_moment) = self.update_partition_expiration_moment {
            if let Some(update_partition_expiration_moment) = update_partition_expiration_moment {
                fl_url_request = fl_url_request.with_header(
                    "setPartitionExpirationTime",
                    update_partition_expiration_moment.to_rfc3339(),
                );
            } else {
                fl_url_request = fl_url_request.with_header("setPartitionExpirationTime", "Null");
            }
        }

        if let Some(update_rows_expiration_moment) = self.update_rows_expiration_moment {
            if let Some(update_rows_expiration_moment) = update_rows_expiration_moment {
                fl_url_request = fl_url_request.with_header(
                    "setRowsExpirationTime",
                    update_rows_expiration_moment.to_rfc3339(),
                );
            } else {
                fl_url_request = fl_url_request.with_header("setRowsExpirationTime", "Null");
            }
        }
        fl_url_request
    }
}
