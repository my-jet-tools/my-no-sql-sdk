use std::ops::Deref;

use flurl::{body::HttpRequestBody, FlUrl, FlUrlError, FlUrlResponse};
use rust_extensions::StrOrString;

/// A request of the writer: its FlUrl and the largest answer the writer reads
/// ([`super::DEFAULT_BODY_SIZE_LIMIT`] unless the writer was given another limit).
///
/// FlUrl takes the limit of an answer where the body is read, not where the request is made -
/// and the body is read deep in the checks of an answer, far from the writer which has the
/// limit. So the request carries the limit to its answer, [`WriterFlUrlResponse`].
pub struct WriterFlUrl {
    fl_url: FlUrl,
    body_size_limit: usize,
}

impl WriterFlUrl {
    pub fn new(fl_url: FlUrl, body_size_limit: usize) -> Self {
        Self {
            fl_url,
            body_size_limit,
        }
    }

    /// For what is done to the FlUrl of the request by code which takes a plain FlUrl -
    /// `CreateTableParams::populate_params`, `UpdateReadStatistics::fill_fields`.
    pub fn map(self, f: impl FnOnce(FlUrl) -> FlUrl) -> Self {
        Self {
            fl_url: f(self.fl_url),
            body_size_limit: self.body_size_limit,
        }
    }

    pub fn append_path_segment<'s>(self, path_segment: impl Into<StrOrString<'s>>) -> Self {
        self.map(|fl_url| fl_url.append_path_segment(path_segment))
    }

    pub fn append_query_param<'n, 'v>(
        self,
        param_name: impl Into<StrOrString<'n>>,
        value: Option<impl Into<StrOrString<'v>>>,
    ) -> Self {
        self.map(|fl_url| fl_url.append_query_param(param_name, value))
    }

    pub fn with_header<'n, 'v>(
        self,
        name: impl Into<StrOrString<'n>>,
        value: impl Into<StrOrString<'v>>,
    ) -> Self {
        self.map(|fl_url| fl_url.with_header(name, value))
    }

    pub fn with_retries(self, max_retries: usize) -> Self {
        self.map(|fl_url| fl_url.with_retries(max_retries))
    }

    pub async fn get(self) -> Result<WriterFlUrlResponse, FlUrlError> {
        let response = self.fl_url.get().await?;
        Ok(WriterFlUrlResponse::new(response, self.body_size_limit))
    }

    pub async fn post(
        self,
        body: impl Into<HttpRequestBody>,
    ) -> Result<WriterFlUrlResponse, FlUrlError> {
        let response = self.fl_url.post(body).await?;
        Ok(WriterFlUrlResponse::new(response, self.body_size_limit))
    }

    pub async fn put(
        self,
        body: impl Into<HttpRequestBody>,
    ) -> Result<WriterFlUrlResponse, FlUrlError> {
        let response = self.fl_url.put(body).await?;
        Ok(WriterFlUrlResponse::new(response, self.body_size_limit))
    }

    pub async fn delete(self) -> Result<WriterFlUrlResponse, FlUrlError> {
        let response = self.fl_url.delete().await?;
        Ok(WriterFlUrlResponse::new(response, self.body_size_limit))
    }
}

/// An answer to a [`WriterFlUrl`]: the status and the headers are those of the FlUrl answer, the
/// body is read with the limit of the writer.
///
/// FlUrl gives the body of an answer away once, and the checks of an answer read it one after
/// another - `is_record_not_found` and then `unexpected_response`, `is_table_not_found` and
/// then `check_error`. So the body is read once, on the first call of
/// [`Self::get_body_as_slice`], and kept for the calls after it.
pub struct WriterFlUrlResponse {
    response: FlUrlResponse,
    body_size_limit: usize,
    body: Option<Vec<u8>>,
}

impl WriterFlUrlResponse {
    fn new(response: FlUrlResponse, body_size_limit: usize) -> Self {
        Self {
            response,
            body_size_limit,
            body: None,
        }
    }

    /// The whole body. One over the limit of the writer fails with
    /// `FlUrlError::ResponseBodyTooLarge`.
    pub async fn get_body_as_slice(&mut self) -> Result<&[u8], FlUrlError> {
        if self.body.is_none() {
            let body = self
                .response
                .get_body()?
                .into_vec(self.body_size_limit)
                .await?;

            self.body = Some(body);
        }

        Ok(self.body.as_deref().unwrap_or_default())
    }
}

/// The status, the headers and the url of the answer. Not `DerefMut`: the body is read through
/// [`WriterFlUrlResponse::get_body_as_slice`] only, or the next read of it would find it taken.
impl Deref for WriterFlUrlResponse {
    type Target = FlUrlResponse;

    fn deref(&self) -> &FlUrlResponse {
        &self.response
    }
}
