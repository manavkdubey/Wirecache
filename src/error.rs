use hyper::{
    Response, StatusCode,
    header::{CONTENT_TYPE, RETRY_AFTER},
};

use crate::body::{ProxyBody, full};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchError {
    Transport,
    Timeout,
    Overloaded,
    Shutdown,
    Internal,
}

impl FetchError {
    pub(crate) fn response(self) -> Response<ProxyBody> {
        let (status, message) = match self {
            Self::Transport => (StatusCode::BAD_GATEWAY, "upstream transfer failed\n"),
            Self::Timeout => (StatusCode::GATEWAY_TIMEOUT, "request timed out\n"),
            Self::Overloaded => (StatusCode::SERVICE_UNAVAILABLE, "capacity exhausted\n"),
            Self::Shutdown => (StatusCode::SERVICE_UNAVAILABLE, "proxy shutting down\n"),
            Self::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "flight task failed\n"),
        };
        let mut response = reply(status, message);
        if status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(RETRY_AFTER, "1".parse().unwrap());
        }
        response
    }
}

pub(crate) fn reply(status: StatusCode, body: impl Into<bytes::Bytes>) -> Response<ProxyBody> {
    let mut response = Response::new(full(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, "text/plain; charset=utf-8".parse().unwrap());
    response
}
