use hyper::{
    Response, StatusCode,
    header::{CONTENT_TYPE, RETRY_AFTER},
};
use std::{error::Error, fmt};

use crate::body::{ProxyBody, full};
use crate::telemetry::End;

#[derive(Clone, Copy, Debug)]
pub(crate) enum BodyError {
    Limit,
    Timeout,
}

impl fmt::Display for BodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Limit => "request body exceeds limit",
            Self::Timeout => "body transfer timed out",
        })
    }
}

impl Error for BodyError {}

pub(crate) fn body_error(error: &(dyn Error + 'static)) -> Option<BodyError> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<BodyError>() {
            return Some(*error);
        }
        current = error.source();
    }
    None
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchError {
    Transport,
    Timeout,
    Overloaded,
    Shutdown,
    Internal,
    BodyLimit,
}

impl FetchError {
    pub(crate) fn end(self) -> End {
        match self {
            Self::Timeout => End::Timeout,
            Self::BodyLimit | Self::Overloaded => End::Limit,
            Self::Shutdown => End::Cancelled,
            Self::Transport | Self::Internal => End::Transport,
        }
    }

    pub(crate) fn from_source(error: &(dyn Error + 'static)) -> Self {
        match body_error(error) {
            Some(BodyError::Limit) => Self::BodyLimit,
            Some(BodyError::Timeout) => Self::Timeout,
            None => Self::Transport,
        }
    }

    pub(crate) fn response(self) -> Response<ProxyBody> {
        let (status, message) = match self {
            Self::Transport => (StatusCode::BAD_GATEWAY, "upstream transfer failed\n"),
            Self::Timeout => (StatusCode::GATEWAY_TIMEOUT, "request timed out\n"),
            Self::Overloaded => (StatusCode::SERVICE_UNAVAILABLE, "capacity exhausted\n"),
            Self::Shutdown => (StatusCode::SERVICE_UNAVAILABLE, "proxy shutting down\n"),
            Self::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "flight task failed\n"),
            Self::BodyLimit => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body exceeds limit\n",
            ),
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
