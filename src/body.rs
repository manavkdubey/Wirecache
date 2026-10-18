use std::{
    collections::VecDeque,
    error::Error,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use tokio::{
    sync::OwnedSemaphorePermit,
    time::{Instant, Sleep},
};

pub(crate) type BoxError = Box<dyn Error + Send + Sync>;
pub(crate) type ProxyBody = UnsyncBoxBody<Bytes, BoxError>;

pub(crate) fn full(value: impl Into<Bytes>) -> ProxyBody {
    Full::new(value.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

pub(crate) struct PrefixBody {
    prefix: VecDeque<Bytes>,
    tail: StreamBody,
}

impl PrefixBody {
    pub(crate) fn new(prefix: Bytes, overflow: Bytes, tail: StreamBody) -> Self {
        Self {
            prefix: [prefix, overflow]
                .into_iter()
                .filter(|chunk| !chunk.is_empty())
                .collect(),
            tail,
        }
    }
}

impl Body for PrefixBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        if let Some(chunk) = this.prefix.pop_front() {
            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        Pin::new(&mut this.tail).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.prefix.is_empty() && self.tail.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        let prefix = self
            .prefix
            .iter()
            .map(|chunk| chunk.len() as u64)
            .sum::<u64>();
        let tail = self.tail.size_hint();
        let mut hint = SizeHint::new();
        hint.set_lower(prefix.saturating_add(tail.lower()));
        if let Some(upper) = tail.upper().and_then(|upper| upper.checked_add(prefix)) {
            hint.set_upper(upper);
        }
        hint
    }
}

pub(crate) struct StreamBody {
    inner: Incoming,
    deadline: Pin<Box<Sleep>>,
    remaining: Option<usize>,
    done: bool,
    _permit: Option<OwnedSemaphorePermit>,
}

impl StreamBody {
    pub(crate) fn new(
        inner: Incoming,
        deadline: Instant,
        remaining: Option<usize>,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            inner,
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            remaining,
            done: false,
            _permit: permit,
        }
    }

    fn fail(
        &mut self,
        kind: io::ErrorKind,
        message: &str,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        self.done = true;
        Poll::Ready(Some(Err(io::Error::new(kind, message).into())))
    }
}

impl Body for StreamBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        if this.deadline.as_mut().poll(cx).is_ready() {
            return this.fail(io::ErrorKind::TimedOut, "body transfer timed out");
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    if let Some(remaining) = &mut this.remaining {
                        if data.len() > *remaining {
                            return this
                                .fail(io::ErrorKind::InvalidData, "request body exceeds limit");
                        }
                        *remaining -= data.len();
                    }
                    Poll::Ready(Some(Ok(frame)))
                } else {
                    this.done = true;
                    Poll::Ready(None)
                }
            }
            Poll::Ready(Some(Err(error))) => {
                this.done = true;
                Poll::Ready(Some(Err(error.into())))
            }
            Poll::Ready(None) => {
                this.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done || self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
