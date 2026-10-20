use std::{
    collections::VecDeque,
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context, Poll},
};

use crate::{
    error::{BodyError, body_error},
    telemetry::{End, Observation},
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

pub(crate) struct ObservedBody {
    inner: ProxyBody,
    observation: Observation,
}

impl ObservedBody {
    pub(crate) fn new(inner: ProxyBody, mut observation: Observation) -> Self {
        if inner.is_end_stream() {
            observation.finish(End::Success);
        }
        Self { inner, observation }
    }
}

impl Body for ObservedBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_frame(cx);
        match &result {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.observation.add_bytes(data.len());
                }
                if this.inner.is_end_stream() {
                    this.observation.finish(End::Success);
                }
            }
            Poll::Ready(Some(Err(error))) => {
                this.observation.finish(match body_error(error.as_ref()) {
                    Some(BodyError::Timeout) => End::Timeout,
                    Some(BodyError::Limit) => End::Limit,
                    None => End::Transport,
                })
            }
            Poll::Ready(None) => this.observation.finish(End::Success),
            Poll::Pending => {}
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

pub(crate) struct UpstreamLease {
    permit: Option<OwnedSemaphorePermit>,
    observation: Observation,
    deadline: TransferDeadline,
}

const TRANSFER_ACTIVE: u8 = 0;
const TRANSFER_FINISHED: u8 = 1;
const TRANSFER_EXPIRED: u8 = 2;

#[derive(Clone)]
pub(crate) struct TransferDeadline {
    pub(crate) at: Instant,
    state: Arc<AtomicU8>,
}

impl TransferDeadline {
    pub(crate) fn active(&self) -> bool {
        self.state.load(Ordering::Acquire) == TRANSFER_ACTIVE
    }
    pub(crate) fn expire(&self) -> bool {
        self.state
            .compare_exchange(
                TRANSFER_ACTIVE,
                TRANSFER_EXPIRED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

impl UpstreamLease {
    pub(crate) fn new(
        permit: OwnedSemaphorePermit,
        observation: Observation,
        deadline: Instant,
    ) -> Self {
        Self {
            permit: Some(permit),
            observation,
            deadline: TransferDeadline {
                at: deadline,
                state: Arc::new(AtomicU8::new(TRANSFER_ACTIVE)),
            },
        }
    }
    pub(crate) fn deadline(&self) -> TransferDeadline {
        self.deadline.clone()
    }
    pub(crate) fn add_bytes(&mut self, bytes: usize) {
        self.observation.add_bytes(bytes);
    }
    pub(crate) fn finish(&mut self, end: End) {
        let timed_out = self
            .deadline
            .state
            .swap(TRANSFER_FINISHED, Ordering::AcqRel)
            == TRANSFER_EXPIRED;
        self.observation
            .finish(if timed_out { End::Timeout } else { end });
        self.permit.take();
    }
}

impl Drop for UpstreamLease {
    fn drop(&mut self) {
        self.finish(End::Cancelled);
    }
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
    lease: Option<UpstreamLease>,
}

impl StreamBody {
    pub(crate) fn new(
        inner: Incoming,
        deadline: Instant,
        remaining: Option<usize>,
        mut lease: Option<UpstreamLease>,
    ) -> Self {
        let done = inner.is_end_stream();
        if done && let Some(lease) = &mut lease {
            lease.finish(End::Success);
        }
        Self {
            inner,
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            remaining,
            done,
            lease,
        }
    }

    fn fail(&mut self, error: BodyError) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        self.done = true;
        if let Some(lease) = &mut self.lease {
            lease.finish(match error {
                BodyError::Limit => End::Limit,
                BodyError::Timeout => End::Timeout,
            });
        }
        Poll::Ready(Some(Err(error.into())))
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
            return this.fail(BodyError::Timeout);
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    if let Some(remaining) = &mut this.remaining {
                        if data.len() > *remaining {
                            return this.fail(BodyError::Limit);
                        }
                        *remaining -= data.len();
                    }
                    if let Some(lease) = &mut this.lease {
                        lease.add_bytes(data.len());
                        if this.inner.is_end_stream() {
                            lease.finish(End::Success);
                        }
                    }
                    Poll::Ready(Some(Ok(frame)))
                } else {
                    this.done = true;
                    if let Some(lease) = &mut this.lease {
                        lease.finish(End::Success);
                    }
                    Poll::Ready(None)
                }
            }
            Poll::Ready(Some(Err(error))) => {
                this.done = true;
                if let Some(lease) = &mut this.lease {
                    lease.finish(End::Transport);
                }
                Poll::Ready(Some(Err(error.into())))
            }
            Poll::Ready(None) => {
                this.done = true;
                if let Some(lease) = &mut this.lease {
                    lease.finish(End::Success);
                }
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
