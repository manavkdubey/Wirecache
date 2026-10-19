use crate::{
    Config,
    body::{PrefixBody, ProxyBody, StreamBody, full},
    cache::{Fill, Snapshot},
    error::FetchError,
    headers, policy,
};
use http_body_util::BodyExt;
use hyper::{Request, Response, StatusCode, body::Incoming, header::CONTENT_LENGTH};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioTimer},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout_at},
};

pub(crate) enum FetchResult {
    Shared(Arc<Snapshot>),
    Private(Response<ProxyBody>),
    Failed(FetchError),
}

pub(crate) struct Upstream {
    config: Config,
    client: Client<HttpConnector, ProxyBody>,
    slots: Arc<Semaphore>,
    attempts: AtomicU64,
}

impl Upstream {
    pub(crate) fn new(config: &Config) -> Arc<Self> {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(Duration::from_millis(config.upstream_timeout_ms)));
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(config.max_upstream_requests)
            .http1_max_buf_size(32 * 1024)
            .retry_canceled_requests(false)
            .build(connector);
        Arc::new(Self {
            config: config.clone(),
            client,
            slots: Arc::new(Semaphore::new(config.max_upstream_requests)),
            attempts: AtomicU64::new(0),
        })
    }

    pub(crate) fn attempts(&self) -> u64 {
        self.attempts.load(Ordering::Relaxed)
    }

    pub(crate) async fn fetch(
        &self,
        request: Request<ProxyBody>,
        fill: Option<Fill>,
        deadline: Instant,
    ) -> FetchResult {
        let Ok(permit) = self.slots.clone().try_acquire_owned() else {
            return FetchResult::Failed(FetchError::Overloaded);
        };
        let requested_at = Instant::now();
        self.attempts.fetch_add(1, Ordering::Relaxed);
        let response = match timeout_at(deadline, self.client.request(request)).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => return FetchResult::Failed(FetchError::Transport),
            Err(_) => return FetchResult::Failed(FetchError::Timeout),
        };
        self.forward_response(response, fill, requested_at, deadline, permit)
            .await
    }

    async fn forward_response(
        &self,
        response: Response<Incoming>,
        fill: Option<Fill>,
        requested_at: Instant,
        deadline: Instant,
        permit: OwnedSemaphorePermit,
    ) -> FetchResult {
        let received_at = Instant::now();
        let response_time = SystemTime::now();
        let freshness = fill.as_ref().and_then(|_| {
            policy::response_freshness(
                response.status(),
                response.headers(),
                requested_at,
                received_at,
                response_time,
                Duration::from_secs(self.config.max_ttl_secs),
            )
        });
        let (mut parts, mut body) = response.into_parts();
        if parts.status == StatusCode::SWITCHING_PROTOCOLS
            || headers::sanitize(&mut parts.headers).is_err()
        {
            return FetchResult::Failed(FetchError::Transport);
        }
        let limit = self.config.max_entry_kib * 1024;
        let oversized = parts.headers.get(CONTENT_LENGTH).is_some_and(|value| {
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .is_none_or(|size| size > limit)
        });
        let Some(freshness) =
            freshness.filter(|_| !oversized && parts.headers.contains_key("cache-control"))
        else {
            return FetchResult::Private(Response::from_parts(
                parts,
                StreamBody::new(body, deadline, None, Some(permit)).boxed_unsync(),
            ));
        };
        let mut buffered = Vec::new();
        let mut trailers = false;
        loop {
            let frame = match timeout_at(deadline, body.frame()).await {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(_))) => {
                    return FetchResult::Failed(FetchError::Transport);
                }
                Err(_) => return FetchResult::Failed(FetchError::Timeout),
                Ok(None) => break,
            };
            let Ok(chunk) = frame.into_data() else {
                trailers = true;
                continue;
            };
            if chunk.len() > limit - buffered.len() {
                let prefix = bytes::Bytes::from(buffered.into_boxed_slice());
                let tail = StreamBody::new(body, deadline, None, Some(permit));
                return FetchResult::Private(Response::from_parts(
                    parts,
                    PrefixBody::new(prefix, chunk, tail).boxed_unsync(),
                ));
            }
            let needed = buffered.len() + chunk.len();
            if needed > buffered.capacity() {
                let capacity = needed
                    .checked_next_power_of_two()
                    .unwrap_or(limit)
                    .min(limit);
                buffered.reserve_exact(capacity - buffered.len());
            }
            buffered.extend_from_slice(&chunk);
        }
        let bytes = bytes::Bytes::from(buffered.into_boxed_slice());
        if trailers {
            return FetchResult::Private(Response::from_parts(parts, full(bytes)));
        }
        let snapshot = Arc::new(Snapshot::new(
            parts.headers,
            bytes,
            freshness,
            response_time,
        ));
        if !snapshot.is_fresh(Instant::now()) {
            return FetchResult::Private(snapshot.response(Instant::now()));
        }
        if let Some(fill) = fill {
            fill.insert(snapshot.clone(), Instant::now());
        }
        FetchResult::Shared(snapshot)
    }
}
