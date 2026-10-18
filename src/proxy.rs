use std::{
    convert::Infallible,
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use http_body_util::BodyExt;
use hyper::{
    Request, Response, StatusCode, Uri, Version,
    body::{Body, Incoming},
    header::{ACCEPT, ACCEPT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HOST, RETRY_AFTER, UPGRADE},
};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioTimer},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout_at},
};

use crate::{
    Config,
    body::{PrefixBody, ProxyBody, StreamBody, full},
    cache::{Cache, CacheKey, Fill, Snapshot},
    headers, policy,
};

pub struct Proxy {
    pub(crate) config: Config,
    client: Client<HttpConnector, ProxyBody>,
    upstream_slots: Arc<Semaphore>,
    requests: AtomicU64,
    origin_requests: AtomicU64,
    overloads: AtomicU64,
    cache: Arc<Cache>,
}

impl Proxy {
    pub fn new(config: Config) -> io::Result<Arc<Self>> {
        config.validate()?;
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(Duration::from_millis(config.upstream_timeout_ms)));
        let client = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(config.max_upstream_requests)
            .http1_max_buf_size(32 * 1024)
            .retry_canceled_requests(false)
            .build(connector);
        Ok(Arc::new(Self {
            cache: Cache::new(
                config.max_cache_mib * 1024 * 1024,
                config.max_cache_entries,
                config.max_entry_kib * 1024,
            ),
            upstream_slots: Arc::new(Semaphore::new(config.max_upstream_requests)),
            config,
            client,
            requests: AtomicU64::new(0),
            origin_requests: AtomicU64::new(0),
            overloads: AtomicU64::new(0),
        }))
    }

    pub(crate) async fn handle(
        &self,
        mut request: Request<Incoming>,
    ) -> Result<Response<ProxyBody>, Infallible> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if request.version() != Version::HTTP_11 {
            return Ok(reply(
                StatusCode::HTTP_VERSION_NOT_SUPPORTED,
                "HTTP/1.1 required\n",
            ));
        }
        if request.method() == hyper::Method::CONNECT
            || request.headers().contains_key(UPGRADE)
            || headers::has_connection_token(request.headers(), "upgrade")
        {
            return Ok(reply(
                StatusCode::NOT_IMPLEMENTED,
                "protocol upgrades are unsupported\n",
            ));
        }
        if matches!(request.uri().path(), "/healthz" | "/metrics") {
            if request.method() != hyper::Method::GET && request.method() != hyper::Method::HEAD {
                let mut response = reply(StatusCode::METHOD_NOT_ALLOWED, "GET or HEAD required\n");
                response
                    .headers_mut()
                    .insert(hyper::header::ALLOW, "GET, HEAD".parse().unwrap());
                return Ok(response);
            }
            if request.uri().path() == "/healthz" {
                return Ok(reply(StatusCode::OK, "ok\n"));
            }
            let metrics = format!(
                "wirecache_requests_total {}\nwirecache_origin_requests_total {}\nwirecache_overloads_total {}\n",
                self.requests.load(Ordering::Relaxed),
                self.origin_requests.load(Ordering::Relaxed),
                self.overloads.load(Ordering::Relaxed)
            );
            let stats = self.cache.stats();
            let metrics = format!(
                "{metrics}wirecache_cache_bytes {}\nwirecache_cache_entries {}\nwirecache_cache_hits_total {}\nwirecache_cache_misses_total {}\nwirecache_cache_evictions_total {}\n",
                stats.bytes, stats.entries, stats.hits, stats.misses, stats.evictions
            );
            let mut response = reply(StatusCode::OK, metrics);
            response.headers_mut().insert(
                CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8".parse().unwrap(),
            );
            return Ok(response);
        }
        let path = request
            .uri()
            .path_and_query()
            .map(|path| path.as_str())
            .unwrap_or("/");
        if !path.starts_with('/') {
            return Ok(reply(
                StatusCode::BAD_REQUEST,
                "origin-form path required\n",
            ));
        }
        let uri = Uri::builder()
            .scheme("http")
            .authority(self.config.upstream.authority().unwrap().clone())
            .path_and_query(path)
            .build();
        let Ok(uri) = uri else {
            return Ok(reply(StatusCode::BAD_REQUEST, "invalid request target\n"));
        };
        let candidate = policy::request_candidate(
            request.method(),
            request.uri().path(),
            request.headers(),
            request.body().is_end_stream(),
            self.config.cache_path_prefix.as_deref(),
        );
        let key = CacheKey(uri.to_string());
        if candidate && let Some(snapshot) = self.cache.get(&key, Instant::now()) {
            return Ok(snapshot.response(Instant::now()));
        }
        if request.headers().get(CONTENT_LENGTH).is_some_and(|length| {
            length
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .is_none_or(|length| length > self.config.max_request_body_bytes as u64)
        }) {
            return Ok(reply(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body exceeds limit\n",
            ));
        }
        if headers::sanitize(request.headers_mut()).is_err() {
            return Ok(reply(
                StatusCode::BAD_REQUEST,
                "invalid Connection header\n",
            ));
        }
        *request.uri_mut() = uri;
        request.headers_mut().insert(
            HOST,
            self.config
                .upstream
                .authority()
                .unwrap()
                .as_str()
                .parse()
                .unwrap(),
        );
        let Ok(permit) = self.upstream_slots.clone().try_acquire_owned() else {
            self.overloads.fetch_add(1, Ordering::Relaxed);
            let mut response = reply(
                StatusCode::SERVICE_UNAVAILABLE,
                "upstream capacity exhausted\n",
            );
            response
                .headers_mut()
                .insert(RETRY_AFTER, "1".parse().unwrap());
            return Ok(response);
        };
        let _mutation = (self.config.cache_path_prefix.is_some() && !request.method().is_safe())
            .then(|| self.cache.begin_mutation(key.clone()));
        let fill = candidate.then(|| self.cache.begin_fill(key));
        if candidate {
            request.headers_mut().remove(CONTENT_LENGTH);
            request.headers_mut().insert(ACCEPT, "*/*".parse().unwrap());
            request
                .headers_mut()
                .insert(ACCEPT_ENCODING, "identity".parse().unwrap());
        }
        let requested_at = Instant::now();
        let deadline = requested_at + Duration::from_millis(self.config.upstream_timeout_ms);
        let request = request.map(|body| {
            StreamBody::new(
                body,
                deadline,
                Some(self.config.max_request_body_bytes),
                None,
            )
            .boxed_unsync()
        });
        self.origin_requests.fetch_add(1, Ordering::Relaxed);
        let response = match timeout_at(deadline, self.client.request(request)).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => return Ok(reply(StatusCode::BAD_GATEWAY, "upstream request failed\n")),
            Err(_) => return Ok(reply(StatusCode::GATEWAY_TIMEOUT, "upstream timed out\n")),
        };
        Ok(self
            .forward_response(response, fill, requested_at, deadline, permit)
            .await)
    }

    async fn forward_response(
        &self,
        response: Response<Incoming>,
        fill: Option<Fill>,
        requested_at: Instant,
        deadline: Instant,
        permit: OwnedSemaphorePermit,
    ) -> Response<ProxyBody> {
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
            return reply(StatusCode::BAD_GATEWAY, "unsupported upstream response\n");
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
            return Response::from_parts(
                parts,
                StreamBody::new(body, deadline, None, Some(permit)).boxed_unsync(),
            );
        };
        let mut buffered = Vec::new();
        let mut trailers = false;
        loop {
            let frame = match timeout_at(deadline, body.frame()).await {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(_))) => {
                    return reply(StatusCode::BAD_GATEWAY, "upstream body failed\n");
                }
                Err(_) => return reply(StatusCode::GATEWAY_TIMEOUT, "upstream body timed out\n"),
                Ok(None) => break,
            };
            let Ok(chunk) = frame.into_data() else {
                trailers = true;
                continue;
            };
            if chunk.len() > limit - buffered.len() {
                let prefix = bytes::Bytes::from(buffered.into_boxed_slice());
                let tail = StreamBody::new(body, deadline, None, Some(permit));
                return Response::from_parts(
                    parts,
                    PrefixBody::new(prefix, chunk, tail).boxed_unsync(),
                );
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
            return Response::from_parts(parts, full(bytes));
        }
        let snapshot = Arc::new(Snapshot::new(
            parts.headers,
            bytes,
            freshness,
            response_time,
        ));
        if let Some(fill) = fill {
            fill.insert(snapshot.clone(), Instant::now());
        }
        snapshot.response(Instant::now())
    }
}

fn reply(status: StatusCode, body: impl Into<bytes::Bytes>) -> Response<ProxyBody> {
    let mut response = Response::new(full(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, "text/plain; charset=utf-8".parse().unwrap());
    response
}
