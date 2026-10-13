use std::{
    convert::Infallible,
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use http_body_util::BodyExt;
use hyper::{
    Request, Response, StatusCode, Uri, Version,
    body::Incoming,
    header::{CONTENT_LENGTH, CONTENT_TYPE, HOST, RETRY_AFTER, UPGRADE},
};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioTimer},
};
use tokio::{
    sync::Semaphore,
    time::{Instant, timeout_at},
};

use crate::{
    Config,
    body::{ProxyBody, StreamBody, full},
    headers,
};

pub struct Proxy {
    pub(crate) config: Config,
    client: Client<HttpConnector, ProxyBody>,
    upstream_slots: Arc<Semaphore>,
    requests: AtomicU64,
    origin_requests: AtomicU64,
    overloads: AtomicU64,
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
        let deadline = Instant::now() + Duration::from_millis(self.config.upstream_timeout_ms);
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
        let (mut parts, body) = response.into_parts();
        if parts.status == StatusCode::SWITCHING_PROTOCOLS
            || headers::sanitize(&mut parts.headers).is_err()
        {
            return Ok(reply(
                StatusCode::BAD_GATEWAY,
                "unsupported upstream response\n",
            ));
        }
        Ok(Response::from_parts(
            parts,
            StreamBody::new(body, deadline, None, Some(permit)).boxed_unsync(),
        ))
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
