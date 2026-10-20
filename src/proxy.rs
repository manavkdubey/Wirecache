use std::{convert::Infallible, io, sync::Arc, time::Duration};

use http_body_util::BodyExt;
use hyper::{
    Request, Response, StatusCode, Uri, Version,
    body::{Body, Incoming},
    header::{ACCEPT, ACCEPT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HOST, UPGRADE},
};
use tokio::time::{Instant, timeout_at};

use crate::{
    Config,
    body::{ObservedBody, ProxyBody, StreamBody, full},
    cache::{Cache, CacheKey},
    error::{FetchError, reply},
    flight::{Admission, Coordinator, Outcome},
    headers, policy,
    telemetry::{Scope, Telemetry},
    upstream::{FetchResult, Upstream},
};

pub struct Proxy {
    pub(crate) config: Config,
    upstream: Arc<Upstream>,
    flights: Arc<Coordinator>,
    pub(crate) telemetry: Arc<Telemetry>,
    cache: Arc<Cache>,
}

impl Proxy {
    pub fn new(config: Config) -> io::Result<Arc<Self>> {
        config.validate()?;
        let cache = Cache::new(
            config.max_cache_mib * 1024 * 1024,
            config.max_cache_entries,
            config.max_entry_kib * 1024,
        );
        let telemetry = Arc::new(Telemetry::default());
        Ok(Arc::new(Self {
            upstream: Upstream::new(&config, telemetry.clone()),
            flights: Coordinator::new(cache.clone(), &config),
            config,
            cache,
            telemetry,
        }))
    }

    pub(crate) async fn shutdown(&self) {
        self.flights.shutdown().await;
    }

    pub(crate) async fn handle(
        &self,
        request: Request<Incoming>,
    ) -> Result<Response<ProxyBody>, Infallible> {
        let head = request.method() == hyper::Method::HEAD;
        let mut observation = self.telemetry.start(Scope::Request);
        let response = self.route(request).await?;
        self.telemetry.response(response.status());
        if head {
            observation.finish(crate::telemetry::End::Success);
        }
        Ok(response.map(|body| ObservedBody::new(body, observation).boxed_unsync()))
    }

    async fn route(
        &self,
        mut request: Request<Incoming>,
    ) -> Result<Response<ProxyBody>, Infallible> {
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
            let metrics = self.telemetry.render();
            let stats = self.cache.stats();
            let metrics = format!(
                "{metrics}wirecache_cache_bytes {}\nwirecache_cache_entries {}\nwirecache_cache_hits_total {}\nwirecache_cache_misses_total {}\nwirecache_cache_evictions_total {}\n",
                stats.bytes, stats.entries, stats.hits, stats.misses, stats.evictions
            );
            let flights = self.flights.stats();
            let metrics = format!(
                "{metrics}wirecache_flights_created_total {}\nwirecache_singleflight_join_total {}\nwirecache_unshared_total {}\nwirecache_inflight_flights {}\nwirecache_waiters {}\n",
                flights.created, flights.joined, flights.unshared, flights.active, flights.waiters
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
        if candidate {
            request.headers_mut().remove(CONTENT_LENGTH);
            request.headers_mut().insert(ACCEPT, "*/*".parse().unwrap());
            request
                .headers_mut()
                .insert(ACCEPT_ENCODING, "identity".parse().unwrap());
            let template = Template {
                uri: request.uri().clone(),
                headers: request.headers().clone(),
            };
            return Ok(self.candidate(key, template).await);
        }
        let _mutation = (self.config.cache_path_prefix.is_some() && !request.method().is_safe())
            .then(|| self.cache.begin_mutation(key));
        let deadline = self.upstream_deadline();
        let request = request.map(|body| {
            StreamBody::new(
                body,
                deadline,
                Some(self.config.max_request_body_bytes),
                None,
            )
            .boxed_unsync()
        });
        Ok(self.private_result(self.upstream.fetch(request, None, deadline).await))
    }

    fn upstream_deadline(&self) -> Instant {
        Instant::now() + Duration::from_millis(self.config.upstream_timeout_ms)
    }

    fn failed(&self, error: FetchError) -> Response<ProxyBody> {
        if error == FetchError::Overloaded {
            self.telemetry.overload();
        }
        error.response()
    }

    fn private_result(&self, result: FetchResult) -> Response<ProxyBody> {
        match result {
            FetchResult::Shared(snapshot) => snapshot.response(Instant::now()),
            FetchResult::Private(response) => response,
            FetchResult::Failed(error) => self.failed(error),
        }
    }

    async fn candidate(&self, key: CacheKey, template: Template) -> Response<ProxyBody> {
        let deadline = Instant::now() + Duration::from_millis(self.config.waiter_timeout_ms);
        let mut ticket = match self.flights.admit(key) {
            Ok(Admission::Hit(snapshot)) => return snapshot.response(Instant::now()),
            Ok(Admission::Bypass) => {
                return self.private_result(
                    self.upstream
                        .fetch(template.request(), None, self.upstream_deadline())
                        .await,
                );
            }
            Ok(Admission::Wait(ticket)) => ticket,
            Ok(Admission::Start(ticket, work)) => {
                self.flights.spawn(
                    work,
                    self.upstream.clone(),
                    template.request(),
                    self.upstream_deadline(),
                );
                ticket
            }
            Err(error) => return self.failed(error),
        };
        let mut waiting = self.telemetry.start(Scope::Wait);
        let outcome = ticket.wait(deadline).await;
        waiting.finish(match &outcome {
            Outcome::Failed(error) => error.end(),
            _ => crate::telemetry::End::Success,
        });
        let private = ticket.private.take();
        drop(ticket);
        match outcome {
            Outcome::Shared(snapshot) if snapshot.is_fresh(Instant::now()) => {
                snapshot.response(Instant::now())
            }
            Outcome::Failed(error) => self.failed(error),
            Outcome::RetryUnshared if private.is_some() => {
                match timeout_at(deadline, private.unwrap()).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(_)) => self.failed(FetchError::Internal),
                    Err(_) => self.failed(FetchError::Timeout),
                }
            }
            Outcome::RetryUnshared | Outcome::Shared(_) => self.private_result(
                self.upstream
                    .fetch(
                        template.request(),
                        None,
                        self.upstream_deadline().min(deadline),
                    )
                    .await,
            ),
        }
    }
}

struct Template {
    uri: Uri,
    headers: hyper::HeaderMap,
}

impl Template {
    fn request(&self) -> Request<ProxyBody> {
        let mut request = Request::new(full(bytes::Bytes::new()));
        *request.uri_mut() = self.uri.clone();
        *request.headers_mut() = self.headers.clone();
        request
    }
}
