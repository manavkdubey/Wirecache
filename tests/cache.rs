use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{
    Request, Response, StatusCode, body::Incoming, header::HeaderMap, service::service_fn,
};
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
};

#[path = "support/client.rs"]
mod client;
use client::request;
mod support;
use support::RunningProxy;

struct Fixture {
    origin_address: std::net::SocketAddr,
    proxy: RunningProxy,
    count: Arc<AtomicUsize>,
    last_headers: Arc<Mutex<HeaderMap>>,
    gate: Arc<Semaphore>,
    task: JoinHandle<()>,
}

impl Fixture {
    async fn start(options: &[&str]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let last_headers = Arc::new(Mutex::new(HeaderMap::new()));
        let gate = Arc::new(Semaphore::new(0));
        let request_count = count.clone();
        let recorded_headers = last_headers.clone();
        let release = gate.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    Some(_) = connections.join_next(), if !connections.is_empty() => {}
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let count = request_count.clone();
                        let last_headers = recorded_headers.clone();
                        let gate = release.clone();
                        connections.spawn(async move {
                            let service = service_fn(move |request: Request<Incoming>| {
                                let count = count.clone();
                                let last_headers = last_headers.clone();
                                let gate = gate.clone();
                                async move {
                                    let number = count.fetch_add(1, Ordering::SeqCst) + 1;
                                    *last_headers.lock().unwrap() = request.headers().clone();
                                    if request.uri().path() == "/public/race" && request.method() == hyper::Method::GET && number == 1 {
                                        gate.acquire().await.unwrap().forget();
                                    }
                                    let mut response = Response::new(Full::new(Bytes::from(format!("version-{number}"))));
                                    response.headers_mut().insert("cache-control", "public, max-age=60".parse().unwrap());
                                    if request.uri().path().starts_with("/public/etag") {
                                        response.headers_mut().insert("etag", "\"v1\"".parse().unwrap());
                                        if request.headers().contains_key("if-none-match") {
                                            assert_eq!(request.headers()["if-none-match"], "\"v1\"");
                                            tokio::time::sleep(Duration::from_millis(100)).await;
                                            if request.uri().path() == "/public/etag-changed" {
                                                response.headers_mut().insert("etag", "\"v2\"".parse().unwrap());
                                            } else {
                                                *response.status_mut() = StatusCode::NOT_MODIFIED;
                                                *response.body_mut() = Full::new(Bytes::new());
                                                response.headers_mut().insert("x-validated", "yes".parse().unwrap());
                                                if request.uri().path() == "/public/etag-private" {
                                                    response.headers_mut().insert("set-cookie", "secret=1".parse().unwrap());
                                                }
                                            }
                                        }
                                    }
                                    match request.uri().path() {
                                        "/public/private" => { response.headers_mut().insert("cache-control", "private, max-age=60".parse().unwrap()); }
                                        "/public/no-store" => { response.headers_mut().insert("cache-control", "public, max-age=60, no-store".parse().unwrap()); }
                                        "/public/no-cache" => { response.headers_mut().insert("cache-control", "public, max-age=60, no-cache".parse().unwrap()); }
                                        "/public/malformed" => { response.headers_mut().append("cache-control", "max-age=120".parse().unwrap()); }
                                        "/public/cookie" => { response.headers_mut().insert("set-cookie", "session=private".parse().unwrap()); }
                                        "/public/vary" => { response.headers_mut().insert("vary", "Accept-Language".parse().unwrap()); }
                                        "/public/partial" => { *response.status_mut() = StatusCode::PARTIAL_CONTENT; response.headers_mut().insert("content-range", "bytes 0-3/4".parse().unwrap()); }
                                        "/public/encoded" => { response.headers_mut().insert("content-encoding", "gzip".parse().unwrap()); }
                                        "/public/stale" => { response.headers_mut().insert("age", "60".parse().unwrap()); }
                                        "/public/aged" => {
                                            response.headers_mut().insert("age", "50".parse().unwrap());
                                            response.headers_mut().insert("date", httpdate::fmt_http_date(SystemTime::now() - Duration::from_secs(50)).parse().unwrap());
                                        }
                                        "/public/large" => { *response.body_mut() = Full::new(Bytes::from(vec![b'x'; 4096])); }
                                        _ => {}
                                    }
                                    Ok::<_, Infallible>(response)
                                }
                            });
                            let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
                        });
                    }
                }
            }
        });
        let mut args = vec!["--cache-path-prefix", "/public/"];
        args.extend_from_slice(options);
        let proxy = RunningProxy::start(address, &args).await;
        Self {
            origin_address: address,
            proxy,
            count,
            last_headers,
            gate,
            task,
        }
    }

    async fn request(&self, method: &str, path: &str, extra: &[(&str, &str)]) -> Response<Bytes> {
        request(self.proxy.address, method, path, extra).await
    }

    async fn get(&self, path: &str) -> Response<Bytes> {
        self.request("GET", path, &[]).await
    }
    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn fresh_hits_keep_queries_distinct_and_standardize_only_candidates() {
    let mut fixture = Fixture::start(&[]).await;
    let first = fixture.get("/public/data?a=1&b=2").await;
    assert_eq!(first.body(), "version-1");
    let headers = fixture.last_headers.lock().unwrap().clone();
    assert_eq!(headers["accept"], "*/*");
    assert_eq!(headers["accept-encoding"], "identity");
    assert!(
        fixture
            .get("/public/data?a=1&b=2")
            .await
            .headers()
            .contains_key("age")
    );
    assert_eq!(fixture.count(), 1);
    assert_eq!(
        fixture.get("/public/data?b=2&a=1").await.body(),
        "version-2"
    );
    assert_eq!(
        fixture.get("/public/data?a=2&b=2").await.body(),
        "version-3"
    );
    assert_eq!(
        fixture.get("/public/data?a=1&b=2").await.body(),
        "version-1"
    );
    let bypass = fixture
        .request("GET", "/public/data?a=1&b=2", &[("accept-language", "fr")])
        .await;
    assert_eq!(bypass.body(), "version-4");
    assert_eq!(
        fixture.last_headers.lock().unwrap()["accept-language"],
        "fr"
    );
    assert_eq!(
        fixture.get("/public/data?a=1&b=2").await.body(),
        "version-1"
    );
    fixture
        .request("GET", "/public/framing", &[("content-length", "0")])
        .await;
    assert!(
        !fixture
            .last_headers
            .lock()
            .unwrap()
            .contains_key("content-length")
    );
    fixture.proxy.shutdown().await;
}

#[tokio::test]
async fn request_privacy_and_directives_bypass_even_when_a_public_entry_exists() {
    let mut fixture = Fixture::start(&[]).await;
    assert_eq!(fixture.get("/public/data").await.body(), "version-1");
    for headers in [
        vec![("authorization", "Bearer secret")],
        vec![("cookie", "session=secret")],
        vec![("range", "bytes=0-1")],
        vec![("if-none-match", "etag")],
        vec![("cache-control", "no-cache")],
        vec![("cache-control", "no-store")],
        vec![("pragma", "no-cache")],
        vec![("user-agent", "different-client")],
        vec![("accept", "text/html")],
        vec![("accept-encoding", "gzip")],
        vec![("x-user", "alice"), ("connection", "x-user")],
    ] {
        let before = fixture.count();
        let response = fixture.request("GET", "/public/data", &headers).await;
        assert_eq!(fixture.count(), before + 1, "{headers:?}");
        assert_ne!(response.body(), "version-1");
    }
    assert_eq!(fixture.get("/public/data").await.body(), "version-1");
    for path in [
        "/outside",
        "/publicity/data",
        "/public/../private",
        "/public/%2e%2e/private",
    ] {
        let before = fixture.count();
        fixture.get(path).await;
        fixture.get(path).await;
        assert_eq!(fixture.count(), before + 2, "{path}");
    }
    fixture.proxy.shutdown().await;
}

#[tokio::test]
async fn response_privacy_variance_and_invalid_freshness_never_cache() {
    let mut fixture = Fixture::start(&[]).await;
    for path in [
        "private",
        "no-store",
        "no-cache",
        "malformed",
        "cookie",
        "vary",
        "partial",
        "encoded",
        "stale",
    ] {
        let before = fixture.count();
        let path = format!("/public/{path}");
        fixture.get(&path).await;
        fixture.get(&path).await;
        assert_eq!(fixture.count(), before + 2, "{path}");
    }
    fixture.proxy.shutdown().await;
}

#[tokio::test]
async fn cached_age_keeps_existing_origin_age() {
    let mut fixture = Fixture::start(&[]).await;
    let first = fixture.get("/public/aged").await;
    let second = fixture.get("/public/aged").await;
    assert_eq!(first.body(), second.body());
    assert!(
        second.headers()["age"]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            >= 50
    );
    assert_eq!(fixture.count(), 1);
    fixture.proxy.shutdown().await;
}

#[tokio::test]
async fn oversized_content_length_streams_complete_without_caching() {
    let mut fixture = Fixture::start(&["--max-entry-kib", "1"]).await;
    for _ in 0..2 {
        assert_eq!(
            fixture.get("/public/large").await.body().as_ref(),
            vec![b'x'; 4096]
        );
    }
    assert_eq!(fixture.count(), 2);
    let metrics = fixture.get("/metrics").await;
    assert!(
        std::str::from_utf8(metrics.body())
            .unwrap()
            .contains("wirecache_cache_entries 0\n")
    );
    fixture.proxy.shutdown().await;
}

#[tokio::test]
async fn lru_eviction_keeps_popular_entries_and_reports_bounds() {
    let mut fixture = Fixture::start(&["--max-cache-entries", "2"]).await;
    fixture.get("/public/a").await;
    fixture.get("/public/b").await;
    fixture.get("/public/a").await;
    fixture.get("/public/c").await;
    assert_eq!(fixture.count(), 3);
    assert_eq!(fixture.get("/public/a").await.body(), "version-1");
    assert_eq!(fixture.get("/public/b").await.body(), "version-4");
    let metrics = fixture.get("/metrics").await;
    let metrics = std::str::from_utf8(metrics.body()).unwrap();
    assert!(metrics.contains("wirecache_cache_entries 2\n"));
    assert!(metrics.contains("wirecache_cache_evictions_total 2\n"));
    fixture.proxy.shutdown().await;
}

#[tokio::test]
async fn mutation_invalidates_a_hit_and_an_older_pending_fill() {
    let mut fixture = Fixture::start(&[]).await;
    fixture.get("/public/data").await;
    fixture.request("PUT", "/public/data", &[]).await;
    assert_eq!(fixture.get("/public/data").await.body(), "version-3");
    fixture.proxy.shutdown().await;

    let mut fixture = Fixture::start(&[]).await;
    let old = fixture.get("/public/race");
    let update = async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        fixture.request("PUT", "/public/race", &[]).await;
        fixture.gate.add_permits(1);
    };
    let (old, ()) = tokio::join!(old, update);
    assert_eq!(old.body(), "version-1");
    assert_eq!(fixture.get("/public/race").await.body(), "version-3");
    assert_eq!(fixture.get("/public/race").await.body(), "version-3");
    assert_eq!(fixture.count(), 3);
    fixture.proxy.shutdown().await;
}

#[tokio::test]
async fn caching_is_disabled_without_an_explicit_public_prefix() {
    let mut fixture = Fixture::start(&[]).await;
    let mut proxy = RunningProxy::start(fixture.origin_address, &[]).await;
    for _ in 0..2 {
        request(proxy.address, "GET", "/public/data", &[]).await;
    }
    assert_eq!(fixture.count(), 2);
    proxy.shutdown().await;
    fixture.proxy.shutdown().await;
}

async fn raw_response_is_never_cached(
    raw: Vec<u8>,
    expected_status: StatusCode,
    expected_body: Option<&[u8]>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut proxy = RunningProxy::start(
        listener.local_addr().unwrap(),
        &["--cache-path-prefix", "/public/", "--max-entry-kib", "1"],
    )
    .await;
    let origin = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.windows(4).any(|window| window == b"\r\n\r\n") {
                let mut buffer = [0; 1024];
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                header.extend_from_slice(&buffer[..count]);
                assert!(header.len() <= 32 * 1024);
            }
            stream.write_all(&raw).await.unwrap();
        }
    });
    for _ in 0..2 {
        let response = request(proxy.address, "GET", "/public/raw", &[]).await;
        assert_eq!(response.status(), expected_status);
        if let Some(body) = expected_body {
            assert_eq!(response.body().as_ref(), body);
        }
    }
    tokio::time::timeout(Duration::from_secs(2), origin)
        .await
        .unwrap()
        .unwrap();
    let metrics = request(proxy.address, "GET", "/metrics", &[]).await;
    assert!(
        std::str::from_utf8(metrics.body())
            .unwrap()
            .contains("wirecache_cache_entries 0\n")
    );
    proxy.shutdown().await;
}

#[tokio::test]
async fn unknown_length_overflow_replays_every_byte_and_streams_the_remainder() {
    let chunks = [vec![b'a'; 512], vec![b'b'; 1024], vec![b'c'; 512]];
    let expected: Vec<u8> = chunks.iter().flatten().copied().collect();
    let mut raw = b"HTTP/1.1 200 OK\r\nCache-Control: public, max-age=60\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
    for chunk in chunks {
        raw.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
        raw.extend_from_slice(&chunk);
        raw.extend_from_slice(b"\r\n");
    }
    raw.extend_from_slice(b"0\r\n\r\n");
    raw_response_is_never_cached(raw, StatusCode::OK, Some(&expected)).await;
}

#[tokio::test]
async fn broken_cache_candidate_returns_502_and_does_not_poison_retries() {
    let raw = b"HTTP/1.1 200 OK\r\nCache-Control: public, max-age=60\r\nContent-Length: 1000\r\n\r\npartial".to_vec();
    raw_response_is_never_cached(raw, StatusCode::BAD_GATEWAY, None).await;
}

#[tokio::test]
async fn unannounced_trailers_prevent_cache_admission() {
    let raw = b"HTTP/1.1 200 OK\r\nCache-Control: public, max-age=60\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ndata\r\n0\r\nSet-Cookie: session=private\r\n\r\n".to_vec();
    raw_response_is_never_cached(raw, StatusCode::OK, Some(b"data")).await;
}

#[tokio::test]
async fn connection_nominations_cannot_hide_response_privacy_from_policy() {
    for extra in [
        "Connection: Set-Cookie\r\nSet-Cookie: session=private\r\n",
        "Connection: Vary\r\nVary: Accept-Language\r\n",
        "Connection: Cache-Control\r\n",
    ] {
        let raw = format!("HTTP/1.1 200 OK\r\nCache-Control: public, max-age=60\r\nContent-Length: 4\r\n{extra}\r\ndata").into_bytes();
        raw_response_is_never_cached(raw, StatusCode::OK, Some(b"data")).await;
    }
}

#[tokio::test]
async fn stalled_candidate_returns_504_without_storing_a_partial_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut proxy = RunningProxy::start(
        listener.local_addr().unwrap(),
        &[
            "--cache-path-prefix",
            "/public/",
            "--upstream-timeout-ms",
            "200",
        ],
    )
    .await;
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let origin = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 4096];
        assert!(stream.read(&mut buffer).await.unwrap() > 0);
        stream.write_all(b"HTTP/1.1 200 OK\r\nCache-Control: public, max-age=60\r\nContent-Length: 100\r\n\r\npartial").await.unwrap();
        let _ = released.await;
    });
    let response = request(proxy.address, "GET", "/public/stalled", &[]).await;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let metrics = request(proxy.address, "GET", "/metrics", &[]).await;
    assert!(
        std::str::from_utf8(metrics.body())
            .unwrap()
            .contains("wirecache_cache_entries 0\n")
    );
    release.send(()).unwrap();
    origin.await.unwrap();
    proxy.shutdown().await;
}

#[tokio::test]
async fn expired_etag_coalesces_validation_and_reuses_body_with_updated_headers() {
    let mut fixture = Fixture::start(&["--max-ttl-secs", "1"]).await;
    let first = fixture.get("/public/etag").await;
    assert_eq!(first.body(), "version-1");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let mut callers = JoinSet::new();
    for _ in 0..20 {
        let address = fixture.proxy.address;
        callers.spawn(async move { request(address, "GET", "/public/etag", &[]).await });
    }
    while let Some(result) = callers.join_next().await {
        let response = result.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body(), "version-1");
        assert_eq!(response.headers()["x-validated"], "yes");
    }
    assert_eq!(fixture.count(), 2);
    assert_eq!(fixture.get("/public/etag").await.body(), "version-1");
    assert_eq!(fixture.count(), 2);
    fixture.proxy.shutdown().await;
}

#[tokio::test]
async fn changed_etag_replaces_body_and_unsafe_304_never_serves_stale() {
    for (path, expected) in [
        ("/public/etag-changed", StatusCode::OK),
        ("/public/etag-private", StatusCode::BAD_GATEWAY),
    ] {
        let mut fixture = Fixture::start(&["--max-ttl-secs", "1"]).await;
        assert_eq!(fixture.get(path).await.body(), "version-1");
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let response = fixture.get(path).await;
        assert_eq!(response.status(), expected);
        assert_ne!(response.body(), "version-1");
        let again = fixture.get(path).await;
        if expected == StatusCode::OK {
            assert_eq!(again.body(), "version-2");
            assert_eq!(fixture.count(), 2);
        } else {
            assert_eq!(again.body(), "version-3");
            assert!(
                !fixture
                    .last_headers
                    .lock()
                    .unwrap()
                    .contains_key("if-none-match")
            );
        }
        fixture.proxy.shutdown().await;
    }
}
