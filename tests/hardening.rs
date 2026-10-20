use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use hyper::StatusCode;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};

#[path = "support/client.rs"]
mod client;
mod support;
use client::request;
use support::RunningProxy;

struct OriginTask(JoinHandle<()>);
impl Drop for OriginTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn read_headers(stream: &mut TcpStream) -> bool {
    let mut header = Vec::new();
    while !header.windows(4).any(|part| part == b"\r\n\r\n") {
        let mut buffer = [0; 1024];
        let count = stream.read(&mut buffer).await.unwrap_or(0);
        if count == 0 {
            return false;
        }
        header.extend_from_slice(&buffer[..count]);
        assert!(header.len() <= 32 * 1024);
    }
    true
}

async fn metrics(address: SocketAddr) -> String {
    String::from_utf8(
        request(address, "GET", "/metrics", &[])
            .await
            .into_body()
            .to_vec(),
    )
    .unwrap()
}

fn metric(text: &str, name: &str) -> usize {
    text.lines()
        .find_map(|line| {
            let (key, value) = line.split_once(' ')?;
            (key == name).then(|| value.parse().unwrap())
        })
        .unwrap()
}

async fn wait_metric(address: SocketAddr, name: &str, value: usize) -> String {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let text = metrics(address).await;
            if metric(&text, name) == value {
                return text;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

async fn raw_exchange(address: SocketAddr, bytes: &[u8]) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let _ = stream.write_all(bytes).await;
        let mut response = Vec::new();
        let _ = stream.take(64 * 1024).read_to_end(&mut response).await;
        response
    })
    .await
    .unwrap()
}

async fn upload_origin() -> (SocketAddr, OriginTask) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut received = 0;
        loop {
            let mut buffer = [0; 1024];
            let count = stream.read(&mut buffer).await.unwrap_or(0);
            if count == 0 {
                break;
            }
            received += count;
            assert!(received <= 32 * 1024);
        }
    });
    (address, OriginTask(task))
}

#[tokio::test]
async fn oversized_chunked_upload_returns_413_and_releases_the_origin_permit() {
    let (origin, _task) = upload_origin().await;
    let mut proxy = RunningProxy::start(
        origin,
        &[
            "--max-request-body-bytes",
            "4",
            "--max-upstream-requests",
            "1",
        ],
    )
    .await;
    let response = raw_exchange(proxy.address, b"POST /upload HTTP/1.1\r\nHost: local\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n8\r\nabcdefgh\r\n0\r\n\r\n").await;
    assert!(
        response.starts_with(b"HTTP/1.1 413"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    let text = wait_metric(proxy.address, "wirecache_active_upstream", 0).await;
    assert_eq!(
        metric(
            &text,
            "wirecache_upstream_outcomes_total{outcome=\"limit\"}"
        ),
        1
    );
    assert_eq!(metric(&text, "wirecache_upstream_latency_seconds_count"), 1);
    proxy.shutdown().await;
}

#[tokio::test]
async fn stalled_chunked_upload_returns_504_and_records_a_timeout() {
    let (origin, _task) = upload_origin().await;
    let mut proxy = RunningProxy::start(origin, &["--upstream-timeout-ms", "100"]).await;
    let response = raw_exchange(proxy.address, b"POST /upload HTTP/1.1\r\nHost: local\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n").await;
    assert!(
        response.starts_with(b"HTTP/1.1 504"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    let text = wait_metric(proxy.address, "wirecache_active_upstream", 0).await;
    assert_eq!(
        metric(
            &text,
            "wirecache_upstream_outcomes_total{outcome=\"timeout\"}"
        ),
        1
    );
    proxy.shutdown().await;
}

async fn stalled_reader_releases_capacity(options: &[&str], expected: &str) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = listener.local_addr().unwrap();
    let (started, ready) = oneshot::channel();
    let count = Arc::new(AtomicUsize::new(0));
    let attempts = count.clone();
    let _origin = OriginTask(tokio::spawn(async move {
        let mut connections = JoinSet::new();
        let mut started = Some(started);
        loop {
            tokio::select! {
                Some(result) = connections.join_next(), if !connections.is_empty() => { result.unwrap(); }
                accepted = listener.accept() => {
                    let (mut stream, _) = accepted.unwrap();
                    let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
                    let signal = if first { started.take() } else { None };
                    connections.spawn(async move {
                        if !read_headers(&mut stream).await { return; }
                        if first {
                            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1099511627776\r\n\r\n").await.unwrap();
                            signal.unwrap().send(()).unwrap();
                            let chunk = [b'x'; 8192];
                            while stream.write_all(&chunk).await.is_ok() {}
                        } else {
                            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok").await.unwrap();
                        }
                    });
                }
            }
        }
    }));
    let mut args = vec!["--max-upstream-requests", "1"];
    args.extend_from_slice(options);
    let mut proxy = RunningProxy::start(origin, &args).await;
    let mut stalled = TcpStream::connect(proxy.address).await.unwrap();
    stalled
        .write_all(b"GET /stream HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), ready)
        .await
        .unwrap()
        .unwrap();
    wait_metric(proxy.address, expected, 1).await;
    wait_metric(proxy.address, "wirecache_active_upstream", 0).await;
    let recovered = request(proxy.address, "GET", "/after", &[]).await;
    assert_eq!(recovered.body(), "ok");
    assert_eq!(count.load(Ordering::SeqCst), 2);
    drop(stalled);
    proxy.shutdown().await;
}

#[tokio::test]
async fn client_that_stops_reading_is_closed_by_the_independent_idle_watchdog() {
    stalled_reader_releases_capacity(
        &["--connection-idle-timeout-ms", "100"],
        "wirecache_connection_timeouts_total",
    )
    .await;
}

#[tokio::test]
async fn origin_deadline_releases_capacity_when_client_backpressure_stops_body_polling() {
    stalled_reader_releases_capacity(
        &["--upstream-timeout-ms", "500"],
        "wirecache_upstream_outcomes_total{outcome=\"timeout\"}",
    )
    .await;
}

#[tokio::test]
async fn two_candidate_buffers_stay_under_their_caps_and_release_after_completion() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = listener.local_addr().unwrap();
    let (sent, mut release) = mpsc::channel(2);
    let _origin = OriginTask(tokio::spawn(async move {
        let mut connections = JoinSet::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let sent = sent.clone();
            connections.spawn(async move {
                assert!(read_headers(&mut stream).await);
                stream.write_all(b"HTTP/1.1 200 OK\r\nCache-Control: public, max-age=60\r\nTransfer-Encoding: chunked\r\n\r\n200\r\n").await.unwrap();
                stream.write_all(&[b'x'; 512]).await.unwrap();
                stream.write_all(b"\r\n").await.unwrap();
                let (go, released) = oneshot::channel();
                sent.send(go).await.unwrap();
                released.await.unwrap();
                stream.write_all(b"0\r\n\r\n").await.unwrap();
            });
        }
        while let Some(result) = connections.join_next().await {
            result.unwrap();
        }
    }));
    let mut proxy = RunningProxy::start(
        origin,
        &[
            "--cache-path-prefix",
            "/public/",
            "--max-flights",
            "2",
            "--max-entry-kib",
            "1",
        ],
    )
    .await;
    let mut clients = JoinSet::new();
    for path in ["/public/a", "/public/b"] {
        let address = proxy.address;
        clients.spawn(async move { request(address, "GET", path, &[]).await });
    }
    let text = wait_metric(proxy.address, "wirecache_buffered_bytes", 1024).await;
    assert_eq!(metric(&text, "wirecache_active_upstream"), 2);
    assert!(metric(&text, "wirecache_buffered_bytes_peak") <= 2 * 1024);
    for _ in 0..2 {
        release.recv().await.unwrap().send(()).unwrap();
    }
    while let Some(response) = clients.join_next().await {
        assert_eq!(response.unwrap().body().as_ref(), &[b'x'; 512]);
    }
    let text = wait_metric(proxy.address, "wirecache_buffered_bytes", 0).await;
    assert_eq!(metric(&text, "wirecache_active_upstream"), 0);
    assert_eq!(metric(&text, "wirecache_upstream_latency_seconds_count"), 2);
    assert_eq!(metric(&text, "wirecache_wait_latency_seconds_count"), 2);
    assert_eq!(metric(&text, "wirecache_cache_entries"), 2);
    proxy.shutdown().await;
}

async fn held_health(address: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: local\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !response.ends_with(b"ok\n") {
            let mut buffer = [0; 1024];
            let count = stream.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            response.extend_from_slice(&buffer[..count]);
            assert!(response.len() <= 4096);
        }
    })
    .await
    .unwrap();
    stream
}

#[tokio::test]
async fn downstream_admission_rejects_excess_connections_and_idle_slots_recover() {
    let mut proxy = RunningProxy::start(
        "127.0.0.1:9".parse().unwrap(),
        &[
            "--max-connections",
            "2",
            "--connection-idle-timeout-ms",
            "500",
        ],
    )
    .await;
    let mut first = held_health(proxy.address).await;
    let mut second = held_health(proxy.address).await;
    let rejected = raw_exchange(
        proxy.address,
        b"GET /healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(!rejected.starts_with(b"HTTP/1.1 200"));
    for stream in [&mut first, &mut second] {
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }
    let text = metrics(proxy.address).await;
    assert_eq!(metric(&text, "wirecache_connection_rejections_total"), 1);
    assert_eq!(metric(&text, "wirecache_connection_timeouts_total"), 2);
    assert_eq!(
        request(proxy.address, "GET", "/healthz", &[])
            .await
            .status(),
        StatusCode::OK
    );
    proxy.shutdown().await;
}

#[tokio::test]
async fn head_metrics_count_bodyless_success_without_false_cancellation() {
    let mut proxy = RunningProxy::start("127.0.0.1:9".parse().unwrap(), &[]).await;
    let response = request(proxy.address, "HEAD", "/healthz", &[]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.body().is_empty());
    let text = metrics(proxy.address).await;
    assert_eq!(
        metric(
            &text,
            "wirecache_request_outcomes_total{outcome=\"success\"}"
        ),
        1
    );
    assert_eq!(
        metric(
            &text,
            "wirecache_request_outcomes_total{outcome=\"cancelled\"}"
        ),
        0
    );
    assert_eq!(metric(&text, "wirecache_request_latency_seconds_count"), 1);
    assert_eq!(metric(&text, "wirecache_origin_requests_total"), 0);
    assert_eq!(metric(&text, "wirecache_response_body_bytes_total"), 0);
    assert!(!text.contains("local"));
    proxy.shutdown().await;
}

#[tokio::test]
async fn oversized_and_ambiguous_headers_are_rejected_before_origin_admission() {
    let mut huge = b"GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\nX-Large: ".to_vec();
    huge.extend_from_slice(&vec![b'x'; 96 * 1024]);
    huge.extend_from_slice(b"\r\n\r\n");
    let ambiguous = b"POST / HTTP/1.1\r\nHost: local\r\nConnection: close\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nxx".to_vec();
    for bytes in [huge, ambiguous] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut proxy = RunningProxy::start(listener.local_addr().unwrap(), &[]).await;
        let response = raw_exchange(proxy.address, &bytes).await;
        assert!(response.is_empty() || response.starts_with(b"HTTP/1.1 4"));
        let text = wait_metric(proxy.address, "wirecache_connection_errors_total", 1).await;
        assert_eq!(metric(&text, "wirecache_origin_requests_total"), 0);
        proxy.shutdown().await;
    }
}
