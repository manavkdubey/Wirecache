use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use wirecache::{Config, Proxy};

mod support;
use support::RunningProxy;

struct Origin {
    address: SocketAddr,
    requests: mpsc::Receiver<Request<Bytes>>,
    release: Arc<Semaphore>,
    task: JoinHandle<()>,
}

impl Origin {
    async fn start(blocked: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sent, requests) = mpsc::channel(16);
        let release = Arc::new(Semaphore::new(if blocked { 0 } else { 100 }));
        let gate = release.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    Some(_) = connections.join_next(), if !connections.is_empty() => {}
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let sent = sent.clone();
                        let gate = gate.clone();
                        connections.spawn(async move {
                            let service = service_fn(move |request: Request<Incoming>| {
                                let sent = sent.clone();
                                let gate = gate.clone();
                                async move {
                                    let (parts, body) = request.into_parts();
                                    let bytes = body.collect().await.unwrap().to_bytes();
                                    sent.send(Request::from_parts(parts, bytes.clone())).await.unwrap();
                                    let _permit = gate.acquire().await.unwrap();
                                    let mut response = Response::new(Full::new(if bytes.is_empty() { Bytes::from_static(b"origin body") } else { bytes }));
                                    *response.status_mut() = StatusCode::CREATED;
                                    response.headers_mut().insert("connection", "x-origin-secret".parse().unwrap());
                                    response.headers_mut().insert("x-origin-secret", "hidden".parse().unwrap());
                                    response.headers_mut().append("set-cookie", "a=1".parse().unwrap());
                                    response.headers_mut().append("set-cookie", "b=2".parse().unwrap());
                                    Ok::<_, Infallible>(response)
                                }
                            });
                            let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
                        });
                    }
                }
            }
        });
        Self {
            address,
            requests,
            release,
            task,
        }
    }

    async fn next(&mut self) -> Request<Bytes> {
        tokio::time::timeout(Duration::from_secs(2), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
}

impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn exchange(address: SocketAddr, request: &str) -> String {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    })
    .await
    .unwrap()
}

async fn receive_headers(stream: &mut TcpStream) {
    let mut headers = Vec::new();
    while !headers.windows(4).any(|window| window == b"\r\n\r\n") {
        let mut buffer = [0; 1024];
        let count = stream.read(&mut buffer).await.unwrap();
        assert!(count > 0, "connection closed before headers");
        headers.extend_from_slice(&buffer[..count]);
        assert!(headers.len() <= 32 * 1024);
    }
}

#[tokio::test]
async fn forwards_method_raw_target_body_and_end_to_end_headers() {
    let mut origin = Origin::start(false).await;
    let mut proxy = RunningProxy::start(origin.address, &[]).await;
    let response = exchange(proxy.address, "POST /products%2Fpart?b=2&a=1&a=3 HTTP/1.1\r\nHost: evil.example\r\nConnection: close, x-secret\r\nX-Secret: hidden\r\nX-Keep: retained\r\nProxy-Authorization: secret\r\nContent-Length: 5\r\n\r\nhello").await;
    let request = origin.next().await;
    assert_eq!(request.method(), "POST");
    assert_eq!(request.uri().to_string(), "/products%2Fpart?b=2&a=1&a=3");
    assert_eq!(request.headers()["host"], origin.address.to_string());
    assert_eq!(request.headers()["x-keep"], "retained");
    assert!(!request.headers().contains_key("x-secret"));
    assert!(!request.headers().contains_key("proxy-authorization"));
    assert_eq!(request.body(), "hello");
    assert!(response.starts_with("HTTP/1.1 201"));
    assert!(response.ends_with("hello"));
    assert!(!response.contains("x-origin-secret"));
    assert!(response.contains("set-cookie: a=1"));
    assert!(response.contains("set-cookie: b=2"));
    proxy.shutdown().await;
}

#[tokio::test]
async fn absolute_form_cannot_change_origin_and_get_is_forwarded() {
    let mut origin = Origin::start(false).await;
    let mut proxy = RunningProxy::start(origin.address, &[]).await;
    let response = exchange(proxy.address, "GET http://evil.invalid/data?q=%2F HTTP/1.1\r\nHost: evil.invalid\r\nConnection: close\r\n\r\n").await;
    let request = origin.next().await;
    assert_eq!(request.uri().to_string(), "/data?q=%2F");
    assert_eq!(request.headers()["host"], origin.address.to_string());
    assert!(response.ends_with("origin body"));
    proxy.shutdown().await;
}

#[tokio::test]
async fn local_routes_and_rejected_requests_do_not_reach_origin() {
    let mut origin = Origin::start(false).await;
    let mut proxy = RunningProxy::start(origin.address, &["--max-request-body-bytes", "4"]).await;
    for (request, status) in [
        (
            "GET /healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
            200,
        ),
        (
            "GET /metrics HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
            200,
        ),
        (
            "POST /healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
            405,
        ),
        (
            "CONNECT evil.example:80 HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
            501,
        ),
        (
            "GET / HTTP/1.1\r\nHost: local\r\nConnection: close, upgrade\r\nUpgrade: websocket\r\n\r\n",
            501,
        ),
        (
            "GET / HTTP/1.1\r\nHost: local\r\nConnection: close, content-length\r\n\r\n",
            400,
        ),
        (
            "POST / HTTP/1.1\r\nHost: local\r\nConnection: close\r\nContent-Length: 5\r\n\r\nhello",
            413,
        ),
    ] {
        let response = exchange(proxy.address, request).await;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status}")),
            "{response}"
        );
    }
    assert!(origin.requests.try_recv().is_err());
    proxy.shutdown().await;
}

#[tokio::test]
async fn overload_is_immediate_and_completed_work_releases_capacity() {
    let mut origin = Origin::start(true).await;
    let mut proxy = RunningProxy::start(origin.address, &["--max-upstream-requests", "1"]).await;
    let address = proxy.address;
    let first = tokio::spawn(async move {
        exchange(
            address,
            "GET /first HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        )
        .await
    });
    origin.next().await;
    let response = exchange(
        address,
        "GET /second HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 503"));
    assert!(response.contains("retry-after: 1"));
    origin.release.add_permits(1);
    assert!(first.await.unwrap().starts_with("HTTP/1.1 201"));
    assert!(
        exchange(
            address,
            "GET /third HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
        )
        .await
        .starts_with("HTTP/1.1 201")
    );
    assert_eq!(origin.next().await.uri().path(), "/third");
    proxy.shutdown().await;
}

#[tokio::test]
async fn upstream_header_timeout_returns_504_and_recovers() {
    let mut origin = Origin::start(true).await;
    let mut proxy = RunningProxy::start(
        origin.address,
        &[
            "--upstream-timeout-ms",
            "100",
            "--max-upstream-requests",
            "1",
        ],
    )
    .await;
    let address = proxy.address;
    let first = tokio::spawn(async move {
        exchange(
            address,
            "GET /slow HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        )
        .await
    });
    origin.next().await;
    assert!(first.await.unwrap().starts_with("HTTP/1.1 504"));
    origin.release.add_permits(2);
    let response = exchange(
        address,
        "GET /recovered HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 201"));
    proxy.shutdown().await;
}

#[tokio::test]
async fn connection_failure_returns_502() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut proxy = RunningProxy::start(address, &[]).await;
    let response = exchange(
        proxy.address,
        "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 502"));
    proxy.shutdown().await;
}

#[test]
fn rejects_invalid_origin_and_limits() {
    for origin in [
        "https://example.com",
        "http://example.com/base",
        "http://user:pass@example.com",
        "http://example.com/?q=1",
    ] {
        let config = Config::try_parse_from(["wirecache", "--upstream", origin]).unwrap();
        assert!(Proxy::new(config).is_err());
    }
    let config = Config::try_parse_from([
        "wirecache",
        "--upstream",
        "http://localhost:9000",
        "--max-connections",
        "0",
    ])
    .unwrap();
    assert!(Proxy::new(config).is_err());
    for (option, value) in [
        ("--cache-path-prefix", "public/"),
        ("--cache-path-prefix", "/public"),
        ("--cache-path-prefix", "/public/../"),
        ("--cache-path-prefix", "/public/%2f/"),
        ("--max-cache-mib", "0"),
        ("--max-entry-kib", "65537"),
        ("--max-cache-entries", "0"),
        ("--max-ttl-secs", "0"),
        ("--max-flights", "0"),
        ("--max-waiters", "0"),
        ("--max-waiters-per-key", "0"),
        ("--waiter-timeout-ms", "0"),
    ] {
        let config = Config::try_parse_from([
            "wirecache",
            "--upstream",
            "http://localhost:9000",
            option,
            value,
        ])
        .unwrap();
        assert!(Proxy::new(config).is_err(), "{option} {value}");
    }
}

#[tokio::test]
async fn response_streams_before_completion_and_holds_upstream_capacity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut proxy = RunningProxy::start(
        listener.local_addr().unwrap(),
        &["--max-upstream-requests", "1"],
    )
    .await;
    let (release, released) = oneshot::channel();
    let origin = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        receive_headers(&mut stream).await;
        stream.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-End\r\n\r\n5\r\nfirst\r\n").await.unwrap();
        released.await.unwrap();
        stream
            .write_all(b"6\r\nsecond\r\n0\r\nX-End: omitted\r\n\r\n")
            .await
            .unwrap();
    });
    let stream = TcpStream::connect(proxy.address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    let response = sender
        .send_request(
            Request::builder()
                .uri("/stream")
                .header("host", "local")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(!response.headers().contains_key("trailer"));
    let mut body = response.into_body();
    let first = tokio::time::timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(first, "first");
    let overloaded = exchange(
        proxy.address,
        "GET /other HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(overloaded.starts_with("HTTP/1.1 503"));
    release.send(()).unwrap();
    let mut rest = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.unwrap();
        assert!(!frame.is_trailers());
        rest.extend_from_slice(frame.data_ref().unwrap());
    }
    assert_eq!(rest, b"second");
    origin.await.unwrap();
    drop(sender);
    connection.await.unwrap().unwrap();
    proxy.shutdown().await;
}

#[tokio::test]
async fn stalled_response_body_terminates_with_an_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut proxy = RunningProxy::start(
        listener.local_addr().unwrap(),
        &["--upstream-timeout-ms", "200"],
    )
    .await;
    let (release, released) = oneshot::channel::<()>();
    let origin = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        receive_headers(&mut stream).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nprefix")
            .await
            .unwrap();
        let _ = released.await;
    });
    let stream = TcpStream::connect(proxy.address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    let response = sender
        .send_request(
            Request::builder()
                .uri("/stalled")
                .header("host", "local")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), response.into_body().collect())
            .await
            .unwrap()
            .is_err()
    );
    drop(release);
    origin.await.unwrap();
    drop(sender);
    let _ = connection.await.unwrap();
    proxy.shutdown().await;
}

#[tokio::test]
async fn shutdown_drains_a_pending_request() {
    let mut origin = Origin::start(true).await;
    let mut proxy = RunningProxy::start(origin.address, &[]).await;
    let address = proxy.address;
    let request = tokio::spawn(async move {
        exchange(
            address,
            "GET /pending HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        )
        .await
    });
    origin.next().await;
    proxy.stop.take().unwrap().send(()).unwrap();
    origin.release.add_permits(1);
    assert!(request.await.unwrap().starts_with("HTTP/1.1 201"));
    tokio::time::timeout(Duration::from_secs(2), &mut proxy.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
