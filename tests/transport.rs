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
use wirecache::{Config, Proxy, serve};

struct RunningProxy {
    address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<std::io::Result<()>>,
}

impl RunningProxy {
    async fn start(origin: SocketAddr, options: &[&str]) -> Self {
        let mut args = vec![
            "wirecache".to_owned(),
            "--upstream".to_owned(),
            format!("http://{origin}"),
        ];
        args.extend(options.iter().map(|value| (*value).to_owned()));
        let config = Config::try_parse_from(args).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(serve(listener, Proxy::new(config).unwrap(), async {
            let _ = stopped.await;
        }));
        Self {
            address,
            stop: Some(stop),
            task,
        }
    }

    async fn shutdown(&mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(7), &mut self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

impl Drop for RunningProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

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
}

