use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::{AbortHandle, JoinHandle, JoinSet},
};

mod support;
use support::RunningProxy;

struct Pending {
    target: String,
    reply: oneshot::Sender<Option<Vec<u8>>>,
}

impl Pending {
    fn respond(self, headers: &[(&str, &str)], body: &[u8]) {
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\nCache-Control: public, max-age=60\r\n",
            body.len()
        );
        for (name, value) in headers {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        let mut bytes = response.into_bytes();
        bytes.extend_from_slice(body);
        let _ = self.reply.send(Some(bytes));
    }

    fn fail(self) {
        let _ = self.reply.send(None);
    }
}

struct Origin {
    address: SocketAddr,
    requests: mpsc::Receiver<Pending>,
    count: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Origin {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sent, requests) = mpsc::channel(256);
        let count = Arc::new(AtomicUsize::new(0));
        let attempts = count.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    Some(result) = connections.join_next(), if !connections.is_empty() => { result.unwrap(); }
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.unwrap();
                        let sent = sent.clone();
                        let attempts = attempts.clone();
                        connections.spawn(async move {
                            let mut headers = Vec::new();
                            while !headers.windows(4).any(|part| part == b"\r\n\r\n") {
                                let mut buffer = [0; 1024];
                                let count = stream.read(&mut buffer).await.unwrap();
                                if count == 0 { return; }
                                headers.extend_from_slice(&buffer[..count]);
                                assert!(headers.len() <= 32 * 1024);
                            }
                            let target = String::from_utf8(headers).unwrap().lines().next().unwrap().to_owned();
                            attempts.fetch_add(1, Ordering::SeqCst);
                            let (reply, response) = oneshot::channel();
                            if sent.send(Pending { target, reply }).await.is_err() { return; }
                            if let Ok(Some(bytes)) = response.await { let _ = stream.write_all(&bytes).await; }
                        });
                    }
                }
            }
        });
        Self {
            address,
            requests,
            count,
            task,
        }
    }

    async fn next(&mut self) -> Pending {
        tokio::time::timeout(Duration::from_secs(2), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct AbortConnection(AbortHandle);
impl Drop for AbortConnection {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    extra: &[(&str, &str)],
) -> Response<Bytes> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let stream = TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let connection = tokio::spawn(connection);
        let _guard = AbortConnection(connection.abort_handle());
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "local")
            .header("connection", "close");
        for (name, value) in extra {
            builder = builder.header(*name, *value);
        }
        let response = sender
            .send_request(builder.body(Full::new(Bytes::new())).unwrap())
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        drop(sender);
        connection.await.unwrap().unwrap();
        Response::from_parts(parts, body)
    })
    .await
    .unwrap()
}

fn launch(address: SocketAddr, path: &'static str) -> JoinHandle<Response<Bytes>> {
    tokio::spawn(async move { request(address, "GET", path, &[]).await })
}

async fn proxy(origin: &Origin, options: &[&str]) -> RunningProxy {
    let mut args = vec!["--cache-path-prefix", "/public/"];
    args.extend_from_slice(options);
    RunningProxy::start(origin.address, &args).await
}

async fn metric(address: SocketAddr, name: &str) -> usize {
    let response = request(address, "GET", "/metrics", &[]).await;
    std::str::from_utf8(response.body())
        .unwrap()
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(' ')?;
            (key == name).then(|| value.parse().unwrap())
        })
        .unwrap()
}

async fn wait_metric(address: SocketAddr, name: &str, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if metric(address, name).await == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_hundred_pending_compatible_requests_make_exactly_one_origin_fetch() {
    let mut origin = Origin::start().await;
    let mut proxy = proxy(&origin, &[]).await;
    let mut requests = JoinSet::new();
    for _ in 0..100 {
        let address = proxy.address;
        requests.spawn(async move { request(address, "GET", "/public/burst?q=1", &[]).await });
    }
    let pending = origin.next().await;
    wait_metric(proxy.address, "wirecache_waiters", 100).await;
    assert_eq!(origin.count(), 1);
    assert_eq!(
        metric(proxy.address, "wirecache_singleflight_join_total").await,
        99
    );
    pending.respond(&[("x-preserved", "yes")], b"one complete shared body");
    while let Some(response) = requests.join_next().await {
        let response = response.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body(), "one complete shared body");
        assert_eq!(response.headers()["x-preserved"], "yes");
    }
    let warm = request(proxy.address, "GET", "/public/burst?q=1", &[]).await;
    assert_eq!(warm.body(), "one complete shared body");
    assert_eq!(origin.count(), 1);
    wait_metric(proxy.address, "wirecache_inflight_flights", 0).await;
    wait_metric(proxy.address, "wirecache_waiters", 0).await;
    proxy.shutdown().await;
}

#[tokio::test]
async fn different_keys_fetch_independently() {
    let mut origin = Origin::start().await;
    let mut proxy = proxy(&origin, &[]).await;
    let first = launch(proxy.address, "/public/a");
    let second = launch(proxy.address, "/public/b");
    let a = origin.next().await;
    let b = origin.next().await;
    assert_ne!(a.target, b.target);
    let a_body = a.target.clone();
    let b_body = b.target.clone();
    a.respond(&[], a_body.as_bytes());
    b.respond(&[], b_body.as_bytes());
    assert_ne!(first.await.unwrap().body(), second.await.unwrap().body());
    assert_eq!(origin.count(), 2);
    proxy.shutdown().await;
}

#[tokio::test]
async fn authenticated_and_cookie_requests_cannot_join_a_public_flight() {
    for header in [
        ("authorization", "Bearer secret"),
        ("cookie", "session=secret"),
    ] {
        let mut origin = Origin::start().await;
        let mut proxy = proxy(&origin, &[]).await;
        let public = launch(proxy.address, "/public/data");
        let public_pending = origin.next().await;
        let address = proxy.address;
        let private =
            tokio::spawn(async move { request(address, "GET", "/public/data", &[header]).await });
        origin
            .next()
            .await
            .respond(&[("set-cookie", "secret=private")], b"private body");
        let private = private.await.unwrap();
        assert_eq!(private.body(), "private body");
        assert_eq!(private.headers()["set-cookie"], "secret=private");
        public_pending.respond(&[], b"public body");
        let public = public.await.unwrap();
        assert_eq!(public.body(), "public body");
        assert!(!public.headers().contains_key("set-cookie"));
        assert_eq!(
            request(proxy.address, "GET", "/public/data", &[])
                .await
                .body(),
            "public body"
        );
        assert_eq!(origin.count(), 2);
        proxy.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_disconnect_does_not_cancel_the_owned_fetch() {
    let mut origin = Origin::start().await;
    let mut proxy = proxy(&origin, &[]).await;
    let first = launch(proxy.address, "/public/data");
    let pending = origin.next().await;
    let second = launch(proxy.address, "/public/data");
    wait_metric(proxy.address, "wirecache_waiters", 2).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    wait_metric(proxy.address, "wirecache_waiters", 1).await;
    pending.respond(&[], b"survives disconnect");
    assert_eq!(second.await.unwrap().body(), "survives disconnect");
    assert_eq!(origin.count(), 1);
    proxy.shutdown().await;
}

#[tokio::test]
async fn follower_disconnect_releases_capacity_and_last_caller_can_leave() {
    let mut origin = Origin::start().await;
    let mut proxy = proxy(&origin, &["--max-waiters-per-key", "2"]).await;
    let first = launch(proxy.address, "/public/data");
    let pending = origin.next().await;
    let second = launch(proxy.address, "/public/data");
    wait_metric(proxy.address, "wirecache_waiters", 2).await;
    second.abort();
    let _ = second.await;
    wait_metric(proxy.address, "wirecache_waiters", 1).await;
    first.abort();
    let _ = first.await;
    wait_metric(proxy.address, "wirecache_waiters", 0).await;
    assert_eq!(metric(proxy.address, "wirecache_inflight_flights").await, 1);
    pending.respond(&[], b"cached without callers");
    wait_metric(proxy.address, "wirecache_inflight_flights", 0).await;
    assert_eq!(
        request(proxy.address, "GET", "/public/data", &[])
            .await
            .body(),
        "cached without callers"
    );
    assert_eq!(origin.count(), 1);
    proxy.shutdown().await;
}

#[tokio::test]
async fn unsafe_and_oversized_responses_are_delivered_privately() {
    for headers in [
        vec![("set-cookie", "session=private")],
        vec![("vary", "Accept-Language")],
        vec![("cache-control", "private")],
        vec![("cache-control", "no-store")],
        Vec::new(),
    ] {
        let mut origin = Origin::start().await;
        let mut proxy = proxy(&origin, &["--max-entry-kib", "1"]).await;
        let first = launch(proxy.address, "/public/data");
        let pending = origin.next().await;
        let second = launch(proxy.address, "/public/data");
        wait_metric(proxy.address, "wirecache_waiters", 2).await;
        let length = if headers.is_empty() { 2048 } else { 16 };
        let first_body = vec![b'a'; length];
        let second_body = vec![b'b'; length];
        pending.respond(&headers, &first_body);
        origin.next().await.respond(&headers, &second_body);
        assert_eq!(first.await.unwrap().body().as_ref(), first_body);
        assert_eq!(second.await.unwrap().body().as_ref(), second_body);
        assert_eq!(origin.count(), 2);
        assert_eq!(metric(proxy.address, "wirecache_cache_entries").await, 0);
        proxy.shutdown().await;
    }
}

#[tokio::test]
async fn disconnected_private_leader_releases_its_response_for_follower_retry() {
    let mut origin = Origin::start().await;
    let mut proxy = proxy(&origin, &["--max-upstream-requests", "1"]).await;
    let first = launch(proxy.address, "/public/data");
    let pending = origin.next().await;
    let second = launch(proxy.address, "/public/data");
    wait_metric(proxy.address, "wirecache_waiters", 2).await;
    first.abort();
    let _ = first.await;
    wait_metric(proxy.address, "wirecache_waiters", 1).await;
    pending.respond(&[("set-cookie", "secret=first")], b"discard this");
    origin
        .next()
        .await
        .respond(&[("set-cookie", "secret=second")], b"private second");
    let response = second.await.unwrap();
    assert_eq!(response.body(), "private second");
    assert_eq!(response.headers()["set-cookie"], "secret=second");
    assert_eq!(origin.count(), 2);
    proxy.shutdown().await;
}

#[tokio::test]
async fn origin_failure_reaches_every_waiter_and_a_later_request_retries() {
    let mut origin = Origin::start().await;
    let mut proxy = proxy(&origin, &[]).await;
    for wave in 1..=3 {
        let mut requests = Vec::new();
        for _ in 0..10 {
            requests.push(launch(proxy.address, "/public/data"));
        }
        let pending = origin.next().await;
        wait_metric(proxy.address, "wirecache_waiters", 10).await;
        pending.fail();
        for response in requests {
            assert_eq!(response.await.unwrap().status(), StatusCode::BAD_GATEWAY);
        }
        assert_eq!(origin.count(), wave);
    }
    let retry = launch(proxy.address, "/public/data");
    origin.next().await.respond(&[], b"recovered");
    assert_eq!(retry.await.unwrap().body(), "recovered");
    assert_eq!(origin.count(), 4);
    proxy.shutdown().await;
}

#[tokio::test]
async fn origin_timeout_cleans_the_flight_and_allows_retry() {
    let mut origin = Origin::start().await;
    let mut proxy = proxy(&origin, &["--upstream-timeout-ms", "500"]).await;
    let first = launch(proxy.address, "/public/data");
    let pending = origin.next().await;
    let second = launch(proxy.address, "/public/data");
    wait_metric(proxy.address, "wirecache_waiters", 2).await;
    assert_eq!(first.await.unwrap().status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(second.await.unwrap().status(), StatusCode::GATEWAY_TIMEOUT);
    pending.fail();
    wait_metric(proxy.address, "wirecache_inflight_flights", 0).await;
    let retry = launch(proxy.address, "/public/data");
    origin.next().await.respond(&[], b"after timeout");
    assert_eq!(retry.await.unwrap().body(), "after timeout");
    proxy.shutdown().await;
}

#[tokio::test]
async fn waiter_deadline_does_not_cancel_a_fetch_that_later_callers_can_join() {
    let mut origin = Origin::start().await;
    let mut proxy = proxy(&origin, &["--waiter-timeout-ms", "250"]).await;
    let first = launch(proxy.address, "/public/data");
    let pending = origin.next().await;
    assert_eq!(first.await.unwrap().status(), StatusCode::GATEWAY_TIMEOUT);
    wait_metric(proxy.address, "wirecache_waiters", 0).await;
    let second = launch(proxy.address, "/public/data");
    wait_metric(proxy.address, "wirecache_waiters", 1).await;
    pending.respond(&[], b"later caller");
    assert_eq!(second.await.unwrap().body(), "later caller");
    assert_eq!(origin.count(), 1);
    proxy.shutdown().await;
}

#[tokio::test]
async fn overload_bounds_keys_and_waiters_without_blocking_existing_work() {
    let mut origin = Origin::start().await;
    let mut proxy = proxy(
        &origin,
        &["--max-flights", "1", "--max-waiters-per-key", "2"],
    )
    .await;
    let first = launch(proxy.address, "/public/data");
    let pending = origin.next().await;
    let second = launch(proxy.address, "/public/data");
    wait_metric(proxy.address, "wirecache_waiters", 2).await;
    for path in ["/public/data", "/public/other"] {
        let response = request(proxy.address, "GET", path, &[]).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
    }
    assert_eq!(origin.count(), 1);
    pending.respond(&[], b"within limits");
    assert_eq!(first.await.unwrap().body(), "within limits");
    assert_eq!(second.await.unwrap().body(), "within limits");
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutation_replaces_old_flight_and_old_cleanup_preserves_the_replacement() {
    let mut origin = Origin::start().await;
    let mut proxy = proxy(&origin, &[]).await;
    let old = launch(proxy.address, "/public/data");
    let old_pending = origin.next().await;
    let address = proxy.address;
    let mutation = tokio::spawn(async move { request(address, "PUT", "/public/data", &[]).await });
    let update = origin.next().await;
    assert!(update.target.starts_with("PUT "));
    update.respond(&[], b"updated");
    assert_eq!(mutation.await.unwrap().status(), StatusCode::OK);
    let new = launch(proxy.address, "/public/data");
    let new_pending = origin.next().await;
    let follower = launch(proxy.address, "/public/data");
    wait_metric(proxy.address, "wirecache_waiters", 3).await;
    old_pending.respond(&[], b"old");
    assert_eq!(old.await.unwrap().body(), "old");
    wait_metric(proxy.address, "wirecache_waiters", 2).await;
    let late = launch(proxy.address, "/public/data");
    wait_metric(proxy.address, "wirecache_waiters", 3).await;
    new_pending.respond(&[], b"new");
    for response in [new, follower, late] {
        assert_eq!(response.await.unwrap().body(), "new");
    }
    assert_eq!(
        request(proxy.address, "GET", "/public/data", &[])
            .await
            .body(),
        "new"
    );
    assert_eq!(origin.count(), 3);
    proxy.shutdown().await;
}

#[tokio::test]
async fn shutdown_publishes_terminal_failures_and_finishes_with_pending_origin() {
    let mut origin = Origin::start().await;
    let mut proxy = proxy(&origin, &[]).await;
    let first = launch(proxy.address, "/public/data");
    let pending = origin.next().await;
    let second = launch(proxy.address, "/public/data");
    wait_metric(proxy.address, "wirecache_waiters", 2).await;
    proxy.stop.take().unwrap().send(()).unwrap();
    assert_eq!(
        first.await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        second.await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    tokio::time::timeout(Duration::from_secs(2), &mut proxy.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    pending.fail();
}
