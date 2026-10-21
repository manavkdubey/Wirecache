use std::{
    convert::Infallible,
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use clap::Parser;
use http_body_util::Full;
use hyper::{Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::{net::TcpListener, sync::Semaphore, task::JoinSet};

#[derive(Parser)]
#[command(about = "Local deterministic origin for the WireCache demo and benchmarks")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9000")]
    listen: SocketAddr,
    #[arg(long, default_value_t = 200)]
    delay_ms: u64,
    #[arg(long, default_value_t = 4096)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 60)]
    ttl_secs: u64,
}

struct Origin {
    payload: Bytes,
    delay: Duration,
    cache_control: hyper::header::HeaderValue,
    requests: AtomicU64,
}

impl Origin {
    async fn handle(
        &self,
        request: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        if request.method() != hyper::Method::GET && request.method() != hyper::Method::HEAD {
            let mut response =
                Response::new(Full::new(Bytes::from_static(b"GET or HEAD required\n")));
            *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
            response
                .headers_mut()
                .insert("allow", "GET, HEAD".parse().unwrap());
            return Ok(response);
        }
        let mut response = match request.uri().path() {
            "/healthz" => Response::new(Full::new(Bytes::from_static(b"ok\n"))),
            "/stats" => {
                let body = format!(
                    "{{\"requests\":{}}}\n",
                    self.requests.load(Ordering::Relaxed)
                );
                let mut response = Response::new(Full::new(Bytes::from(body)));
                response
                    .headers_mut()
                    .insert("content-type", "application/json".parse().unwrap());
                response
            }
            "/public/data" | "/private/data" => {
                self.requests.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(self.delay).await;
                let mut response = Response::new(Full::new(self.payload.clone()));
                response.headers_mut().insert(
                    "cache-control",
                    if request.uri().path() == "/public/data" {
                        self.cache_control.clone()
                    } else {
                        "no-store".parse().unwrap()
                    },
                );
                response
                    .headers_mut()
                    .insert("content-type", "application/octet-stream".parse().unwrap());
                response
            }
            _ => {
                let mut response = Response::new(Full::new(Bytes::from_static(b"not found\n")));
                *response.status_mut() = StatusCode::NOT_FOUND;
                response
            }
        };
        if request.uri().path() != "/public/data" && request.uri().path() != "/private/data" {
            response
                .headers_mut()
                .insert("cache-control", "no-store".parse().unwrap());
        }
        Ok(response)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    if args.delay_ms > 5000
        || args.payload_bytes == 0
        || args.payload_bytes > 8 * 1024 * 1024
        || args.ttl_secs == 0
        || args.ttl_secs > 86_400
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "delay must be <=5000 ms, payload 1..8388608 bytes, and TTL 1..86400 seconds",
        )
        .into());
    }
    let listener = TcpListener::bind(args.listen).await?;
    eprintln!("demo origin listening on {}", listener.local_addr()?);
    let origin = Arc::new(Origin {
        payload: Bytes::from(vec![b'x'; args.payload_bytes]),
        delay: Duration::from_millis(args.delay_ms),
        cache_control: format!("public, max-age={}", args.ttl_secs).parse()?,
        requests: AtomicU64::new(0),
    });
    let slots = Arc::new(Semaphore::new(512));
    let mut connections = JoinSet::new();
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            result = &mut shutdown => { result?; break; }
            Some(result) = connections.join_next(), if !connections.is_empty() => { result?; }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let Ok(permit) = slots.clone().try_acquire_owned() else { continue; };
                let origin = origin.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    let mut builder = hyper::server::conn::http1::Builder::new();
                    builder.timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(5)).max_headers(100).max_buf_size(32 * 1024);
                    let _ = builder.serve_connection(TokioIo::new(stream), service_fn(move |request| {
                        let origin = origin.clone();
                        async move { origin.handle(request).await }
                    })).await;
                });
            }
        }
    }
    connections.shutdown().await;
    Ok(())
}
