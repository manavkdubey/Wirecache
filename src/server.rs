use std::{future::Future, io, sync::Arc, time::Duration};

use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
};

use crate::Proxy;

pub async fn serve(
    listener: TcpListener,
    proxy: Arc<Proxy>,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let slots = Arc::new(Semaphore::new(proxy.config.max_connections));
    let (stop, _) = watch::channel(false);
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            biased;
            () = &mut shutdown => break Ok(()),
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result { tracing::error!(%error, "connection task failed"); }
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted { Ok(value) => value, Err(error) => break Err(error) };
                let Ok(permit) = slots.clone().try_acquire_owned() else { continue; };
                let proxy = proxy.clone();
                let mut stopping = stop.subscribe();
                connections.spawn(async move {
                    let _permit = permit;
                    let mut builder = http1::Builder::new();
                    builder.timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(5)).max_headers(100).max_buf_size(32 * 1024);
                    let connection = builder.serve_connection(TokioIo::new(stream), service_fn(move |request| {
                        let proxy = proxy.clone();
                        async move { proxy.handle(request).await }
                    }));
                    tokio::pin!(connection);
                    let result = tokio::select! {
                        result = &mut connection => result,
                        _ = stopping.changed() => {
                            connection.as_mut().graceful_shutdown();
                            connection.await
                        }
                    };
                    if result.is_err() { tracing::debug!("HTTP connection ended with an error"); }
                });
            }
        }
    };
    stop.send_replace(true);
    proxy.shutdown().await;
    if tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                tracing::error!(%error, "connection task failed during shutdown");
            }
        }
    })
    .await
    .is_err()
    {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }
    result
}
