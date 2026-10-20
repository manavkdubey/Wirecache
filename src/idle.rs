use std::{
    io::{self, IoSlice},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use crate::body::TransferDeadline;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::watch,
    time::Instant,
};

pub(crate) struct ActivityIo<T> {
    inner: T,
    activity: Arc<Mutex<Instant>>,
}

impl<T> ActivityIo<T> {
    pub(crate) fn new(inner: T) -> (Self, Activity) {
        let activity = Arc::new(Mutex::new(Instant::now()));
        (
            Self {
                inner,
                activity: activity.clone(),
            },
            Activity(activity),
        )
    }

    fn progress(&self) {
        *self.activity.lock().unwrap() = Instant::now();
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for ActivityIo<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buffer);
        if matches!(result, Poll::Ready(Ok(()))) && buffer.filled().len() > before {
            this.progress();
        }
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for ActivityIo<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, buffer);
        if matches!(result, Poll::Ready(Ok(count)) if count > 0) {
            this.progress();
        }
        result
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write_vectored(cx, buffers);
        if matches!(result, Poll::Ready(Ok(count)) if count > 0) {
            this.progress();
        }
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

pub(crate) struct Activity(Arc<Mutex<Instant>>);

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Expired {
    Idle,
    Transfer,
}

impl Activity {
    pub(crate) async fn monitor(
        &self,
        timeout: Duration,
        mut transfer: watch::Receiver<Option<TransferDeadline>>,
    ) -> Expired {
        let mut open = true;
        loop {
            let current = transfer.borrow_and_update().clone();
            let idle_at = *self.0.lock().unwrap() + timeout;
            let deadline = current
                .as_ref()
                .filter(|deadline| deadline.active())
                .map_or(idle_at, |deadline| idle_at.min(deadline.at));
            tokio::select! {
                () = tokio::time::sleep_until(deadline) => {
                    if let Some(current) = &current && Instant::now() >= current.at && current.expire() { return Expired::Transfer; }
                    if Instant::now() >= *self.0.lock().unwrap() + timeout { return Expired::Idle; }
                }
                changed = transfer.changed(), if open => { if changed.is_err() { open = false; } }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test(start_paused = true)]
    async fn real_read_and_write_progress_extend_the_deadline() {
        let (stream, mut peer) = tokio::io::duplex(8);
        let (mut io, activity) = ActivityIo::new(stream);
        let (_transfer, deadlines) = watch::channel(None);
        let monitor = tokio::spawn(async move {
            activity.monitor(Duration::from_secs(10), deadlines).await;
        });
        tokio::time::advance(Duration::from_secs(9)).await;
        io.write_all(b"a").await.unwrap();
        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(!monitor.is_finished());
        peer.write_all(b"b").await.unwrap();
        let mut byte = [0];
        io.read_exact(&mut byte).await.unwrap();
        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(!monitor.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        monitor.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn empty_writes_and_flushes_do_not_keep_a_connection_alive() {
        let (stream, _peer) = tokio::io::duplex(8);
        let (mut io, activity) = ActivityIo::new(stream);
        let (_transfer, deadlines) = watch::channel(None);
        let monitor = tokio::spawn(async move {
            activity.monitor(Duration::from_secs(10), deadlines).await;
        });
        tokio::time::advance(Duration::from_secs(9)).await;
        assert_eq!(io.write(b"").await.unwrap(), 0);
        io.flush().await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        monitor.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn active_transfer_deadline_expires_despite_recent_socket_progress() {
        use crate::{
            body::UpstreamLease,
            telemetry::{Scope, Telemetry},
        };
        let telemetry = Arc::new(Telemetry::default());
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let lease = UpstreamLease::new(
            slots.clone().acquire_owned().await.unwrap(),
            telemetry.start(Scope::Upstream),
            Instant::now() + Duration::from_secs(3),
        );
        let (stream, _peer) = tokio::io::duplex(8);
        let (mut io, activity) = ActivityIo::new(stream);
        let (_transfer, deadlines) = watch::channel(Some(lease.deadline()));
        let monitor =
            tokio::spawn(async move { activity.monitor(Duration::from_secs(10), deadlines).await });
        tokio::time::advance(Duration::from_secs(2)).await;
        io.write_all(b"progress").await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(monitor.await.unwrap(), Expired::Transfer);
        drop(lease);
        assert_eq!(slots.available_permits(), 1);
        assert!(
            telemetry
                .render()
                .contains("wirecache_upstream_outcomes_total{outcome=\"timeout\"} 1\n")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn completed_transfer_does_not_close_a_keep_alive_connection_at_its_old_deadline() {
        use crate::{
            body::UpstreamLease,
            telemetry::{End, Scope, Telemetry},
        };
        let telemetry = Arc::new(Telemetry::default());
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let mut lease = UpstreamLease::new(
            slots.acquire_owned().await.unwrap(),
            telemetry.start(Scope::Upstream),
            Instant::now() + Duration::from_secs(3),
        );
        let (stream, _peer) = tokio::io::duplex(8);
        let (_io, activity) = ActivityIo::new(stream);
        let (_transfer, deadlines) = watch::channel(Some(lease.deadline()));
        let monitor =
            tokio::spawn(async move { activity.monitor(Duration::from_secs(10), deadlines).await });
        lease.finish(End::Success);
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(!monitor.is_finished());
        tokio::time::advance(Duration::from_secs(7)).await;
        assert_eq!(monitor.await.unwrap(), Expired::Idle);
    }
}
