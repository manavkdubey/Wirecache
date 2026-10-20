use std::{
    fmt::Write,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use hyper::StatusCode;
use tokio::time::Instant;

const BUCKETS_US: [u64; 11] = [
    100, 500, 1000, 5000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 30_000_000,
];

#[derive(Clone, Copy)]
pub(crate) enum Scope {
    Request,
    Upstream,
    Wait,
}

impl Scope {
    fn name(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Upstream => "upstream",
            Self::Wait => "wait",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum End {
    Success,
    Timeout,
    Transport,
    Limit,
    Cancelled,
}

impl End {
    fn name(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::Limit => "limit",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Default)]
struct Histogram {
    bins: [AtomicU64; 12],
    sum_us: AtomicU64,
}

impl Histogram {
    fn observe(&self, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        let index = BUCKETS_US
            .iter()
            .position(|bound| micros <= *bound)
            .unwrap_or(BUCKETS_US.len());
        self.sum_us.fetch_add(micros, Ordering::Relaxed);
        self.bins[index].fetch_add(1, Ordering::Relaxed);
    }

    fn render(&self, name: &str, output: &mut String) {
        writeln!(output, "# TYPE {name} histogram").unwrap();
        let mut count = 0;
        for (index, bin) in self.bins.iter().enumerate() {
            count += bin.load(Ordering::Relaxed);
            if let Some(bound) = BUCKETS_US.get(index) {
                writeln!(
                    output,
                    "{name}_bucket{{le=\"{}\"}} {count}",
                    *bound as f64 / 1_000_000.0
                )
                .unwrap();
            }
        }
        writeln!(
            output,
            "{name}_bucket{{le=\"+Inf\"}} {count}\n{name}_count {count}\n{name}_sum {}",
            self.sum_us.load(Ordering::Relaxed) as f64 / 1_000_000.0
        )
        .unwrap();
    }
}

#[derive(Default)]
pub(crate) struct Telemetry {
    started: [AtomicU64; 3],
    active: [AtomicUsize; 3],
    outcomes: [[AtomicU64; 5]; 3],
    durations: [Histogram; 3],
    bytes: [AtomicU64; 3],
    statuses: [AtomicU64; 6],
    overloads: AtomicU64,
    connections: AtomicUsize,
    connection_rejections: AtomicU64,
    connection_timeouts: AtomicU64,
    connection_errors: AtomicU64,
    transfer_deadline_closures: AtomicU64,
    buffered: AtomicUsize,
    peak_buffered: AtomicUsize,
}

impl Telemetry {
    pub(crate) fn start(self: &Arc<Self>, scope: Scope) -> Observation {
        self.started[scope as usize].fetch_add(1, Ordering::Relaxed);
        self.active[scope as usize].fetch_add(1, Ordering::Relaxed);
        Observation {
            telemetry: self.clone(),
            scope,
            started: Instant::now(),
            bytes: 0,
            finished: false,
        }
    }

    pub(crate) fn response(&self, status: StatusCode) {
        let index = (status.as_u16() as usize / 100)
            .checked_sub(1)
            .filter(|index| *index < 5)
            .unwrap_or(5);
        self.statuses[index].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn overload(&self) {
        self.overloads.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn connection_rejected(&self) {
        self.connection_rejections.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn connection_timed_out(&self) {
        self.connection_timeouts.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn connection_failed(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn transfer_deadline_closed(&self) {
        self.transfer_deadline_closures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn connection(self: &Arc<Self>) -> ConnectionCount {
        self.connections.fetch_add(1, Ordering::Relaxed);
        ConnectionCount(self.clone())
    }

    pub(crate) fn buffer(self: &Arc<Self>) -> BufferedBytes {
        BufferedBytes {
            telemetry: self.clone(),
            bytes: 0,
        }
    }

    pub(crate) fn render(&self) -> String {
        let mut output = format!(
            "wirecache_requests_total {}\nwirecache_origin_requests_total {}\nwirecache_overloads_total {}\nwirecache_active_connections {}\nwirecache_connection_rejections_total {}\nwirecache_connection_timeouts_total {}\nwirecache_buffered_bytes {}\nwirecache_buffered_bytes_peak {}\n",
            self.started[Scope::Request as usize].load(Ordering::Relaxed),
            self.started[Scope::Upstream as usize].load(Ordering::Relaxed),
            self.overloads.load(Ordering::Relaxed),
            self.connections.load(Ordering::Relaxed),
            self.connection_rejections.load(Ordering::Relaxed),
            self.connection_timeouts.load(Ordering::Relaxed),
            self.buffered.load(Ordering::Relaxed),
            self.peak_buffered.load(Ordering::Relaxed)
        );
        writeln!(
            output,
            "wirecache_transfer_deadline_closures_total {}",
            self.transfer_deadline_closures.load(Ordering::Relaxed)
        )
        .unwrap();
        writeln!(
            output,
            "wirecache_connection_errors_total {}",
            self.connection_errors.load(Ordering::Relaxed)
        )
        .unwrap();
        for scope in [Scope::Request, Scope::Upstream, Scope::Wait] {
            let name = scope.name();
            writeln!(
                output,
                "wirecache_active_{name} {}",
                self.active[scope as usize].load(Ordering::Relaxed)
            )
            .unwrap();
            if !matches!(scope, Scope::Wait) {
                let direction = if matches!(scope, Scope::Request) {
                    "response"
                } else {
                    "upstream"
                };
                writeln!(
                    output,
                    "wirecache_{direction}_body_bytes_total {}",
                    self.bytes[scope as usize].load(Ordering::Relaxed)
                )
                .unwrap();
            }
            for end in [
                End::Success,
                End::Timeout,
                End::Transport,
                End::Limit,
                End::Cancelled,
            ] {
                writeln!(
                    output,
                    "wirecache_{name}_outcomes_total{{outcome=\"{}\"}} {}",
                    end.name(),
                    self.outcomes[scope as usize][end as usize].load(Ordering::Relaxed)
                )
                .unwrap();
            }
            self.durations[scope as usize]
                .render(&format!("wirecache_{name}_latency_seconds"), &mut output);
        }
        for (class, count) in ["1xx", "2xx", "3xx", "4xx", "5xx", "other"]
            .iter()
            .zip(&self.statuses)
        {
            writeln!(
                output,
                "wirecache_responses_total{{class=\"{class}\"}} {}",
                count.load(Ordering::Relaxed)
            )
            .unwrap();
        }
        output
    }
}

pub(crate) struct Observation {
    telemetry: Arc<Telemetry>,
    scope: Scope,
    started: Instant,
    bytes: u64,
    finished: bool,
}

impl Observation {
    pub(crate) fn add_bytes(&mut self, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes as u64);
    }

    pub(crate) fn finish(&mut self, end: End) {
        if self.finished {
            return;
        }
        let index = self.scope as usize;
        self.telemetry.durations[index].observe(self.started.elapsed());
        self.telemetry.outcomes[index][end as usize].fetch_add(1, Ordering::Relaxed);
        self.telemetry.bytes[index].fetch_add(self.bytes, Ordering::Relaxed);
        self.telemetry.active[index].fetch_sub(1, Ordering::Relaxed);
        self.finished = true;
        tracing::debug!(
            scope = self.scope.name(),
            outcome = end.name(),
            elapsed_us = self.started.elapsed().as_micros() as u64,
            body_bytes = self.bytes,
            "exchange finished"
        );
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        self.finish(End::Cancelled);
    }
}

pub(crate) struct ConnectionCount(Arc<Telemetry>);
impl Drop for ConnectionCount {
    fn drop(&mut self) {
        self.0.connections.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(crate) struct BufferedBytes {
    telemetry: Arc<Telemetry>,
    bytes: usize,
}

impl BufferedBytes {
    pub(crate) fn set_capacity(&mut self, capacity: usize) {
        let total = if capacity >= self.bytes {
            self.telemetry
                .buffered
                .fetch_add(capacity - self.bytes, Ordering::Relaxed)
                + capacity
                - self.bytes
        } else {
            self.telemetry
                .buffered
                .fetch_sub(self.bytes - capacity, Ordering::Relaxed)
                - (self.bytes - capacity)
        };
        self.telemetry
            .peak_buffered
            .fetch_max(total, Ordering::Relaxed);
        self.bytes = capacity;
    }
}

impl Drop for BufferedBytes {
    fn drop(&mut self) {
        self.set_capacity(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_renders_cumulative_seconds_and_matching_infinity_count() {
        let histogram = Histogram::default();
        histogram.observe(Duration::from_micros(100));
        histogram.observe(Duration::from_micros(501));
        histogram.observe(Duration::from_secs(31));
        let mut text = String::new();
        histogram.render("latency", &mut text);
        assert!(text.contains("latency_bucket{le=\"0.0001\"} 1\n"));
        assert!(text.contains("latency_bucket{le=\"0.001\"} 2\n"));
        assert!(text.contains("latency_bucket{le=\"+Inf\"} 3\nlatency_count 3\n"));
        assert!(text.contains("latency_sum 31.000601\n"));
    }

    #[test]
    fn guards_account_once_and_buffer_capacity_releases_on_drop() {
        let telemetry = Arc::new(Telemetry::default());
        let mut first = telemetry.start(Scope::Upstream);
        first.add_bytes(17);
        first.finish(End::Timeout);
        first.finish(End::Success);
        drop(first);
        drop(telemetry.start(Scope::Request));
        let mut a = telemetry.buffer();
        a.set_capacity(1024);
        {
            let mut b = telemetry.buffer();
            b.set_capacity(2048);
        }
        assert_eq!(telemetry.buffered.load(Ordering::Relaxed), 1024);
        drop(a);
        let text = telemetry.render();
        assert!(text.contains("wirecache_active_upstream 0\n"));
        assert!(text.contains("wirecache_upstream_outcomes_total{outcome=\"timeout\"} 1\n"));
        assert!(text.contains("wirecache_request_outcomes_total{outcome=\"cancelled\"} 1\n"));
        assert!(text.contains("wirecache_upstream_body_bytes_total 17\n"));
        assert!(text.contains("wirecache_buffered_bytes 0\nwirecache_buffered_bytes_peak 3072\n"));
    }
}
