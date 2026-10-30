# Metric semantics

`GET /metrics` exposes a fixed set of Prometheus text samples. No URL, query, host, credential, cookie, or user identifier becomes a label. HTTP response classes and scope outcomes are fixed enumerations. Debug tracing records scope, outcome, elapsed microseconds, and body-byte count; it does not record request headers or targets. Enable with `RUST_LOG=wirecache=debug`.

## Latency and outcomes

| Histogram | Observation lifetime |
|---|---|
| `wirecache_request_latency_seconds` | Handler admission through response-body production, error, or cancellation. HEAD ends at response preparation. |
| `wirecache_upstream_latency_seconds` | Admitted origin attempt through origin-body consumption, transfer error, or cancellation; streamed bodies keep the observation alive. |
| `wirecache_wait_latency_seconds` | Flight wait through terminal outcome, waiter timeout, or caller cancellation. |

These are classic cumulative histograms in seconds, with finite boundaries from 0.1 ms to 30 seconds, plus `+Inf`, `_count`, and `_sum`. They retain counts, not individual samples. Upstream admission rejection is not an origin attempt or upstream-duration observation.

Each scope has `wirecache_{scope}_outcomes_total{outcome="success|timeout|transport|limit|cancelled"}`. These describe scope/body completion, not HTTP success. A successfully produced 504 response body has request outcome `success`; its HTTP class is separately counted by `wirecache_responses_total{class="5xx"}`. Response-class counters describe application-prepared responses; Hyper parser failures are connection errors and may generate their own response.

Request timing ends when the body is produced, not when the peer acknowledges or consumes all bytes. [The benchmark harness](BENCHMARKS.md) supplies client-measured end-to-end latency. An early client disconnect may occur after body production was already counted as successful.

## Resources and bytes

- `wirecache_requests_total`, `wirecache_origin_requests_total`: started application requests and admitted origin attempts. A metrics scrape includes itself in request starts.
- `wirecache_active_request`, `wirecache_active_upstream`, `wirecache_active_wait`: unfinished observations. They can differ briefly from independently sampled admission gauges.
- `wirecache_response_body_bytes_total`: downstream payload bytes produced, including locally generated responses; not upload bytes or confirmed delivery.
- `wirecache_upstream_body_bytes_total`: origin payload bytes consumed, including consumed prefixes of cancelled or failed transfers.
- `wirecache_active_connections`: admitted connection tasks; `wirecache_connection_rejections_total` counts immediate socket rejection at capacity.
- `wirecache_connection_timeouts_total`: closures for network inactivity; `wirecache_transfer_deadline_closures_total` counts watchdog closures for an active origin deadline. Body-polled timeouts can close a connection without incrementing the watchdog counter.
- `wirecache_connection_errors_total`: HTTP connection errors, including malformed input and peer/transfer errors.
- `wirecache_buffered_bytes`, `wirecache_buffered_bytes_peak`: current and process-lifetime peak capacity of candidate accumulation vectors. They exclude transport buffers, compaction copies, cache snapshots, allocator overhead, and kernel socket memory.
- Cache byte/entry gauges and hit/miss/eviction counters retain their existing meaning. Cache bytes account for stored body, header, and key data, not process RSS.
- Flight creation/join/unshareable counters and active-flight/waiter gauges retain their admission semantics. `wirecache_overloads_total` counts application capacity errors; socket admission rejection has its own counter.

Observations and capacity guards finalize once. Dropping unfinished work records cancellation and releases its accounting. Concurrent scrapes are approximate snapshots across atomic counters, not a transaction spanning all metrics.

Reference: [Prometheus histograms](https://prometheus.io/docs/practices/histograms/).
