# Resource bounds and deadlines

## Admission and storage

| Resource | Default bound | Exhaustion behavior |
|---|---:|---|
| Downstream connections | 128 | Close the newly accepted socket; count rejection. |
| Active origin exchanges | 64 | Immediate 503 with Retry-After; no admission queue. |
| Owned flights, including obsolete generations | 64 | Immediate 503. |
| Enrolled callers per target / globally | 128 / 1024 | Immediate 503. The first caller and replaced generations count. |
| Stored body/header/key data | 64 MiB | LRU eviction or admission rejection. |
| Stored entries | 4096 | LRU eviction. |
| Candidate body | 1 MiB | Private replay-and-stream fallback, never shared or cached. |
| Forwarded request body | 8 MiB | Known length: reject before forwarding. Streaming: stop at the limit. |
| Request headers / HTTP read buffer | 100 / 32 KiB | Hyper rejects over-limit/malformed input. |

Configuration is validated before listening. Limits apply to a single process. There is no disk spool or unbounded body collection. Tracker keys disappear when their bounded active work or enrolled callers disappear. A regression test admits 1,000 distinct keys under a four-flight cap, verifies 996 rejections, and checks cleanup.

## Timeouts

Origin requests have a default three-second deadline spanning headers and origin-body consumption. Flight waits have a separate five-second deadline; a timed-out or disconnected caller does not cancel owned shared work. Request headers have a five-second read deadline.

`--connection-idle-timeout-ms` defaults to 10000. Only actual successful reads/writes extend inactivity time. Empty writes, flush calls, and application activity do not. An independent connection monitor runs even when Hyper is blocked on body production or socket backpressure.

Streaming responses carry the active origin deadline to the connection monitor. Expiry can close the downstream connection even when the body is not polled. Completion atomically disarms this token, so an old deadline cannot close a later keep-alive exchange. If headers were already sent, termination cannot replace the response status. An idle closure counts as cancellation of abandoned upstream work; an origin deadline records timeout.

Fully consumed buffered/cache-hit bodies no longer hold an origin permit or its deadline; their network delivery uses inactivity limits. Healthy active keep-alive connections have no fixed total lifetime. Shutdown cancels and joins owned flights, drains direct connections for up to five seconds, then aborts remaining connection tasks.

## Memory model

Let K be the configured cache-data budget, E entry count, B maximum entry body, F active-flight bound, C connection bound, W global waiter bound, and H the bounded HTTP metadata/read-buffer size plus configured origin identity. A conservative logical-allocation model is:

```text
stored cache data and temporary eviction-key copies: O(K)
cache containers and header allocation overhead:     O(E * H)
candidate buffers and temporary compaction copies:  O(F * B)
retained/evicted snapshot bodies:                    O((C + W) * B)
flight/waiter/request metadata and transport state:  O((F + W + C) * H)
```

Bodies are shared through immutable Bytes/Arc snapshots, so callers sharing one result do not each allocate a body copy. The retained-snapshot term conservatively permits distinct results whose waiters or responses have not yet dropped. Candidate vector capacity never exceeds B per fetch; current/peak gauges expose that allocation component. A controlled two-flight test holds both origins after 512 bytes, verifies the combined capacity and limits, then checks release after completion.

This model is not a process-RSS ceiling: allocator retention, Tokio/runtime state, connection pools, and kernel buffers also consume memory. Count caps bound metadata separately from the accounted cache-data budget. Actual RSS/CPU measurements remain part of the benchmark milestone; do not present cache-byte or candidate-buffer gauges as resident-memory measurements.
