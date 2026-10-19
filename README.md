# WireCache

A Rust HTTP/1.1 reverse proxy for one trusted HTTP origin, with an opt-in bounded response cache and single-flight request coalescing.

```sh
cargo run -- --upstream http://127.0.0.1:9000
curl -i 'http://127.0.0.1:8080/products?q=1'
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/metrics
```

Use an existing local HTTP server on port 9000. Run `cargo run -- --help` for limits. Ctrl-C stops admission, cancels shared fetches with explicit terminal failures, and allows direct connections five seconds to drain.

## Current behavior

- One pooled HTTP client; client Host and absolute request targets cannot select another upstream.
- Request methods, encoded paths, query order, status, and end-to-end headers are preserved. Connection-specific headers and trailers are removed; Hyper rebuilds framing.
- Bodies stream with backpressure. Defaults: 128 downstream connections, 64 active upstream exchanges, 8 MiB request bodies, 3-second upstream exchange deadline, 100 request headers, and 32 KiB HTTP read buffers.
- Upstream capacity exhaustion returns 503 with `Retry-After: 1`. Downstream connection exhaustion closes the newly accepted socket.
- Upstream failures before response headers return 502; deadline expiry returns 504. Once response headers are sent, transfer failure or timeout terminates the body rather than replacing the status.
- Known oversized request bodies return 413 before forwarding. Chunked bodies exceeding the limit abort the upstream upload; an origin might already have consumed a prefix. Do not interpret an aborted mutation as proof that the origin made no changes.
- `/healthz` and `/metrics` are reserved locally for GET/HEAD. Metrics include requests, origin attempts, overloads, cache accounting, flight creation/joins, unshareable outcomes, active flights, and enrolled waiters.

## Response caching

Caching is disabled unless an explicitly public route prefix is configured:

```sh
cargo run -- --upstream http://127.0.0.1:9000 --cache-path-prefix /public/
curl -i -H 'User-Agent:' http://127.0.0.1:8080/public/data
```

The origin must mark the response `Cache-Control: public, max-age=N` or `s-maxage=N`. Only complete 200 responses without cookies, Vary, content encoding, ranges, or trailers qualify. Unknown or malformed cache directives bypass storage.

The fixed request profile accepts only Host, bodyless framing, Connection close/keep-alive, Accept `*/*`, and Accept-Encoding `identity`. Missing Accept/Accept-Encoding are standardized. Every other header bypasses caching, including User-Agent, authorization, cookies, conditionals, and cache directives. This is why the example removes curl's default User-Agent. Encoded paths and dot segments also bypass. Query bytes and order remain part of the key.

Defaults: 64 MiB of accounted cache data, 4096 entries, 1 MiB per body, and 300 seconds maximum local retention. LRU eviction respects byte and entry limits. Accounting includes body, header, and key data; entry count bounds container overhead. These gauges are not a resident-memory measurement. Active responses can retain evicted snapshots, and candidate buffering adds up to one entry limit per active fetch plus transport buffers and transient copies.

Freshness accounts for Date, Age, origin response delay, body buffering, and monotonic residence time. Cache hits include updated Age. Candidate overflow privately replays buffered bytes and streams the remainder; partial transfers never enter storage.

Unsafe attempts invalidate the same fixed-origin target at admission and completion, even on failure or cancellation. Version tokens prevent older fills from inserting afterward; overlapping mutations block insertion. Tracking exists only while bounded upstream work is active. This is conservative same-target invalidation, not complete RFC invalidation or protection against out-of-band origin changes.

## Single-flight coordination

Compatible cache misses join one coordinator-owned fetch. The first caller can disconnect without canceling it. Only verified, complete, fresh snapshots are shared. Every caller gets independent headers and shared immutable body bytes. A retained terminal result prevents a caller from missing completion.

Unshareable responses go privately to the original caller; followers issue independent uncached fetches under upstream limits, without re-entering a flight. If the original caller left, its private response is discarded. Failures broadcast a mapped error and allow a later request to retry. There are no automatic transport retries.

Defaults: 64 active flights (including replaced flights still finishing), 128 enrolled callers per key, 1024 enrolled callers globally, and a five-second waiter deadline. Limits include the first caller and callers still waiting on replaced flights for the same key. Rejection is immediate 503 with `Retry-After: 1`. Caller cancellation releases its waiter slot. Owned tasks continue until completion, origin timeout, or shutdown, even if all callers leave.

Admission rechecks cache and mutation versions under the coordinator lock. Cache publication precedes flight removal; old cleanup checks flight identity before removal. Calls already enrolled before a mutation may complete with their original result, while later callers cannot join that older flight. No synchronous lock spans an await.

The deterministic burst test holds the origin until all 100 callers are enrolled, then verifies one origin request and 100 correct responses. This is a healthy, bounded, compatible-burst guarantee, not exactly-once execution across failures or restarts. Performance and resident-memory benchmarks remain pending.

Next: broader failure/resource hardening, latency instrumentation, CI, and a reproducible benchmark/demo harness.

Decisions: [public-route policy and freshness](docs/adr/ADR-002-cache-policy.md), [oversized responses](docs/adr/ADR-003-body-buffering.md), [flight ownership](docs/adr/ADR-004-flight-lifecycle.md), [publication ordering](docs/adr/ADR-005-flight-publication.md), [LRU accounting and invalidation](docs/adr/ADR-006-cache-storage.md).

## Verification

```sh
cargo fmt --check
cargo clippy --offline --all-targets -- -D warnings
cargo test --offline
```

Dependencies must be downloaded first. Integration tests use local TCP sockets and controlled origin gates; no external service is needed.

## Scope and limitations

Local development with a trusted origin, loopback by default. Not suitable for public exposure: there is no TLS, authentication, or full RFC 9111 cache. HTTP/2, CONNECT, upgrades, and trailers are unsupported. Body deadlines are checked while polling bodies; a stalled downstream socket can retain its bounded connection slot until disconnect or shutdown. Transport buffering and concurrency are bounded, but resident-memory benchmarking has not been performed.

Requirements: [PRD](WireCache_PRD.md), [technical design](WireCache_Technical_Design.md), and [collaboration guidelines](WireCache_Agent_Prompt.md).
