# WireCache

A Rust HTTP/1.1 reverse proxy for one trusted HTTP origin, with an opt-in bounded response cache. Single-flight coordination is the next milestone.

```sh
cargo run -- --upstream http://127.0.0.1:9000
curl -i 'http://127.0.0.1:8080/products?q=1'
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/metrics
```

Use an existing local HTTP server on port 9000. Run `cargo run -- --help` for limits. Ctrl-C stops admission and allows connections five seconds to drain.

## Current behavior

- One pooled HTTP client; client Host and absolute request targets cannot select another upstream.
- Request methods, encoded paths, query order, status, and end-to-end headers are preserved. Connection-specific headers and trailers are removed; Hyper rebuilds framing.
- Bodies stream with backpressure. Defaults: 128 downstream connections, 64 active upstream exchanges, 8 MiB request bodies, 3-second upstream exchange deadline, 100 request headers, and 32 KiB HTTP read buffers.
- Upstream capacity exhaustion returns 503 with `Retry-After: 1`. Downstream connection exhaustion closes the newly accepted socket.
- Upstream failures before response headers return 502; deadline expiry returns 504. Once response headers are sent, transfer failure or timeout terminates the body rather than replacing the status.
- Known oversized request bodies return 413 before forwarding. Chunked bodies exceeding the limit abort the upstream upload; an origin might already have consumed a prefix. Do not interpret an aborted mutation as proof that the origin made no changes.
- `/healthz` and `/metrics` are reserved locally for GET/HEAD. Metrics currently count requests, origin attempts, and upstream capacity rejections.

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

Next: coordinator-owned shared fetches, retained terminal outcomes, follower deadlines, and independently bounded retries for unshareable responses. Concurrent misses currently fetch independently.

Decisions: [public-route policy and freshness](docs/adr/ADR-002-cache-policy.md), [oversized responses](docs/adr/ADR-003-body-buffering.md), [LRU accounting and invalidation](docs/adr/ADR-006-cache-storage.md).

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
