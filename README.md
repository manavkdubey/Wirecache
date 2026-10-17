# WireCache

A Rust HTTP/1.1 reverse proxy for one trusted HTTP origin. Transport is implemented; caching and single-flight coordination are upcoming milestones.

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

## Design decisions agreed for later milestones

- Explicit public cache routes with a fixed request-header profile; other requests bypass cache/coalescing.
- Buffer candidate responses up to the cache limit, then privately stream oversized responses while followers fetch independently under admission limits.
- Calculate remaining freshness using Date, Age, and elapsed time.
- Use per-target generations to prevent invalidated fetches from repopulating the cache or accepting new followers.
- Coordinator-owned fetch tasks survive the first caller disconnecting; retained terminal outcomes and deadlines prevent missed completion and indefinite waits.

## Verification

```sh
cargo fmt --check
cargo clippy --offline --all-targets -- -D warnings
cargo test --offline
```

Dependencies must be downloaded first. Integration tests use local TCP sockets and controlled origin gates; no external service is needed.

## Scope and limitations

Local development with a trusted origin, loopback by default. Not suitable for public exposure: there is no TLS, authentication, or production HTTP cache. HTTP/2, CONNECT, upgrades, and trailers are unsupported. The body deadline includes downstream backpressure, so slow clients can lose a transfer. Transport buffering and concurrency are bounded, but resident-memory benchmarking has not been performed.

Requirements: [PRD](WireCache_PRD.md), [technical design](WireCache_Technical_Design.md), and [collaboration guidelines](WireCache_Agent_Prompt.md).
