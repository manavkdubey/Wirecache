# WireCache — Product Requirements Document

**Version:** 1.0 | **Status:** Ready for implementation | **Audience:** developer, reviewer, technical interviewer

## 1. Executive summary
WireCache is a single-instance Rust HTTP/1.1 reverse proxy with an opt-in, conservative shared response cache. Its core feature is **single-flight request coalescing**: many simultaneous compatible cache misses cause one upstream fetch, while each caller receives a correct independent response. It exposes benchmarks and failure-injection tests that demonstrate the guarantees rather than merely claiming speed.

**Positioning:** A serious portfolio systems project for Rust backend, infrastructure, and code-review roles. It is explicitly **not** a production-ready CDN, general-purpose RFC 9111 cache, or multi-node distributed system.

## 2. Problem
When a popular resource expires, numerous clients can simultaneously miss cache and stampede the origin. An ordinary proxy may forward all requests, increasing upstream load and tail latency. Coalescing is hard to implement correctly because of concurrent leader election, cancellation, failures, response eligibility, bounded memory, and HTTP metadata variance.

## 3. Users and jobs to be done
- **Backend engineer:** run `wirecache` in front of a local demo upstream and compare origin request counts under a load burst.
- **Reviewer:** inspect implementation and verify correctness using deterministic fault-injection tests.
- **Recruiter/interviewer:** understand an end-to-end system and review reproducible p50/p95/p99 numbers and known limitations.

## 4. Goals and success criteria
1. Forward supported HTTP/1.1 requests with accurate response status, headers, and body.
2. Cache only eligible, bounded `GET` responses with explicit freshness. Default to bypass on ambiguity.
3. For 100 simultaneous identical **eligible** requests to a deliberately slow upstream, demonstrate exactly one origin fetch in a deterministic integration test (the test must control arrival while the first fetch is pending).
4. Never leave a waiter blocked indefinitely after upstream success, error, leader cancellation, or shutdown; all wait paths have deadlines.
5. Show bounded resident cache memory and bounded in-flight work, with explicit rejection/backpressure behavior.
6. Prove that personal/authenticated, private, no-store, `Vary`, request directives, cookies, and partial/range data do not leak through cache/coalescing.
7. Supply a scripted reproducible benchmark comparing direct origin, pass-through proxy, warm cache, and cold burst. Report machine details, throughput, p50/p95/p99, errors, upstream requests, and memory.
8. Include architecture decision records (ADRs), structured tracing, and concise CLI usage.

**Success is correctness first; no performance-improvement percentage is a release criterion.**

## 5. Scope
### P0 — portfolio MVP (required)
- One configured trusted HTTP upstream; listen on localhost by default; HTTP/1.1 only.
- `/healthz` for process health and `/metrics` for local Prometheus-style metrics; reserve these paths locally.
- Request forwarding, method/path/query preservation, response status/headers, content length semantics through HTTP library.
- Shared connection-pooled upstream client; request and response hop-by-hop header sanitization.
- Conservative cache: only GET, 200, no request authorization/cookies/range/conditional headers, no request no-cache/no-store, response must have explicit `Cache-Control: public, max-age=N` or `s-maxage=N` (positive), no response `Set-Cookie`, `private`, `no-store`, `no-cache`, `Vary`, or ambiguous/malformed directives, no partial content. Disable caching if not safely bounded.
- Cache key: normalized scheme + configured upstream authority + path + query + GET method; never key on untrusted Host. Because all `Vary` responses are bypassed in P0, no variable-header variants are cached.
- Buffer eligible candidate bodies up to configurable maximum (default 1 MiB), with timeout and early content-length rejection. A body that exceeds max or fails during transfer must never be cached or shared as a completed response. For this portfolio MVP, large/unknown/bypassed bodies may be fully buffered only under a separate strictly bounded pass-through policy **or preferably streamed**; see technical design.
- Per-key single-flight coordination only for requests meeting conservative request eligibility. One independent leader task owned by a coordinator, not by the first request handler; followers subscribe with deadlines.
- Cache TTL based on monotonic `Instant` at insertion and parsed freshness. Do not serve expired entries. Simplified HTTP `Age` semantics: cached response eligibility excludes pre-existing Age/Date where freshness calculations cannot be performed correctly; the preferred production-like extension is explicit age calculation. See technical design.
- Deterministic tests: concurrent miss, leader disconnect, upstream timeout, upstream error, cancellation, cache expiry, no-store, private, authorization, cookies, `Vary`, GET query isolation, oversized response, shutdown, and repeated failed fetch.
- Bounded admission: max concurrent upstream flights, max waiters per key, max distinct flights, cache byte cap, entry cap, timeouts; overload returns 503 with Retry-After or defined fail-open behavior.

### P1 — after MVP
- RFC-aware Age/Date freshness, conditional requests with validators (ETag/If-None-Match, 304), additional `Vary` variants (e.g. Accept-Encoding) with canonicalized variant matching, safe invalidation on unsafe methods, streaming follower responses if feasible, graceful drain, CLI config validation.

### P2 — intentionally not a requirement
- HTTPS termination, HTTP/2/3, CONNECT/WebSockets, arbitrary origin selection, multi-node coordination, disk persistence, CDN functionality, full RFC 9111 conformance, public internet exposure, distributed cache consistency.

## 6. Expected behavior and acceptance cases
| ID | Given | When | Then |
|---|---|---|---|
| AC-01 | 100 clients hit same uncached eligible URL | upstream is held for 200 ms | exactly 1 upstream fetch; all 100 receive byte-identical successful body and correct status |
| AC-02 | one cached response with fresh TTL | next request arrives | origin count unchanged; cache hit counter increases |
| AC-03 | cache expired | new request arrives | cache is not reused; one new fetch serves eligible coalesced callers |
| AC-04 | leader HTTP client disconnects | others still waiting | background coordinator owns flight; remaining callers complete |
| AC-05 | upstream fails or times out | multiple waiters pending | each gets consistent mapped error before deadline; registry cleans up; next request can retry |
| AC-06 | response is `private`/`no-store`, sets cookie, or has Vary | concurrent clients request | response is never committed to shared cache or shared across possibly non-equivalent callers; conservative bypass may create many origin calls |
| AC-07 | request has Authorization/Cookie/Range/no-store or conditional headers | request sent | bypass cache and coalescing; forwarded safely |
| AC-08 | response exceeds max cacheable size | request arrives | never cached; bound memory and preserve complete correct client response or return defined limit error according to policy |
| AC-09 | 1,000 unique cold keys at once | bounds reached | inflight resource usage bounded; overload behavior visible/consistent |
| AC-10 | successful non-GET updates resource | client then GETs | P0 documented: cache bypass for unsafe methods and conservative invalidation of the affected target; full RFC invalidation is not claimed |
| AC-11 | graceful shutdown | waiters are pending | bounded termination and explicit outcome; no indefinite waits |
| AC-12 | upstream yields conflicting or malformed caching headers | GET request | cache bypass, no leak or panic |

## 7. CLI and demo experience
Example (illustrative):
```bash
cargo run --release -- --listen 127.0.0.1:8080 --upstream http://127.0.0.1:9000 --max-cache-mib 64 --max-entry-kib 1024 --upstream-timeout-ms 3000
curl -i http://127.0.0.1:8080/data?x=1
curl -s http://127.0.0.1:8080/metrics
./scripts/demo_stampede.sh
./scripts/bench.sh
```
Sample metrics: `wirecache_requests_total{result="hit|miss|bypass|overload"}`, `wirecache_origin_requests_total`, `wirecache_singleflight_join_total`, `wirecache_inflight_flights`, `wirecache_cache_bytes`, `wirecache_upstream_latency_seconds`, `wirecache_request_latency_seconds`.

## 8. Nonfunctional requirements
- Stable Rust, `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test`.
- No unsafe Rust unless formally justified in an ADR (aim for zero).
- `cargo test` must run offline once dependencies downloaded, without live internet.
- P0 proxy bound to loopback by default; no external URL selection; no SSRF-style open-proxy behavior.
- Request, header and body limits with no unbounded in-memory collection.
- Logging redacts Authorization, Cookie and other sensitive headers; debug logs off by default.
- CI on Linux; optional cross-platform development.
- Explicit limitations and threat model in README.

## 9. Measurement methodology
- Use an isolated local upstream with a configured delay (e.g. 200 ms) and request counter.
- Warm up both proxy and origin, benchmark ≥3 repetitions, report distributions instead of one best number.
- Compare direct/upstream, pass-through, warm cached, cold stampede; do not claim that caching makes the origin itself faster.
- Measure cache hit rate, coalescing factor = compatible downstream cache misses / actual origin fetches, error rate, memory peak, CPU, p50/p95/p99.
- Clearly label hardware/OS, client load generator, concurrency, payload bytes, TTL, timeouts and sample count.

## 10. Delivery milestones
- **M1 (transport):** transparent forwarding for supported scope and hop-by-hop sanitization, tests.
- **M2 (cache policy):** conservative eligibility parser, bounded cache, expiry, tests.
- **M3 (single-flight):** coordinator lifecycle, state machine, cancellation-safe followers, race tests.
- **M4 (hardening):** limits, shutdown, timeouts, adversarial and privacy tests.
- **M5 (portfolio):** benchmark harness, ADRs, README diagrams, demo transcript, CI.

## 11. Risks, constraints and release gates
- **Incorrect HTTP caching may leak data:** fail closed; bypass unknown semantics.
- **Single-flight deadlock/race:** no locks across `.await`; explicit terminal event, deadline, registry cleanup.
- **Response body fanout can balloon memory:** bounded body sizes + no duplicate body per waiter where possible.
- **Performance numbers can mislead:** scripted controlled comparison and publish raw results.
- **AI-generated code can hide defects:** require the author to explain design invariants and tests personally.

A release is blocked if any privacy isolation test, cancellation liveness test, or bounded-memory test fails.

## 12. References
- RFC 9111 HTTP Caching: https://www.rfc-editor.org/rfc/rfc9111.html
- Hyper: https://docs.rs/hyper/latest/hyper/
- hyper-util client: https://docs.rs/hyper-util/latest/hyper_util/client/legacy/
- Tokio watch: https://docs.rs/tokio/latest/tokio/sync/watch/
