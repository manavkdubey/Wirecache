# Threat model

WireCache is a local portfolio system with untrusted downstream HTTP input and one trusted operator-configured HTTP origin. It is not intended for public deployment.

| Boundary | Protection | Remaining limit |
|---|---|---|
| Destination selection | Fixed origin/Host rewriting; CONNECT and upgrades rejected | Origin configuration and DNS are trusted; no TLS. |
| Shared responses | Opt-in public prefix, fixed request profile, auth/cookie/conditional/range bypass, strict response policy | The operator must guarantee that the route is public for that profile. |
| Origin metadata | Vary/cookies/private/no-store/no-cache/ambiguous directives reject sharing and storage | No general RFC cache, validator revalidation, or representation variants. |
| Mutations | Same-target version tokens fence old fills and prevent later callers joining invalidated flights | Related URLs and updates outside this proxy are not invalidated. |
| Resource exhaustion | Connection, upstream, flight, waiter, body, header, entry, and cache-data caps; independent deadlines | Healthy connections can remain active; OS/runtime/allocator memory adds overhead. |
| Cancellation/failure | Owned tasks, retained terminal outcomes, drop cleanup, identity-safe removal, shutdown | No durable coordination or exactly-once guarantee across failures/restarts. |
| Observability | Fixed labels and static outcome fields; no target/header logging in application tracing | Metrics are local plaintext; broad third-party logging settings are operator-controlled. |

Privacy regressions live in `tests/cache.rs` and `tests/coalescing.rs`; framing/resource regressions live in `tests/transport.rs` and `tests/hardening.rs`. Release checks include formatting, Clippy, offline local-origin tests, and the reproducible demo. Benchmark measurements describe observed behavior on their recorded machine; they do not establish a production security or availability guarantee.
