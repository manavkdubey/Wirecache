# ADR-007: Coalesced ETag revalidation

Status: accepted and implemented.

An expired response with one syntactically valid ETag may remain in the existing bounded LRU cache. It cannot satisfy a fresh lookup. Entry admission can reclaim expired entries, so validator retention is opportunistic rather than guaranteed. Entries without validators retain the previous expiry behavior.

The coordinator admits validation through the existing single-flight and upstream limits. A version-fenced fill removes the expired entry from cache accounting and owns its immutable snapshot for the exchange. Its body is bounded by the existing per-entry limit; outstanding validation snapshots are bounded by flight admission. Mutations remove retained entries and fence insertion exactly as they do for ordinary fills.

The origin request uses `If-None-Match`. A `200` follows normal buffering, privacy checks, and replacement. A `304` updates stored headers and retains the complete stored body, returning `200` to these unconditional clients. If supplied, the response ETag must weakly match the stored tag; absent ETag identifies the single stored candidate. Duplicate or malformed tags are rejected. A supplied Content-Length must equal the stored body length. Date/Age are recalculated for the validation exchange rather than inherited from the old response. Freshness caps and the conservative response privacy policy still apply.

Unsafe or malformed `304` metadata returns `502` to all flight subscribers. Timeout and transport failures retain their existing error mapping. The selected stale entry is discarded, so the next request fetches unconditionally. No stale-on-error response or automatic retry is added. Downstream conditional, authenticated, cookie-bearing, and extra-header requests continue to bypass caching unchanged.

This implements the validation model in [RFC 9111 sections 4.3 and 4.3.4](https://www.rfc-editor.org/rfc/rfc9111.html#section-4.3), with [RFC 9110 entity-tag syntax and weak comparison](https://www.rfc-editor.org/rfc/rfc9110.html#section-8.8.3). The conservative rejection policy does not claim general RFC cache conformance. Last-Modified validation and downstream conditional cache handling remain future work.

Validation covers validator syntax, metadata/privacy rejection, accounting and mutation fencing, concurrent expiry revalidation, changed representations, and recovery after rejected validation. The checked-in P0 benchmarks remain historical measurements of their recorded source revision.
