# ADR-006: Bounded LRU snapshots and mutation version tokens

Status: accepted.

A short synchronous mutex protects a HashMap of immutable snapshots and a BTreeMap of recency ticks. Expired entries with a valid ETag remain eligible for bounded retention and validation (see [ADR-007](ADR-007-etag-revalidation.md)); they are never returned as fresh hits. Hits update recency; admission removes expired entries and evicts the least recently used until both byte and entry limits fit. Recency operations are O(log n); expiry cleanup currently scans entries on insertion. No mutex spans an await.

The byte gauge counts body bytes, header data, and both retained key copies. Entry limits bound map/container overhead. Active responses can retain evicted Bytes, so this gauge is not total resident memory. Flight, upstream, and downstream admission bound candidate buffering, mutation/fill tracking, and active responses. Waiter permits bound retained flight outcomes and request metadata.

Unsafe attempts invalidate at admission and completion, including failure and cancellation. Each active target has a unique reference-counted version token. Fills may publish only against the same token with no mutation active. New tokens avoid numeric-generation wraparound; simultaneous mutations each fence older work. Drop guards release target tracking, which disappears when its active fills and mutations reach zero.

This prevents stale repopulation through this proxy for the same target. It does not provide full HTTP invalidation, invalidate related URLs, or observe external origin updates. Public immutable routes remain the safest deployment contract.
