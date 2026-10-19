# ADR-004: Owned fetches and retained terminal fanout

Status: accepted.

The coordinator owns a bounded JoinSet of fetch tasks. Tasks capture the upstream client and cache-fill guard, without a strong reference back to the coordinator. A drop guard retains the flight and a weak coordinator reference, publishes failure on panic/cancellation, and performs identity-safe cleanup. Completed task failures are reaped and logged; shutdown aborts and joins remaining tasks.

Each flight retains exactly one immutable terminal outcome in Tokio watch: Shared, RetryUnshared, or Failed. Callers read with borrow_and_update before awaiting changed, never holding a watch borrow across an await. A visible terminal outcome wins; otherwise the caller's deadline bounds waiting.

Only complete, fresh, policy-approved snapshots may enter Shared. A oneshot privately transfers unshareable response ownership to the designated first caller. Followers receiving RetryUnshared fetch independently under upstream admission limits, never re-entering the same flight. A disconnected original caller causes its private response to be discarded.

Flight permits include obsolete tasks still finishing after mutation. Global and per-key waiter limits include the first caller and callers on replaced generations; per-key tracking disappears when its last caller leaves. Caller drop releases its waiter capacity; it does not cancel owned work. Shutdown rejects admission, publishes terminal shutdown failure, aborts tasks, and drains their join results. This is local request coalescing, not durable exactly-once fetching.

Synchronization reference: [Tokio watch](https://docs.rs/tokio/latest/tokio/sync/watch/index.html).
