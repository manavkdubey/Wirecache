# ADR-005: Cache admission, publication, and identity-safe cleanup

Status: accepted.

The coordinator mutex serializes cache recheck, existing-flight validation, and new-flight installation. Lock order is coordinator then cache. Cache methods never acquire the coordinator lock. No synchronous lock spans an await.

A successful fetch validates the complete response and attempts cache insertion before publishing Shared and removing its registry entry. A concurrent admission therefore observes a fresh cache entry or the pending/completed registered flight, preventing a miss-to-election gap from starting a redundant fetch. If cache capacity rejects an otherwise safe snapshot, current waiters can still share it; future callers may fetch again.

Mutation version tokens prevent old fills from populating storage. Admission also checks the registered flight's token and that no mutation is active. Invalidated entries can be replaced while older tasks finish under the active-flight bound. Cleanup removes a registry entry only when its Arc identity matches the completing flight, so old cleanup cannot erase a replacement.

Callers enrolled before mutation may receive their original fetch result. New callers cannot join that invalidated flight; requests during an active mutation bypass coalescing. Cached results and shared outcomes are checked for freshness before delivery. Expired completed snapshots trigger a bounded private fetch rather than a stale cache response.
