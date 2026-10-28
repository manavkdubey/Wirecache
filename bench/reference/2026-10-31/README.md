# Recorded local reference run

Measured on 2026-10-31 UTC from clean source commit `845766e554124a58bc7e6802999b0abba2d4d795`. These measurement dates are real; commit author/committer dates in this repository are an explicitly reconstructed development timeline.

`summary.json` contains machine/toolchain settings, source checksums, exact commands, per-run latency/throughput/error counts, origin counters, CPU seconds, and RSS observations. `summary.csv` is the latency/origin-count projection. Each `*.samples.json.gz` retains every request latency and outcome. All summaries were independently recalculated from these compressed samples before publication. No logs or build binaries are included.

Three repetitions per mode: 600 requests at concurrency 32 for direct/pass-through, 100000 at concurrency 32 for warm cache, and one 100-request/concurrency-100 cold burst. Payload 4096 bytes, origin delay 200 ms, TTL 60 seconds, two Tokio workers per Rust process. Warmups are excluded. All 12 cases passed their correctness checks with zero errors.

This is a shared local environment and a small working set. It is not a production capacity or maximum-memory claim. Full methodology and interpretation: [benchmark guide](../../../docs/BENCHMARKS.md).

CSV line endings were normalized to LF for Git; all fields and numerical values are unchanged.
