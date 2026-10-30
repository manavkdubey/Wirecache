# Project facts for a resume

- Built a Rust/Tokio/Hyper HTTP/1.1 reverse proxy with pooled origin connections, conservative shared caching, bounded LRU storage, and explicit freshness calculation.
- Implemented cancellation-safe single-flight coalescing: a controlled multithreaded test serves 100 compatible pending misses with one origin fetch, with private-response isolation, mutation fencing, and bounded failure cleanup.
- Added 71 Rust regression tests, seven Python benchmark-analysis/export tests, GitHub CI, and reproducible four-mode benchmarks with raw latency samples, origin counts, process CPU, and RSS measurements.

Optional measured result: median run-level warm-cache p99 **0.776 ms** across three local runs of 100,000 requests at concurrency 32, with zero timed origin fetches and zero client errors. Each 100-client cold burst made one origin fetch with 99 joins.

These numbers refer to the recorded 4 KiB / 200 ms-origin local workload, with two Tokio workers per process on a Ryzen 7 4800H. They are not production capacity claims. Link the [methodology and raw evidence](BENCHMARKS.md) alongside any performance statement. Describe this as a single-process portfolio proxy, not a full RFC cache or distributed CDN.
