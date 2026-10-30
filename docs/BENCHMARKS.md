# Reproducible benchmark

Run `./scripts/bench.sh` from a Linux checkout with stable Rust and Python 3. It builds locked release binaries and uses only existing Rust dependencies and the Python standard library. Results go into a new timestamped directory under ignored `bench/results/`. `--output` must name a directory that does not already exist.

```sh
./scripts/bench.sh
./scripts/bench.sh --requests 1200 --warm-requests 100000 --concurrency 32 --burst 100 --repetitions 3
```

## Workloads and control

Each case starts fresh origin/proxy processes. All modes use the same configurable origin delay, payload, and TTL. The Rust runner validates every successful response byte against the fixture payload, uses a pooled HTTP/1.1 client, bounds concurrency, and synchronizes the initial request wave. The workload is closed-loop, not an open-loop arrival-rate or coordinated-omission-corrected saturation test.

The runner performs at least 32 warmup requests (or concurrency, if greater), signals READY, and waits. The controller snapshots origin/proxy counters and process resources, then starts the timed phase. Warmups use the same URL only for warm-cache measurement; other modes use a distinct query key. This preserves the warm client pool without warming the cold-burst target.

| Mode | Default timed requests | Concurrency | Required timed origin count |
|---|---:|---:|---:|
| Direct origin | 600 | 32 | 600 |
| Pass-through proxy | 600 | 32 | 600 |
| Warm cache | 100,000 | 32 | 0, with 100,000 cache hits |
| Cold burst | 100 | 100 | 1, with coalescing observed |

Every request has a five-second client timeout. Proxy defaults are a three-second origin timeout, five-second waiter timeout, ten-second idle timeout, 64 MiB cache-data cap, 4096 entries, 1 MiB body cap, and 300-second retention cap. The fixture uses a 60-second TTL. Harness connection/upstream/per-key admission limits are raised when needed to accommodate requested concurrency; exact commands are recorded. Rust processes use two Tokio workers by default.

Origin counts come from the fixture's data-request counter before/after measurement, not inferred from throughput. Compatible misses and joins come from proxy metric deltas. Coalescing factor is measured compatible misses divided by origin fetches, not all downstream requests divided by origin fetches. Current reference cold bursts each recorded 100 misses, 99 joins, and one origin fetch. A separate deterministic Rust test holds the origin until all 100 callers have enrolled.

## Reference results

Measured **2026-10-31 UTC**, from clean source `845766e554124a58bc7e6802999b0abba2d4d795`. Machine: AMD Ryzen 7 4800H, 16 logical/affinity CPUs, Linux 7.2.11 Arch, glibc 2.44, Rust 1.99.1, Python 3.14.8. Payload 4096 bytes; origin delay 200 ms. This was a shared local environment, not dedicated benchmark hardware.

All three runs of all four modes passed payload, error-count, origin-count, and cache/coalescing checks. The table reports medians of the three run-level values; it is not a pooled percentile. RSS is the maximum recorded proxy peak across the three cases, including process startup and warmup.

| Mode | p50 ms | p95 ms | p99 ms | Requests/s | Proxy CPU seconds | Proxy RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| Direct origin | 201.495 | 202.341 | 202.709 | 156.53 | — | — |
| Pass-through | 202.137 | 203.076 | 203.441 | 156.09 | 0.06 | 6.75 |
| Warm cache | 0.347 | 0.622 | 0.776 | 72,339.41 | 2.09 | 6.04 |
| Cold burst | 202.379 | 203.475 | 203.678 | 490.06 | 0.01 | 7.02 |

Warm-cache runs lasted 1.14–1.40 seconds each. Their p99 ranged from 0.654–0.795 ms and throughput from approximately 71,431–87,909 requests/s. A pilot with only 600 warm requests lasted about 13 ms; the committed harness increases the warm sample count to avoid using that pilot for throughput claims.

The result demonstrates cache reuse and origin-load suppression. It does not make the origin faster. Cold-burst throughput divides one batch by its elapsed time and is not comparable to a sustained saturation rate. The working set is tiny; RSS does not establish a maximum under a full cache or adversarial load.

## Artifacts and analysis

The [reference directory](../bench/reference/2026-10-31/) retains:

- `summary.json`: all per-run summaries, exact commands, before/after metrics, resource observations, source checksums, and machine/toolchain configuration.
- `summary.csv`: latency/origin-count projection with unchanged numeric values and normalized LF line endings.
- Twelve `*.samples.json.gz` files: every request's nanosecond latency and fixed outcome, losslessly compressed. Quantiles use nearest-rank over successful requests; errors remain separately counted and retained.

All reference summaries were recalculated from the compressed samples before publication. Total timed samples: 303,900, with zero errors. Actual measurement timestamps remain in the data even though commit dates use a labeled reconstructed development timeline. The later LF-only export correction does not alter the recorded workload or calculations.

For example, inspect the smaller burst sample:

```sh
gzip -dc bench/reference/2026-10-31/cold_burst-1.samples.json.gz | python3 -m json.tool
python3 -m unittest discover -s scripts/tests
```

## Resource interpretation

Proxy, origin, and load generator are separate processes. The harness samples origin/proxy RSS every 10 ms and reads Linux VmHWM; both values are retained. Published proxy peak uses the maximum recorded observation. HWM includes startup/warmup; interval sampling can miss short spikes, and independently read Linux counters can differ slightly. Neither value is the accounted cache-data gauge.

CPU seconds are origin/proxy user+system tick deltas from READY through load-runner exit, including brief result serialization and client-pool teardown. They exclude warmup and the separate generator's CPU. Clock resolution is 100 ticks/second on this machine, so a recorded zero can mean activity below that resolution. Multithreaded CPU seconds may exceed wall time.

The harness uses fresh temporary loopback ports, bounded workloads, and only terminates processes it created. A port-bind or child-process failure aborts the run; failed correctness checks return nonzero. Keep all repetitions when reporting results. CI runs the two-mode demo as a correctness smoke test, not a performance gate.
