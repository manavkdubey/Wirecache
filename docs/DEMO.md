# One-to-two-minute demo

```sh
./scripts/demo_stampede.sh
```

The script builds release binaries, starts a delayed loopback origin and fresh proxy processes, warms client pools, and compares two 100-client batches. It uses temporary ports and cleans up its own processes. The initial build may take longer on a fresh checkout.

Recorded local smoke output:

```text
pass_through rep=1 requests=100 origin=100 errors=0 p99_ms=203.734367
cold_burst   rep=1 requests=100 origin=1 errors=0 p99_ms=203.020028
```

Timings vary. Assertions are zero client errors, 100 pass-through origin fetches, and one coalesced cold-burst origin fetch with joins observed. This smoke run recorded 100 compatible misses and 99 joins. Inspect the printed artifact directory for counters, CPU/RSS, commands, and raw samples. The larger reference run is described in [the benchmark guide](BENCHMARKS.md).

## Manual inspection

In one terminal:

```sh
cargo run --release --example demo_origin -- --delay-ms 200 --payload-bytes 4096
```

In another:

```sh
cargo run --release -- --upstream http://127.0.0.1:9000 --cache-path-prefix /public/
```

Then:

```sh
curl -s -D - -o /dev/null -H 'User-Agent:' http://127.0.0.1:8080/public/data
curl -s -D - -o /dev/null -H 'User-Agent:' http://127.0.0.1:8080/public/data
curl -s http://127.0.0.1:9000/stats
curl -s http://127.0.0.1:8080/metrics
```

The second public request reuses the cached response; the data-request counter stays at one. Default curl User-Agent would intentionally bypass this strict profile, so the command removes it. `/private/data` returns no-store and is never shared or cached. Ctrl-C stops each manual service.

For the race guarantee, run `cargo test --offline --test coalescing one_hundred_pending_compatible_requests_make_exactly_one_origin_fetch`. That test holds the origin until all 100 callers are pending, rather than depending on the demo's fixed delay.
