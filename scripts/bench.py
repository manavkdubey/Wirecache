#!/usr/bin/env python3
import argparse
import collections
import csv
import datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import selectors
import signal
import socket
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
BIN = Path("target/release")
MODES = ("direct", "pass_through", "warm_cache", "cold_burst")


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def summarize(raw):
    samples = raw["samples"]
    elapsed = raw["elapsed_seconds"]
    if not samples or not math.isfinite(elapsed) or elapsed <= 0:
        raise ValueError("invalid load-runner result")
    outcomes = collections.Counter(sample["outcome"] for sample in samples)
    latency = [sample["latency_ns"] / 1_000_000 for sample in samples if sample["outcome"] == "success"]
    return {
        "requests": len(samples), "successful": outcomes["success"],
        "errors": len(samples) - outcomes["success"], "outcomes": dict(outcomes),
        "elapsed_seconds": elapsed, "throughput_rps": len(samples) / elapsed,
        "p50_ms": percentile(latency, 0.50), "p95_ms": percentile(latency, 0.95),
        "p99_ms": percentile(latency, 0.99),
    }


def scalar_metrics(text):
    result = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            name, value = line.rsplit(" ", 1)
            result[name] = float(value)
    return result


def get(port, path):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        body = response.read(256 * 1024)
        if response.status != 200:
            raise RuntimeError(f"control endpoint {path} returned {response.status}")
        return body.decode()
    finally:
        connection.close()


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def proc_stats(pid):
    status = Path(f"/proc/{pid}/status").read_text()
    stat = Path(f"/proc/{pid}/stat").read_text()
    return parse_proc_stats(status, stat, os.sysconf("SC_CLK_TCK"))


def parse_proc_stats(status, stat, ticks):
    fields = stat.rsplit(")", 1)[1].split()
    memory = {}
    for line in status.splitlines():
        if line.startswith(("VmRSS:", "VmHWM:")):
            key, value, _unit = line.split()
            memory[key.rstrip(":")] = int(value)
    return {
        "cpu_seconds": (int(fields[11]) + int(fields[12])) / ticks,
        "rss_kib": memory.get("VmRSS", 0), "lifetime_peak_rss_kib": memory.get("VmHWM", 0),
    }


class Sampler:
    def __init__(self, processes):
        self.processes = processes
        self.start = {name: proc_stats(process.pid) for name, process in processes.items()}
        self.peaks = {name: values["rss_kib"] for name, values in self.start.items()}
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.sample, daemon=True)

    def sample(self):
        while not self.stopped.wait(0.01):
            for name, process in self.processes.items():
                try:
                    self.peaks[name] = max(self.peaks[name], proc_stats(process.pid)["rss_kib"])
                except (FileNotFoundError, ProcessLookupError):
                    pass

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.stopped.set()
        self.thread.join(timeout=1)

    def result(self):
        result = {}
        for name, process in self.processes.items():
            after = proc_stats(process.pid)
            result[name] = {
                "cpu_seconds": after["cpu_seconds"] - self.start[name]["cpu_seconds"],
                "sampled_peak_rss_kib": max(self.peaks[name], after["rss_kib"]),
                "lifetime_peak_rss_kib": after["lifetime_peak_rss_kib"],
            }
        return result


class Service:
    def __init__(self, command, port, log, environment):
        self.command, self.port = command, port
        self.log = open(log, "w")
        self.process = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=self.log, stderr=subprocess.STDOUT)

    def __enter__(self):
        deadline = time.monotonic() + 10
        try:
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError(f"service exited: {self.command}")
                try:
                    get(self.port, "/healthz")
                    return self
                except (OSError, http.client.HTTPException):
                    time.sleep(0.02)
            raise TimeoutError(f"service readiness timed out: {self.command}")
        except BaseException:
            self.__exit__()
            raise

    def __exit__(self, *_args):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.log.close()


def wait_ready(process):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        if not selector.select(timeout=30) or process.stdout.readline().strip() != "READY":
            raise RuntimeError("load-runner warmup failed or timed out; inspect its stderr log")


def measured_load(command, environment, stderr_path, before, processes):
    with open(stderr_path, "w") as error_log:
        process = subprocess.Popen(command, cwd=ROOT, env=environment, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=error_log, text=True)
        try:
            wait_ready(process)
            initial = before()
            with Sampler(processes) as sampler:
                process.stdin.write("start\n")
                process.stdin.flush()
                output, _ = process.communicate(timeout=180)
            resources = sampler.result()
            if process.returncode != 0:
                raise RuntimeError("load runner failed; inspect its stderr log")
            return json.loads(output), initial, resources
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


def case(mode, repetition, args, output, environment):
    stem = f"{mode}-{repetition}"
    origin_port = free_port()
    origin_command = [str(BIN / "examples/demo_origin"), "--listen", f"127.0.0.1:{origin_port}",
                      "--delay-ms", str(args.delay_ms), "--payload-bytes", str(args.payload_bytes), "--ttl-secs", "60"]
    proxy_port = free_port()
    while proxy_port == origin_port:
        proxy_port = free_port()
    concurrency = args.burst if mode == "cold_burst" or args.demo else args.concurrency
    requests = args.burst if mode == "cold_burst" or args.demo else args.requests
    target_port = origin_port if mode == "direct" else proxy_port
    path = f"/public/data?mode={mode}&repetition={repetition}"
    url = f"http://127.0.0.1:{target_port}{path}"
    warmup_url = url if mode == "warm_cache" else f"http://127.0.0.1:{target_port}/public/data?warmup={stem}"
    load_command = [str(BIN / "examples/load"), "--url", url, "--warmup-url", warmup_url,
                    "--requests", str(requests), "--concurrency", str(concurrency),
                    "--warmup-requests", str(max(32, concurrency)), "--expect-bytes", str(args.payload_bytes), "--controlled"]
    proxy_command = [str(BIN / "wirecache"), "--listen", f"127.0.0.1:{proxy_port}", "--upstream", f"http://127.0.0.1:{origin_port}",
                     "--max-connections", str(max(128, concurrency + 16)), "--max-upstream-requests", str(max(64, concurrency)),
                     "--max-waiters-per-key", str(max(128, concurrency))]
    if mode in ("warm_cache", "cold_burst"):
        proxy_command.extend(["--cache-path-prefix", "/public/"])

    with Service(origin_command, origin_port, output / f"{stem}.origin.log", environment) as origin:
        def snapshot():
            return {"origin": json.loads(get(origin_port, "/stats"))["requests"],
                    "proxy": scalar_metrics(get(proxy_port, "/metrics")) if mode != "direct" else {}}

        def measure(processes):
            raw, initial, resources = measured_load(load_command, environment, output / f"{stem}.load.log", snapshot, processes)
            final = snapshot()
            summary = summarize(raw)
            origin_requests = final["origin"] - initial["origin"]
            misses = final["proxy"].get("wirecache_cache_misses_total", 0) - initial["proxy"].get("wirecache_cache_misses_total", 0)
            joins = final["proxy"].get("wirecache_singleflight_join_total", 0) - initial["proxy"].get("wirecache_singleflight_join_total", 0)
            hits = final["proxy"].get("wirecache_cache_hits_total", 0) - initial["proxy"].get("wirecache_cache_hits_total", 0)
            expected_origin = 0 if mode == "warm_cache" else 1 if mode == "cold_burst" else requests
            checks = {"zero_errors": summary["errors"] == 0, "expected_origin_requests": origin_requests == expected_origin}
            if mode == "cold_burst":
                checks["coalescing_observed"] = joins > 0
            if mode == "warm_cache":
                checks["all_requests_hit_cache"] = hits == requests
            result = {"mode": mode, "repetition": repetition, "concurrency": concurrency, **summary,
                      "origin_requests": origin_requests, "compatible_misses": int(misses), "flight_joins": int(joins), "cache_hits": int(hits),
                      "coalescing_factor": misses / origin_requests if origin_requests else None,
                      "resources": resources, "checks": checks,
                      "commands": {"origin": origin_command, "load": load_command, "proxy": proxy_command if mode != "direct" else None},
                      "metrics_before": initial["proxy"], "metrics_after": final["proxy"]}
            (output / f"{stem}.samples.json").write_text(json.dumps(raw, indent=2) + "\n")
            return result

        if mode == "direct":
            return measure({"origin": origin.process})
        with Service(proxy_command, proxy_port, output / f"{stem}.proxy.log", environment) as proxy:
            return measure({"origin": origin.process, "proxy": proxy.process})


def command_output(*command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def optional_text(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def metadata(args):
    cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), platform.machine())
    sources = sorted([ROOT / "Cargo.toml", ROOT / "Cargo.lock", *ROOT.glob("src/*.rs"), *ROOT.glob("examples/*.rs"), ROOT / "scripts/bench.py"])
    return {
        "measured_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "git_revision": command_output("git", "rev-parse", "HEAD"),
        "working_tree_dirty": bool(command_output("git", "status", "--porcelain")),
        "source_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources},
        "os": platform.platform(), "cpu_model": cpu, "logical_cpus": os.cpu_count(),
        "affinity_cpus": len(os.sched_getaffinity(0)), "cpu_quota": optional_text("/sys/fs/cgroup/cpu.max"),
        "memory_limit_bytes": optional_text("/sys/fs/cgroup/memory.max"),
        "rustc": command_output("rustc", "--version"), "cargo": command_output("cargo", "--version"),
        "python": platform.python_version(), "tokio_worker_threads": args.worker_threads,
        "payload_bytes": args.payload_bytes, "origin_delay_ms": args.delay_ms, "origin_ttl_seconds": 60,
        "requests": args.requests, "concurrency": args.concurrency, "cold_burst_requests": args.burst,
        "repetitions": args.repetitions, "sample_interval_seconds": 0.01,
        "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
        "load_generator": "examples/load.rs; closed-loop HTTP/1.1 pooled client; synchronized initial wave",
        "percentile_method": "nearest-rank over successful requests; errors reported separately",
        "resource_interval": "after warmup READY through load-runner exit; HWM includes startup and warmup",
        "environment_note": "local processes in a shared environment; not an isolated hardware performance claim",
    }


def write_report(output, report):
    (output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    fields = ["mode", "repetition", "requests", "concurrency", "errors", "elapsed_seconds", "throughput_rps", "p50_ms", "p95_ms", "p99_ms", "origin_requests", "compatible_misses", "flight_joins", "coalescing_factor"]
    with (output / "summary.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for result in report["results"]:
            writer.writerow({key: result[key] for key in fields})


def main():
    parser = argparse.ArgumentParser(description="Reproducible local WireCache benchmark (Linux /proc required)")
    parser.add_argument("--requests", type=int, default=600)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--burst", type=int, default=100)
    parser.add_argument("--delay-ms", type=int, default=200)
    parser.add_argument("--payload-bytes", type=int, default=4096)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--worker-threads", type=int, default=2)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--demo", action="store_true")
    args = parser.parse_args()
    if not Path("/proc/self/status").exists():
        parser.error("this resource-measurement harness requires Linux /proc")
    if not (1 <= args.requests <= 1_000_000 and 1 <= args.concurrency <= 256 and 2 <= args.burst <= 256
            and 0 <= args.delay_ms <= 1000 and 1 <= args.payload_bytes <= 1024 * 1024
            and 1 <= args.worker_threads <= 64 and 1 <= args.repetitions <= 20):
        parser.error("invalid workload limits; see --help")
    if args.demo:
        args.repetitions = 1
    elif args.repetitions < 3:
        parser.error("benchmarks require at least three repetitions; use --demo for a smoke run")
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    output = (args.output or ROOT / "bench/results" / stamp).resolve()
    output.mkdir(parents=True, exist_ok=False)
    environment = dict(os.environ, TOKIO_WORKER_THREADS=str(args.worker_threads), RUST_LOG="warn", NO_COLOR="1")
    report = {"metadata": metadata(args), "results": []}
    for repetition in range(1, args.repetitions + 1):
        for mode in (("pass_through", "cold_burst") if args.demo else MODES):
            result = case(mode, repetition, args, output, environment)
            report["results"].append(result)
            write_report(output, report)
            print(f"{mode:12} rep={repetition} requests={result['requests']} origin={result['origin_requests']} errors={result['errors']} p99_ms={result['p99_ms']}", flush=True)
    passed = all(all(result["checks"].values()) for result in report["results"])
    report["passed"] = passed
    write_report(output, report)
    print(f"Artifacts: {output}", flush=True)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
