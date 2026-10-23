import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("bench", Path(__file__).resolve().parents[1] / "bench.py")
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class BenchmarkTests(unittest.TestCase):
    def test_nearest_rank_percentiles_do_not_interpolate_or_drop_tail(self):
        values = list(range(1, 101))
        self.assertEqual(bench.percentile(values, 0.50), 50)
        self.assertEqual(bench.percentile(values, 0.95), 95)
        self.assertEqual(bench.percentile(values, 0.99), 99)
        self.assertIsNone(bench.percentile([], 0.99))

    def test_failures_are_counted_and_excluded_from_success_latency(self):
        raw = {"elapsed_seconds": 2, "samples": [
            {"latency_ns": 1_000_000, "outcome": "success"},
            {"latency_ns": 2_000_000, "outcome": "success"},
            {"latency_ns": 90_000_000, "outcome": "timeout"},
        ]}
        result = bench.summarize(raw)
        self.assertEqual(result["errors"], 1)
        self.assertEqual(result["p99_ms"], 2)
        self.assertEqual(result["throughput_rps"], 1.5)

    def test_prometheus_samples_keep_labels_and_ignore_type_comments(self):
        result = bench.scalar_metrics('# TYPE latency histogram\nrequests 3\nlatency_bucket{le="+Inf"} 2\n')
        self.assertEqual(result["requests"], 3)
        self.assertEqual(result['latency_bucket{le="+Inf"}'], 2)

    def test_empty_or_zero_duration_results_are_rejected(self):
        for raw in [{"elapsed_seconds": 0, "samples": [{}]}, {"elapsed_seconds": 1, "samples": []}]:
            with self.assertRaises(ValueError):
                bench.summarize(raw)

    def test_cpu_and_memory_fields_survive_spaces_and_parentheses_in_process_name(self):
        fields = ["0"] * 20
        fields[0], fields[11], fields[12] = "S", "20", "10"
        stat = "42 (worker ) name) " + " ".join(fields)
        result = bench.parse_proc_stats("VmRSS:\t100 kB\nVmHWM:\t200 kB\n", stat, 100)
        self.assertEqual(result, {"cpu_seconds": 0.3, "rss_kib": 100, "lifetime_peak_rss_kib": 200})


if __name__ == "__main__":
    unittest.main()
