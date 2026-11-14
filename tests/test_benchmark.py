"""Fast validation for the reproducible benchmark harness."""

import unittest
import contextlib
import io
import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from benchmarks.run import percentile, run, shape, worker_command


class BenchmarkHarnessTests(unittest.TestCase):
    def test_workload_shapes_have_valid_unique_dependencies(self):
        for kind in ("linear", "fan", "mixed"):
            with self.subTest(kind=kind):
                tasks = shape(kind, "wf", 8, .01)
                identifiers = {task["id"] for task in tasks}
                self.assertEqual(len(identifiers), len(tasks))
                self.assertTrue(all(set(task.get("dependencies", [])) <= identifiers
                                    for task in tasks))

    def test_small_samples_do_not_claim_tail_percentiles(self):
        values = list(range(19))
        self.assertIsNone(percentile(values, .95, 20))
        self.assertIsNone(percentile(values, .99, 100))
        self.assertEqual(percentile(list(range(20)), .95, 20), 18)

    def test_benchmark_workers_do_not_bind_fixed_metrics_ports(self):
        command = worker_command()
        self.assertIn("--no-metrics", command)
        self.assertNotIn("--metrics-port", command)

    def test_matrix_sample_is_classified_for_percentile_eligibility(self):
        self.assertIsNone(percentile(list(range(20)), .99, 100))
        self.assertIsNotNone(percentile(list(range(100)), .99, 100))

    def test_attempt_history_is_collected_only_after_terminal_observation(self):
        calls = []
        now = datetime.now(timezone.utc).isoformat()
        def fake_request(url, _token, method="GET", body=None, idempotency=None, timeout=5):
            calls.append((method, url))
            if method == "POST":
                return {"workflow_id": "warm-id" if idempotency.startswith("warm-") else "measured-id",
                        "created_at": now}
            if "/tasks?" in url:
                return {"items": [{"task_id": "only"}], "next_cursor": None}
            if "/attempts?" in url:
                return {"items": [{"attempt_number": 1, "claimed_at": now,
                                   "finished_at": now, "outcome": "succeeded"}],
                        "next_cursor": None}
            return {"state": "succeeded"}

        args = SimpleNamespace(api="http://localhost", workers=1, redis=False,
                               smoke=False, sample_size=1, repetitions=1,
                               workload="linear", task_count=2, delay_seconds=0,
                               deadline=5)
        class Child:
            def terminate(self): pass
            def wait(self, timeout=None): pass
        stdout = io.StringIO()
        with patch.dict(os.environ, {"WORKFLOW_API_TOKEN": "test", "WORKFLOW_DATABASE_URL": "unused"}), \
             patch("benchmarks.run.subprocess.Popen", return_value=Child()), \
             patch("benchmarks.run.time.sleep"), \
             patch("benchmarks.run.wait_terminal", return_value=True), \
             patch("benchmarks.run.request", side_effect=fake_request), \
             patch("benchmarks.run.metadata", return_value={}), \
             contextlib.redirect_stdout(stdout):
            run(args)

        measured_terminal_index = calls.index(("GET", "http://localhost/api/v1/workflows/measured-id"))
        attempts_index = next(i for i, (_, url) in enumerate(calls) if "/attempts?" in url)
        self.assertGreater(attempts_index, measured_terminal_index)
        output = json.loads(stdout.getvalue())
        self.assertIn("submit_to_terminal_client_observed_seconds", output["results"][0])
        self.assertIn("submit_to_first_claim_durable_timestamp_seconds", output["results"][0])


if __name__ == "__main__":
    unittest.main()
