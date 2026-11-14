"""HTTP, PostgreSQL 16, Redis 7, and worker-discovery integration tests."""

import concurrent.futures
import os
import threading
import time
import unittest
import uuid

import redis
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict
from psycopg_pool import ConnectionPool

from workflow_engine.actions import ActionRegistry, NoopParameters, default_action_registry
from workflow_engine.api import create_app
from workflow_engine.postgres import PostgresStore, psycopg
from workflow_engine.worker import PostgresWorker


DSN = os.environ.get("WORKFLOW_TEST_POSTGRES_DSN")
REDIS_URL = os.environ.get("WORKFLOW_TEST_REDIS_URL")


class RecordParameters(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    group: str


class APIIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not DSN or not REDIS_URL:
            if os.environ.get("CI"):
                raise RuntimeError("CI requires PostgreSQL and Redis URLs; refusing skipped API integration tests")
            raise unittest.SkipTest("set WORKFLOW_TEST_POSTGRES_DSN and WORKFLOW_TEST_REDIS_URL")
        cls.store = PostgresStore(DSN)
        cls.redis = redis.Redis.from_url(REDIS_URL, socket_connect_timeout=1,
                                         socket_timeout=2)
        cls.redis.ping()

    def setUp(self):
        self.client = TestClient(create_app(store=self.store, redis_client=self.redis))
        self.workflow_ids = []

    def tearDown(self):
        self.client.close()
        if self.workflow_ids:
            with self.store._connect() as conn:
                with conn.transaction():
                    conn.execute("DELETE FROM workflows WHERE workflow_id=ANY(%s)",
                                 (self.workflow_ids,))

    def submission(self, action="noop.v1", *, task_id="only", dependencies=None,
                   params=None, retry=None):
        task = {"id": task_id, "dependencies": dependencies or [],
                "action": {"type": action, "parameters": params or {}}}
        if retry is not None:
            task["retry_policy"] = retry
        return {"tasks": [task]}

    def post(self, key, payload=None, client=None):
        client = client or self.client
        response = client.post("/api/v1/workflows",
                               headers={"Idempotency-Key": key},
                               json=payload or self.submission())
        if response.status_code in (200, 201):
            self.workflow_ids.append(response.json()["workflow_id"])
        return response

    def test_http_submission_read_queries_and_cursor_pagination(self):
        response = self.post("api-read-" + uuid.uuid4().hex,
                             {"tasks": [
                                 {"id": "a", "action": {"type": "noop.v1"}},
                                 {"id": "b", "dependencies": ["a"],
                                  "action": {"type": "text.require_nonempty.v1",
                                             "parameters": {"text": "ok"}},
                                  "retry_policy": {"max_attempts": 3,
                                                   "initial_delay": 1,
                                                   "multiplier": 2,
                                                   "max_delay": 8,
                                                   "jitter": 0}}
                             ]})
        self.assertEqual(response.status_code, 201, response.text)
        workflow_id = response.json()["workflow_id"]
        detail = self.client.get(f"/api/v1/workflows/{workflow_id}")
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["state"], "pending")
        self.assertEqual(detail.json()["task_counts"]["pending"], 2)

        page1 = self.client.get(f"/api/v1/workflows/{workflow_id}/tasks?limit=1")
        self.assertEqual([row["task_id"] for row in page1.json()["items"]], ["a"])
        page2 = self.client.get(f"/api/v1/workflows/{workflow_id}/tasks",
                                params={"limit": 1, "cursor": page1.json()["next_cursor"]})
        self.assertEqual(page2.json()["items"][0]["task_id"], "b")
        self.assertEqual(page2.json()["items"][0]["dependencies"], ["a"])
        self.assertEqual(page2.json()["items"][0]["max_attempts"], 3)
        self.assertIsNone(page2.json()["next_cursor"])

        # Give the collection cursor a deterministic second pending row. Other
        # integration cases may have completed their workflows earlier in the
        # same shared PostgreSQL test database.
        second = self.post("api-read-pagination-" + uuid.uuid4().hex,
                           {"tasks": [{"id": "only", "action": {"type": "noop.v1"}}]})
        self.assertEqual(second.status_code, 201, second.text)
        filtered = self.client.get("/api/v1/workflows", params={"state": "pending", "limit": 1})
        self.assertEqual(filtered.status_code, 200)
        self.assertTrue(filtered.json()["next_cursor"])
        self.assertEqual(filtered.json()["items"][0]["state"], "pending")
        next_page = self.client.get("/api/v1/workflows", params={
            "state": "pending", "limit": 1,
            "cursor": filtered.json()["next_cursor"]})
        self.assertEqual(next_page.status_code, 200)
        self.assertEqual(next_page.json()["items"][0]["state"], "pending")
        self.assertNotEqual(filtered.json()["items"][0]["workflow_id"],
                            next_page.json()["items"][0]["workflow_id"])
        self.assertEqual(self.client.get("/api/v1/workflows/not-a-workflow").status_code, 404)

    def test_simultaneous_identical_submissions_create_one_workflow(self):
        key = "concurrent-" + uuid.uuid4().hex
        payload = self.submission()
        barrier = threading.Barrier(2)
        app = create_app(store=self.store, redis_client=self.redis)

        def send():
            with TestClient(app) as client:
                barrier.wait(timeout=5)
                return client.post("/api/v1/workflows", headers={"Idempotency-Key": key},
                                   json=payload)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            responses = list(executor.map(lambda _n: send(), range(2)))
        self.assertEqual(sorted(response.status_code for response in responses), [200, 201])
        ids = {response.json()["workflow_id"] for response in responses}
        self.assertEqual(len(ids), 1)
        self.workflow_ids.extend(ids)
        with self.store._connect() as conn:
            count = conn.execute("SELECT count(*) FROM workflows WHERE idempotency_key=%s",
                                 (key,)).fetchone()[0]
        self.assertEqual(count, 1)

    def test_simultaneous_conflicting_submissions_are_atomic(self):
        key = "conflict-" + uuid.uuid4().hex
        payloads = [self.submission(task_id="one"), self.submission(task_id="two")]
        barrier = threading.Barrier(2)
        app = create_app(store=self.store, redis_client=self.redis)

        def send(payload):
            with TestClient(app) as client:
                barrier.wait(timeout=5)
                return client.post("/api/v1/workflows", headers={"Idempotency-Key": key},
                                   json=payload)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            responses = list(executor.map(send, payloads))
        self.assertEqual(sorted(response.status_code for response in responses), [201, 409])
        created = next(response for response in responses if response.status_code == 201)
        self.workflow_ids.append(created.json()["workflow_id"])
        self.assertNotIn("psycopg", next(r for r in responses if r.status_code == 409).text)

    def test_attempt_inspection_and_retry_dead_letter_errors_are_sanitized(self):
        response = self.post("attempts-" + uuid.uuid4().hex,
                             self.submission(retry={"max_attempts": 2,
                                                    "initial_delay": 0,
                                                    "multiplier": 2,
                                                    "max_delay": 0,
                                                    "jitter": 0}))
        workflow_id = response.json()["workflow_id"]
        self.store.start_workflow(workflow_id)
        first = self.store.claim_task(workflow_id, "secret-worker", 30)
        self.assertTrue(self.store.fail_task(workflow_id, "only", "secret-worker",
                                             first["fencing_token"],
                                             "RuntimeError: private-api-key"))
        with self.store._connect() as conn:
            conn.execute("UPDATE tasks SET next_eligible_at=clock_timestamp()-interval '1 second' "
                         "WHERE workflow_id=%s AND task_id='only'", (workflow_id,))
        second = self.store.claim_task(workflow_id, "another-secret-worker", 30)
        self.assertTrue(self.store.fail_task(workflow_id, "only", "another-secret-worker",
                                             second["fencing_token"],
                                             "RuntimeError: second-private-key"))
        task = self.client.get(f"/api/v1/workflows/{workflow_id}/tasks/only")
        attempts = self.client.get(f"/api/v1/workflows/{workflow_id}/tasks/only/attempts")
        self.assertEqual(task.json()["state"], "dead_lettered")
        self.assertEqual(task.json()["error"]["code"], "attempts_exhausted")
        self.assertEqual([item["outcome"] for item in attempts.json()["items"]],
                         ["retry_scheduled", "dead_lettered"])
        for response in (task, attempts):
            for forbidden in ("private-api-key", "second-private-key", "secret-worker",
                              "fencing_token", "worker_id"):
                self.assertNotIn(forbidden, response.text)

    def test_postgres_pool_app_health_metrics_and_openapi(self):
        app = create_app(dsn=DSN, redis_client=self.redis, host="127.0.0.1")
        with TestClient(app) as client:
            self.assertEqual(client.get("/health/live").status_code, 200)
            ready = client.get("/health/ready")
            self.assertEqual(ready.status_code, 200, ready.text)
            self.assertEqual(ready.json()["dependencies"]["postgres"], "ok")
            metrics = client.get("/metrics")
            self.assertEqual(metrics.status_code, 200)
            self.assertIn("workflow_database_snapshot_age_seconds", metrics.text)
            schema = client.get("/openapi.json")
            self.assertEqual(schema.status_code, 200)
            self.assertIn("/api/v1/workflows", schema.json()["paths"])

    def test_exhausted_postgres_pool_returns_sanitized_503_for_api_and_readiness(self):
        pool = ConnectionPool(
            conninfo=DSN, min_size=1, max_size=1, timeout=0.1, open=True,
            kwargs={"options": "-c statement_timeout=3000 -c lock_timeout=500"})
        app = create_app(dsn=DSN, redis_client=self.redis, host="127.0.0.1", pool=pool)
        try:
            with TestClient(app) as client:
                self.assertEqual(client.get("/health/ready").status_code, 200)
                with pool.connection():
                    requests = [
                        client.get("/api/v1/workflows"),
                        client.get("/api/v1/workflows/pool-test"),
                        client.get("/api/v1/workflows/pool-test/tasks"),
                        client.get("/api/v1/workflows/pool-test/tasks/task"),
                        client.get("/api/v1/workflows/pool-test/tasks/task/attempts"),
                        client.post("/api/v1/workflows",
                                    headers={"Idempotency-Key": "pool-exhausted"},
                                    json=self.submission()),
                        client.get("/health/ready"),
                    ]
                for response in requests[:-1]:
                    self.assertEqual(response.status_code, 503, response.text)
                    self.assertNotIn("psycopg_pool", response.text)
                    self.assertNotIn(DSN, response.text)
                self.assertEqual(requests[-1].status_code, 503)
                self.assertEqual(requests[-1].json()["dependencies"]["postgres"],
                                 "unavailable")
        finally:
            pool.close()

    def test_worker_discovery_recovers_without_redis(self):
        response = self.post("discover-" + uuid.uuid4().hex,
                             self.submission(action="text.require_nonempty.v1",
                                             params={"text": "discovered"}))
        workflow_id = response.json()["workflow_id"]
        stop = threading.Event()
        worker = PostgresWorker(self.store, poll_interval=0.01,
                                fallback_scan_interval=0.05)
        thread = threading.Thread(target=worker.run_forever,
                                  kwargs={"registry": default_action_registry(),
                                          "stop_event": stop})
        thread.start()
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.store.workflow_state(workflow_id) == "succeeded":
                break
            time.sleep(0.02)
        stop.set()
        thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.store.workflow_state(workflow_id), "succeeded")

    def test_discovery_worker_rotates_one_task_turn_per_workflow(self):
        old_id, new_id = "000-fairness-long-" + uuid.uuid4().hex, "zzz-fairness-new-" + uuid.uuid4().hex
        names = [f"task-{index}" for index in range(8)]
        old_tasks = [{"id": name, "dependencies": [],
                      "action": {"type": "record.v1", "parameters": {"group": "long"}}}
                     for name in names]
        new_tasks = [{"id": "new-task", "dependencies": [],
                      "action": {"type": "record.v1", "parameters": {"group": "new"}}}]
        for workflow_id, tasks in ((old_id, old_tasks), (new_id, new_tasks)):
            self.store.initialize(workflow_id,
                                  {"api_definition_version": 1, "tasks": tasks},
                                  [task["id"] for task in tasks])
            self.workflow_ids.append(workflow_id)
        events, stop = [], threading.Event()
        lock = threading.Lock()

        def record(parameters):
            with lock:
                events.append(parameters.group)
                if parameters.group == "new":
                    stop.set()

        registry = ActionRegistry()
        registry.register("record.v1", RecordParameters, record)
        worker = PostgresWorker(self.store, poll_interval=0.01,
                                fallback_scan_interval=0.05)
        thread = threading.Thread(target=worker.run_forever,
                                  kwargs={"registry": registry, "stop_event": stop})
        thread.start()
        thread.join(5)
        self.assertFalse(thread.is_alive(), "worker did not stop after newer workflow ran")
        self.assertEqual(events, ["long", "new"])
        self.assertEqual(self.store.workflow_state(new_id), "succeeded")
        self.assertLess(len([event for event in events if event == "long"]), len(names))

    def test_idle_full_discovery_batches_back_off_and_skip_action_resolution(self):
        workflow_ids = [f"idle-batch-{uuid.uuid4().hex}-{index:02d}" for index in range(32)]
        definition = {"api_definition_version": 1, "tasks": [{
            "id": "only", "dependencies": [],
            "action": {"type": "noop.v1", "parameters": {}}}]}
        for workflow_id in workflow_ids:
            self.store.initialize(workflow_id, definition, ["only"])
            self.store.start_workflow(workflow_id)
            with self.store._connect() as conn:
                conn.execute("UPDATE tasks SET next_eligible_at=clock_timestamp()+interval '1 hour' "
                             "WHERE workflow_id=%s AND task_id='only'", (workflow_id,))
            self.workflow_ids.append(workflow_id)

        class CountingRegistry:
            def __init__(self):
                self.delegate = default_action_registry()
                self.resolutions = 0

            def resolve(self, *args, **kwargs):
                self.resolutions += 1
                return self.delegate.resolve(*args, **kwargs)

        registry, waits = CountingRegistry(), []

        class StopAfterIdleCycle(PostgresWorker):
            def _wait_for_discovery(self, wake, delay):
                waits.append(delay)
                stop.set()

        stop = threading.Event()
        worker = StopAfterIdleCycle(self.store, poll_interval=0.01,
                                    fallback_scan_interval=0.2)
        worker.run_forever(registry=registry, stop_event=stop, discovery_batch_size=32)
        self.assertEqual(registry.resolutions, 0)
        self.assertEqual(len(waits), 1)
        self.assertGreaterEqual(waits[0], 0.01)
        self.assertLessEqual(waits[0], 0.2)

    def test_multiple_discovery_workers_claim_independent_tasks(self):
        workflow_id = "multi-discovery-" + uuid.uuid4().hex
        tasks = [{"id": name, "dependencies": [],
                  "action": {"type": "record.v1", "parameters": {"group": name}}}
                 for name in ("one", "two")]
        self.store.initialize(workflow_id,
                              {"api_definition_version": 1, "tasks": tasks},
                              [task["id"] for task in tasks])
        self.workflow_ids.append(workflow_id)
        barrier, stop = threading.Barrier(2), threading.Event()
        seen, lock = [], threading.Lock()

        def record(parameters):
            with lock:
                seen.append(parameters.group)
                if len(seen) == 2:
                    stop.set()
            barrier.wait(timeout=5)

        registry = ActionRegistry()
        registry.register("record.v1", RecordParameters, record)
        workers = [PostgresWorker(self.store, poll_interval=0.01,
                                  fallback_scan_interval=0.05,
                                  redis_client=self.redis) for _ in range(2)]
        threads = [threading.Thread(target=worker.run_forever,
                                    kwargs={"registry": registry, "stop_event": stop})
                   for worker in workers]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(8)
            self.assertFalse(thread.is_alive())
        self.assertEqual(sorted(seen), ["one", "two"])
        self.assertEqual(self.store.workflow_state(workflow_id), "succeeded")

    def test_redis_outage_does_not_block_discovery_worker(self):
        response = self.post("redis-down-" + uuid.uuid4().hex)
        workflow_id = response.json()["workflow_id"]
        probe = __import__("socket").socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        unavailable = redis.Redis(host="127.0.0.1", port=port,
                                  socket_connect_timeout=0.1, socket_timeout=0.1)
        stop = threading.Event()
        worker = PostgresWorker(self.store, poll_interval=0.01,
                                fallback_scan_interval=0.05,
                                redis_client=unavailable)
        thread = threading.Thread(target=worker.run_forever,
                                  kwargs={"registry": default_action_registry(),
                                          "stop_event": stop})
        thread.start()
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.store.workflow_state(workflow_id) == "succeeded":
                break
            time.sleep(0.02)
        stop.set()
        thread.join(3)
        unavailable.close()
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.store.workflow_state(workflow_id), "succeeded")


if __name__ == "__main__":
    unittest.main()
