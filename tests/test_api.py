"""Fast unit tests for the API contract, validation, and redaction."""

import json
import unittest
from datetime import datetime, timezone

from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict

from workflow_engine.api import _encode_cursor, create_app
from workflow_engine.observability import JsonLogFormatter
from workflow_engine.postgres import WorkflowSubmissionConflict


NOW = datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc)


class MemoryStore:
    def __init__(self):
        self.submissions = {}
        self.definitions = {}
        self.healthy = True
        self.metrics_calls = 0

    def migrate(self):
        return None

    def submit_workflow(self, key, definition):
        if key in self.submissions:
            workflow_id, previous = self.submissions[key]
            if previous != definition:
                raise WorkflowSubmissionConflict("sensitive internal detail")
            return {"workflow_id": workflow_id, "created_at": NOW, "created": False}
        workflow_id = f"wf-{len(self.submissions) + 1}"
        self.submissions[key] = (workflow_id, definition)
        self.definitions[workflow_id] = definition
        return {"workflow_id": workflow_id, "created_at": NOW, "created": True}

    def workflow_state(self, workflow_id):
        return "running" if workflow_id == "wf-detail" else None

    def workflow_detail(self, workflow_id):
        if workflow_id != "wf-detail":
            return None
        return {"workflow_id": workflow_id, "state": "failed", "error": "Secret: db-password",
                "created_at": NOW, "updated_at": NOW,
                "task_counts": {"failed": 1, "dead_lettered": 1}}

    def list_workflows(self, **_kwargs):
        return []

    def list_tasks(self, *_args, **_kwargs):
        return [{"task_id": "secret-task", "state": "running", "error": None,
                 "attempt_count": 1, "max_attempts": 3, "next_eligible_at": NOW,
                 "dead_lettered_at": None, "created_at": NOW, "updated_at": NOW,
                 "lease_expires_at": NOW, "dependencies": []}]

    def task_detail(self, workflow_id, task_id):
        if workflow_id != "wf-detail" or task_id != "secret-task":
            return None
        return {"task_id": task_id, "state": "dead_lettered",
                "error": "RuntimeError: private-token-value", "attempt_count": 2,
                "max_attempts": 2, "next_eligible_at": NOW, "dead_lettered_at": NOW,
                "created_at": NOW, "updated_at": NOW,
                "lease_expires_at": None, "dependencies": []}

    def list_attempts(self, *_args, **_kwargs):
        return [{"attempt_number": 1, "claimed_at": NOW, "lease_expires_at": NOW,
                 "finished_at": NOW, "outcome": "dead_lettered",
                 "error": "RuntimeError: private-token-value"}]

    def health_check(self):
        if not self.healthy:
            from workflow_engine.postgres import psycopg
            raise psycopg.OperationalError("postgres://user:password@internal")

    def metrics_snapshot(self):
        self.metrics_calls += 1
        return {"workflows": {"pending": 1}, "attempts": {"succeeded": 2},
                "dead_lettered_tasks": 1, "active_leases": 1}


class APIUnitTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.app = create_app(store=self.store)
        self.client = TestClient(self.app)

    def tearDown(self):
        self.client.close()

    def submit(self, *, key="request-1", tasks=None):
        return self.client.post("/api/v1/workflows", headers={"Idempotency-Key": key},
                                json={"tasks": tasks or [{
                                    "id": "check", "dependencies": [],
                                    "action": {"type": "text.require_nonempty.v1",
                                               "parameters": {"text": "ready"}},
                                    "retry_policy": {"max_attempts": 3,
                                                     "initial_delay": 1,
                                                     "multiplier": 2,
                                                     "max_delay": 5,
                                                     "jitter": 0}}]})

    def test_valid_submission_and_idempotent_replay_return_stable_id(self):
        first = self.submit()
        replay = self.submit()
        self.assertEqual(first.status_code, 201)
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(first.json()["workflow_id"], replay.json()["workflow_id"])
        self.assertEqual(replay.headers["Idempotency-Replayed"], "true")
        task = self.store.definitions[first.json()["workflow_id"]]["tasks"][0]
        self.assertEqual(task["retry_policy"]["max_attempts"], 3)
        self.assertEqual(task["action"]["type"], "text.require_nonempty.v1")

    def test_same_idempotency_key_with_different_definition_conflicts(self):
        self.assertEqual(self.submit().status_code, 201)
        response = self.submit(tasks=[{"id": "other", "action": {
            "type": "noop.v1", "parameters": {}}}])
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "idempotency_key_conflict")
        self.assertNotIn("sensitive internal detail", response.text)

    def test_dag_retry_and_action_types_are_validated(self):
        cycle = self.submit(tasks=[
            {"id": "a", "dependencies": ["b"], "action": {"type": "noop.v1"}},
            {"id": "b", "dependencies": ["a"], "action": {"type": "noop.v1"}},
        ])
        self.assertEqual(cycle.status_code, 422)
        self.assertEqual(cycle.json()["error"]["code"], "invalid_workflow")

        retry = self.submit(key="retry", tasks=[{"id": "a", "action": {
            "type": "noop.v1"}, "retry_policy": {"max_attempts": 0}}])
        self.assertEqual(retry.status_code, 422)

        unknown = self.submit(key="unknown", tasks=[{"id": "a", "action": {
            "type": "python.eval.v9", "parameters": {"code": "print(1)"}}}])
        self.assertEqual(unknown.status_code, 422)
        self.assertEqual(unknown.json()["error"]["code"], "unknown_action_type")
        self.assertEqual(len(self.store.submissions), 0)

        invalid_parameters = self.submit(key="bad-parameters", tasks=[{
            "id": "a", "action": {"type": "text.require_nonempty.v1",
                                     "parameters": {"text": 12}}}])
        self.assertEqual(invalid_parameters.status_code, 422)
        self.assertEqual(invalid_parameters.json()["error"]["code"],
                         "invalid_action_parameters")
        missing_key = self.client.post("/api/v1/workflows", json={"tasks": []})
        self.assertEqual(missing_key.status_code, 422)

    def test_request_body_limit_and_redis_degraded_readiness(self):
        oversized = self.client.post("/api/v1/workflows",
                                     content=b" " * (256 * 1024 + 1),
                                     headers={"Idempotency-Key": "large"})
        self.assertEqual(oversized.status_code, 413)
        self.assertEqual(oversized.json()["error"]["code"], "request_too_large")

        class DownRedis:
            def ping(self):
                raise ConnectionError("secret redis endpoint")

        self.client.close()
        self.app = create_app(store=self.store, redis_client=DownRedis())
        self.client = TestClient(self.app)
        response = self.client.get("/health/ready")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "degraded")
        self.assertEqual(response.json()["dependencies"]["redis"], "degraded")
        self.assertNotIn("secret redis endpoint", response.text)

    def test_malformed_cursor_shapes_types_and_values_return_422(self):
        cases = [
            ("/api/v1/workflows", {"wrong": "shape"}),
            ("/api/v1/workflows", ["not-a-date", "wf-detail"]),
            ("/api/v1/workflows", ["2026-01-01T00:00:00", "wf-detail"]),
            ("/api/v1/workflows", ["2026-01-01T00:00:00+00:00", ["bad-id"]]),
            ("/api/v1/workflows/wf-detail/tasks", {"task_id": "x"}),
            ("/api/v1/workflows/wf-detail/tasks/secret-task/attempts", True),
            ("/api/v1/workflows/wf-detail/tasks/secret-task/attempts", 0),
        ]
        for path, value in cases:
            with self.subTest(path=path, value=value):
                response = self.client.get(path, params={"cursor": _encode_cursor(value)})
                self.assertEqual(response.status_code, 422, response.text)
                self.assertEqual(response.json()["error"]["code"], "invalid_cursor")

    def test_reads_redact_raw_errors_and_internal_lease_fields(self):
        workflow = self.client.get("/api/v1/workflows/wf-detail")
        self.assertEqual(workflow.status_code, 200)
        self.assertNotIn("db-password", workflow.text)
        self.assertEqual(workflow.json()["task_counts"]["dead_lettered"], 1)

        task = self.client.get("/api/v1/workflows/wf-detail/tasks/secret-task")
        self.assertEqual(task.status_code, 200)
        self.assertNotIn("private-token-value", task.text)
        self.assertNotIn("worker_id", task.text)
        self.assertNotIn("fencing_token", task.text)

        attempts = self.client.get(
            "/api/v1/workflows/wf-detail/tasks/secret-task/attempts")
        self.assertEqual(attempts.status_code, 200)
        self.assertNotIn("private-token-value", attempts.text)

    def test_attempt_cursor_is_bounded_and_ordered(self):
        class TwoAttemptsStore(MemoryStore):
            def list_attempts(self, *_args, after_attempt_number=None, **_kwargs):
                rows = [
                    {"attempt_number": number, "claimed_at": NOW,
                     "lease_expires_at": NOW, "finished_at": NOW,
                     "outcome": "retry_scheduled", "error": "secret"}
                    for number in (1, 2)
                ]
                if after_attempt_number:
                    rows = [row for row in rows if row["attempt_number"] > after_attempt_number]
                return rows

        self.client.close()
        self.store = TwoAttemptsStore()
        self.app = create_app(store=self.store)
        self.client = TestClient(self.app)
        first = self.client.get(
            "/api/v1/workflows/wf-detail/tasks/secret-task/attempts?limit=1")
        self.assertEqual(first.json()["items"][0]["attempt_number"], 1)
        second = self.client.get(
            "/api/v1/workflows/wf-detail/tasks/secret-task/attempts",
            params={"limit": 1, "cursor": first.json()["next_cursor"]})
        self.assertEqual(second.json()["items"][0]["attempt_number"], 2)
        self.assertIsNone(second.json()["next_cursor"])

    def test_health_distinguishes_postgres_failure_from_optional_redis(self):
        response = self.client.get("/health/ready")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["dependencies"]["redis"], "disabled")
        self.store.healthy = False
        unavailable = self.client.get("/health/ready")
        self.assertEqual(unavailable.status_code, 503)
        self.assertEqual(unavailable.json()["dependencies"]["postgres"], "unavailable")

    def test_metrics_exposition_and_json_log_redaction(self):
        self.client.get("/health/live")
        response = self.client.get("/metrics")
        self.assertEqual(response.status_code, 200)
        self.assertIn("workflow_api_requests", response.text)
        self.assertIn("workflow_active_task_leases", response.text)
        self.assertNotIn("wf-detail", response.text)

        import logging
        record = logging.LogRecord("test", logging.WARNING, __file__, 1,
                                   "private-token-value", (), None)
        record.event = "test_event"
        record.error_message = "db-password"
        encoded = JsonLogFormatter().format(record)
        parsed = json.loads(encoded)
        self.assertEqual(parsed["event"], "test_event")
        self.assertNotIn("private-token-value", encoded)
        self.assertNotIn("db-password", encoded)

    def test_remote_bind_requires_token_and_auth_failure_is_sanitized(self):
        with self.assertRaises(ValueError):
            create_app(store=self.store, host="0.0.0.0")
        app = create_app(store=self.store, host="0.0.0.0", api_token="secret-token")
        with TestClient(app) as client:
            response = client.get("/metrics")
        self.assertEqual(response.status_code, 401)
        self.assertNotIn("secret-token", response.text)

    def test_dashboard_summary_is_cached_database_snapshot_with_freshness(self):
        response = self.client.get("/api/v1/summary")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["workflows"]["pending"], 1)
        self.assertEqual(body["attempts"]["succeeded"], 2)
        self.assertEqual(body["active_leases"], 1)
        self.assertIn("snapshot_as_of", body)
        self.assertFalse(body["snapshot_stale"])
        self.assertNotIn("workflow_id", body)
        self.assertNotIn("task_id", body)
        self.assertEqual(self.client.get("/api/v1/summary").status_code, 200)
        self.assertEqual(self.store.metrics_calls, 1)


if __name__ == "__main__":
    unittest.main()
