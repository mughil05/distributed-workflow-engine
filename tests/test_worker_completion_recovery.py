"""Completion retries must never rerun an already completed task action."""

import unittest

from workflow_engine.observability import EngineMetrics
from workflow_engine.postgres import psycopg
from workflow_engine.actions import default_action_registry
from workflow_engine.worker import PostgresWorker


class CompletionStore:
    def __init__(self, *, acknowledgement_lost=False):
        self.acknowledgement_lost = acknowledgement_lost
        self.completions = 0
        self.renewals = 0

    def complete_task(self, *_args):
        self.completions += 1
        if self.completions == 1:
            raise psycopg.OperationalError("connection dropped during commit")
        return True

    def renew_lease(self, *_args):
        self.renewals += 1
        # An already committed completion makes renewal fail because the task
        # is no longer running. The caller must stop without replaying the action.
        return not self.acknowledgement_lost

    def metrics_snapshot(self):
        return {"workflows": {"succeeded": 1}, "attempts": {"succeeded": 1},
                "dead_lettered_tasks": 0, "active_leases": 0}


class WorkerCompletionRecoveryTests(unittest.TestCase):
    def claim(self):
        return {"workflow_id": "wf", "task_id": "task", "fencing_token": 4}

    def test_transient_completion_disconnect_retries_write_without_action_replay(self):
        store = CompletionStore()
        metrics = EngineMetrics()
        worker = PostgresWorker(store, lease_seconds=5, renewal_interval=4,
                                poll_interval=0.001, metrics=metrics)
        worker._sleep_database_backoff = lambda _attempt: None
        actions = []

        worker._execute(self.claim(), {"task": lambda: actions.append("ran")})

        self.assertEqual(actions, ["ran"])
        self.assertEqual(store.completions, 2)
        self.assertEqual(metrics.task_attempts.labels(outcome="succeeded")._value.get(), 1)

    def test_lost_completion_ack_does_not_rerun_or_double_count(self):
        store = CompletionStore(acknowledgement_lost=True)
        metrics = EngineMetrics()
        worker = PostgresWorker(store, lease_seconds=5, renewal_interval=4,
                                poll_interval=0.001, metrics=metrics)
        worker._sleep_database_backoff = lambda _attempt: None
        actions = []

        worker._execute(self.claim(), {"task": lambda: actions.append("ran")})

        self.assertEqual(actions, ["ran"])
        self.assertEqual(store.completions, 1)
        self.assertEqual(metrics.task_attempts.labels(outcome="succeeded")._value.get(), 0)
        metrics.refresh_database_snapshot(store)
        self.assertEqual(metrics.workflow_snapshot.labels(state="succeeded")._value.get(), 1)
        self.assertEqual(metrics.attempt_snapshot.labels(outcome="succeeded")._value.get(), 1)

    def test_action_cache_is_bounded_and_terminal_workflows_are_evicted(self):
        class DefinitionStore:
            def workflow_definition(self, _workflow_id):
                return {"api_definition_version": 1, "tasks": [{
                    "id": "task", "action": {"type": "noop.v1", "parameters": {}}}]}

        worker = PostgresWorker(DefinitionStore())
        registry = default_action_registry()
        for index in range(worker._discovery_action_cache_limit + 1):
            worker._registered_actions(f"wf-{index}", registry)
        self.assertEqual(len(worker._discovery_action_cache),
                         worker._discovery_action_cache_limit)
        self.assertNotIn("wf-0", worker._discovery_action_cache)
        worker._evict_terminal_actions("wf-1", "succeeded")
        self.assertNotIn("wf-1", worker._discovery_action_cache)


if __name__ == "__main__":
    unittest.main()
