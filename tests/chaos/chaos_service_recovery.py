"""Physical service interruption scenarios for run-chaos.sh only."""

import os
import multiprocessing
import subprocess
import threading
import time
import unittest
import uuid

import redis

from workflow_engine import PostgresStore, PostgresWorker, Task


DSN = os.environ.get("WORKFLOW_TEST_POSTGRES_DSN")
REDIS_URL = os.environ.get("WORKFLOW_TEST_REDIS_URL")
PROJECT = os.environ.get("WORKFLOW_CHAOS_PROJECT")
COMPOSE_FILE = os.path.abspath("docker-compose.chaos.yml")


def _crashing_worker(dsn, workflow_id, entered, action_calls):
    store = PostgresStore(dsn)
    worker = PostgresWorker(store, lease_seconds=2, renewal_interval=1,
                            poll_interval=.05, fallback_scan_interval=.1,
                            worker_id="chaos-child-" + uuid.uuid4().hex)

    def action():
        with action_calls.get_lock():
            action_calls.value += 1
        entered.set()
        while True:
            time.sleep(.1)

    worker.run(workflow_id, {"only": action})


class OwnedServices(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not all((DSN, REDIS_URL, PROJECT)):
            raise RuntimeError("run via scripts/run-chaos.sh to provide isolated services")
        cls.store = PostgresStore(DSN)
        cls.redis = redis.Redis.from_url(REDIS_URL, socket_connect_timeout=.5,
                                        socket_timeout=1)

    @classmethod
    def tearDownClass(cls):
        cls.redis.close()

    @classmethod
    def service(cls, action, name):
        command = ["docker", "compose", "--project-name", PROJECT,
                   "-f", COMPOSE_FILE]
        command += (["up", "-d", "--wait", name] if action == "start"
                    else [action, name])
        subprocess.run(command, check=True, timeout=30,
                       stdout=subprocess.DEVNULL)

    def test_postgres_interrupt_after_action_recovers_completion_without_replay(self):
        workflow_id = "chaos-pg-recovery-" + uuid.uuid4().hex
        self.store.initialize(workflow_id, {"tasks": [{"id": "only"}]}, ["only"])
        entered, release, completion_failed = (threading.Event(), threading.Event(),
                                               threading.Event())
        action_calls = []
        original_complete = self.store.complete_task

        def monitored_complete(*args, **kwargs):
            try:
                return original_complete(*args, **kwargs)
            except Exception:
                completion_failed.set()
                raise

        self.store.complete_task = monitored_complete

        def action():
            action_calls.append("effect")
            entered.set()
            if not release.wait(10):
                raise TimeoutError("chaos action release deadline expired")

        worker = PostgresWorker(self.store, lease_seconds=15, renewal_interval=3,
                                poll_interval=.05, fallback_scan_interval=.2,
                                redis_client=self.redis)
        result = []
        thread = threading.Thread(target=lambda: result.append(
            worker.run(workflow_id, {"only": action})), daemon=True)
        thread.start()
        try:
            self.assertTrue(entered.wait(10), "worker did not enter its gated action")
            self.service("stop", "postgres")
            release.set()
            self.assertTrue(completion_failed.wait(8), "completion did not observe PostgreSQL outage")
            self.service("start", "postgres")
            thread.join(20)
            self.assertFalse(thread.is_alive(), "worker did not recover before deadline")
            self.assertEqual(result, ["succeeded"])
            self.assertEqual(action_calls, ["effect"],
                             "lost completion acknowledgement must not rerun action")
            self.assertEqual(self.store.workflow_state(workflow_id), "succeeded")
        finally:
            release.set()
            if thread.is_alive():
                self.service("start", "postgres")
                thread.join(10)
            self.store.complete_task = original_complete

    def test_redis_outage_does_not_block_execution_and_service_recovers(self):
        workflow_id = "chaos-redis-recovery-" + uuid.uuid4().hex
        self.store.initialize(workflow_id, {"tasks": [{"id": "only"}]}, ["only"])
        self.service("stop", "redis")
        calls = []
        try:
            worker = PostgresWorker(self.store, lease_seconds=10, renewal_interval=2,
                                    poll_interval=.05, fallback_scan_interval=.2,
                                    redis_client=self.redis)
            self.assertEqual(worker.run(workflow_id, {"only": lambda: calls.append("ran")}),
                             "succeeded")
            self.assertEqual(calls, ["ran"])
        finally:
            self.service("start", "redis")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                self.redis.ping()
                break
            except redis.RedisError:
                time.sleep(.1)
        else:
            self.fail("owned Redis service did not recover before deadline")

    def test_owned_worker_process_kill_expires_lease_and_second_worker_recovers(self):
        workflow_id = "chaos-child-crash-" + uuid.uuid4().hex
        self.store.initialize(workflow_id, {"tasks": [{"id": "only",
                                                        "retry_policy": {"max_attempts": 3}}]},
                              ["only"])
        context = multiprocessing.get_context("spawn")
        entered = context.Event()
        action_calls = context.Value("i", 0)
        child = context.Process(target=_crashing_worker,
                                args=(DSN, workflow_id, entered, action_calls))
        child.start()
        recovery_entered = threading.Event()
        release_recovery = threading.Event()
        recovery_result = []
        try:
            self.assertTrue(entered.wait(12), "owned worker child did not enter its action")
            with self.store._connect() as conn:
                old = conn.execute("""
                    SELECT lease_owner,fencing_token FROM tasks
                    WHERE workflow_id=%s AND task_id='only' AND state='running'
                """, (workflow_id,)).fetchone()
            self.assertIsNotNone(old, "first worker must own a running lease before termination")
            old_owner, old_fence = old

            child.terminate()  # exact Process object created by this test
            child.join(5)
            self.assertFalse(child.is_alive(), "owned child process did not terminate")

            lease_expired = time.monotonic() + 8
            while time.monotonic() < lease_expired:
                with self.store._connect() as conn:
                    expired = conn.execute("""
                        SELECT lease_expires_at <= clock_timestamp() FROM tasks
                        WHERE workflow_id=%s AND task_id='only' AND state='running'
                    """, (workflow_id,)).fetchone()
                if expired and expired[0]:
                    break
                time.sleep(.05)
            else:
                self.fail("first worker lease did not expire before recovery deadline")

            def recovered_action():
                with action_calls.get_lock():
                    action_calls.value += 1
                recovery_entered.set()
                if not release_recovery.wait(8):
                    raise TimeoutError("recovery action release deadline expired")

            recovery_worker = PostgresWorker(self.store, lease_seconds=10,
                                             renewal_interval=2, poll_interval=.03,
                                             worker_id="chaos-recovery-" + uuid.uuid4().hex)
            thread = threading.Thread(target=lambda: recovery_result.append(
                recovery_worker.run(workflow_id, {"only": recovered_action})), daemon=True)
            thread.start()
            self.assertTrue(recovery_entered.wait(8), "second worker did not recover the task")
            self.assertFalse(self.store.complete_task(workflow_id, "only", old_owner, old_fence),
                             "stale fencing token must not complete the second worker's lease")
            release_recovery.set()
            thread.join(12)
            self.assertFalse(thread.is_alive(), "second worker did not finish before deadline")
            self.assertEqual(recovery_result, ["succeeded"])
            self.assertEqual(action_calls.value, 2,
                             "an action interrupted after its effect can run again: at-least-once")
            with self.store._connect() as conn:
                history = conn.execute("""
                    SELECT attempt_number,fencing_token,outcome FROM task_attempts
                    WHERE workflow_id=%s AND task_id='only' ORDER BY attempt_number
                """, (workflow_id,)).fetchall()
            self.assertEqual([(row[0], row[2]) for row in history],
                             [(1, "lease_expired"), (2, "succeeded")])
            self.assertGreater(history[1][1], history[0][1])
        finally:
            release_recovery.set()
            if child.is_alive():
                child.terminate()
            child.join(5)


if __name__ == "__main__":
    unittest.main()
