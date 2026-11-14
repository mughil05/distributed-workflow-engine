"""Real Redis Pub/Sub and PostgreSQL outbox integration tests."""

import multiprocessing
import os
import queue
import socket
import subprocess
import threading
import time
import unittest
import uuid

import redis

from workflow_engine import PostgresStore, PostgresWorker, RetryPolicy, Task, Workflow
from workflow_engine.worker import _RedisWakeup


PG_DSN = os.environ.get("WORKFLOW_TEST_POSTGRES_DSN")
REDIS_URL = os.environ.get("WORKFLOW_TEST_REDIS_URL")


def _run_parallel_redis_worker(dsn, redis_url, workflow_id, barrier, results):
    store = PostgresStore(dsn, migrate=False)
    client = redis.Redis.from_url(redis_url, socket_connect_timeout=1, socket_timeout=2)

    def action(task_id):
        results.put(("action", task_id))
        barrier.wait(timeout=15)

    worker = PostgresWorker(store, lease_seconds=8, renewal_interval=2,
                            poll_interval=0.05, fallback_scan_interval=0.2,
                            redis_client=client)
    try:
        results.put(("workflow", worker.run(workflow_id,
                                             {"alpha": lambda: action("alpha"),
                                              "beta": lambda: action("beta")})))
    except BaseException as exc:
        results.put(("error", type(exc).__name__, str(exc)))
    finally:
        client.close()


class RedisIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not PG_DSN or not REDIS_URL:
            if os.environ.get("CI"):
                raise RuntimeError("CI requires PostgreSQL and Redis test URLs; refusing skipped integration tests")
            raise unittest.SkipTest("set WORKFLOW_TEST_POSTGRES_DSN and WORKFLOW_TEST_REDIS_URL")
        cls.store = PostgresStore(PG_DSN)
        cls.redis_client = redis.Redis.from_url(REDIS_URL, socket_connect_timeout=1,
                                                socket_timeout=2, health_check_interval=15)
        cls.redis_client.ping()

    def setUp(self):
        self.workflow_id = "redis-it-" + uuid.uuid4().hex

    def initialize(self, tasks):
        workflow = Workflow.from_tasks(self.workflow_id, tasks)
        items = []
        for task in workflow.tasks:
            item = {"id": task.task_id, "dependencies": list(task.dependencies)}
            if task.retry_policy != RetryPolicy():
                item["retry_policy"] = {
                    "max_attempts": task.retry_policy.max_attempts,
                    "initial_delay": task.retry_policy.initial_delay,
                    "multiplier": task.retry_policy.multiplier,
                    "max_delay": task.retry_policy.max_delay,
                    "jitter": task.retry_policy.jitter,
                }
            items.append(item)
        self.store.initialize(self.workflow_id, {"tasks": items},
                              [task.task_id for task in tasks])

    def test_pubsub_broadcast_and_duplicate_hints_do_not_grant_ownership(self):
        self.initialize([Task("only")])
        channel = PostgresWorker._channel(self.workflow_id)
        listeners = [self.redis_client.pubsub(ignore_subscribe_messages=True) for _ in range(2)]
        try:
            for listener in listeners:
                listener.subscribe(channel)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                counts = dict(self.redis_client.pubsub_numsub(channel))
                if counts.get(channel.encode(), 0) == 2:
                    break
                time.sleep(0.02)
            else:
                self.fail("Redis Pub/Sub subscriptions were not established")
            self.redis_client.publish(channel, "task_ready")
            self.redis_client.publish(channel, "duplicate")
            for listener in listeners:
                seen = []
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline and len(seen) < 2:
                    message = listener.get_message(timeout=0.1)
                    if message:
                        seen.append(message["data"])
                self.assertEqual(seen, [b"task_ready", b"duplicate"])
            claims = [self.store.claim_task(self.workflow_id, worker, 30)
                      for worker in ("one", "two")]
            self.assertEqual(sum(item is not None for item in claims), 1)
        finally:
            for listener in listeners:
                listener.close()

    def test_multiple_process_workers_execute_independent_tasks(self):
        self.initialize([Task("alpha"), Task("beta")])
        context = multiprocessing.get_context("spawn")
        barrier, results = context.Barrier(2), context.Queue()
        processes = [context.Process(target=_run_parallel_redis_worker,
                                     args=(PG_DSN, REDIS_URL, self.workflow_id, barrier, results))
                     for _ in range(2)]
        for process in processes:
            process.start()
        messages = [results.get(timeout=25) for _ in range(4)]
        for process in processes:
            process.join(20)
            self.assertFalse(process.is_alive())
            self.assertEqual(process.exitcode, 0)
        self.assertFalse([message for message in messages if message[0] == "error"], messages)
        self.assertEqual(sorted(message[1] for message in messages if message[0] == "action"),
                         ["alpha", "beta"])
        self.assertEqual([message for message in messages if message[0] == "workflow"],
                         [("workflow", "succeeded")] * 2)

    def test_lost_retry_notification_recovers_from_postgresql_deadline(self):
        self.initialize([Task("only", retry_policy=RetryPolicy(
            max_attempts=2, initial_delay=0.4, max_delay=0.4))])
        first = self.store.claim_task(self.workflow_id, "first", 30)
        self.assertTrue(self.store.fail_task(self.workflow_id, "only", "first",
                                             first["fencing_token"], "transient"))
        # Lease and acknowledge the outbox row without publishing it: model a
        # notification lost after dispatch. PostgreSQL remains the recovery source.
        events = self.store.lease_notifications("lost-notification-test")
        for event in events:
            self.store.mark_notification_published(event["event_id"], "lost-notification-test")
        calls = []
        worker = PostgresWorker(self.store, lease_seconds=8, renewal_interval=2,
                                poll_interval=0.05, fallback_scan_interval=0.5,
                                redis_client=self.redis_client, worker_id="retry-worker")
        self.assertEqual(worker.run(self.workflow_id, {"only": lambda: calls.append("retried")}),
                         "succeeded")
        self.assertEqual(calls, ["retried"])

    def test_redis_unavailable_at_startup_does_not_block_postgres_work(self):
        self.initialize([Task("only")])
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        unused_port = probe.getsockname()[1]
        probe.close()
        unavailable = redis.Redis(host="127.0.0.1", port=unused_port,
                                  socket_connect_timeout=0.1, socket_timeout=0.1)
        calls = []
        try:
            worker = PostgresWorker(self.store, lease_seconds=8, renewal_interval=2,
                                    poll_interval=0.05, fallback_scan_interval=0.1,
                                    redis_client=unavailable)
            self.assertEqual(worker.run(self.workflow_id,
                                        {"only": lambda: calls.append("ran")}), "succeeded")
            self.assertEqual(calls, ["ran"])
        finally:
            unavailable.close()

    def test_redis_disconnect_after_action_does_not_block_database_completion(self):
        self.initialize([Task("only")])

        class TogglePublisher:
            def __init__(self, client):
                self.client = client
                self.available = True

            def pubsub(self, **kwargs):
                return self.client.pubsub(**kwargs)

            def publish(self, *args, **kwargs):
                if not self.available:
                    raise redis.exceptions.ConnectionError("simulated Redis outage")
                return self.client.publish(*args, **kwargs)

        publisher = TogglePublisher(self.redis_client)

        def action():
            publisher.available = False

        worker = PostgresWorker(self.store, lease_seconds=8, renewal_interval=2,
                                poll_interval=0.05, fallback_scan_interval=0.1,
                                redis_client=publisher)
        self.assertEqual(worker.run(self.workflow_id, {"only": action}), "succeeded")
        self.assertEqual(self.store.workflow_state(self.workflow_id), "succeeded")

    def test_outbox_dispatch_backs_off_during_redis_outage(self):
        self.initialize([Task("only")])
        class CountingStore:
            def __init__(self, store):
                self.store = store
                self.lease_calls = 0
            def __getattr__(self, name):
                return getattr(self.store, name)
            def lease_notifications(self, *args, **kwargs):
                self.lease_calls += 1
                return self.store.lease_notifications(*args, **kwargs)
        class DownPublisher:
            def pubsub(self, **kwargs):
                return self.redis.pubsub(**kwargs)
            def publish(self, *args, **kwargs):
                raise redis.exceptions.ConnectionError("simulated Redis outage")
        store = CountingStore(self.store)
        publisher = DownPublisher()
        publisher.redis = self.redis_client
        worker = PostgresWorker(store, poll_interval=0.02, redis_client=publisher)
        with self.assertLogs("workflow_engine.worker", level="WARNING"):
            worker._dispatch_notifications()
            worker._dispatch_notifications()
        self.assertEqual(store.lease_calls, 1)
        # The publisher cooldown does not affect the authoritative PostgreSQL path.
        claim = self.store.claim_task(self.workflow_id, "postgres-still-works", 30)
        self.assertIsNotNone(claim)

    def test_listener_reconnects_after_real_redis_restart(self):
        # Own this exact container for restart isolation; never infer a target
        # by image name or disturb the shared integration Redis service.
        container = "wf005-redis-restart-" + uuid.uuid4().hex[:12]
        port_probe = socket.socket()
        port_probe.bind(("127.0.0.1", 0))
        isolated_port = port_probe.getsockname()[1]
        port_probe.close()
        try:
            subprocess.run(["docker", "run", "-d", "--name", container,
                            "-p", f"127.0.0.1:{isolated_port}:6379", "redis:7"], check=True,
                           timeout=30, stdout=subprocess.DEVNULL)
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            if os.environ.get("CI"):
                raise RuntimeError("CI requires Docker to run the isolated Redis restart test") from exc
            self.skipTest("Docker is required for the isolated Redis restart scenario")
        client = None
        listener = None
        channel = "redis-restart:" + uuid.uuid4().hex
        wake = threading.Event()
        try:
            mapped = subprocess.check_output(["docker", "port", container, "6379/tcp"],
                                             text=True, timeout=10).strip().splitlines()[0]
            port = int(mapped.rsplit(":", 1)[1])
            client = redis.Redis(host="127.0.0.1", port=port,
                                 socket_connect_timeout=0.5, socket_timeout=1)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    client.ping()
                    break
                except redis.RedisError:
                    time.sleep(.1)
            else:
                self.fail("isolated Redis container did not become ready")
            listener = _RedisWakeup(client, channel, wake)
            listener.start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                client.publish(channel, "before-restart")
                if wake.wait(0.1):
                    break
            self.assertTrue(wake.is_set(), "listener did not receive the initial message")
            wake.clear()
            subprocess.run(["docker", "restart", container], check=True,
                           timeout=20, stdout=subprocess.DEVNULL)
            # Docker Desktop may reassign dynamically published host ports as
            # a container restarts. Refresh the owned container's exact mapping.
            mapped = subprocess.check_output(["docker", "port", container, "6379/tcp"],
                                             text=True, timeout=10).strip().splitlines()[0]
            client.connection_pool.connection_kwargs["port"] = int(mapped.rsplit(":", 1)[1])

            deadline = time.monotonic() + 15
            last_error = None
            while time.monotonic() < deadline:
                try:
                    client.connection_pool.disconnect()
                    client.ping()
                    break
                except redis.exceptions.RedisError as exc:
                    last_error = exc
                    time.sleep(0.1)
            else:
                logs = subprocess.run(["docker", "logs", container], capture_output=True,
                                      text=True, timeout=5, check=False).stdout[-1000:]
                self.fail(f"owned Redis did not restart: {last_error}; logs={logs}")

            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                client.publish(channel, "after-restart")
                if wake.wait(0.1):
                    break
            self.assertTrue(wake.is_set(), "listener did not reconnect after Redis restarted")
        finally:
            if listener:
                listener.close()
            if client:
                client.close()
            subprocess.run(["docker", "rm", "-f", container], check=False,
                           timeout=20, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    unittest.main()
