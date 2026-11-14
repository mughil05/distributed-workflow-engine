"""Real PostgreSQL integration tests. Set WORKFLOW_TEST_POSTGRES_DSN to enable."""

import multiprocessing
import os
import queue
import threading
import time
import unittest
import uuid
from dataclasses import asdict
from pathlib import Path

from workflow_engine.definitions import Task, Workflow
from workflow_engine.postgres import PostgresStore, psycopg
from workflow_engine.retries import PermanentTaskError, RetryPolicy
from workflow_engine.worker import PostgresWorker


DSN = os.environ.get("WORKFLOW_TEST_POSTGRES_DSN")


def _parallel_worker(dsn, workflow_id, barrier, result):
    store = PostgresStore(dsn, migrate=False)
    def run_task(task_id):
        result.put(("started", task_id))
        barrier.wait(timeout=15)
    try:
        state = PostgresWorker(store, lease_seconds=12, renewal_interval=2,
                               poll_interval=0.02).run(
            workflow_id, {"alpha": lambda: run_task("alpha"),
                          "beta": lambda: run_task("beta")})
        result.put(("finished", state))
    except BaseException as exc:
        result.put(("error", type(exc).__name__, str(exc)))


def _initialize_process(dsn, workflow_id, barrier, result):
    store = PostgresStore(dsn, migrate=False)
    workflow = Workflow.from_tasks(workflow_id, [Task("root"), Task("child", ("root",))])
    definition = {"tasks": [{"id": task.task_id, "dependencies": list(task.dependencies)}
                            for task in workflow.tasks]}
    try:
        barrier.wait(timeout=15)
        store.initialize(workflow_id, definition, ["root", "child"])
        result.put("ok")
    except BaseException as exc:
        result.put((type(exc).__name__, str(exc)))


def _crash_after_claim(dsn, workflow_id, claimed):
    store = PostgresStore(dsn, migrate=False)
    store.claim_task(workflow_id, "crashed-process", 30)
    claimed.set()
    os._exit(0)


def _crash_during_action(dsn, workflow_id, action_started):
    store = PostgresStore(dsn, migrate=False)
    def action():
        action_started.set()  # Represents an external side effect already applied.
        os._exit(0)            # Simulate death before the succeeded checkpoint.
    PostgresWorker(store, lease_seconds=30, renewal_interval=5).run(workflow_id, {"only": action})


def _claim_once(dsn, workflow_id, worker_id, result):
    store = PostgresStore(dsn, migrate=False)
    result.put(store.claim_task(workflow_id, worker_id, 30))


def _claim_race(dsn, workflow_id, worker_id, barrier, result):
    store = PostgresStore(dsn, migrate=False)
    barrier.wait(timeout=15)
    result.put((worker_id, store.claim_task(workflow_id, worker_id, 30)))


def _complete_race(dsn, workflow_id, task_id, worker_id, token, barrier, result):
    store = PostgresStore(dsn, migrate=False)
    barrier.wait(timeout=15)
    result.put((task_id, store.complete_task(workflow_id, task_id, worker_id, token)))


class PostgreSQLIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not DSN:
            if os.environ.get("CI"):
                raise RuntimeError("CI requires WORKFLOW_TEST_POSTGRES_DSN; refusing skipped integration tests")
            raise unittest.SkipTest("set WORKFLOW_TEST_POSTGRES_DSN to run PostgreSQL integration tests")
        cls.store = PostgresStore(DSN)

    def setUp(self):
        self.workflow_id = "it-" + uuid.uuid4().hex

    def initialize(self, tasks):
        workflow = Workflow.from_tasks(self.workflow_id, tasks)
        definition_tasks = []
        for task in workflow.tasks:
            item = {"id": task.task_id, "dependencies": list(task.dependencies)}
            if task.retry_policy != RetryPolicy():
                item["retry_policy"] = asdict(task.retry_policy)
            definition_tasks.append(item)
        definition = {"tasks": definition_tasks}
        self.store.initialize(self.workflow_id, definition, [task.task_id for task in tasks])
        self.store.start_workflow(self.workflow_id)

    def task_state(self, task_id):
        with self.store._connect() as conn:
            row = conn.execute("SELECT state FROM tasks WHERE workflow_id=%s AND task_id=%s",
                               (self.workflow_id, task_id)).fetchone()
        return row[0]

    def test_two_process_workers_claim_independent_tasks(self):
        self.initialize([Task("alpha"), Task("beta")])
        context = multiprocessing.get_context("spawn")
        barrier, result = context.Barrier(2), context.Queue()
        workers = [context.Process(target=_parallel_worker,
                                   args=(DSN, self.workflow_id, barrier, result)) for _ in range(2)]
        for worker in workers:
            worker.start()
        messages = [result.get(timeout=25) for _ in range(4)]
        for worker in workers:
            worker.join(20)
            self.assertFalse(worker.is_alive(), "worker process did not finish")
            self.assertEqual(worker.exitcode, 0)
        self.assertEqual(sorted(task for kind, task in messages if kind == "started"), ["alpha", "beta"])
        self.assertEqual([m for m in messages if m[0] == "finished"], [("finished", "succeeded")] * 2)

    def test_two_workers_racing_for_one_task_produce_one_claim(self):
        self.initialize([Task("only")])
        context = multiprocessing.get_context("spawn")
        barrier, result = context.Barrier(2), context.Queue()
        workers = [context.Process(target=_claim_race,
                                   args=(DSN, self.workflow_id, name, barrier, result))
                   for name in ("race-a", "race-b")]
        for process in workers:
            process.start()
        claims = [result.get(timeout=15) for _ in workers]
        for process in workers:
            process.join(15)
            self.assertFalse(process.is_alive())
            self.assertEqual(process.exitcode, 0)
        self.assertEqual(sum(claim is not None for _, claim in claims), 1)

    def test_independent_initial_claims_do_not_wait_on_workflow_row_lock(self):
        workflow = Workflow.from_tasks(self.workflow_id, [Task("alpha"), Task("beta")])
        definition = {"tasks": [{"id": task.task_id, "dependencies": list(task.dependencies)}
                                for task in workflow.tasks]}
        self.store.initialize(self.workflow_id, definition, ["alpha", "beta"])
        context = multiprocessing.get_context("spawn")
        result = context.Queue()
        with self.store._connect() as conn:
            with conn.transaction():
                conn.execute("UPDATE workflows SET error='lock probe' WHERE workflow_id=%s",
                             (self.workflow_id,))
                process = context.Process(target=_claim_once,
                                          args=(DSN, self.workflow_id, "claim-probe", result))
                process.start()
                try:
                    claim = result.get(timeout=5)
                except queue.Empty:
                    claim = None
                process.join(10)
                self.assertFalse(process.is_alive())
                self.assertEqual(process.exitcode, 0)
        self.assertIsNotNone(claim, "task claim blocked on an unrelated workflow-row update")

    def test_simultaneous_final_task_completion_finalizes_once(self):
        self.initialize([Task("alpha"), Task("beta")])
        claims = [self.store.claim_task(self.workflow_id, worker, 30)
                  for worker in ("finish-a", "finish-b")]
        context = multiprocessing.get_context("spawn")
        barrier, result = context.Barrier(2), context.Queue()
        workers = [context.Process(target=_complete_race,
                                   args=(DSN, self.workflow_id, claim["task_id"], worker,
                                         claim["fencing_token"], barrier, result))
                   for worker, claim in zip(("finish-a", "finish-b"), claims)]
        for process in workers:
            process.start()
        outcomes = [result.get(timeout=15) for _ in workers]
        for process in workers:
            process.join(15)
            self.assertFalse(process.is_alive())
            self.assertEqual(process.exitcode, 0)
        self.assertTrue(all(completed for _, completed in outcomes))
        self.assertEqual(self.store.workflow_state(self.workflow_id), "succeeded")
        with self.store._connect() as conn:
            timestamp = conn.execute("SELECT updated_at FROM workflows WHERE workflow_id=%s",
                                     (self.workflow_id,)).fetchone()[0]
            with conn.transaction():
                self.assertEqual(PostgresStore._finalize_if_terminal(
                    conn, self.workflow_id, serialize=True), "succeeded")
            self.assertEqual(conn.execute("SELECT updated_at FROM workflows WHERE workflow_id=%s",
                                          (self.workflow_id,)).fetchone()[0], timestamp)

    def test_claim_overlapping_finalization_cannot_claim_after_terminal_state(self):
        self.initialize([Task("only")])
        claim = self.store.claim_task(self.workflow_id, "finisher", 30)
        finish_started = threading.Event()
        claim_started = threading.Event()
        outcomes = queue.Queue()

        def finish():
            finish_started.set()
            outcomes.put(("finish", self.store.complete_task(
                self.workflow_id, "only", "finisher", claim["fencing_token"])))

        def race_claim():
            claim_started.set()
            outcomes.put(("claim", self.store.claim_task(self.workflow_id, "racer", 30)))

        # Hold the shared side of the gate as a claim would. The completing
        # transaction must wait for an exclusive gate before making the workflow terminal.
        with self.store._connect() as gate:
            with gate.transaction():
                gate.execute(
                    "SELECT pg_advisory_xact_lock_shared(hashtext('workflow_claim_gate'), hashtext(%s))",
                    (self.workflow_id,),
                )
                finisher = threading.Thread(target=finish)
                finisher.start()
                self.assertTrue(finish_started.wait(2))
                racer = threading.Thread(target=race_claim)
                racer.start()
                self.assertTrue(claim_started.wait(2))
            finisher.join(10)
            racer.join(10)

        self.assertFalse(finisher.is_alive())
        self.assertFalse(racer.is_alive())
        results = dict(outcomes.get_nowait() for _ in range(2))
        self.assertTrue(results["finish"])
        self.assertIsNone(results["claim"])
        self.assertEqual(self.store.workflow_state(self.workflow_id), "succeeded")
        self.assertIsNone(self.store.claim_task(self.workflow_id, "after-terminal", 30))

    def test_finisher_waits_for_gate_before_locking_task_row(self):
        self.initialize([Task("only")])
        claim = self.store.claim_task(self.workflow_id, "finisher", 30)
        original_connect = self.store._connect
        started = threading.Event()
        pid_queue = queue.Queue()
        result = queue.Queue()

        def tagged_connect():
            conn = original_connect()
            conn.execute("SET application_name='wf002_finish_order_test'")
            conn.execute("SET lock_timeout='5s'")
            conn.execute("SET deadlock_timeout='100ms'")
            pid_queue.put(conn.execute("SELECT pg_backend_pid()").fetchone()[0])
            return conn

        def finish():
            started.set()
            try:
                result.put(self.store.complete_task(
                    self.workflow_id, "only", "finisher", claim["fencing_token"]))
            except BaseException as exc:
                result.put(exc)

        self.store._connect = tagged_connect
        try:
            original_gate = self.store._workflow_claim_gate
            gate_called = threading.Event()

            def observe_gate(conn, workflow_id, *, shared):
                if workflow_id == self.workflow_id and not shared:
                    gate_called.set()
                return original_gate(conn, workflow_id, shared=shared)

            self.store._workflow_claim_gate = observe_gate
            with original_connect() as gate:
                with gate.transaction():
                    gate.execute("SET LOCAL lock_timeout='5s'")
                    gate.execute("SET LOCAL deadlock_timeout='100ms'")
                    gate.execute(
                        "SELECT pg_advisory_xact_lock_shared(hashtext('workflow_claim_gate'), hashtext(%s))",
                        (self.workflow_id,),
                    )
                    thread = threading.Thread(target=finish)
                    thread.start()
                    self.assertTrue(started.wait(2))
                    self.assertTrue(gate_called.wait(2))
                    pid = pid_queue.get(timeout=2)

                    # Wait until complete_task is actually queued on the gate.
                    deadline = time.monotonic() + 5
                    waiting = False
                    while time.monotonic() < deadline:
                        waiting = gate.execute("""
                            SELECT EXISTS (SELECT 1 FROM pg_locks
                                WHERE pid=%s AND locktype='advisory' AND NOT granted)
                        """, (pid,)).fetchone()[0]
                        if waiting:
                            waiting = True
                            break
                        time.sleep(0.02)
                    self.assertTrue(waiting, "finisher did not wait for the shared claim gate")

                    # The finisher must not already hold the task row while waiting
                    # for the exclusive gate. Under the old ordering NOWAIT failed.
                    row = gate.execute("""
                        SELECT task_id FROM tasks
                        WHERE workflow_id=%s AND task_id='only' FOR UPDATE NOWAIT
                    """, (self.workflow_id,)).fetchone()
                    self.assertEqual(row[0], "only")
            thread.join(10)
            self.assertFalse(thread.is_alive())
            outcome = result.get_nowait()
            if isinstance(outcome, BaseException):
                raise outcome
            self.assertTrue(outcome)
            self.assertEqual(self.store.workflow_state(self.workflow_id), "succeeded")
        finally:
            self.store._connect = original_connect
            if "original_gate" in locals():
                self.store._workflow_claim_gate = original_gate

    def test_reclaim_increments_fence_and_rejects_stale_completion(self):
        self.initialize([Task("only", retry_policy=RetryPolicy(max_attempts=2))])
        first = self.store.claim_task(self.workflow_id, "worker-one", 30)
        self.assertIsNotNone(first)
        with self.store._connect() as conn:
            conn.execute("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' "
                         "WHERE workflow_id=%s AND task_id='only'", (self.workflow_id,))
        second = self.store.claim_task(self.workflow_id, "worker-two", 30)
        self.assertGreater(second["fencing_token"], first["fencing_token"])
        self.assertFalse(self.store.complete_task(self.workflow_id, "only", "worker-one",
                                                  first["fencing_token"]))
        self.assertFalse(self.store.renew_lease(self.workflow_id, "only", "worker-one",
                                                first["fencing_token"], 30))
        self.assertTrue(self.store.complete_task(self.workflow_id, "only", "worker-two",
                                                 second["fencing_token"]))
        self.assertEqual(self.store.workflow_state(self.workflow_id), "succeeded")
        with self.store._connect() as conn:
            outcomes = conn.execute("""
                SELECT outcome FROM task_attempts WHERE workflow_id=%s AND task_id='only'
                ORDER BY attempt_number
            """, (self.workflow_id,)).fetchall()
        self.assertEqual([row[0] for row in outcomes], ["lease_expired", "succeeded"])
        with self.store._connect() as conn:
            outcomes = conn.execute("""
                SELECT outcome FROM task_attempts WHERE workflow_id=%s AND task_id='only'
                ORDER BY attempt_number
            """, (self.workflow_id,)).fetchall()
        self.assertEqual([row[0] for row in outcomes], ["lease_expired", "succeeded"])

    def test_expired_lease_rejects_renewal_and_completion_before_reclaim(self):
        self.initialize([Task("only")])
        claim = self.store.claim_task(self.workflow_id, "worker-one", 30)
        with self.store._connect() as conn:
            conn.execute("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' "
                         "WHERE workflow_id=%s AND task_id='only'", (self.workflow_id,))
        self.assertFalse(self.store.renew_lease(self.workflow_id, "only", "worker-one",
                                                claim["fencing_token"], 30))
        self.assertFalse(self.store.complete_task(self.workflow_id, "only", "worker-one",
                                                  claim["fencing_token"]))
        self.assertEqual(self.store.workflow_state(self.workflow_id), "running")

    def test_nonfinite_and_nonpositive_lease_durations_are_rejected(self):
        self.initialize([Task("only")])
        claim = self.store.claim_task(self.workflow_id, "worker", 30)
        for duration in (0, -1, float("nan"), float("inf"), float("-inf")):
            with self.subTest(duration=duration):
                with self.assertRaises(ValueError):
                    self.store.claim_task(self.workflow_id, "worker", duration)
                with self.assertRaises(ValueError):
                    self.store.renew_lease(self.workflow_id, "only", "worker",
                                           claim["fencing_token"], duration)
                with self.assertRaises(ValueError):
                    PostgresWorker(self.store, lease_seconds=duration)
                with self.assertRaises(ValueError):
                    PostgresWorker(self.store, renewal_interval=duration)

    def test_heartbeat_database_error_is_reported_and_work_recovers_after_expiry(self):
        self.initialize([Task("only", retry_policy=RetryPolicy(max_attempts=2))])
        renewal_failed = threading.Event()

        class TemporaryHeartbeatFailure:
            def __init__(self, store):
                self.store = store
                self.failed_once = False

            def __getattr__(self, name):
                return getattr(self.store, name)

            def renew_lease(self, *args):
                if not self.failed_once:
                    self.failed_once = True
                    renewal_failed.set()
                    raise psycopg.OperationalError("injected temporary database unavailability")
                return self.store.renew_lease(*args)

        proxy = TemporaryHeartbeatFailure(self.store)
        worker = PostgresWorker(proxy, lease_seconds=0.5, renewal_interval=0.05,
                                poll_interval=0.01, worker_id="recovery")
        action_calls = []
        result = queue.Queue()

        def action():
            action_calls.append("called")
            if len(action_calls) == 1:
                if not renewal_failed.wait(3):
                    raise TimeoutError("heartbeat did not fail during the task")

        def run_worker():
            try:
                result.put(worker.run(self.workflow_id, {"only": action}))
            except BaseException as exc:
                result.put(exc)

        with self.assertLogs("workflow_engine.worker", level="WARNING") as captured:
            thread = threading.Thread(target=run_worker)
            thread.start()
            thread.join(8)
        self.assertFalse(thread.is_alive(), "worker did not recover after the transient heartbeat failure")
        outcome = result.get_nowait()
        if isinstance(outcome, BaseException):
            raise outcome
        self.assertEqual(outcome, "succeeded")
        self.assertEqual(len(action_calls), 2)
        records = [record for record in captured.records
                   if getattr(record, "event", None) == "workflow_lease_renewal_failed"]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].reason, "database_error")

    def test_worker_retries_claim_after_temporary_database_unavailability(self):
        self.initialize([Task("only")])

        class TemporaryClaimFailure:
            def __init__(self, store):
                self.store = store
                self.failed_once = False

            def __getattr__(self, name):
                return getattr(self.store, name)

            def claim_task(self, *args):
                if not self.failed_once:
                    self.failed_once = True
                    raise psycopg.OperationalError("injected temporary database unavailability")
                return self.store.claim_task(*args)

        calls = []
        worker = PostgresWorker(TemporaryClaimFailure(self.store), poll_interval=0.01)
        with self.assertLogs("workflow_engine.worker", level="WARNING") as captured:
            state = worker.run(self.workflow_id, {"only": lambda: calls.append("ran")})
        self.assertEqual(state, "succeeded")
        self.assertEqual(calls, ["ran"])
        retries = [record for record in captured.records
                   if getattr(record, "event", None) == "workflow_database_retry"
                   and record.operation == "claim_task"]
        self.assertEqual(len(retries), 1)

    def test_worker_permanent_error_dead_letters_without_using_remaining_attempts(self):
        self.initialize([Task("only", retry_policy=RetryPolicy(max_attempts=4))])
        worker = PostgresWorker(self.store, poll_interval=0.02,
                                fallback_scan_interval=0.1)

        def action():
            raise PermanentTaskError("invalid input")

        self.assertEqual(worker.run(self.workflow_id, {"only": action}), "failed")
        self.assertEqual(self.task_state("only"), "dead_lettered")
        with self.store._connect() as conn:
            row = conn.execute("""
                SELECT attempt_count FROM tasks WHERE workflow_id=%s AND task_id='only'
            """, (self.workflow_id,)).fetchone()
            attempts = conn.execute("""
                SELECT attempt_number,outcome,error FROM task_attempts
                WHERE workflow_id=%s AND task_id='only'
            """, (self.workflow_id,)).fetchall()
        self.assertEqual(row[0], 1)
        self.assertEqual([tuple(item) for item in attempts],
                         [(1, "dead_lettered", "PermanentTaskError: invalid input")])

    def test_process_crash_after_claim_is_recovered(self):
        self.initialize([Task("only", retry_policy=RetryPolicy(max_attempts=2))])
        context = multiprocessing.get_context("spawn")
        claimed = context.Event()
        process = context.Process(target=_crash_after_claim,
                                  args=(DSN, self.workflow_id, claimed))
        process.start()
        self.assertTrue(claimed.wait(10))
        process.join(10)
        self.assertFalse(process.is_alive())
        self.assertEqual(process.exitcode, 0)
        with self.store._connect() as conn:
            conn.execute("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' "
                         "WHERE workflow_id=%s AND task_id='only'", (self.workflow_id,))
        recovered = self.store.claim_task(self.workflow_id, "recovery-worker", 30)
        self.assertEqual(recovered["attempt_count"], 2)
        with self.store._connect() as conn:
            outcomes = conn.execute("""
                SELECT outcome FROM task_attempts WHERE workflow_id=%s AND task_id='only'
                ORDER BY attempt_number
            """, (self.workflow_id,)).fetchall()
        self.assertEqual([row[0] for row in outcomes], ["lease_expired", "running"])
        with self.store._connect() as conn:
            outcomes = conn.execute("""
                SELECT outcome FROM task_attempts WHERE workflow_id=%s AND task_id='only'
                ORDER BY attempt_number
            """, (self.workflow_id,)).fetchall()
        self.assertEqual([row[0] for row in outcomes], ["lease_expired", "running"])

    def test_crash_after_external_effect_repeats_action_at_least_once(self):
        self.initialize([Task("only", retry_policy=RetryPolicy(max_attempts=2))])
        context = multiprocessing.get_context("spawn")
        effected = context.Event()
        process = context.Process(target=_crash_during_action,
                                  args=(DSN, self.workflow_id, effected))
        process.start()
        self.assertTrue(effected.wait(10))
        process.join(10)
        self.assertFalse(process.is_alive())
        self.assertEqual(process.exitcode, 0)
        with self.store._connect() as conn:
            conn.execute("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' "
                         "WHERE workflow_id=%s AND task_id='only'", (self.workflow_id,))
        retry_effect = []
        state = PostgresWorker(self.store, lease_seconds=12, renewal_interval=2,
                               poll_interval=0.02).run(
            self.workflow_id, {"only": lambda: retry_effect.append("applied-again")})
        self.assertEqual(state, "succeeded")
        self.assertEqual(retry_effect, ["applied-again"])

    def test_child_claim_cannot_observe_uncommitted_parent_completion(self):
        self.initialize([Task("parent"), Task("child", ("parent",))])
        parent = self.store.claim_task(self.workflow_id, "parent-worker", 30)
        with self.store._connect() as conn:
            with conn.transaction():
                conn.execute("UPDATE tasks SET state='succeeded',lease_owner=NULL,lease_expires_at=NULL "
                             "WHERE workflow_id=%s AND task_id='parent'", (self.workflow_id,))
                context = multiprocessing.get_context("spawn")
                result = context.Queue()
                process = context.Process(target=_claim_once,
                                          args=(DSN, self.workflow_id, "child-worker", result))
                process.start()
                self.assertIsNone(result.get(timeout=10))
                process.join(10)
                self.assertFalse(process.is_alive())
                self.assertEqual(process.exitcode, 0)
        child = self.store.claim_task(self.workflow_id, "child-worker", 30)
        self.assertEqual(child["task_id"], "child")

    def test_interrupted_transaction_rolls_back_partial_state(self):
        self.initialize([Task("only")])
        with self.store._connect() as victim:
            with self.assertRaises(Exception):
                with victim.transaction():
                    victim.execute("UPDATE workflows SET state='failed' WHERE workflow_id=%s",
                                   (self.workflow_id,))
                    pid = victim.execute("SELECT pg_backend_pid()").fetchone()[0]
                    with self.store._connect() as killer:
                        self.assertTrue(killer.execute(
                            "SELECT pg_terminate_backend(%s)", (pid,)).fetchone()[0])
                    victim.execute("SELECT 1")
        self.assertEqual(self.store.workflow_state(self.workflow_id), "running")

    def test_dependency_claim_waits_for_parent_commit(self):
        self.initialize([Task("parent"), Task("child", ("parent",))])
        parent = self.store.claim_task(self.workflow_id, "w1", 30)
        self.assertEqual(parent["task_id"], "parent")
        self.assertIsNone(self.store.claim_task(self.workflow_id, "w2", 30))
        self.assertTrue(self.store.complete_task(self.workflow_id, "parent", "w1",
                                                 parent["fencing_token"]))
        child = self.store.claim_task(self.workflow_id, "w2", 30)
        self.assertEqual(child["task_id"], "child")

    def test_failed_parent_propagates_without_finalizing_running_independent_task(self):
        self.initialize([Task("parent"), Task("child", ("parent",)), Task("independent")])
        claims = {claim["task_id"]: (worker, claim) for worker in ("w1", "w2")
                  if (claim := self.store.claim_task(self.workflow_id, worker, 30))}
        parent_worker, parent = claims["parent"]
        independent_worker, independent = claims["independent"]
        self.assertTrue(self.store.fail_task(self.workflow_id, "parent", parent_worker,
                                             parent["fencing_token"], "expected test failure"))
        self.assertEqual(self.store.workflow_state(self.workflow_id), "running")
        self.assertTrue(self.store.complete_task(self.workflow_id, "independent", independent_worker,
                                                 independent["fencing_token"]))
        self.assertEqual(self.store.workflow_state(self.workflow_id), "failed")

    def test_concurrent_workflow_initialization_is_idempotent(self):
        context = multiprocessing.get_context("spawn")
        barrier, result = context.Barrier(2), context.Queue()
        processes = [context.Process(target=_initialize_process,
                                     args=(DSN, self.workflow_id, barrier, result)) for _ in range(2)]
        for process in processes:
            process.start()
        outcomes = [result.get(timeout=20) for _ in processes]
        for process in processes:
            process.join(15)
            self.assertFalse(process.is_alive())
            self.assertEqual(process.exitcode, 0)
        self.assertEqual(outcomes, ["ok", "ok"])

    def test_delayed_retry_is_ineligible_until_database_deadline(self):
        policy = RetryPolicy(max_attempts=2, initial_delay=3600, max_delay=3600)
        self.initialize([Task("only", retry_policy=policy)])
        first = self.store.claim_task(self.workflow_id, "retry-worker", 30)
        self.assertEqual(first["attempt_count"], 1)
        self.assertTrue(self.store.fail_task(self.workflow_id, "only", "retry-worker",
                                             first["fencing_token"], "temporary failure"))
        self.assertEqual(self.store.workflow_state(self.workflow_id), "running")
        self.assertIsNone(self.store.claim_task(self.workflow_id, "early-worker", 30))
        with self.store._connect() as conn:
            row = conn.execute("""
                SELECT next_eligible_at > clock_timestamp(),state,attempt_count
                FROM tasks WHERE workflow_id=%s AND task_id='only'
            """, (self.workflow_id,)).fetchone()
            self.assertEqual(tuple(row), (True, "pending", 1))
            conn.execute("""
                UPDATE tasks SET next_eligible_at=clock_timestamp()-interval '1 second'
                WHERE workflow_id=%s AND task_id='only'
            """, (self.workflow_id,))
        second = self.store.claim_task(self.workflow_id, "retry-worker", 30)
        self.assertEqual(second["attempt_count"], 2)
        self.assertGreater(second["fencing_token"], first["fencing_token"])
        self.assertTrue(self.store.complete_task(self.workflow_id, "only", "retry-worker",
                                                 second["fencing_token"]))
        with self.store._connect() as conn:
            attempts = conn.execute("""
                SELECT attempt_number,outcome FROM task_attempts
                WHERE workflow_id=%s AND task_id='only' ORDER BY attempt_number
            """, (self.workflow_id,)).fetchall()
        self.assertEqual([tuple(row) for row in attempts],
                         [(1, "retry_scheduled"), (2, "succeeded")])

    def test_attempt_exhaustion_dead_letters_and_finishes_workflow(self):
        self.initialize([Task("only", retry_policy=RetryPolicy(max_attempts=2))])
        first = self.store.claim_task(self.workflow_id, "exhaust", 30)
        self.assertTrue(self.store.fail_task(self.workflow_id, "only", "exhaust",
                                             first["fencing_token"], "failure one"))
        with self.store._connect() as conn:
            conn.execute("UPDATE tasks SET next_eligible_at=clock_timestamp()-interval '1 second' "
                         "WHERE workflow_id=%s AND task_id='only'", (self.workflow_id,))
        second = self.store.claim_task(self.workflow_id, "exhaust", 30)
        self.assertTrue(self.store.fail_task(self.workflow_id, "only", "exhaust",
                                             second["fencing_token"], "failure two"))
        self.assertEqual(self.store.workflow_state(self.workflow_id), "failed")
        self.assertEqual(self.task_state("only"), "dead_lettered")
        with self.store._connect() as conn:
            outcomes = conn.execute("""
                SELECT outcome,error FROM task_attempts WHERE workflow_id=%s AND task_id='only'
                ORDER BY attempt_number
            """, (self.workflow_id,)).fetchall()
        self.assertEqual([row[0] for row in outcomes], ["retry_scheduled", "dead_lettered"])
        self.assertEqual([row[1] for row in outcomes], ["failure one", "failure two"])

    def test_dead_letter_propagates_to_dependents_but_independent_work_runs(self):
        self.initialize([Task("parent"), Task("child", ("parent",)), Task("independent")])
        claims = {worker: claim for worker in ("w1", "w2")
                  if (claim := self.store.claim_task(self.workflow_id, worker, 30))}
        parent_worker, parent = next((worker, claim) for worker, claim in claims.items()
                                     if claim["task_id"] == "parent")
        self.assertTrue(self.store.fail_task(self.workflow_id, "parent", parent_worker,
                                             parent["fencing_token"], "permanent",
                                             retryable=False))
        self.assertEqual(self.task_state("parent"), "dead_lettered")
        self.assertEqual(self.task_state("child"), "failed")
        self.assertEqual(self.store.workflow_state(self.workflow_id), "running")
        independent_worker, independent = next((worker, claim) for worker, claim in claims.items()
                                               if claim["task_id"] == "independent")
        self.assertTrue(self.store.complete_task(self.workflow_id, "independent", independent_worker,
                                                 independent["fencing_token"]))
        self.assertEqual(self.store.workflow_state(self.workflow_id), "failed")

    def test_exhausted_expired_lease_dead_letters_without_reclaim(self):
        self.initialize([Task("only")])
        claim = self.store.claim_task(self.workflow_id, "crashed-worker", 30)
        with self.store._connect() as conn:
            conn.execute("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' "
                         "WHERE workflow_id=%s AND task_id='only'", (self.workflow_id,))
        self.assertIsNone(self.store.claim_task(self.workflow_id, "reaper", 30))
        self.assertEqual(self.task_state("only"), "dead_lettered")
        self.assertEqual(self.store.workflow_state(self.workflow_id), "failed")
        with self.store._connect() as conn:
            attempt = conn.execute("""
                SELECT attempt_number,fencing_token,outcome,error FROM task_attempts
                WHERE workflow_id=%s AND task_id='only'
            """, (self.workflow_id,)).fetchone()
        self.assertEqual(tuple(attempt), (1, claim["fencing_token"], "lease_expired",
                                          "lease expired; maximum attempts exhausted"))

    def test_locked_expired_exhausted_task_does_not_block_independent_claim(self):
        self.initialize([Task("expired"), Task("ready")])
        self.store.claim_task(self.workflow_id, "crashed", 30)
        with self.store._connect() as conn:
            conn.execute("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' "
                         "WHERE workflow_id=%s AND task_id='expired'", (self.workflow_id,))
        locker = self.store._connect()
        try:
            with locker.transaction():
                locker.execute("SELECT task_id FROM tasks WHERE workflow_id=%s AND task_id='expired' "
                               "FOR UPDATE", (self.workflow_id,))
                result = queue.Queue()
                thread = threading.Thread(target=lambda: result.put(
                    self.store.claim_task(self.workflow_id, "independent", 30)))
                thread.start()
                try:
                    claim = result.get(timeout=3)
                except queue.Empty:
                    self.fail("claim waited for unrelated locked expired task")
            thread.join(3)
        finally:
            locker.close()
        self.assertIsNotNone(claim)
        self.assertEqual(claim["task_id"], "ready")

    def test_concurrent_expiry_cleanup_preserves_single_attempt_history(self):
        self.initialize([Task("expired"), Task("ready")])
        self.store.claim_task(self.workflow_id, "crashed", 30)
        with self.store._connect() as conn:
            conn.execute("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' "
                         "WHERE workflow_id=%s AND task_id='expired'", (self.workflow_id,))
        barrier, results = threading.Barrier(2), queue.Queue()
        def claim(worker):
            barrier.wait(timeout=3)
            try:
                results.put(self.store.claim_task(self.workflow_id, worker, 30))
            except BaseException as exc:
                results.put(exc)
        threads = [threading.Thread(target=claim, args=(f"cleaner-{i}",)) for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
            self.assertFalse(thread.is_alive())
        outcomes = [results.get_nowait() for _ in threads]
        self.assertFalse([item for item in outcomes if isinstance(item, BaseException)], outcomes)
        self.assertEqual(sum(item is not None for item in outcomes), 1)
        with self.store._connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM task_attempts WHERE workflow_id=%s "
                                          "AND task_id='expired'", (self.workflow_id,)).fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT outcome FROM task_attempts WHERE workflow_id=%s "
                                          "AND task_id='expired'", (self.workflow_id,)).fetchone()[0],
                             "lease_expired")

    def test_completion_acknowledgement_loss_retries_checkpoint_without_action(self):
        self.initialize([Task("only")])
        class LostAcknowledgement:
            def __init__(self, store):
                self.store = store
                self.first = True
            def __getattr__(self, name):
                return getattr(self.store, name)
            def complete_task(self, *args):
                result = self.store.complete_task(*args)
                if self.first:
                    self.first = False
                    raise psycopg.OperationalError("connection lost after commit")
                return result
        calls = []
        worker = PostgresWorker(LostAcknowledgement(self.store), poll_interval=0.01)
        self.assertEqual(worker.run(self.workflow_id, {"only": lambda: calls.append("ran")}),
                         "succeeded")
        self.assertEqual(calls, ["ran"])
        self.assertEqual(self.task_state("only"), "succeeded")

    def test_notification_retention_cleanup_is_bounded(self):
        self.initialize([Task("retention")])
        with self.store._connect() as conn:
            conn.execute("""
                INSERT INTO notification_outbox(workflow_id,event_type,payload,created_at)
                SELECT %s,'old','{}'::jsonb,'1900-01-01 00:00:00+00'::timestamptz
                FROM generate_series(1,7)
            """, (self.workflow_id,))
        self.assertEqual(self.store.cleanup_notification_retention(limit=3), 3)
        with self.store._connect() as conn:
            count = conn.execute("SELECT count(*) FROM notification_outbox WHERE workflow_id=%s "
                                 "AND created_at < clock_timestamp()-interval '1 day'",
                                 (self.workflow_id,)).fetchone()[0]
        self.assertEqual(count, 4)

    def test_attempt_history_uniqueness_rejects_duplicate_lease_record(self):
        self.initialize([Task("only")])
        claim = self.store.claim_task(self.workflow_id, "worker", 30)
        with self.store._connect() as conn:
            with self.assertRaises(psycopg.errors.UniqueViolation):
                with conn.transaction():
                    conn.execute("""
                        INSERT INTO task_attempts(workflow_id,task_id,attempt_number,fencing_token,
                            worker_id,lease_expires_at,outcome)
                        VALUES (%s,'only',1,%s,'worker',clock_timestamp()+interval '1 second','running')
                    """, (self.workflow_id, claim["fencing_token"]))
        with self.store._connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM task_attempts WHERE workflow_id=%s",
                                          (self.workflow_id,)).fetchone()[0], 1)

    def test_wf002_migration_preserves_existing_running_lease(self):
        from psycopg.conninfo import conninfo_to_dict, make_conninfo

        schema = "migration_" + uuid.uuid4().hex
        schema_dsn = make_conninfo(**dict(conninfo_to_dict(DSN), options=f"-c search_path={schema}"))
        with psycopg.connect(DSN) as conn:
            conn.execute(f'CREATE SCHEMA "{schema}"')
            conn.execute(f'SET search_path TO "{schema}"')
            migration = Path(__file__).parents[1] / "workflow_engine" / "migrations" / "001_initial.sql"
            for statement in migration.read_text().split(";"):
                if statement.strip():
                    conn.execute(statement)
            conn.execute("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())")
            conn.execute("INSERT INTO schema_migrations(version) VALUES (1)")
            definition = {"tasks": [{"id": "legacy", "dependencies": []}]}
            conn.execute("INSERT INTO workflows(workflow_id,state,definition) VALUES (%s,'running',%s)",
                         ("legacy-wf", psycopg.types.json.Jsonb(definition)))
            conn.execute("""
                INSERT INTO tasks(workflow_id,task_id,state,attempt_count,lease_owner,
                                  lease_expires_at,fencing_token)
                VALUES ('legacy-wf','legacy','running',2,'legacy-worker',
                        clock_timestamp()+interval '1 hour',2)
            """)
        try:
            migrated = PostgresStore(schema_dsn)
            with migrated._connect() as conn:
                self.assertEqual(conn.execute("SELECT definition FROM workflows WHERE workflow_id='legacy-wf'").fetchone()[0],
                                 definition)
                task = conn.execute("SELECT state,max_attempts,attempt_count FROM tasks WHERE task_id='legacy'").fetchone()
                self.assertEqual(tuple(task), ("running", 1, 2))
                history = conn.execute("""
                    SELECT attempt_number,fencing_token,worker_id,outcome
                    FROM task_attempts WHERE task_id='legacy'
                """).fetchone()
                self.assertEqual(tuple(history), (2, 2, "legacy-worker", "running"))
                versions = conn.execute("SELECT array_agg(version ORDER BY version) FROM schema_migrations").fetchone()[0]
                self.assertEqual(versions, [1, 2, 3])
                self.assertIsNone(conn.execute(
                    "SELECT idempotency_key FROM workflows WHERE workflow_id='legacy-wf'").fetchone()[0])
        finally:
            with psycopg.connect(DSN) as conn:
                conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


if __name__ == "__main__":
    unittest.main()
