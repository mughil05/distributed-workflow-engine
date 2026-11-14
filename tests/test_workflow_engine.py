import multiprocessing
import os
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from workflow_engine.dag import InvalidWorkflow, topological_order
from workflow_engine.definitions import Task, Workflow
from workflow_engine.engine import WorkflowEngine
from workflow_engine.storage import SQLiteStore, WorkflowAlreadyRunning, WorkflowLockUnavailable


def _blocking_workflow_process(database, entered, release, result, workflow_id="owned"):
    store = SQLiteStore(database)
    try:
        workflow = Workflow.from_tasks(workflow_id, [Task("task")])

        def action():
            entered.set()
            if not release.wait(10):
                raise TimeoutError("test did not release task")

        result.put(WorkflowEngine(store).run(workflow, {"task": action}))
    except BaseException as exc:
        result.put((type(exc).__name__, str(exc)))
    finally:
        store.close()


def _contending_workflow_process(database, result, workflow_id="owned"):
    store = None
    try:
        store = SQLiteStore(database)
        workflow = Workflow.from_tasks(workflow_id, [Task("task")])
        result.put(WorkflowEngine(store).run(workflow, {"task": lambda: None}))
    except BaseException as exc:
        result.put((type(exc).__name__, str(exc)))
    finally:
        if store is not None:
            store.close()


def _paused_database_creator(database, created, release, result):
    original_connect = SQLiteStore._connect

    def paused_connect(path):
        connection = original_connect(path)
        created.set()
        if not release.wait(10):
            connection.close()
            raise TimeoutError("test did not release database creator")
        return connection

    SQLiteStore._connect = staticmethod(paused_connect)
    store = None
    try:
        store = SQLiteStore(database)
        result.put(("opened", bool(store.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchone())))
    except BaseException as exc:
        result.put((type(exc).__name__, str(exc)))
    finally:
        if store is not None:
            store.close()


class WorkflowTests(unittest.TestCase):
    def workflow(self, tasks, workflow_id="test"):
        return Workflow.from_tasks(workflow_id, tasks)

    def test_stable_topological_order(self):
        workflow = self.workflow([
            Task("z"), Task("b", ("z",)), Task("a"), Task("c", ("a", "b")),
        ])
        self.assertEqual(topological_order(workflow), ["a", "z", "b", "c"])

    def test_unknown_dependency_rejected(self):
        with self.assertRaisesRegex(InvalidWorkflow, "unknown task"):
            topological_order(self.workflow([Task("a", ("missing",))]))

    def test_cycle_rejected(self):
        with self.assertRaisesRegex(InvalidWorkflow, "cycle"):
            topological_order(self.workflow([Task("a", ("b",)), Task("b", ("a",))]))

    def test_duplicate_task_ids_rejected(self):
        with self.assertRaisesRegex(ValueError, "unique"):
            self.workflow([Task("a"), Task("a")])

    def test_failure_marks_dependents_failed_and_independent_work_runs(self):
        workflow = self.workflow([Task("a"), Task("b", ("a",)), Task("c")])
        called = []
        def fail():
            called.append("a")
            raise RuntimeError("boom")
        store = SQLiteStore(":memory:")
        try:
            state = WorkflowEngine(store).run(workflow, {
                "a": fail, "b": lambda: called.append("b"),
                "c": lambda: called.append("c"),
            })
            self.assertEqual(state, "failed")
            self.assertEqual(called, ["a", "c"])
            rows = store.task_rows("test")
            self.assertEqual(rows["a"]["state"], "failed")
            self.assertEqual(rows["b"]["state"], "failed")
            self.assertIn("blocked", rows["b"]["error"])
        finally:
            store.close()

    def test_sqlite_persistence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "state.sqlite")
            workflow = self.workflow([Task("a")])
            store = SQLiteStore(path)
            WorkflowEngine(store).run(workflow, {"a": lambda: None})
            store.close()
            reopened = SQLiteStore(path)
            try:
                self.assertEqual(reopened.workflow_row("test")["state"], "succeeded")
                self.assertEqual(reopened.task_rows("test")["a"]["state"], "succeeded")
            finally:
                reopened.close()

    def test_crash_recovery_never_repeats_successful_checkpoint(self):
        workflow = self.workflow([Task("first"), Task("second", ("first",))], "crash-demo")
        calls = []
        class SimulatedCrash(BaseException):
            pass
        store = SQLiteStore(":memory:")
        try:
            with self.assertRaises(SimulatedCrash):
                WorkflowEngine(store).run(workflow, {
                    "first": lambda: calls.append("first"),
                    "second": lambda: (calls.append("second"), (_ for _ in ()).throw(SimulatedCrash())),
                })
            self.assertEqual(store.task_rows("crash-demo")["first"]["state"], "succeeded")
            self.assertEqual(store.task_rows("crash-demo")["second"]["state"], "running")
            result = WorkflowEngine(store).run(workflow, {
                "first": lambda: calls.append("first"),
                "second": lambda: calls.append("second-retry"),
            })
            self.assertEqual(result, "succeeded")
            self.assertEqual(calls, ["first", "second", "second-retry"])
            self.assertEqual(store.task_rows("crash-demo")["first"]["state"], "succeeded")
        finally:
            store.close()

    def test_file_backed_crash_recovery_after_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "recovery.sqlite")
            workflow = self.workflow(
                [Task("first"), Task("second", ("first",))], "durable-recovery"
            )
            calls = []
            store = SQLiteStore(path)
            store.initialize("durable-recovery", {
                "tasks": [
                    {"id": "first", "dependencies": []},
                    {"id": "second", "dependencies": ["first"]},
                ]
            }, ["first", "second"])
            store.set_task("durable-recovery", "first", "succeeded")
            store.set_task("durable-recovery", "second", "running")
            store.set_workflow("durable-recovery", "running")
            store.close()

            reopened = SQLiteStore(path)
            try:
                result = WorkflowEngine(reopened).run(workflow, {
                    "first": lambda: calls.append("first"),
                    "second": lambda: calls.append("second-retry"),
                })
                self.assertEqual(result, "succeeded")
                self.assertEqual(calls, ["second-retry"])
            finally:
                reopened.close()

            final_store = SQLiteStore(path)
            try:
                self.assertEqual(final_store.workflow_row("durable-recovery")["state"], "succeeded")
                rows = final_store.task_rows("durable-recovery")
                self.assertEqual(rows["first"]["state"], "succeeded")
                self.assertEqual(rows["second"]["state"], "succeeded")
            finally:
                final_store.close()

    def test_workflow_lock_rejects_second_process_and_releases_on_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "ownership.sqlite")
            context = multiprocessing.get_context("spawn")
            entered = context.Event()
            release = context.Event()
            result = context.Queue()
            owner = context.Process(
                target=_blocking_workflow_process,
                args=(path, entered, release, result),
            )
            owner.start()
            self.assertTrue(entered.wait(10), "owner did not start its task")

            contender_result = context.Queue()
            contender = context.Process(
                target=_contending_workflow_process,
                args=(path, contender_result),
            )
            contender.start()
            contender.join(10)
            self.assertFalse(contender.is_alive(), "contender did not exit")
            outcome = contender_result.get(timeout=2)
            self.assertEqual(outcome[0], "WorkflowAlreadyRunning")

            release.set()
            owner.join(10)
            self.assertFalse(owner.is_alive(), "owner did not exit")
            self.assertEqual(result.get(timeout=2), "succeeded")

    def _assert_contended(self, database, workflow_id="owned"):
        context = multiprocessing.get_context("spawn")
        result = context.Queue()
        contender = context.Process(target=_contending_workflow_process,
                                    args=(str(database), result, workflow_id))
        contender.start()
        contender.join(10)
        self.assertFalse(contender.is_alive(), "contender did not exit")
        self.assertEqual(result.get(timeout=2)[0], "WorkflowAlreadyRunning")

    def test_database_ownership_is_acquired_before_schema_initialization(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "held.sqlite"
            context = multiprocessing.get_context("spawn")
            entered, release, result = context.Event(), context.Event(), context.Queue()
            owner = context.Process(target=_blocking_workflow_process,
                                    args=(str(database), entered, release, result))
            owner.start()
            self.assertTrue(entered.wait(10))
            self._assert_contended(database, "different-id")
            release.set()
            owner.join(10)
            self.assertFalse(owner.is_alive())
            self.assertEqual(result.get(timeout=2), "succeeded")

    def test_same_database_different_workflow_ids_contend_at_store_open(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite"
            owner = SQLiteStore(str(database))
            try:
                context = multiprocessing.get_context("spawn")
                result = context.Queue()
                contender = context.Process(target=_contending_workflow_process,
                                            args=(str(database), result, "other-workflow"))
                contender.start(); contender.join(10)
                self.assertFalse(contender.is_alive())
                self.assertEqual(result.get(timeout=2)[0], "WorkflowAlreadyRunning")
            finally:
                owner.close()

    def _make_hard_link(self, source, destination):
        try:
            os.link(source, destination)
        except (NotImplementedError, OSError) as exc:
            self.skipTest(f"hard links unavailable: {exc}")

    def test_existing_hard_link_is_rejected_before_schema_initialization(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite"
            alias = Path(directory) / "alias.sqlite"
            connection = __import__("sqlite3").connect(database)
            connection.close()
            self._make_hard_link(database, alias)

            with self.assertRaisesRegex(WorkflowLockUnavailable, "hard-linked database files are unsupported"):
                SQLiteStore(str(database))
            connection = __import__("sqlite3").connect(database)
            try:
                tables = {row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
                self.assertEqual(tables, set())
            finally:
                connection.close()

    def test_wal_database_hard_link_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "wal.sqlite"
            store = SQLiteStore(str(database))
            try:
                store.connection.execute("PRAGMA journal_mode=WAL")
            finally:
                store.close()
            self._make_hard_link(database, Path(directory) / "wal-alias.sqlite")
            with self.assertRaisesRegex(WorkflowLockUnavailable, "including in WAL mode"):
                SQLiteStore(str(database))

    def test_hard_link_created_while_store_open_is_rejected_before_operation(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "open.sqlite"
            alias = Path(directory) / "new-alias.sqlite"
            store = SQLiteStore(str(database))
            self._make_hard_link(database, alias)
            try:
                with self.assertRaisesRegex(WorkflowLockUnavailable, "hard-linked database files are unsupported"):
                    store.initialize("must-not-run", {"tasks": []}, [])
                self.assertIsNone(store.connection.execute(
                    "SELECT workflow_id FROM workflows WHERE workflow_id='must-not-run'").fetchone())
            finally:
                store.close()

    def test_concurrent_first_creation_is_serialized(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "new.sqlite"
            context = multiprocessing.get_context("spawn")
            entered, release, result = context.Event(), context.Event(), context.Queue()
            owner = context.Process(target=_blocking_workflow_process,
                                    args=(str(database), entered, release, result))
            owner.start()
            self.assertTrue(entered.wait(10))
            self._assert_contended(database)
            release.set(); owner.join(10)
            self.assertFalse(owner.is_alive())
            self.assertEqual(result.get(timeout=2), "succeeded")

    def test_contender_cannot_bypass_creator_bootstrap_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "bootstrap-race.sqlite"
            context = multiprocessing.get_context("spawn")
            created, release, result = context.Event(), context.Event(), context.Queue()
            creator = context.Process(target=_paused_database_creator,
                                      args=(str(database), created, release, result))
            creator.start()
            self.assertTrue(created.wait(10), "creator did not create the SQLite file")
            self.assertTrue(database.exists())

            contender_result = context.Queue()
            contender = context.Process(target=_contending_workflow_process,
                                        args=(str(database), contender_result, "racer"))
            contender.start()
            contender.join(10)
            self.assertFalse(contender.is_alive(), "contender did not exit")
            self.assertEqual(contender_result.get(timeout=2)[0], "WorkflowAlreadyRunning")

            release.set()
            creator.join(10)
            self.assertFalse(creator.is_alive(), "creator did not exit")
            self.assertEqual(result.get(timeout=2)[0], "opened")

    def test_termination_releases_database_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "terminated.sqlite"
            context = multiprocessing.get_context("spawn")
            entered, release, result = context.Event(), context.Event(), context.Queue()
            owner = context.Process(target=_blocking_workflow_process,
                                    args=(str(database), entered, release, result))
            owner.start()
            self.assertTrue(entered.wait(10))
            owner.terminate(); owner.join(10)
            self.assertFalse(owner.is_alive())
            reacquirer_result = context.Queue()
            reacquirer = context.Process(target=_contending_workflow_process,
                                         args=(str(database), reacquirer_result))
            reacquirer.start(); reacquirer.join(10)
            self.assertFalse(reacquirer.is_alive())
            self.assertEqual(reacquirer_result.get(timeout=2), "succeeded")

    def test_database_replacement_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "replace.sqlite"
            replacement = Path(directory) / "replacement.sqlite"
            store = SQLiteStore(str(database))
            other = SQLiteStore(str(replacement)); other.close()
            moved = Path(directory) / "old.sqlite"
            os.replace(database, moved)
            os.replace(replacement, database)
            try:
                with self.assertRaisesRegex(Exception, "identity changed"):
                    store.task_rows("any")
            finally:
                store.close()

    def test_invalid_lock_location_fails_before_database_open(self):
        with tempfile.TemporaryDirectory() as directory:
            lock_dir = Path(directory) / "locks-link"
            target = Path(directory) / "target"
            target.mkdir()
            try:
                os.symlink(target, lock_dir)
            except (NotImplementedError, OSError) as exc:
                self.skipTest(f"symlinks unavailable: {exc}")
            database = Path(directory) / "must-not-exist.sqlite"
            previous = os.environ.get(SQLiteStore.LOCK_DIR_ENV)
            os.environ[SQLiteStore.LOCK_DIR_ENV] = str(lock_dir)
            try:
                with self.assertRaises(Exception):
                    SQLiteStore(str(database))
                self.assertFalse(database.exists())
            finally:
                if previous is None:
                    os.environ.pop(SQLiteStore.LOCK_DIR_ENV, None)
                else:
                    os.environ[SQLiteStore.LOCK_DIR_ENV] = previous

    def test_initialization_exception_releases_lock_and_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "failed-init.sqlite"
            with mock.patch("workflow_engine.storage.SQLiteStore._connect",
                            side_effect=RuntimeError("injected connect failure")):
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    SQLiteStore(str(database))
            reopened = SQLiteStore(str(database))
            reopened.close()

    def test_database_identity_is_stable_for_relative_and_symlink_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite"
            symlink = Path(directory) / "state-symlink.sqlite"
            setup = SQLiteStore(str(database))
            expected_identity = setup.database_identity
            setup.close()
            try:
                os.symlink(database, symlink)
            except (NotImplementedError, OSError) as exc:
                self.skipTest(f"symlinks unavailable: {exc}")

            relative_path = os.path.relpath(database, Path.cwd())
            relative_store = SQLiteStore(relative_path)
            self.assertEqual(relative_store.database_identity, expected_identity)
            relative_store.close()
            symlink_store = SQLiteStore(str(symlink))
            try:
                self.assertEqual(symlink_store.database_identity, expected_identity)
            finally:
                symlink_store.close()

    def test_symlink_alias_contends_with_open_store(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite"
            alias = Path(directory) / "state-link.sqlite"
            owner = SQLiteStore(str(database))
            try:
                os.symlink(database, alias)
            except (NotImplementedError, OSError) as exc:
                owner.close()
                self.skipTest(f"symlinks unavailable: {exc}")
            try:
                self._assert_contended(alias)
            finally:
                owner.close()


if __name__ == "__main__":
    unittest.main()
