"""Sequential execution with durable checkpoints and safe restart behavior."""

from typing import Callable, Dict, Optional

from .dag import topological_order
from .definitions import Workflow
from .store import WorkflowStore
from .retries import RetryPolicy


class WorkflowEngine:
    def __init__(self, store: WorkflowStore) -> None:
        self.store = store

    def run(self, workflow: Workflow, actions: Dict[str, Callable[[], object]]) -> str:
        # Ownership must be acquired before recovery can reset interrupted tasks.
        with self.store.workflow_lock(workflow.workflow_id):
            return self._run_owned(workflow, actions)

    def _run_owned(self, workflow: Workflow, actions: Dict[str, Callable[[], object]]) -> str:
        if any(task.retry_policy != RetryPolicy() for task in workflow.tasks):
            raise ValueError("SQLite execution does not support PostgreSQL retry policies")
        order = topological_order(workflow)
        task_map = {task.task_id: task for task in workflow.tasks}
        definition = {"tasks": [{"id": t.task_id,
                                  "dependencies": list(t.dependencies)}
                                 for t in workflow.tasks]}
        self.store.initialize(workflow.workflow_id, definition, order)
        self.store.recover_running(workflow.workflow_id)
        rows = self.store.task_rows(workflow.workflow_id)
        # A previous ordinary failure remains terminal until an explicit new workflow ID.
        if any(row["state"] == "failed" for row in rows.values()):
            self.store.set_workflow(workflow.workflow_id, "failed", "previous run failed")
            return "failed"
        self.store.set_workflow(workflow.workflow_id, "running")
        for task_id in order:
            state = self.store.task_rows(workflow.workflow_id)[task_id]["state"]
            if state == "succeeded":
                continue
            task = task_map[task_id]
            failed_dependency = next((dep for dep in task.dependencies
                                      if self.store.task_rows(workflow.workflow_id)[dep]["state"] != "succeeded"), None)
            if failed_dependency:
                reason = f"blocked by unsuccessful dependency {failed_dependency!r}"
                self.store.set_task(workflow.workflow_id, task_id, "failed", reason)
                continue
            if task_id not in actions:
                error = f"no action registered for task {task_id!r}"
                self.store.set_task(workflow.workflow_id, task_id, "failed", error)
                continue
            self.store.set_task(workflow.workflow_id, task_id, "running")
            try:
                actions[task_id]()
            except Exception as exc:
                self.store.set_task(workflow.workflow_id, task_id, "failed",
                                    f"{type(exc).__name__}: {exc}")
            else:
                self.store.set_task(workflow.workflow_id, task_id, "succeeded")
        failed = [task_id for task_id, row in self.store.task_rows(workflow.workflow_id).items()
                  if row["state"] == "failed"]
        if failed:
            self.store.set_workflow(workflow.workflow_id, "failed",
                                    "failed tasks: " + ", ".join(sorted(failed)))
            return "failed"
        self.store.set_workflow(workflow.workflow_id, "succeeded")
        return "succeeded"
