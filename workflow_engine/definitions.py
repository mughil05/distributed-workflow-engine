"""Immutable workflow definition objects."""

from dataclasses import dataclass
from typing import Iterable, Tuple

from .retries import RetryPolicy


@dataclass(frozen=True)
class Task:
    """A named unit of work and the task IDs that must finish before it."""

    task_id: str
    dependencies: Tuple[str, ...] = ()
    retry_policy: RetryPolicy = RetryPolicy()

    def __post_init__(self) -> None:
        if not self.task_id or not self.task_id.strip():
            raise ValueError("task_id must be a non-empty string")
        if len(set(self.dependencies)) != len(self.dependencies):
            raise ValueError(f"task {self.task_id!r} has duplicate dependencies")
        if self.task_id in self.dependencies:
            raise ValueError(f"task {self.task_id!r} cannot depend on itself")
        if not isinstance(self.retry_policy, RetryPolicy):
            raise TypeError("retry_policy must be a RetryPolicy")


@dataclass(frozen=True)
class Workflow:
    workflow_id: str
    tasks: Tuple[Task, ...]

    def __post_init__(self) -> None:
        if not self.workflow_id or not self.workflow_id.strip():
            raise ValueError("workflow_id must be a non-empty string")
        ids = [task.task_id for task in self.tasks]
        if len(set(ids)) != len(ids):
            raise ValueError("task IDs must be unique within a workflow")

    @classmethod
    def from_tasks(cls, workflow_id: str, tasks: Iterable[Task]) -> "Workflow":
        return cls(workflow_id, tuple(tasks))
