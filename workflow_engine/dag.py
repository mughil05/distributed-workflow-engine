"""DAG validation and stable topological scheduling."""

import heapq
from typing import Dict, List

from .definitions import Workflow


class InvalidWorkflow(ValueError):
    """Raised when a workflow is not a valid directed acyclic graph."""


def topological_order(workflow: Workflow) -> List[str]:
    """Return a deterministic order, choosing lexicographically among ready tasks."""
    tasks = {task.task_id: task for task in workflow.tasks}
    indegree: Dict[str, int] = {task_id: 0 for task_id in tasks}
    children: Dict[str, List[str]] = {task_id: [] for task_id in tasks}
    for task in workflow.tasks:
        for dependency in task.dependencies:
            if dependency not in tasks:
                raise InvalidWorkflow(
                    f"task {task.task_id!r} depends on unknown task {dependency!r}"
                )
            indegree[task.task_id] += 1
            children[dependency].append(task.task_id)
    ready = [task_id for task_id, count in indegree.items() if count == 0]
    heapq.heapify(ready)
    order: List[str] = []
    while ready:
        task_id = heapq.heappop(ready)
        order.append(task_id)
        for child in sorted(children[task_id]):
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, child)
    if len(order) != len(tasks):
        cyclic = sorted(task_id for task_id, count in indegree.items() if count)
        raise InvalidWorkflow(f"workflow contains a cycle involving {cyclic}")
    return order
