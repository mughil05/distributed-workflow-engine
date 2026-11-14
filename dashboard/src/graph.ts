import type { Edge, Node } from '@xyflow/react';
import type { TaskRow } from './api';

export interface TaskNodeData extends Record<string, unknown> {
  label: string;
  state: string;
  attempts: number;
  selected: boolean;
}

export function buildTaskGraph(
  tasks: TaskRow[],
  selectedTaskId: string | null,
): { nodes: Node<TaskNodeData>[]; edges: Edge[] } {
  const byId = new Map(tasks.map((task) => [task.task_id, task]));
  const remaining = new Map(tasks.map((task) => [task.task_id, task.dependencies.length]));
  const children = new Map<string, string[]>();
  for (const task of tasks) {
    for (const dependency of task.dependencies) {
      const next = children.get(dependency) ?? [];
      next.push(task.task_id);
      children.set(dependency, next);
    }
  }

  const depth = new Map<string, number>();
  const ready = tasks.filter((task) => (remaining.get(task.task_id) ?? 0) === 0)
    .map((task) => task.task_id).sort();
  while (ready.length) {
    const current = ready.shift()!;
    for (const child of (children.get(current) ?? []).sort()) {
      depth.set(child, Math.max(depth.get(child) ?? 0, (depth.get(current) ?? 0) + 1));
      remaining.set(child, (remaining.get(child) ?? 1) - 1);
      if (remaining.get(child) === 0) {
        ready.push(child);
        ready.sort();
      }
    }
  }

  const byLayer = new Map<number, string[]>();
  for (const task of tasks) {
    const layer = depth.get(task.task_id) ?? 0;
    byLayer.set(layer, [...(byLayer.get(layer) ?? []), task.task_id]);
  }
  for (const ids of byLayer.values()) ids.sort();
  const layerIndex = new Map<string, number>();
  for (const ids of byLayer.values()) ids.forEach((id, index) => layerIndex.set(id, index));

  const nodes = tasks.map((task) => ({
    id: task.task_id,
    type: 'task' as const,
    position: {
      x: 80 + (depth.get(task.task_id) ?? 0) * 250,
      y: 55 + (layerIndex.get(task.task_id) ?? 0) * 105,
    },
    data: {
      label: task.task_id,
      state: task.state,
      attempts: task.attempt_count,
      selected: task.task_id === selectedTaskId,
    },
    draggable: false,
    selectable: true,
  }));

  const edges = tasks.flatMap((task) => task.dependencies
    .filter((dependency) => byId.has(dependency))
    .map((dependency) => ({
      id: `${dependency}->${task.task_id}`,
      source: dependency,
      target: task.task_id,
      animated: task.state === 'running',
      style: { stroke: '#465865', strokeWidth: 1.6 },
    })));
  return { nodes, edges };
}
