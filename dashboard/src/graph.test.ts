import { describe, expect, it } from 'vitest';
import { buildTaskGraph } from './graph';
import type { TaskRow } from './api';

const tasks = [
  { task_id: 'z', state: 'succeeded', dependencies: ['a'], attempt_count: 1 },
  { task_id: 'a', state: 'running', dependencies: [], attempt_count: 2 },
] as TaskRow[];

describe('read-only DAG projection', () => {
  it('preserves dependency direction, layers, status and selection', () => {
    const { nodes, edges } = buildTaskGraph(tasks, 'z');
    expect(edges).toHaveLength(1);
    expect(edges[0]).toMatchObject({ source: 'a', target: 'z' });
    expect(nodes.find((node) => node.id === 'z')?.position.x).toBeGreaterThan(nodes.find((node) => node.id === 'a')?.position.x ?? 0);
    expect(nodes.find((node) => node.id === 'z')?.data.selected).toBe(true);
    expect(nodes.find((node) => node.id === 'a')?.data.state).toBe('running');
  });

  it('lays out task IDs deterministically independent of API row order', () => {
    const first = buildTaskGraph(tasks, null);
    const second = buildTaskGraph([...tasks].reverse(), null);
    const positions = (nodes: typeof first.nodes) => Object.fromEntries(
      nodes.map(({ id, position }) => [id, position]).sort(([a], [b]) => String(a).localeCompare(String(b))));
    expect(positions(first.nodes)).toEqual(positions(second.nodes));
  });
});
