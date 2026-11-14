import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import App from './App';

vi.mock('@xyflow/react', () => ({
  ReactFlow: ({ nodes, onNodeClick }: { nodes: Array<{ id: string }>; onNodeClick: (_event: unknown, node: { id: string }) => void }) => <div>{nodes.map((node) => <button key={node.id} onClick={(event) => onNodeClick(event, node)}>{node.id}</button>)}</div>,
  Background: () => null,
  Controls: () => null,
  Handle: () => null,
  Position: { Left: 'left', Right: 'right' },
}));

const summary = { workflows: { pending: 0, running: 0, succeeded: 1, failed: 0 }, attempts: {}, active_leases: 0, retry_scheduled_attempts: 0, dead_lettered_tasks: 0, snapshot_as_of: '2026-01-01T00:00:00Z', snapshot_age_seconds: 1, snapshot_stale: false };
const workflow = { workflow_id: 'wf-1', state: 'succeeded', created_at: '2026-01-01T00:00:00Z', updated_at: '2026-01-01T00:01:00Z' };
function json(data: unknown, status = 200) { return Promise.resolve(new Response(JSON.stringify(data), { status, headers: { 'Content-Type': 'application/json' } })); }

afterEach(() => { vi.restoreAllMocks(); });

describe('dashboard', () => {
  it('shows actual workflow, summary, tasks and attempt history', async () => {
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes('/summary')) return json(summary);
      if (url.includes('/health/ready')) return json({ status: 'ready', dependencies: { postgres: 'ok', redis: 'disabled' } });
      if (url.endsWith('/api/v1/workflows?limit=25')) return json({ items: [workflow], next_cursor: null });
      if (url.endsWith('/api/v1/workflows/wf-1')) return json({ ...workflow, error: null, task_counts: { pending: 0, running: 0, succeeded: 1, failed: 0, dead_lettered: 0 }, task_count: 1 });
      if (url.includes('/tasks?')) return json({ items: [{ task_id: 'hello', state: 'succeeded', dependencies: [], attempt_count: 1, max_attempts: 3, retry_eligible_at: null, dead_lettered_at: null, error: null }], next_cursor: null });
      if (url.includes('/attempts')) return json({ items: [{ attempt_number: 1, claimed_at: '2026-01-01T00:00:00Z', lease_expires_at: '2026-01-01T00:01:00Z', finished_at: '2026-01-01T00:00:02Z', outcome: 'succeeded', error: null }], next_cursor: null });
      return json({ error: { code: 'not_found', message: 'Missing.' } }, 404);
    }));
    render(<App />);
    expect(await screen.findByText('wf-1')).toBeTruthy();
    expect(screen.getByText('TOTAL WORKFLOWS').parentElement?.querySelector('strong')?.textContent).toBe('1');
    fireEvent.click(screen.getByText('wf-1'));
    expect(await screen.findByText('Dependency graph')).toBeTruthy();
    fireEvent.click(await screen.findByText('hello'));
    expect(await screen.findByText(/Attempt 1 · succeeded/)).toBeTruthy();
  });

  it('shows sanitized API outages with retry affordance', async () => {
    vi.stubGlobal('fetch', vi.fn(() => json({ error: { code: 'postgres_unavailable', message: 'PostgreSQL is unavailable.' } }, 503)));
    render(<App />);
    expect(await screen.findByRole('alert')).toHaveTextContent('PostgreSQL is unavailable.');
    expect(screen.getByText('Retry')).toBeTruthy();
  });

  it('aborts stale list requests after a filter changes', async () => {
    const signals: AbortSignal[] = [];
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      if (init?.signal) signals.push(init.signal as AbortSignal);
      const url = String(input);
      if (url.includes('/summary')) return json(summary);
      if (url.includes('/health/ready')) return json({ status: 'ready', dependencies: { postgres: 'ok', redis: 'disabled' } });
      if (!url.includes('state=') && init?.signal) return new Promise((_resolve, reject) => {
        (init.signal as AbortSignal).addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), { once: true });
      });
      return json({ items: [], next_cursor: null });
    }));
    const { container } = render(<App />);
    await waitFor(() => expect(signals.length).toBeGreaterThanOrEqual(1));
    fireEvent.click(container.querySelector('.filters button:last-child')!);
    await waitFor(() => expect(signals.some((signal) => signal.aborted)).toBe(true));
    await waitFor(() => expect(fetch).toHaveBeenCalledWith(expect.stringContaining('state=failed'), expect.anything()));
  });

  it('cancels workflow detail responses when selection changes quickly', async () => {
    let firstDetailSignal: AbortSignal | undefined;
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes('/summary')) return json(summary);
      if (url.includes('/health/ready')) return json({ status: 'ready', dependencies: { postgres: 'ok', redis: 'disabled' } });
      if (url.includes('/api/v1/workflows?')) return json({ items: [workflow, { ...workflow, workflow_id: 'wf-2' }], next_cursor: null });
      if (url.endsWith('/api/v1/workflows/wf-1')) {
        firstDetailSignal = init?.signal as AbortSignal;
        return new Promise((_resolve, reject) => firstDetailSignal?.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), { once: true }));
      }
      if (url.endsWith('/api/v1/workflows/wf-2')) return json({ ...workflow, workflow_id: 'wf-2', error: null, task_counts: { pending: 0, running: 0, succeeded: 1, failed: 0, dead_lettered: 0 }, task_count: 1 });
      if (url.includes('/tasks?')) return json({ items: [], next_cursor: null });
      return json({ items: [], next_cursor: null });
    }));
    render(<App />);
    fireEvent.click(await screen.findByText('wf-1'));
    await waitFor(() => expect(firstDetailSignal).toBeDefined());
    fireEvent.click(screen.getByText('← All workflows'));
    fireEvent.click(await screen.findByText('wf-2'));
    expect(await screen.findByText('Dependency graph')).toBeTruthy();
    expect(firstDetailSignal?.aborted).toBe(true);
    expect(screen.getByText('wf-2')).toBeTruthy();
  });

  it('loads subsequent durable attempt pages on demand', async () => {
    const attemptUrls: string[] = [];
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes('/summary')) return json(summary);
      if (url.includes('/health/ready')) return json({ status: 'ready', dependencies: { postgres: 'ok', redis: 'disabled' } });
      if (url.includes('/api/v1/workflows?')) return json({ items: [workflow], next_cursor: null });
      if (url.endsWith('/api/v1/workflows/wf-1')) return json({ ...workflow, error: null, task_counts: { pending: 0, running: 0, succeeded: 1, failed: 0, dead_lettered: 0 }, task_count: 1 });
      if (url.includes('/tasks?')) return json({ items: [{ task_id: 'hello', state: 'succeeded', dependencies: [], attempt_count: 2, max_attempts: 3, retry_eligible_at: null, dead_lettered_at: null, error: null }], next_cursor: null });
      if (url.includes('/attempts')) {
        attemptUrls.push(url);
        return url.includes('cursor=')
          ? json({ items: [{ attempt_number: 2, claimed_at: '2026-01-01T00:00:02Z', lease_expires_at: '2026-01-01T00:01:00Z', finished_at: '2026-01-01T00:00:03Z', outcome: 'succeeded', error: null }], next_cursor: null })
          : json({ items: [{ attempt_number: 1, claimed_at: '2026-01-01T00:00:00Z', lease_expires_at: '2026-01-01T00:01:00Z', finished_at: '2026-01-01T00:00:01Z', outcome: 'retry_scheduled', error: null }], next_cursor: 'attempt-cursor' });
      }
      return json({ items: [], next_cursor: null });
    }));
    render(<App />);
    fireEvent.click(await screen.findByText('wf-1'));
    fireEvent.click(await screen.findByText('hello'));
    expect(await screen.findByText(/Attempt 1 · retry_scheduled/)).toBeTruthy();
    fireEvent.click(await screen.findByRole('button', { name: 'Load more attempts' }));
    expect(await screen.findByText(/Attempt 2 · succeeded/)).toBeTruthy();
    expect(attemptUrls.some((url) => url.includes('cursor=attempt-cursor'))).toBe(true);
  });

  it('cancels a pending attempt page when task selection changes', async () => {
    let nextPageSignal: AbortSignal | undefined;
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes('/summary')) return json(summary);
      if (url.includes('/health/ready')) return json({ status: 'ready', dependencies: { postgres: 'ok', redis: 'disabled' } });
      if (url.includes('/api/v1/workflows?')) return json({ items: [workflow], next_cursor: null });
      if (url.endsWith('/api/v1/workflows/wf-1')) return json({ ...workflow, error: null, task_counts: { pending: 0, running: 0, succeeded: 2, failed: 0, dead_lettered: 0 }, task_count: 2 });
      if (url.includes('/tasks?')) return json({ items: ['task-one', 'task-two'].map((task_id) => ({ task_id, state: 'succeeded', dependencies: [], attempt_count: 2, max_attempts: 3, retry_eligible_at: null, dead_lettered_at: null, error: null })), next_cursor: null });
      if (url.includes('/attempts') && url.includes('cursor=')) {
        nextPageSignal = init?.signal as AbortSignal;
        return new Promise((_resolve, reject) => nextPageSignal?.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), { once: true }));
      }
      if (url.includes('/attempts')) return url.includes('/task-two/')
        ? json({ items: [{ attempt_number: 1, claimed_at: '2026-01-01T00:00:04Z', lease_expires_at: '2026-01-01T00:01:00Z', finished_at: '2026-01-01T00:00:05Z', outcome: 'succeeded', error: null }], next_cursor: null })
        : json({ items: [{ attempt_number: 1, claimed_at: '2026-01-01T00:00:00Z', lease_expires_at: '2026-01-01T00:01:00Z', finished_at: '2026-01-01T00:00:01Z', outcome: 'retry_scheduled', error: null }], next_cursor: 'more' });
      return json({ items: [], next_cursor: null });
    }));
    render(<App />);
    fireEvent.click(await screen.findByText('wf-1'));
    fireEvent.click(await screen.findByText('task-one'));
    await screen.findByRole('button', { name: 'Load more attempts' });
    fireEvent.click(screen.getByRole('button', { name: 'Load more attempts' }));
    await waitFor(() => expect(nextPageSignal).toBeDefined());
    fireEvent.click(screen.getByText('task-two'));
    expect(nextPageSignal?.aborted).toBe(true);
    expect(await screen.findByText(/Attempt 1 · succeeded/)).toBeTruthy();
  });

  it('preserves loaded attempt pages across refresh and resets them on task switch', async () => {
    vi.useFakeTimers();
    let firstTaskPageLoads = 0;
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes('/summary')) return json(summary);
      if (url.includes('/health/ready')) return json({ status: 'ready', dependencies: { postgres: 'ok', redis: 'disabled' } });
      if (url.includes('/api/v1/workflows?')) return json({ items: [workflow], next_cursor: null });
      if (url.endsWith('/api/v1/workflows/wf-1')) return json({ ...workflow, error: null, task_counts: { pending: 0, running: 0, succeeded: 2, failed: 0, dead_lettered: 0 }, task_count: 2 });
      if (url.includes('/tasks?')) return json({ items: ['task-one', 'task-two'].map((task_id) => ({ task_id, state: 'succeeded', dependencies: [], attempt_count: 2, max_attempts: 3, retry_eligible_at: null, dead_lettered_at: null, error: null })), next_cursor: null });
      if (url.includes('/task-one/attempts') && url.includes('cursor=')) return json({ items: [{ attempt_number: 2, claimed_at: '2026-01-01T00:00:02Z', lease_expires_at: '2026-01-01T00:01:00Z', finished_at: '2026-01-01T00:00:03Z', outcome: 'succeeded', error: null }], next_cursor: null });
      if (url.includes('/task-one/attempts')) {
        firstTaskPageLoads += 1;
        const outcome = firstTaskPageLoads === 1 ? 'running' : 'succeeded';
        return json({ items: [{ attempt_number: 1, claimed_at: '2026-01-01T00:00:00Z', lease_expires_at: '2026-01-01T00:01:00Z', finished_at: outcome === 'running' ? null : '2026-01-01T00:00:01Z', outcome, error: null }], next_cursor: 'page-two' });
      }
      if (url.includes('/task-two/attempts')) return json({ items: [{ attempt_number: 1, claimed_at: '2026-01-01T00:00:04Z', lease_expires_at: '2026-01-01T00:01:00Z', finished_at: '2026-01-01T00:00:05Z', outcome: 'dead_lettered', error: null }], next_cursor: null });
      return json({ items: [], next_cursor: null });
    }));

    const { unmount } = render(<App />);
    try {
      const flush = async () => act(async () => { for (let i = 0; i < 12; i += 1) await Promise.resolve(); });
      await flush();
      fireEvent.click(screen.getByText('wf-1'));
      await flush();
      fireEvent.click(screen.getByText('task-one'));
      await flush();
      expect(screen.getByText(/Attempt 1 · running/)).toBeTruthy();
      fireEvent.click(screen.getByRole('button', { name: 'Load more attempts' }));
      await flush();
      expect(screen.getByText(/Attempt 2 · succeeded/)).toBeTruthy();

      await act(async () => { await vi.advanceTimersByTimeAsync(15000); });
      await flush();
      expect(screen.getByText(/Attempt 1 · succeeded/)).toBeTruthy();
      expect(screen.getByText(/Attempt 2 · succeeded/)).toBeTruthy();
      expect(screen.getAllByText(/Attempt 1 · succeeded/)).toHaveLength(1);

      fireEvent.click(screen.getByText('task-two'));
      await flush();
      expect(screen.getByText(/Attempt 1 · dead_lettered/)).toBeTruthy();
      expect(screen.queryByText(/Attempt 2 · succeeded/)).toBeNull();
      expect(screen.queryByText(/Attempt 1 · succeeded/)).toBeNull();
    } finally {
      unmount();
      vi.useRealTimers();
    }
  });

  it('keeps a single list polling chain across visibility changes', async () => {
    vi.useFakeTimers();
    const summaries = vi.fn();
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes('/summary')) { summaries(); return json(summary); }
      if (url.includes('/health/ready')) return json({ status: 'ready', dependencies: { postgres: 'ok', redis: 'disabled' } });
      return json({ items: [], next_cursor: null });
    }));
    const { unmount } = render(<App />);
    await act(async () => { await Promise.resolve(); await Promise.resolve(); });
    expect(summaries).toHaveBeenCalledTimes(1);
    Object.defineProperty(document, 'hidden', { configurable: true, value: true });
    document.dispatchEvent(new Event('visibilitychange'));
    Object.defineProperty(document, 'hidden', { configurable: true, value: false });
    document.dispatchEvent(new Event('visibilitychange'));
    await act(async () => { await Promise.resolve(); await Promise.resolve(); });
    expect(summaries).toHaveBeenCalledTimes(2);
    await act(async () => { vi.advanceTimersByTime(15000); await Promise.resolve(); await Promise.resolve(); });
    expect(summaries).toHaveBeenCalledTimes(3);
    unmount();
    vi.useRealTimers();
    Object.defineProperty(document, 'hidden', { configurable: true, value: false });
  });
});
