export type WorkflowState = 'pending' | 'running' | 'succeeded' | 'failed';
export type TaskState = WorkflowState | 'dead_lettered';

export interface WorkflowRow {
  workflow_id: string;
  state: WorkflowState;
  created_at: string;
  updated_at: string;
}

export interface WorkflowPage {
  items: WorkflowRow[];
  next_cursor: string | null;
}

export interface WorkflowDetail extends WorkflowRow {
  error: { code: string; message: string } | null;
  task_counts: Record<TaskState, number>;
  task_count: number;
}

export interface TaskRow {
  task_id: string;
  state: TaskState;
  dependencies: string[];
  attempt_count: number;
  max_attempts: number;
  retry_eligible_at: string | null;
  dead_lettered_at: string | null;
  error: { code: string; message: string } | null;
}

export interface TaskPage {
  items: TaskRow[];
  next_cursor: string | null;
}

export interface Attempt {
  attempt_number: number;
  claimed_at: string;
  lease_expires_at: string;
  finished_at: string | null;
  outcome: string;
  error: { code: string; message: string } | null;
}

export interface AttemptPage {
  items: Attempt[];
  next_cursor: string | null;
}

export interface DashboardSummary {
  workflows: Record<WorkflowState, number>;
  attempts: Record<string, number>;
  active_leases: number;
  retry_scheduled_attempts: number;
  dead_lettered_tasks: number;
  snapshot_as_of: string;
  snapshot_age_seconds: number;
  snapshot_stale: boolean;
}

export interface Readiness {
  status: 'ready' | 'degraded' | 'not_ready';
  dependencies: {
    postgres: 'ok' | 'unavailable';
    redis: 'ok' | 'disabled' | 'degraded' | 'unknown';
    redis_required?: false;
  };
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export async function requestJson<T>(
  path: string,
  token: string,
  signal?: AbortSignal,
): Promise<T> {
  const controller = new AbortController();
  const onAbort = () => controller.abort();
  signal?.addEventListener('abort', onAbort, { once: true });
  const timeout = window.setTimeout(() => controller.abort(), 8000);
  try {
    const headers = new Headers({ Accept: 'application/json' });
    if (token) headers.set('Authorization', `Bearer ${token}`);
    const response = await fetch(path, { headers, signal: controller.signal });
    if (!response.ok) {
      let code = 'request_failed';
      let message = response.status === 401
        ? 'Enter the configured API token to connect.'
        : `The API returned ${response.status}.`;
      try {
        const body = await response.json();
        code = body?.error?.code ?? code;
        message = body?.error?.message ?? message;
      } catch {
        // Keep the stable, generic error when the server returns no JSON body.
      }
      throw new ApiError(message, response.status, code);
    }
    return await response.json() as T;
  } catch (error) {
    if (controller.signal.aborted && !signal?.aborted) {
      throw new ApiError('The request timed out. Try refreshing.', 408, 'request_timeout');
    }
    throw error;
  } finally {
    window.clearTimeout(timeout);
    signal?.removeEventListener('abort', onAbort);
  }
}

export async function getAllTasks(
  workflowId: string,
  token: string,
  signal: AbortSignal,
): Promise<TaskRow[]> {
  const tasks: TaskRow[] = [];
  let cursor: string | null = null;
  do {
    const query = new URLSearchParams({ limit: '100' });
    if (cursor) query.set('cursor', cursor);
    const page = await requestJson<TaskPage>(
      `/api/v1/workflows/${encodeURIComponent(workflowId)}/tasks?${query}`,
      token,
      signal,
    );
    tasks.push(...page.items);
    cursor = page.next_cursor;
  } while (cursor && tasks.length <= 500);
  return tasks;
}
