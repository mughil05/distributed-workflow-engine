import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ReactFlow, Background, Controls, Handle, Position, type Node, type NodeProps } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { ApiError, getAllTasks, requestJson, type AttemptPage, type DashboardSummary, type Readiness, type TaskRow, type WorkflowDetail, type WorkflowPage, type WorkflowRow } from './api';
import { buildTaskGraph, type TaskNodeData } from './graph';

const colors: Record<string, string> = { succeeded: '#26836f', running: '#bd7c25', failed: '#be5360', dead_lettered: '#ad4653', pending: '#84929b' };
function TaskNode({ data }: NodeProps) {
  const item = data as TaskNodeData;
  return <div className={`task-node ${item.selected ? 'selected' : ''}`} style={{ borderColor: colors[item.state] ?? colors.pending }}>
    <Handle type="target" position={Position.Left} />
    <strong>{item.label}</strong><span>{item.state.replace('_', ' ')} · {item.attempts} attempts</span>
    <Handle type="source" position={Position.Right} />
  </div>;
}
const nodeTypes = { task: TaskNode };
const states = ['all', 'pending', 'running', 'succeeded', 'failed'] as const;

export default function App() {
  const [token, setToken] = useState('');
  const [authDraft, setAuthDraft] = useState('');
  const [workflows, setWorkflows] = useState<WorkflowRow[]>([]);
  const [summary, setSummary] = useState<DashboardSummary | null>(null);
  const [readiness, setReadiness] = useState<Readiness | null>(null);
  const [filter, setFilter] = useState<string>('all');
  const [cursor, setCursor] = useState<string | null>(null);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<WorkflowDetail | null>(null);
  const [tasks, setTasks] = useState<TaskRow[]>([]);
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [attempts, setAttempts] = useState<AttemptPage['items']>([]);
  const attemptsRef = useRef<AttemptPage['items']>([]);
  const [attemptNextCursor, setAttemptNextCursor] = useState<string | null>(null);
  const [loadingMoreAttempts, setLoadingMoreAttempts] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [authRequired, setAuthRequired] = useState(false);
  const [busy, setBusy] = useState(false);
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [summaryReceivedAt, setSummaryReceivedAt] = useState(0);
  const [clock, setClock] = useState(Date.now());
  const [detailRefresh, setDetailRefresh] = useState(0);
  const [tableMode, setTableMode] = useState(false);
  const [taskSearch, setTaskSearch] = useState('');
  const listGeneration = useRef(0);
  const detailGeneration = useRef(0);
  const attemptGeneration = useRef(0);
  const loadMoreController = useRef<AbortController | null>(null);
  const previousWorkflow = useRef<string | null>(null);
  const previousTask = useRef<string | null>(null);
  const selectedTask = tasks.find((task) => task.task_id === selectedTaskId) ?? null;
  const visibleTasks = tasks.filter((task) => task.task_id.toLowerCase().includes(taskSearch.toLowerCase()));
  const graph = useMemo(() => buildTaskGraph(tasks, selectedTaskId), [tasks, selectedTaskId]);

  const reload = useCallback(() => setRefreshVersion((n) => n + 1), []);
  useEffect(() => {
    const generation = ++listGeneration.current;
    let activeController: AbortController | null = null;
    let timer: number | undefined;
    let stopped = false;
    const load = async () => {
      if (document.hidden || stopped) return;
      activeController?.abort();
      const controller = new AbortController();
      activeController = controller;
      setBusy(true); setError(null);
      const query = new URLSearchParams({ limit: '25' });
      if (filter !== 'all') query.set('state', filter);
      if (cursor) query.set('cursor', cursor);
      try {
        const [page, snapshot, health] = await Promise.all([
          requestJson<WorkflowPage>(`/api/v1/workflows?${query}`, token, controller.signal),
          requestJson<DashboardSummary>('/api/v1/summary', token, controller.signal),
          requestJson<Readiness>('/health/ready', token, controller.signal),
        ]);
        if (generation !== listGeneration.current || controller.signal.aborted) return;
        setWorkflows(page.items); setNextCursor(page.next_cursor); setSummary(snapshot); setSummaryReceivedAt(Date.now()); setReadiness(health);
      } catch (e) {
        if (controller.signal.aborted || generation !== listGeneration.current) return;
        if (e instanceof ApiError && e.status === 401) setAuthRequired(true);
        setError(e instanceof Error ? e.message : 'Could not reach the workflow API.');
      } finally {
        if (!controller.signal.aborted && generation === listGeneration.current) setBusy(false);
      }
    };
    const schedule = () => {
      if (timer) clearTimeout(timer);
      if (!stopped && !document.hidden) timer = window.setTimeout(async () => {
        await load();
        schedule();
      }, 15000);
    };
    const visibility = () => {
      if (document.hidden) { if (timer) clearTimeout(timer); activeController?.abort(); }
      else { void load().finally(schedule); }
    };
    void load().finally(schedule);
    document.addEventListener('visibilitychange', visibility);
    return () => { stopped = true; activeController?.abort(); if (timer) clearTimeout(timer); document.removeEventListener('visibilitychange', visibility); };
  }, [filter, cursor, token, refreshVersion]);

  useEffect(() => {
    let timer: number | undefined;
    const update = () => { if (timer) clearTimeout(timer); if (!document.hidden) { setClock(Date.now()); timer = window.setTimeout(update, 5000); } };
    const visibility = () => { if (timer) clearTimeout(timer); if (!document.hidden) update(); };
    update(); document.addEventListener('visibilitychange', visibility);
    return () => { if (timer) clearTimeout(timer); document.removeEventListener('visibilitychange', visibility); };
  }, []);

  useEffect(() => {
    const generation = ++detailGeneration.current;
    const attemptRun = ++attemptGeneration.current;
    const controller = new AbortController();
    let timer: number | undefined;
    const preserveAttemptHistory = previousWorkflow.current === selectedId &&
      previousTask.current === selectedTaskId;
    if (previousWorkflow.current !== selectedId) {
      previousWorkflow.current = selectedId;
      setDetail(null); setTasks([]); attemptsRef.current = []; setAttempts([]); setAttemptNextCursor(null); setSelectedTaskId(null);
    }
    if (previousTask.current !== selectedTaskId) {
      previousTask.current = selectedTaskId;
      attemptsRef.current = []; setAttempts([]); setAttemptNextCursor(null);
    }
    loadMoreController.current?.abort();
    setLoadingMoreAttempts(false);
    const load = async () => {
      if (!selectedId || document.hidden || controller.signal.aborted) return;
      try {
        const d = await requestJson<WorkflowDetail>(`/api/v1/workflows/${encodeURIComponent(selectedId)}`, token, controller.signal);
        const list = await getAllTasks(selectedId, token, controller.signal);
        let attemptPage: AttemptPage | null = null;
        if (selectedTaskId && list.some((task) => task.task_id === selectedTaskId)) {
          const query = new URLSearchParams({ limit: '100' });
          attemptPage = await requestJson<AttemptPage>(`/api/v1/workflows/${encodeURIComponent(selectedId)}/tasks/${encodeURIComponent(selectedTaskId)}/attempts?${query}`, token, controller.signal);
        }
        if (controller.signal.aborted || generation !== detailGeneration.current) return;
        setDetail(d); setTasks(list);
        if (attemptRun === attemptGeneration.current) {
          if (attemptPage) {
            const merged = new Map((preserveAttemptHistory ? attemptsRef.current : [])
              .map((attempt) => [attempt.attempt_number, attempt]));
            for (const attempt of attemptPage.items) merged.set(attempt.attempt_number, attempt);
            attemptsRef.current = [...merged.values()].sort((left, right) => left.attempt_number - right.attempt_number);
            setAttempts(attemptsRef.current);
            const loadedThrough = attemptsRef.current.at(-1)?.attempt_number ?? 0;
            const refreshedSelectedTask = list.find((task) => task.task_id === selectedTaskId);
            setAttemptNextCursor((current) => {
              if (!preserveAttemptHistory) return attemptPage.next_cursor;
              if (current) return current;
              return refreshedSelectedTask && loadedThrough < refreshedSelectedTask.attempt_count
                ? attemptPage.next_cursor : null;
            });
          } else if (!preserveAttemptHistory) {
            attemptsRef.current = [];
            setAttempts([]); setAttemptNextCursor(null);
          }
        }
      } catch (e) {
        if (!controller.signal.aborted && generation === detailGeneration.current) setError(e instanceof Error ? e.message : 'Could not load workflow details.');
      } finally {
        if (!controller.signal.aborted && generation === detailGeneration.current) {
          if (timer) clearTimeout(timer);
          if (!document.hidden) timer = window.setTimeout(() => setDetailRefresh((value) => value + 1), 15000);
        }
      }
    };
    const visibility = () => {
      if (document.hidden) { if (timer) clearTimeout(timer); controller.abort(); loadMoreController.current?.abort(); }
      else setDetailRefresh((value) => value + 1);
    };
    document.addEventListener('visibilitychange', visibility);
    if (!selectedId) { attemptsRef.current = []; setAttempts([]); setAttemptNextCursor(null); return () => { controller.abort(); document.removeEventListener('visibilitychange', visibility); }; }
    void load();
    return () => {
      if (timer) clearTimeout(timer);
      controller.abort(); loadMoreController.current?.abort();
      document.removeEventListener('visibilitychange', visibility);
    };
  }, [selectedId, selectedTaskId, token, refreshVersion, detailRefresh]);

  const loadMoreAttempts = useCallback(async () => {
    if (!selectedId || !selectedTaskId || !attemptNextCursor || loadingMoreAttempts) return;
    loadMoreController.current?.abort();
    const controller = new AbortController();
    loadMoreController.current = controller;
    const generation = attemptGeneration.current;
    const query = new URLSearchParams({ limit: '100', cursor: attemptNextCursor });
    setLoadingMoreAttempts(true);
    try {
      const page = await requestJson<AttemptPage>(`/api/v1/workflows/${encodeURIComponent(selectedId)}/tasks/${encodeURIComponent(selectedTaskId)}/attempts?${query}`, token, controller.signal);
      if (!controller.signal.aborted && generation === attemptGeneration.current) {
        const merged = new Map(attemptsRef.current.map((attempt) => [attempt.attempt_number, attempt]));
        for (const attempt of page.items) merged.set(attempt.attempt_number, attempt);
        attemptsRef.current = [...merged.values()].sort((left, right) => left.attempt_number - right.attempt_number);
        setAttempts(attemptsRef.current);
        setAttemptNextCursor(page.next_cursor);
      }
    } catch (e) {
      if (!controller.signal.aborted && generation === attemptGeneration.current) setError(e instanceof Error ? e.message : 'Could not load more attempts.');
    } finally {
      if (generation === attemptGeneration.current) setLoadingMoreAttempts(false);
    }
  }, [selectedId, selectedTaskId, attemptNextCursor, loadingMoreAttempts, token]);

  return <main className="shell">
    <header className="topbar"><div className="brand"><div className="brand-icon">W</div><div><b>WORKFLOW</b><small>CONTROL PLANE</small></div></div><div className="top-actions"><span className={`health-dot ${readiness?.dependencies.postgres === 'ok' ? 'good' : ''}`} />{readiness?.dependencies.postgres === 'ok' ? 'PostgreSQL connected' : 'Connecting to API'}<button className="refresh" onClick={reload} aria-label="Refresh">↻</button></div></header>
    {error && <div className="error-banner" role="alert"><span>{error}</span><button onClick={() => { setError(null); reload(); }}>Retry</button></div>}
    {authRequired && <form className="auth-card" onSubmit={(event) => { event.preventDefault(); setToken(authDraft); setAuthRequired(false); setError(null); }}><b>API authentication required</b><p>Enter the configured bearer token for this browser session. It is kept in memory only.</p><input type="password" value={authDraft} onChange={(e) => setAuthDraft(e.target.value)} aria-label="API token"/><button>Connect</button></form>}
    <section className="hero"><div><div className="eyebrow">OPERATIONS OVERVIEW</div><h1>Workflow activity</h1><p>Monitor distributed execution across your workers.</p></div><div className="freshness">{summary ? <><span className="fresh-dot" />Snapshot {summary.snapshot_stale || summary.snapshot_age_seconds + Math.max(0, clock - summaryReceivedAt) / 1000 > 30 ? 'stale' : 'current'} · {Math.round(summary.snapshot_age_seconds + Math.max(0, clock - summaryReceivedAt) / 1000)}s old</> : 'Loading database snapshot'}</div></section>
    <section className="metrics-grid">
      {[
        ['TOTAL WORKFLOWS', summary ? Object.values(summary.workflows).reduce((a,b) => a+b,0) : '—', 'Database snapshot'],
        ['RUNNING', summary?.workflows.running ?? '—', 'Current recorded state'],
        ['SUCCEEDED', summary?.workflows.succeeded ?? '—', 'Committed outcomes'],
        ['FAILED', summary?.workflows.failed ?? '—', 'Committed outcomes'],
        ['ACTIVE LEASES', summary?.active_leases ?? '—', 'Not a live worker count'],
        ['RETRIES SCHEDULED', summary?.retry_scheduled_attempts ?? '—', 'Committed retry outcomes'],
        ['DEAD LETTERS', summary?.dead_lettered_tasks ?? '—', 'Tasks requiring attention'],
      ].map(([label,value,note]) => <article className="metric" key={label}><span>{label}</span><strong>{value}</strong><small>{note}</small></article>)}
    </section>
    <section className="content-card">
      <div className="section-head"><div><h2>{selectedId ? 'Workflow details' : 'Recent workflows'}</h2><p>{selectedId ? 'Execution state, dependencies, retries and durable attempts.' : 'A live view of workflows recorded by PostgreSQL.'}</p></div><div className="controls">{selectedId && <button className="subtle" onClick={() => setSelectedId(null)}>← All workflows</button>}<button className="subtle" onClick={reload}>Refresh</button></div></div>
      {!selectedId ? <><div className="filters">{states.map((s) => <button key={s} className={filter === s ? 'active' : ''} onClick={() => { setFilter(s); setCursor(null); }}>{s === 'all' ? 'All states' : s}</button>)}</div>
        {busy && !workflows.length ? <div className="empty">Loading workflows…</div> : workflows.length ? <div className="workflow-list">{workflows.map((w) => <button className="workflow-row" key={w.workflow_id} onClick={() => setSelectedId(w.workflow_id)}><span className={`state-pill ${w.state}`}>{w.state}</span><span className="workflow-id">{w.workflow_id}</span><time>{new Date(w.created_at).toLocaleString()}</time><span className="row-arrow">↗</span></button>)}</div> : <div className="empty"><b>No workflows found</b><span>Submitted workflows will appear here.</span></div>}
        <div className="pagination"><button disabled={!cursor} onClick={() => setCursor(null)}>First page</button><span>{workflows.length ? `${workflows.length} workflows` : '—'}</span><button disabled={!nextCursor} onClick={() => nextCursor && setCursor(nextCursor)}>Next page →</button></div>
      </> : detail ? <div className="detail-view"><div className="detail-heading"><span className={`state-pill ${detail.state}`}>{detail.state}</span><code>{detail.workflow_id}</code><div className="timestamps"><span>Created {new Date(detail.created_at).toLocaleString()}</span><span>Updated {new Date(detail.updated_at).toLocaleString()}</span></div></div>{detail.error && <div className="failure-summary">{detail.error.message}</div>}<div className="progress"><div className="progress-label"><b>Task progress</b><span>{detail.task_counts.succeeded}/{detail.task_count} complete</span></div><div className="progress-track"><i style={{ width: `${detail.task_count ? (detail.task_counts.succeeded / detail.task_count) * 100 : 0}%` }}/></div><div className="count-strip">{Object.entries(detail.task_counts).map(([s,n]) => <span key={s}>{s.replace('_',' ')} <b>{n}</b></span>)}</div></div>
        <div className="graph-head"><div><h3>Dependency graph</h3><p>Read-only DAG · select a task to inspect attempts.</p></div><div><button onClick={() => setTableMode(false)} className={!tableMode ? 'active' : ''}>Graph</button><button onClick={() => setTableMode(true)} className={tableMode ? 'active' : ''}>Task table</button></div></div>
        {tableMode || tasks.length > 120 ? <><input className="task-search" aria-label="Search tasks" placeholder="Search tasks by ID" value={taskSearch} onChange={(event) => setTaskSearch(event.target.value)}/><div className="task-table">{visibleTasks.map((task) => <button key={task.task_id} onClick={() => setSelectedTaskId(task.task_id)}><b>{task.task_id}</b><span className={`state-pill ${task.state}`}>{task.state}</span><small>{task.dependencies.length ? `Depends on ${task.dependencies.join(', ')}` : 'No dependencies'}</small></button>)}{!visibleTasks.length && <div className="empty compact">No matching tasks.</div>}</div></> : <div className="graph-canvas"><ReactFlow nodes={graph.nodes} edges={graph.edges} nodeTypes={nodeTypes} onNodeClick={(_event: React.MouseEvent, node: Node) => setSelectedTaskId(node.id)} nodesDraggable={false} nodesConnectable={false} elementsSelectable fitView fitViewOptions={{ padding: 0.25 }} proOptions={{ hideAttribution: true }}><Background color="#dce4e7" gap={22}/><Controls showInteractive={false}/></ReactFlow></div>}
        {selectedTask && <aside className="task-panel"><div className="panel-title"><div><div className="eyebrow">SELECTED TASK</div><h3>{selectedTask.task_id}</h3></div><button onClick={() => setSelectedTaskId(null)} aria-label="Close task detail">×</button></div><div className="panel-facts"><span>Status <b>{selectedTask.state}</b></span><span>Attempts <b>{selectedTask.attempt_count} / {selectedTask.max_attempts}</b></span><span>Dependencies <b>{selectedTask.dependencies.join(', ') || 'None'}</b></span>{selectedTask.retry_eligible_at && <span>Retry eligible <b>{new Date(selectedTask.retry_eligible_at).toLocaleString()}</b></span>}{selectedTask.dead_lettered_at && <span>Dead-lettered <b>{new Date(selectedTask.dead_lettered_at).toLocaleString()}</b></span>}{selectedTask.error && <span>Failure <b>{selectedTask.error.message}</b></span>}</div><h4>Durable attempt history</h4>{attempts.length ? attempts.map((a) => <div className="attempt" key={a.attempt_number}><b>Attempt {a.attempt_number} · {a.outcome}</b><span>Claimed {new Date(a.claimed_at).toLocaleString()}</span><span>{a.finished_at ? `Finished ${new Date(a.finished_at).toLocaleString()}` : 'Still in progress'}</span>{a.finished_at && <span>Attempt elapsed {Math.max(0, (new Date(a.finished_at).getTime() - new Date(a.claimed_at).getTime()) / 1000).toFixed(2)}s</span>}{a.error && <small>{a.error.message}</small>}</div>) : <div className="empty compact">No attempts recorded yet.</div>}{attemptNextCursor && <button className="subtle load-more-attempts" onClick={() => void loadMoreAttempts()} disabled={loadingMoreAttempts}>{loadingMoreAttempts ? 'Loading attempts…' : 'Load more attempts'}</button>}<small className="attempt-note">Attempt elapsed includes orchestration overhead; it is not pure action time.</small></aside>}
      </div> : <div className="empty">Loading workflow…</div>}
    </section>
    <footer><span>PostgreSQL is authoritative · Redis provides optional wake-up hints</span><span>{readiness?.dependencies.redis === 'degraded' ? 'Redis degraded · database discovery remains active' : readiness?.dependencies.redis === 'disabled' ? 'Redis disabled · database discovery remains active' : readiness?.dependencies.redis === 'ok' ? 'Redis wake-ups available' : ''}</span></footer>
  </main>;
}
