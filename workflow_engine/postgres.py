"""PostgreSQL persistence with atomic, fenced task leases (psycopg 3)."""

import json
import math
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from .dag import topological_order
from .definitions import Task, Workflow
from .retries import RetryPolicy

try:
    import psycopg
    from psycopg.types.json import Jsonb
except ImportError:  # Keep SQLite usable without the optional PostgreSQL extra.
    psycopg = None
    Jsonb = None


class WorkflowSubmissionConflict(ValueError):
    """An idempotency key was reused with a different canonical definition."""


class PostgresStore:
    def __init__(self, dsn: str, *, migrate: bool = True, connection_pool=None) -> None:
        if psycopg is None:
            raise RuntimeError("PostgreSQL support requires psycopg 3; install requirements.txt")
        self.dsn = dsn
        self.connection_pool = connection_pool
        if migrate:
            self.migrate()

    def _connect(self):
        if self.connection_pool is not None:
            return self.connection_pool.connection()
        return psycopg.connect(self.dsn)

    def migrate(self) -> None:
        """Apply each numbered SQL migration atomically and exactly once."""
        migrations = Path(__file__).with_name("migrations")
        with self._connect() as conn:
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(hashtext('workflow_engine_schema_migrations'))")
                conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                )""")
                for path in sorted(migrations.glob("[0-9][0-9][0-9]_*.sql")):
                    version = int(path.name.split("_", 1)[0])
                    present = conn.execute(
                        "SELECT 1 FROM schema_migrations WHERE version = %s", (version,)
                    ).fetchone()
                    if present:
                        continue
                    for statement in path.read_text().split(";"):
                        if statement.strip():
                            conn.execute(statement)
                    conn.execute("INSERT INTO schema_migrations(version) VALUES (%s)", (version,))

    def initialize(self, workflow_id: str, definition: Dict[str, Any], task_ids) -> None:
        task_ids = list(task_ids)
        encoded = json.dumps(definition, sort_keys=True)
        definitions = self._validate_definition(workflow_id, definition, task_ids)
        with self._connect() as conn:
            with conn.transaction():
                created = conn.execute(
                    "INSERT INTO workflows(workflow_id,state,definition) VALUES (%s,'pending',%s) "
                    "ON CONFLICT (workflow_id) DO NOTHING RETURNING workflow_id",
                    (workflow_id, Jsonb(json.loads(encoded))),
                ).fetchone()
                row = conn.execute(
                    "SELECT definition FROM workflows WHERE workflow_id=%s FOR UPDATE", (workflow_id,)
                ).fetchone()
                if row[0] != json.loads(encoded):
                    raise ValueError(f"workflow ID {workflow_id!r} already has a different definition")
                self._insert_task_rows(conn, workflow_id, definitions)
                if created:
                    self._enqueue_notification(conn, workflow_id, "workflow_initialized")

    @staticmethod
    def _validate_definition(workflow_id, definition, task_ids=None):
        definitions = [Task(item["id"], tuple(item.get("dependencies", [])),
                            RetryPolicy(**item.get("retry_policy", {})))
                       for item in definition["tasks"]]
        topological_order(Workflow(workflow_id, tuple(definitions)))
        ids = [task.task_id for task in definitions]
        if task_ids is not None and set(task_ids) != set(ids):
            raise ValueError("task_ids must match the tasks in the workflow definition")
        return definitions

    @staticmethod
    def _insert_task_rows(conn, workflow_id, definitions):
        for task in definitions:
            policy = task.retry_policy
            conn.execute(
                "INSERT INTO tasks(workflow_id,task_id,state,max_attempts,retry_initial_delay,"
                "retry_multiplier,retry_max_delay,retry_jitter) "
                "VALUES (%s,%s,'pending',%s,%s,%s,%s,%s) "
                "ON CONFLICT (workflow_id,task_id) DO NOTHING",
                (workflow_id, task.task_id, policy.max_attempts, policy.initial_delay,
                 policy.multiplier, policy.max_delay, policy.jitter),
            )
        for task in definitions:
            for parent in task.dependencies:
                conn.execute(
                    "INSERT INTO task_dependencies(workflow_id,task_id,dependency_id) "
                    "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                    (workflow_id, task.task_id, parent),
                )

    def submit_workflow(self, idempotency_key: str, definition: Dict[str, Any]):
        """Atomically create one workflow per idempotency key."""
        workflow_id = str(uuid.uuid4())
        definitions = self._validate_definition(workflow_id, definition)
        with self._connect() as conn:
            with conn.transaction():
                created = conn.execute(
                    "INSERT INTO workflows(workflow_id,state,definition,idempotency_key) "
                    "VALUES (%s,'pending',%s,%s) "
                    "ON CONFLICT (idempotency_key) DO NOTHING "
                    "RETURNING workflow_id,created_at",
                    (workflow_id, Jsonb(definition), idempotency_key),
                ).fetchone()
                if created is None:
                    existing = conn.execute(
                        "SELECT workflow_id,definition,created_at FROM workflows "
                        "WHERE idempotency_key=%s", (idempotency_key,),
                    ).fetchone()
                    if existing[1] != definition:
                        raise WorkflowSubmissionConflict(
                            "idempotency key was already used for a different definition")
                    return {"workflow_id": existing[0], "created_at": existing[2],
                            "created": False}
                self._insert_task_rows(conn, created[0], definitions)
                self._enqueue_notification(conn, created[0], "workflow_initialized")
                return {"workflow_id": created[0], "created_at": created[1],
                        "created": True}

    def list_workflows(self, *, limit: int, state=None, before=None):
        query = "SELECT workflow_id,state,created_at,updated_at FROM workflows WHERE TRUE"
        params = []
        if state is not None:
            query += " AND state=%s"
            params.append(state)
        if before is not None:
            query += " AND (created_at,workflow_id)<(%s,%s)"
            params.extend(before)
        query += " ORDER BY created_at DESC,workflow_id DESC LIMIT %s"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [{"workflow_id": row[0], "state": row[1], "created_at": row[2],
                 "updated_at": row[3]} for row in rows]

    def workflow_detail(self, workflow_id: str):
        with self._connect() as conn:
            row = conn.execute("""
                WITH counts AS (
                    SELECT state,count(*) AS task_count FROM tasks
                    WHERE workflow_id=%s GROUP BY state
                )
                SELECT w.workflow_id,w.state,w.error,w.created_at,w.updated_at,
                       COALESCE(jsonb_object_agg(c.state,c.task_count)
                           FILTER (WHERE c.state IS NOT NULL),'{}'::jsonb)
                FROM workflows w LEFT JOIN counts c ON TRUE
                WHERE w.workflow_id=%s
                GROUP BY w.workflow_id
            """, (workflow_id, workflow_id)).fetchone()
        if row is None:
            return None
        return {"workflow_id": row[0], "state": row[1], "error": row[2],
                "created_at": row[3], "updated_at": row[4], "task_counts": row[5]}

    def list_tasks(self, workflow_id: str, *, limit: int, after_task_id=None):
        query = """
            SELECT t.task_id,t.state,t.error,t.attempt_count,t.max_attempts,
                   t.next_eligible_at,t.dead_lettered_at,t.created_at,t.updated_at,
                   t.lease_expires_at,
                   COALESCE(array_agg(d.dependency_id ORDER BY d.dependency_id)
                       FILTER (WHERE d.dependency_id IS NOT NULL),'{}')
            FROM tasks t LEFT JOIN task_dependencies d
              ON d.workflow_id=t.workflow_id AND d.task_id=t.task_id
            WHERE t.workflow_id=%s
        """
        params = [workflow_id]
        if after_task_id is not None:
            query += " AND t.task_id>%s"
            params.append(after_task_id)
        query += " GROUP BY t.workflow_id,t.task_id ORDER BY t.task_id LIMIT %s"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._task_dict(row) for row in rows]

    def task_detail(self, workflow_id: str, task_id: str):
        with self._connect() as conn:
            row = conn.execute("""
                SELECT t.task_id,t.state,t.error,t.attempt_count,t.max_attempts,
                       t.next_eligible_at,t.dead_lettered_at,t.created_at,t.updated_at,
                       t.lease_expires_at,
                       COALESCE(array_agg(d.dependency_id ORDER BY d.dependency_id)
                           FILTER (WHERE d.dependency_id IS NOT NULL),'{}')
                FROM tasks t LEFT JOIN task_dependencies d
                  ON d.workflow_id=t.workflow_id AND d.task_id=t.task_id
                WHERE t.workflow_id=%s AND t.task_id=%s
                GROUP BY t.workflow_id,t.task_id
            """, (workflow_id, task_id)).fetchone()
        return self._task_dict(row) if row else None

    @staticmethod
    def _task_dict(row):
        if row is None:
            return None
        return {"task_id": row[0], "state": row[1], "error": row[2],
                "attempt_count": row[3], "max_attempts": row[4],
                "next_eligible_at": row[5], "dead_lettered_at": row[6],
                "created_at": row[7], "updated_at": row[8],
                "lease_expires_at": row[9], "dependencies": list(row[10])}

    def list_attempts(self, workflow_id: str, task_id: str, *, limit: int,
                      after_attempt_number=None):
        query = """
            SELECT attempt_number,claimed_at,lease_expires_at,finished_at,outcome,error
            FROM task_attempts WHERE workflow_id=%s AND task_id=%s
        """
        params = [workflow_id, task_id]
        if after_attempt_number is not None:
            query += " AND attempt_number>%s"
            params.append(after_attempt_number)
        query += " ORDER BY attempt_number LIMIT %s"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [{"attempt_number": row[0], "claimed_at": row[1],
                 "lease_expires_at": row[2], "finished_at": row[3],
                 "outcome": row[4], "error": row[5]} for row in rows]

    def workflow_definition(self, workflow_id: str):
        with self._connect() as conn:
            row = conn.execute("SELECT definition FROM workflows WHERE workflow_id=%s",
                               (workflow_id,)).fetchone()
        return row[0] if row else None

    def discoverable_workflows(self, *, limit: int, after_workflow_id=None):
        query = """
            SELECT workflow_id FROM workflows
            WHERE state IN ('pending','running')
              AND definition->>'api_definition_version'='1'
        """
        params = []
        if after_workflow_id is not None:
            query += " AND workflow_id>%s"
            params.append(after_workflow_id)
        query += " ORDER BY workflow_id LIMIT %s"
        params.append(limit)
        with self._connect() as conn:
            return [row[0] for row in conn.execute(query, params).fetchall()]

    def metrics_snapshot(self):
        """Return low-cardinality committed totals; callers cache this snapshot."""
        with self._connect() as conn:
            with conn.transaction():
                conn.execute("SET TRANSACTION READ ONLY")
                conn.execute("SET LOCAL statement_timeout='3s'")
                row = conn.execute("""
                    SELECT
                      (SELECT COALESCE(jsonb_object_agg(state,n), '{}'::jsonb)
                       FROM (SELECT state,count(*) AS n FROM workflows GROUP BY state) s),
                      (SELECT COALESCE(jsonb_object_agg(outcome,n), '{}'::jsonb)
                       FROM (SELECT outcome,count(*) AS n FROM task_attempts GROUP BY outcome) a),
                      (SELECT count(*) FROM tasks WHERE state='dead_lettered'),
                      (SELECT count(*) FROM tasks WHERE state='running'
                         AND lease_expires_at>clock_timestamp())
                """).fetchone()
        return {"workflows": row[0], "attempts": row[1],
                "dead_lettered_tasks": row[2], "active_leases": row[3]}

    def health_check(self):
        with self._connect() as conn:
            conn.execute("SELECT 1").fetchone()


    def start_workflow(self, workflow_id: str) -> Optional[str]:
        """Transition pending to running once, before workers enter the claim fast path."""
        with self._connect() as conn:
            row = conn.execute("""
                UPDATE workflows SET state='running',error=NULL,updated_at=clock_timestamp()
                WHERE workflow_id=%s AND state='pending' RETURNING state
            """, (workflow_id,)).fetchone()
            if row:
                return row[0]
            row = conn.execute("SELECT state FROM workflows WHERE workflow_id=%s", (workflow_id,)).fetchone()
            return row[0] if row else None

    def claim_task(self, workflow_id: str, worker_id: str, lease_seconds: float) -> Optional[Dict[str, Any]]:
        self._validate_duration(lease_seconds, "lease_seconds")
        claimed = None
        with self._connect() as conn:
            with conn.transaction():
                # Claims share this per-workflow gate. Finalization takes it exclusively,
                # allowing parallel claims while making its terminal-state check atomic
                # with respect to every claim already in flight.
                self._workflow_claim_gate(conn, workflow_id, shared=True)
                workflow = conn.execute(
                    "SELECT state FROM workflows WHERE workflow_id=%s", (workflow_id,)
                ).fetchone()
                if not workflow or workflow[0] in ("succeeded", "failed"):
                    return None

                exhausted = conn.execute("""
                    WITH expired AS (
                        SELECT workflow_id,task_id FROM tasks
                        WHERE workflow_id=%s AND state='running'
                          AND lease_expires_at <= clock_timestamp()
                          AND attempt_count >= max_attempts
                        ORDER BY task_id FOR UPDATE SKIP LOCKED LIMIT 50
                    )
                    UPDATE tasks AS t SET state='dead_lettered',
                        error='lease expired; maximum attempts exhausted',
                        dead_lettered_at=clock_timestamp(), lease_owner=NULL,
                        lease_expires_at=NULL, updated_at=clock_timestamp()
                    FROM expired e WHERE t.workflow_id=e.workflow_id AND t.task_id=e.task_id
                    RETURNING t.task_id,t.attempt_count,t.fencing_token
                """, (workflow_id,)).fetchall()
                for task_id, attempt_number, fencing_token in exhausted:
                    conn.execute("""
                        UPDATE task_attempts SET outcome='lease_expired',
                            error='lease expired; maximum attempts exhausted',
                            finished_at=clock_timestamp()
                        WHERE workflow_id=%s AND task_id=%s AND attempt_number=%s
                          AND fencing_token=%s AND outcome='running'
                    """, (workflow_id, task_id, attempt_number, fencing_token))
                if exhausted:
                    self._enqueue_notification(conn, workflow_id, "task_dead_lettered")
                self._propagate_failed_dependencies(conn, workflow_id)
                claimed = conn.execute("""
                    WITH candidate AS (
                        SELECT t.workflow_id, t.task_id, t.state AS previous_state,
                               t.attempt_count AS previous_attempt_count,
                               t.fencing_token AS previous_fencing_token
                        FROM tasks AS t
                        WHERE t.workflow_id = %s
                          AND ((t.state = 'pending' AND t.next_eligible_at <= clock_timestamp()) OR
                               (t.state = 'running' AND t.lease_expires_at <= clock_timestamp()
                                AND t.attempt_count < t.max_attempts))
                          AND NOT EXISTS (
                              SELECT 1 FROM task_dependencies d
                              JOIN tasks parent ON parent.workflow_id=d.workflow_id
                                  AND parent.task_id=d.dependency_id
                              WHERE d.workflow_id=t.workflow_id AND d.task_id=t.task_id
                                AND parent.state <> 'succeeded'
                          )
                        ORDER BY t.task_id
                        FOR UPDATE OF t SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE tasks AS t
                    SET state='running', error=NULL, lease_owner=%s,
                        lease_expires_at=clock_timestamp() + (%s * interval '1 second'),
                        attempt_count=t.attempt_count+1, fencing_token=t.fencing_token+1,
                        updated_at=clock_timestamp()
                    FROM candidate c
                    WHERE t.workflow_id=c.workflow_id AND t.task_id=c.task_id
                    RETURNING t.workflow_id,t.task_id,t.attempt_count,t.fencing_token,
                              t.lease_expires_at,c.previous_state,c.previous_attempt_count,
                              c.previous_fencing_token,t.lease_owner
                """, (workflow_id, worker_id, lease_seconds)).fetchone()
                if claimed:
                    (claimed_workflow, task_id, attempt_number, fencing_token,
                     lease_expires_at, previous_state, previous_attempt, previous_fence,
                     claimed_worker) = claimed
                    if previous_state == "running":
                        conn.execute("""
                            UPDATE task_attempts SET outcome='lease_expired',
                                error='lease expired before attempt completed',
                                finished_at=clock_timestamp()
                            WHERE workflow_id=%s AND task_id=%s AND attempt_number=%s
                              AND fencing_token=%s AND outcome='running'
                        """, (workflow_id, task_id, previous_attempt, previous_fence))
                    conn.execute("""
                        INSERT INTO task_attempts(workflow_id,task_id,attempt_number,fencing_token,
                            worker_id,lease_expires_at,outcome)
                        VALUES (%s,%s,%s,%s,%s,%s,'running')
                    """, (workflow_id, task_id, attempt_number, fencing_token,
                          claimed_worker, lease_expires_at))
                    claimed = {"workflow_id": claimed_workflow, "task_id": task_id,
                               "attempt_count": attempt_number, "fencing_token": fencing_token,
                               "lease_expires_at": lease_expires_at}
        if claimed is None:
            # Finalization needs the exclusive gate, so it must run after the shared
            # claim transaction has ended rather than attempting a lock upgrade.
            with self._connect() as conn:
                with conn.transaction():
                    self._finalize_if_terminal(conn, workflow_id)
        return claimed

    def renew_lease(self, workflow_id: str, task_id: str, worker_id: str,
                    fencing_token: int, lease_seconds: float) -> bool:
        self._validate_duration(lease_seconds, "lease_seconds")
        with self._connect() as conn:
            row = conn.execute("""
                UPDATE tasks SET lease_expires_at=clock_timestamp() + (%s * interval '1 second'),
                    updated_at=clock_timestamp()
                WHERE workflow_id=%s AND task_id=%s AND state='running' AND lease_owner=%s
                  AND fencing_token=%s AND lease_expires_at > clock_timestamp()
                RETURNING task_id
            """, (lease_seconds, workflow_id, task_id, worker_id, fencing_token)).fetchone()
            return row is not None

    def complete_task(self, workflow_id: str, task_id: str, worker_id: str, fencing_token: int) -> bool:
        return self._finish_task(workflow_id, task_id, worker_id, fencing_token, "succeeded", None)

    def fail_task(self, workflow_id: str, task_id: str, worker_id: str,
                  fencing_token: int, error: str, *, retryable: bool = True) -> bool:
        return self._finish_task(workflow_id, task_id, worker_id, fencing_token,
                                 "failed", error, retryable=retryable)

    def _finish_task(self, workflow_id, task_id, worker_id, fencing_token, state, error,
                     *, retryable: bool = True) -> bool:
        with self._connect() as conn:
            with conn.transaction():
                # Follow the claim lock order: the exclusive gate must be held
                # before taking any task-row lock. Claims share this gate, so
                # claims remain concurrent with one another.
                self._workflow_claim_gate(conn, workflow_id, shared=False)
                task = conn.execute("""
                    SELECT attempt_count,max_attempts,retry_initial_delay,retry_multiplier,
                           retry_max_delay,retry_jitter
                    FROM tasks WHERE workflow_id=%s AND task_id=%s AND state='running'
                      AND lease_owner=%s AND fencing_token=%s
                      AND lease_expires_at > clock_timestamp()
                    FOR UPDATE
                """, (workflow_id, task_id, worker_id, fencing_token)).fetchone()
                if task is None:
                    return False
                target_state, next_eligible_at, attempt_outcome = "succeeded", None, "succeeded"
                if state == "failed":
                    attempt_count, max_attempts, initial, multiplier, cap, jitter = task
                    if retryable and attempt_count < max_attempts:
                        delay = RetryPolicy(max_attempts, initial, multiplier, cap, jitter).delay_for(attempt_count)
                        target_state, next_eligible_at, attempt_outcome = "pending", delay, "retry_scheduled"
                    else:
                        target_state, attempt_outcome = "dead_lettered", "dead_lettered"
                row = conn.execute("""
                    UPDATE tasks SET state=%s,error=%s,lease_owner=NULL,lease_expires_at=NULL,
                        next_eligible_at=CASE WHEN %s::double precision IS NULL THEN next_eligible_at
                            ELSE clock_timestamp() + (%s * interval '1 second') END,
                        dead_lettered_at=CASE WHEN %s='dead_lettered' THEN clock_timestamp()
                            ELSE NULL END,
                        updated_at=clock_timestamp()
                    WHERE workflow_id=%s AND task_id=%s AND state='running' AND lease_owner=%s
                      AND fencing_token=%s AND lease_expires_at > clock_timestamp()
                    RETURNING task_id
                """, (target_state, error, next_eligible_at, next_eligible_at, target_state,
                      workflow_id, task_id, worker_id, fencing_token)).fetchone()
                if row is None:
                    return False
                attempt = conn.execute("""
                    UPDATE task_attempts SET outcome=%s,error=%s,finished_at=clock_timestamp()
                    WHERE workflow_id=%s AND task_id=%s AND fencing_token=%s AND outcome='running'
                    RETURNING attempt_number
                """, (attempt_outcome, error, workflow_id, task_id, fencing_token)).fetchone()
                if attempt is None:
                    raise RuntimeError("current task attempt history row is missing")
                if target_state == "dead_lettered":
                    self._propagate_failed_dependencies(conn, workflow_id)
                self._finalize_if_terminal(conn, workflow_id, serialize=True)
                self._enqueue_notification(conn, workflow_id,
                                           "task_completed" if target_state == "succeeded"
                                           else "task_retry_scheduled" if target_state == "pending"
                                           else "task_dead_lettered")
                return True

    @staticmethod
    def _propagate_failed_dependencies(conn, workflow_id: str) -> None:
        # Iterate to a fixed point so a failed ancestor blocks every descendant.
        while True:
            cursor = conn.execute("""
                UPDATE tasks AS child SET state='failed',
                    error='blocked by unsuccessful dependency', updated_at=clock_timestamp()
                WHERE child.workflow_id=%s AND child.state='pending'
                  AND EXISTS (
                    SELECT 1 FROM task_dependencies d JOIN tasks parent
                      ON parent.workflow_id=d.workflow_id AND parent.task_id=d.dependency_id
                    WHERE d.workflow_id=child.workflow_id AND d.task_id=child.task_id
                      AND parent.state IN ('failed','dead_lettered')
                  )
            """, (workflow_id,))
            if cursor.rowcount == 0:
                return

    @staticmethod
    def _finalize_if_terminal(conn, workflow_id: str, *, serialize: bool = False) -> Optional[str]:
        PostgresStore._workflow_claim_gate(conn, workflow_id, shared=False)
        def task_summary():
            return conn.execute("""
            SELECT count(*) FILTER (WHERE state IN ('pending','running')) AS unfinished,
                   count(*) FILTER (WHERE state IN ('failed','dead_lettered')) AS failures
            FROM tasks WHERE workflow_id=%s
            """, (workflow_id,)).fetchone()

        workflow = conn.execute(
            "SELECT state FROM workflows WHERE workflow_id=%s FOR UPDATE", (workflow_id,)
        ).fetchone()
        if not workflow or workflow[0] in ("succeeded", "failed"):
            return workflow[0] if workflow else None
        row = task_summary()
        if row[0] != 0:
            return None
        state = "failed" if row[1] else "succeeded"
        conn.execute("""UPDATE workflows SET state=%s,error=%s,updated_at=clock_timestamp()
                     WHERE workflow_id=%s AND state NOT IN ('succeeded','failed')""",
                     (state, "one or more tasks failed" if row[1] else None, workflow_id))
        return state

    @staticmethod
    def _enqueue_notification(conn, workflow_id: str, event_type: str) -> None:
        conn.execute("""
            INSERT INTO notification_outbox(workflow_id,event_type,payload)
            VALUES (%s,%s,%s)
        """, (workflow_id, event_type, Jsonb({"workflow_id": workflow_id})))

    def cleanup_notification_retention(self, *, limit: int = 500) -> int:
        """Delete a bounded batch of old hints outside task-state transactions."""
        with self._connect() as conn:
            rows = conn.execute("""
                WITH expired AS (
                    SELECT event_id FROM notification_outbox
                    WHERE created_at < clock_timestamp()-interval '1 day'
                    ORDER BY created_at,event_id FOR UPDATE SKIP LOCKED LIMIT %s
                )
                DELETE FROM notification_outbox o USING expired e
                WHERE o.event_id=e.event_id RETURNING o.event_id
            """, (limit,)).fetchall()
            return len(rows)

    def lease_notifications(self, worker_id: str, *, limit: int = 50,
                            lease_seconds: float = 10.0):
        self._validate_duration(lease_seconds, "outbox_lease_seconds")
        with self._connect() as conn:
            with conn.transaction():
                rows = conn.execute("""
                    WITH ready AS (
                        SELECT event_id FROM notification_outbox
                        WHERE published_at IS NULL AND available_at <= clock_timestamp()
                          AND (locked_until IS NULL OR locked_until <= clock_timestamp())
                        ORDER BY event_id FOR UPDATE SKIP LOCKED LIMIT %s
                    )
                    UPDATE notification_outbox o SET lock_owner=%s,
                        locked_until=clock_timestamp() + (%s * interval '1 second'),
                        publish_attempts=o.publish_attempts+1
                    FROM ready r WHERE o.event_id=r.event_id
                    RETURNING o.event_id,o.workflow_id,o.event_type,o.payload
                """, (limit, worker_id, lease_seconds)).fetchall()
                return [{"event_id": row[0], "workflow_id": row[1],
                         "event_type": row[2], "payload": row[3]} for row in rows]

    def mark_notification_published(self, event_id: int, worker_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("""
                UPDATE notification_outbox SET published_at=clock_timestamp(),
                    lock_owner=NULL,locked_until=NULL
                WHERE event_id=%s AND lock_owner=%s AND published_at IS NULL
                RETURNING event_id
            """, (event_id, worker_id)).fetchone()
            return row is not None

    def release_notification(self, event_id: int, worker_id: str) -> None:
        with self._connect() as conn:
            conn.execute("""
                UPDATE notification_outbox SET lock_owner=NULL,locked_until=NULL
                WHERE event_id=%s AND lock_owner=%s AND published_at IS NULL
            """, (event_id, worker_id))

    def next_wakeup_delay(self, workflow_id: str) -> Optional[float]:
        with self._connect() as conn:
            return conn.execute("""
                SELECT EXTRACT(EPOCH FROM (min(wake_at)-clock_timestamp()))
                FROM (
                    SELECT t.next_eligible_at AS wake_at FROM tasks t
                    WHERE t.workflow_id=%s AND t.state='pending'
                      AND NOT EXISTS (
                          SELECT 1 FROM task_dependencies d JOIN tasks parent
                            ON parent.workflow_id=d.workflow_id AND parent.task_id=d.dependency_id
                          WHERE d.workflow_id=t.workflow_id AND d.task_id=t.task_id
                            AND parent.state <> 'succeeded'
                      )
                    UNION ALL
                    SELECT lease_expires_at AS wake_at FROM tasks
                    WHERE workflow_id=%s AND state='running'
                ) AS wake_times
            """, (workflow_id, workflow_id)).fetchone()[0]

    @staticmethod
    def _workflow_claim_gate(conn, workflow_id: str, *, shared: bool) -> None:
        lock = "pg_advisory_xact_lock_shared" if shared else "pg_advisory_xact_lock"
        conn.execute(f"SELECT {lock}(hashtext('workflow_claim_gate'), hashtext(%s))", (workflow_id,))

    @staticmethod
    def _validate_duration(value: float, name: str) -> None:
        try:
            valid = math.isfinite(value) and value > 0
        except (TypeError, ValueError):
            valid = False
        if not valid:
            raise ValueError(f"{name} must be finite and strictly positive")

    def workflow_state(self, workflow_id: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute("SELECT state FROM workflows WHERE workflow_id=%s", (workflow_id,)).fetchone()
            return row[0] if row else None
