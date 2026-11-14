ALTER TABLE tasks
    DROP CONSTRAINT IF EXISTS tasks_state_check;

ALTER TABLE tasks
    ADD CONSTRAINT tasks_state_check
        CHECK (state IN ('pending', 'running', 'succeeded', 'failed', 'dead_lettered')),
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 1 CHECK (max_attempts >= 1),
    ADD COLUMN retry_initial_delay DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (retry_initial_delay >= 0),
    ADD COLUMN retry_multiplier DOUBLE PRECISION NOT NULL DEFAULT 2 CHECK (retry_multiplier >= 1),
    ADD COLUMN retry_max_delay DOUBLE PRECISION NOT NULL DEFAULT 300 CHECK (retry_max_delay >= 0),
    ADD COLUMN retry_jitter DOUBLE PRECISION NOT NULL DEFAULT 0
        CHECK (retry_jitter >= 0 AND retry_jitter <= 1),
    ADD COLUMN next_eligible_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN dead_lettered_at TIMESTAMPTZ;

CREATE INDEX tasks_due_retry_idx
    ON tasks(workflow_id, next_eligible_at, task_id) WHERE state = 'pending';

CREATE TABLE task_attempts (
    workflow_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    attempt_number BIGINT NOT NULL CHECK (attempt_number >= 1),
    fencing_token BIGINT NOT NULL CHECK (fencing_token >= 1),
    worker_id TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    outcome TEXT NOT NULL CHECK (outcome IN
        ('running', 'succeeded', 'retry_scheduled', 'failed', 'dead_lettered', 'lease_expired')),
    error TEXT,
    PRIMARY KEY (workflow_id, task_id, attempt_number),
    UNIQUE (workflow_id, task_id, fencing_token),
    FOREIGN KEY (workflow_id, task_id)
        REFERENCES tasks(workflow_id, task_id) ON DELETE CASCADE
);

CREATE INDEX task_attempts_open_idx
    ON task_attempts(workflow_id, task_id, attempt_number) WHERE outcome = 'running';

-- Preserve the currently active WF-002 lease so it can finish or be recovered
-- after an additive migration. Older completed attempt details were not stored.
INSERT INTO task_attempts(workflow_id,task_id,attempt_number,fencing_token,worker_id,
                          claimed_at,lease_expires_at,outcome)
SELECT workflow_id,task_id,attempt_count,fencing_token,lease_owner,
       updated_at,lease_expires_at,'running'
FROM tasks
WHERE state='running' AND attempt_count > 0
ON CONFLICT DO NOTHING;

CREATE TABLE notification_outbox (
    event_id BIGSERIAL PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    locked_until TIMESTAMPTZ,
    lock_owner TEXT,
    publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    published_at TIMESTAMPTZ
);

CREATE INDEX notification_outbox_ready_idx
    ON notification_outbox(available_at, event_id) WHERE published_at IS NULL;

CREATE INDEX notification_outbox_retention_idx ON notification_outbox(created_at);
