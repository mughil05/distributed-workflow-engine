CREATE TABLE workflows (
    workflow_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'succeeded', 'failed')),
    definition JSONB NOT NULL,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE tasks (
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    task_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'succeeded', 'failed')),
    error TEXT,
    attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workflow_id, task_id),
    CHECK ((state = 'running' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (state <> 'running' AND lease_owner IS NULL AND lease_expires_at IS NULL))
);

CREATE TABLE task_dependencies (
    workflow_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    dependency_id TEXT NOT NULL,
    PRIMARY KEY (workflow_id, task_id, dependency_id),
    FOREIGN KEY (workflow_id, task_id) REFERENCES tasks(workflow_id, task_id) ON DELETE CASCADE,
    FOREIGN KEY (workflow_id, dependency_id) REFERENCES tasks(workflow_id, task_id) ON DELETE CASCADE,
    CHECK (task_id <> dependency_id)
);

CREATE INDEX tasks_pending_idx ON tasks(workflow_id, task_id) WHERE state = 'pending';
CREATE INDEX tasks_expired_lease_idx ON tasks(workflow_id, lease_expires_at) WHERE state = 'running';
CREATE INDEX task_dependencies_dependency_idx ON task_dependencies(workflow_id, dependency_id);
