ALTER TABLE workflows
    ADD COLUMN idempotency_key TEXT UNIQUE;

CREATE INDEX workflows_created_cursor_idx
    ON workflows(created_at DESC, workflow_id DESC);

CREATE INDEX workflows_state_created_cursor_idx
    ON workflows(state, created_at DESC, workflow_id DESC);

CREATE INDEX workflows_active_discovery_idx
    ON workflows(workflow_id) WHERE state IN ('pending', 'running');

CREATE INDEX tasks_active_lease_idx
    ON tasks(lease_expires_at) WHERE state = 'running';
