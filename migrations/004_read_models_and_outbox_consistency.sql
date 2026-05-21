-- Read-model tables required by projection and telemetry routes.
CREATE TABLE IF NOT EXISTS read_model_nodes (
    tenant_id UUID NOT NULL,
    node_id UUID NOT NULL,
    lifecycle_state VARCHAR(20) NOT NULL,
    cpu_cores DOUBLE PRECISION NOT NULL,
    memory_gb DOUBLE PRECISION NOT NULL,
    last_sequence_id INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, node_id)
);

CREATE INDEX IF NOT EXISTS idx_read_model_nodes_sequence
ON read_model_nodes(tenant_id, last_sequence_id DESC);

CREATE TABLE IF NOT EXISTS read_model_service_graph_edges (
    tenant_id UUID NOT NULL,
    source_node_id UUID NOT NULL,
    target_node_id UUID NOT NULL,
    last_sequence_id INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, source_node_id, target_node_id)
);

CREATE TABLE IF NOT EXISTS read_model_guardrail_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    node_id UUID NOT NULL,
    severity VARCHAR(32) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    reason TEXT NOT NULL,
    timestamp_utc_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_guardrail_latest
ON read_model_guardrail_alerts(tenant_id, node_id, timestamp_utc_ms DESC);

-- Keep outbox schema aligned with tests and operational polling patterns.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
