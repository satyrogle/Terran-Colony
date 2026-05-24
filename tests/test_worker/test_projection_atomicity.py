from __future__ import annotations

import json
from uuid import uuid4

import pytest

from app.security.hash_chain import generate_event_hash
from app.worker import projection_worker
from app.worker.projection_worker import OutboxWorker


@pytest.mark.asyncio(loop_scope="session")
async def test_projection_and_outbox_status_are_atomic(db_pool, reset_db):
    _ = reset_db
    tenant_id = uuid4()
    node_id = uuid4()
    event_id = uuid4()
    payload = {
        "event_type": "ResourceAllocationRequested",
        "node_id": str(node_id),
        "target_cpu_cores": 2.0,
        "target_memory_gb": 4.0,
        "reason_code": "scale-up",
    }
    timestamp_utc_ms = 1680000000000
    event_hash = generate_event_hash(None, payload, timestamp_utc_ms, 1)

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO events (
                event_id, tenant_id, aggregate_id, sequence_id, timestamp_utc_ms,
                idempotency_key, actor_id, actor_claims, expected_version, event_type,
                payload, previous_hash, event_hash
            ) VALUES ($1, $2, $3, 1, $4, 'idem-1', 'operator-1', $5::jsonb, 0, 'ResourceAllocationRequested', $6::jsonb, NULL, $7)
            """,
            event_id,
            tenant_id,
            node_id,
            timestamp_utc_ms,
            json.dumps([f"allocate:node:{node_id}"]),
            json.dumps(payload),
            event_hash,
        )
        await conn.execute(
            "INSERT INTO outbox (event_id, tenant_id, status) VALUES ($1, $2, 'pending')",
            event_id,
            tenant_id,
        )

    worker = OutboxWorker(db_pool)

    async def _mock_evaluate_reconcile(event):
        _ = event
        return "completed", None

    async def _boom_mark_processed(conn, event_id, reconcile_status):
        _ = conn, event_id, reconcile_status
        raise RuntimeError("boom-before-outbox-commit")

    worker._evaluate_reconcile = _mock_evaluate_reconcile
    worker._mark_processed = _boom_mark_processed

    processed = await worker.process_next_batch(batch_size=1)
    assert processed is True

    async with db_pool.acquire() as conn:
        node_count = await conn.fetchval(
            "SELECT COUNT(*) FROM read_model_nodes WHERE tenant_id = $1 AND node_id = $2",
            tenant_id,
            node_id,
        )
        outbox_status = await conn.fetchval(
            "SELECT status FROM outbox WHERE event_id = $1",
            event_id,
        )
        guardrail_count = await conn.fetchval(
            "SELECT COUNT(*) FROM read_model_guardrail_alerts WHERE tenant_id = $1 AND node_id = $2",
            tenant_id,
            node_id,
        )

    assert node_count == 0
    assert outbox_status == "failed"
    assert guardrail_count == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_replayed_event_is_idempotent_for_projection(db_pool, reset_db):
    _ = reset_db
    tenant_id = uuid4()
    node_id = uuid4()
    genesis_event_id = uuid4()
    event_id = uuid4()
    payload = {
        "event_type": "ResourceAllocationRequested",
        "node_id": str(node_id),
        "target_cpu_cores": 6.0,
        "target_memory_gb": 12.0,
        "reason_code": "replay-check",
    }
    timestamp_utc_ms = 1680000000001
    genesis_hash = generate_event_hash(None, payload, timestamp_utc_ms, 1)
    event_hash = generate_event_hash(genesis_hash, payload, timestamp_utc_ms, 2)

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO events (
                event_id, tenant_id, aggregate_id, sequence_id, timestamp_utc_ms,
                idempotency_key, actor_id, actor_claims, expected_version, event_type,
                payload, previous_hash, event_hash
            ) VALUES ($1, $2, $3, 1, $4, 'idem-replay-genesis', 'operator-1', $5::jsonb, 0, 'ResourceAllocationRequested', $6::jsonb, NULL, $7)
            """,
            genesis_event_id,
            tenant_id,
            node_id,
            timestamp_utc_ms,
            json.dumps([f"allocate:node:{node_id}"]),
            json.dumps(payload),
            genesis_hash,
        )
        await conn.execute(
            """
            INSERT INTO events (
                event_id, tenant_id, aggregate_id, sequence_id, timestamp_utc_ms,
                idempotency_key, actor_id, actor_claims, expected_version, event_type,
                payload, previous_hash, event_hash
            ) VALUES ($1, $2, $3, 2, $4, 'idem-replay', 'operator-1', $5::jsonb, 1, 'ResourceAllocationRequested', $6::jsonb, $7, $8)
            """,
            event_id,
            tenant_id,
            node_id,
            timestamp_utc_ms,
            json.dumps([f"allocate:node:{node_id}"]),
            json.dumps(payload),
            genesis_hash,
            event_hash,
        )
        await conn.execute(
            """
            INSERT INTO read_model_nodes (
                tenant_id, node_id, lifecycle_state, cpu_cores, memory_gb, last_sequence_id, schema_version
            ) VALUES ($1, $2, 'active', 6.0, 12.0, 2, 1)
            """,
            tenant_id,
            node_id,
        )
        await conn.execute(
            "INSERT INTO outbox (event_id, tenant_id, status) VALUES ($1, $2, 'pending')",
            event_id,
            tenant_id,
        )

    worker = OutboxWorker(db_pool)

    async def _mock_evaluate_reconcile(event):
        _ = event
        return "completed", None

    worker._evaluate_reconcile = _mock_evaluate_reconcile

    processed = await worker.process_next_batch(batch_size=1)
    assert processed is True

    async with db_pool.acquire() as conn:
        projection = await conn.fetchrow(
            """
            SELECT cpu_cores, memory_gb, last_sequence_id
            FROM read_model_nodes
            WHERE tenant_id = $1 AND node_id = $2
            """,
            tenant_id,
            node_id,
        )
        outbox_status = await conn.fetchval(
            "SELECT status FROM outbox WHERE event_id = $1",
            event_id,
        )

    assert projection["cpu_cores"] == 6.0
    assert projection["memory_gb"] == 12.0
    assert projection["last_sequence_id"] == 2
    assert outbox_status == "processed"


@pytest.mark.asyncio(loop_scope="session")
async def test_fuzzy_guardrail_state_is_persisted(db_pool, reset_db, monkeypatch):
    _ = reset_db
    tenant_id = uuid4()
    node_id = uuid4()
    event_id = uuid4()
    payload = {
        "event_type": "ResourceAllocationRequested",
        "node_id": str(node_id),
        "target_cpu_cores": 4.0,
        "target_memory_gb": 8.0,
        "reason_code": "guardrail-wire",
    }
    timestamp_utc_ms = 1680000000002
    event_hash = generate_event_hash(None, payload, timestamp_utc_ms, 1)

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO events (
                event_id, tenant_id, aggregate_id, sequence_id, timestamp_utc_ms,
                idempotency_key, actor_id, actor_claims, expected_version, event_type,
                payload, previous_hash, event_hash
            ) VALUES ($1, $2, $3, 1, $4, 'idem-guardrail', 'operator-1', $5::jsonb, 0, 'ResourceAllocationRequested', $6::jsonb, NULL, $7)
            """,
            event_id,
            tenant_id,
            node_id,
            timestamp_utc_ms,
            json.dumps([f"allocate:node:{node_id}"]),
            json.dumps(payload),
            event_hash,
        )
        await conn.execute(
            "INSERT INTO outbox (event_id, tenant_id, status) VALUES ($1, $2, 'pending')",
            event_id,
            tenant_id,
        )

    class _FakePid:
        def observe_resource_change(self, current_utilization, aggregate_id):
            _ = current_utilization, aggregate_id
            return 0.77

        def classify_instability(self, control_signal):
            _ = control_signal
            return {
                "label": "drifting",
                "degree": 0.82,
                "membership": {"stable": 0.0, "drifting": 0.82, "volatile": 0.0},
                "control_signal_abs": 0.77,
            }

    monkeypatch.setattr(projection_worker, "pid_controller", _FakePid())

    worker = OutboxWorker(db_pool)

    async def _mock_evaluate_reconcile(event):
        _ = event
        return "completed", None

    worker._evaluate_reconcile = _mock_evaluate_reconcile

    processed = await worker.process_next_batch(batch_size=1)
    assert processed is True

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT severity, metric_value, reason, timestamp_utc_ms
            FROM read_model_guardrail_alerts
            WHERE tenant_id = $1 AND node_id = $2
            ORDER BY timestamp_utc_ms DESC
            LIMIT 1
            """,
            tenant_id,
            node_id,
        )
        outbox_status = await conn.fetchval(
            "SELECT status FROM outbox WHERE event_id = $1",
            event_id,
        )

    assert outbox_status == "processed"
    assert row is not None
    assert row["severity"] == "warning"
    assert row["metric_value"] == pytest.approx(0.77)
    assert "pid_state=drifting" in row["reason"]
    assert row["timestamp_utc_ms"] > 0
