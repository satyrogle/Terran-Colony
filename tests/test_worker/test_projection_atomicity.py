from __future__ import annotations

import json
from uuid import uuid4

import pytest

from app.security.hash_chain import generate_event_hash
from app.worker.projection_worker import OutboxWorker


@pytest.mark.asyncio
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

    assert node_count == 0
    assert outbox_status == "failed"
