import asyncio
import json
from uuid import uuid4

import asyncpg
import pytest

from app.security.hash_chain import generate_event_hash


@pytest.mark.asyncio
async def test_skip_locked_prevents_duplicate_claims(db_pool: asyncpg.Pool, reset_db):
    _ = reset_db
    tenant_id = uuid4()
    event_ids = [uuid4() for _ in range(5)]

    async with db_pool.acquire() as conn:
        for eid in event_ids:
            payload = {
                "event_type": "ResourceAllocationRequested",
                "node_id": str(eid),
                "target_cpu_cores": 1.0,
                "target_memory_gb": 1.0,
                "reason_code": "claim-test",
            }
            event_hash = generate_event_hash(None, payload, 1680000000000, 1)
            await conn.execute(
                """
                INSERT INTO events (
                    event_id, tenant_id, aggregate_id, sequence_id, timestamp_utc_ms,
                    idempotency_key, actor_id, actor_claims, expected_version, event_type,
                    payload, previous_hash, event_hash
                ) VALUES (
                    $1, $2, $3, 1, 1680000000000, $4, 'worker-test',
                    $5::jsonb, 0, 'ResourceAllocationRequested', $6::jsonb, NULL, $7
                )
                """,
                eid,
                tenant_id,
                eid,
                f"idem-{eid}",
                json.dumps([f"allocate:node:{eid}"]),
                json.dumps(payload),
                event_hash,
            )
            await conn.execute(
                "INSERT INTO outbox (event_id, tenant_id, status) VALUES ($1, $2, 'pending')",
                eid,
                tenant_id,
            )

    async def worker_claim(batch_size: int):
        claim_query = """
            WITH claimed AS (
                SELECT event_id
                FROM outbox
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE outbox
            SET status = 'processing'
            FROM claimed
            WHERE outbox.event_id = claimed.event_id
            RETURNING outbox.event_id;
        """

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                records = await conn.fetch(claim_query, batch_size)
                await asyncio.sleep(0.5)
                return [r["event_id"] for r in records]

    worker_1_claims, worker_2_claims = await asyncio.gather(
        worker_claim(3), worker_claim(3)
    )

    assert len(worker_1_claims) == 3
    assert len(worker_2_claims) == 2
    assert set(worker_1_claims).isdisjoint(set(worker_2_claims))
