from __future__ import annotations

import asyncio
import logging
import os
from uuid import UUID

import asyncpg

from app.api.middleware import backpressure_manager
from app.control.pid_guardrail import PIDGuardrailController
from app.domain.reducers import reduce_node
from app.domain.schemas import EventEnvelope, ResourceAllocationRequested
from app.infrastructure.adapters.mock_aws import MockAWSAdapter
from app.infrastructure.repository import DataCorruptionError, EventRepository
from app.worker.reconciler import ReconcilerLoop

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 5
POLL_INTERVAL_SEC = 1.0

pid_controller = PIDGuardrailController(kp=0.5, ki=0.1, kd=0.2, setpoint=0.8)


class OutboxWorker:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self.repository = EventRepository(pool)
        self.reconciler = ReconcilerLoop(MockAWSAdapter())

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        if stop_event is None:
            stop_event = asyncio.Event()

        logger.info("Starting outbox worker loop")
        while not stop_event.is_set():
            try:
                processed_any = await self.process_next_batch()
                if not processed_any:
                    await asyncio.sleep(POLL_INTERVAL_SEC)
            except Exception as exc:  # safeguard loop
                logger.exception("Worker loop error: %s", exc)
                await asyncio.sleep(POLL_INTERVAL_SEC)
        logger.info("Outbox worker loop stopped.")

    async def process_next_batch(self, batch_size: int = 10) -> bool:
        claim_query = """
            WITH claimed AS (
                SELECT event_id, tenant_id
                FROM outbox
                WHERE status IN ('pending', 'failed')
                  AND (
                    last_attempt_at IS NULL
                    OR last_attempt_at < NOW() - (POWER(2, attempts) * INTERVAL '1 second')
                  )
                ORDER BY created_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE outbox
            SET status = 'processing',
                attempts = attempts + 1,
                last_attempt_at = NOW()
            FROM claimed
            WHERE outbox.event_id = claimed.event_id
            RETURNING outbox.event_id, outbox.tenant_id, outbox.attempts;
        """

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(claim_query, batch_size)

        if not rows:
            return False

        for row in rows:
            await self._process_record(row)
        return True

    async def _process_record(self, row: asyncpg.Record) -> None:
        event_id: UUID = row["event_id"]
        tenant_id: UUID = row["tenant_id"]
        attempts: int = row["attempts"]

        try:
            async with self.pool.acquire() as conn:
                event = await self.repository.get_event_by_id(conn, event_id, tenant_id)
                if event is None:
                    raise DataCorruptionError(
                        f"Missing event {event_id} for tenant {tenant_id} referenced by outbox."
                    )

                reconcile_status, reconcile_error = await self._evaluate_reconcile(event)
                next_state = await self._compute_next_projection_state(conn, event)

                async with conn.transaction():
                    if next_state is not None:
                        await self.repository.upsert_node_projection(conn, tenant_id, next_state)

                    if reconcile_error is None:
                        await self._mark_processed(conn, event_id, reconcile_status)
                    else:
                        await self._mark_failed(conn, event_id, attempts, reconcile_error)

            await backpressure_manager.record_completion()
        except Exception as exc:
            logger.error("Failed processing event %s: %s", event_id, exc)
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await self._mark_failed(conn, event_id, attempts, str(exc))

    async def _compute_next_projection_state(self, conn: asyncpg.Connection, event: EventEnvelope):
        current_state = await self.repository.get_node_projection(
            conn, event.tenant_id, event.aggregate_id
        )
        return reduce_node(current_state, event)

    async def _evaluate_reconcile(self, event: EventEnvelope) -> tuple[str, str | None]:
        if isinstance(event.payload, ResourceAllocationRequested):
            max_limit_cores = 16.0
            current_utilization = event.payload.target_cpu_cores / max_limit_cores
            pid_controller.observe_resource_change(
                current_utilization=current_utilization,
                aggregate_id=str(event.aggregate_id),
            )
            try:
                status = await self.reconciler.execute_intent(
                    str(event.event_id),
                    str(event.aggregate_id),
                    event.payload.model_dump(mode="json"),
                )
                return status, None
            except Exception as exc:
                return "retry_scheduled", str(exc)
        return "not_applicable", None

    async def _mark_processed(
        self, conn: asyncpg.Connection, event_id: UUID, reconcile_status: str
    ) -> None:
        await conn.execute(
            """
            UPDATE outbox
            SET status = 'processed',
                processed_at = NOW(),
                error_payload = jsonb_build_object('reconcile_status', $2)
            WHERE event_id = $1
            """,
            event_id,
            reconcile_status,
        )

    async def _mark_failed(
        self,
        conn: asyncpg.Connection,
        event_id: UUID,
        attempts: int,
        error_msg: str,
    ) -> None:
        status = "dead_letter" if attempts >= MAX_ATTEMPTS else "failed"
        if status == "dead_letter":
            logger.critical("Event %s moved to dead_letter", event_id)

        await conn.execute(
            """
            UPDATE outbox
            SET status = $1,
                error_payload = jsonb_build_object('error', $2)
            WHERE event_id = $3
            """,
            status,
            error_msg,
            event_id,
        )


async def create_pool_from_env() -> asyncpg.Pool:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set for worker startup.")
    return await asyncpg.create_pool(
        dsn=database_url,
        min_size=1,
        max_size=10,
        command_timeout=30,
    )


async def main() -> None:
    pool = await create_pool_from_env()
    worker = OutboxWorker(pool)
    stop_event = asyncio.Event()

    try:
        await worker.run(stop_event=stop_event)
    finally:
        await pool.close()
        logger.info("Worker pool closed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
