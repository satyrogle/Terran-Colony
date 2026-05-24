from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from typing import Optional
from uuid import UUID, uuid4

import asyncpg

from app.api.middleware import backpressure_manager
from app.control.pid_guardrail import PIDGuardrailController
from app.domain.reducers import reduce_node
from app.domain.schemas import (
    CompensationStrategySelected,
    EventEnvelope,
    ResourceAllocationRequested,
    RollbackInitiated,
)
from app.infrastructure.adapters.mock_aws import MockAWSAdapter
from app.infrastructure.repository import DataCorruptionError, EventRepository
from app.worker.reconciler import ReconcilerLoop

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 5
POLL_INTERVAL_SEC = 1.0
RETRY_BACKOFF_BASE_SECONDS = 15
PROCESSING_LEASE_TIMEOUT_SECONDS = 300
MAX_CPU_CORES_FOR_GUARDRAIL = 16.0

GUARDRAIL_SEVERITY_BY_LABEL = {
    "stable": "normal",
    "drifting": "warning",
    "volatile": "approval_required",
}

pid_controller = PIDGuardrailController(kp=0.5, ki=0.1, kd=0.2, setpoint=0.8)


def _termination_signals() -> list[signal.Signals]:
    signals = [signal.SIGINT]
    if hasattr(signal, "SIGTERM"):
        signals.append(signal.SIGTERM)
    return signals


def _install_shutdown_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        logger.info("Shutdown requested; stopping outbox worker.")
        stop_event.set()

    for sig in _termination_signals():
        try:
            loop.add_signal_handler(sig, request_stop)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(request_stop))


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
                WHERE (
                    (
                        status IN ('pending', 'failed')
                        AND (
                            last_attempt_at IS NULL
                            OR last_attempt_at < NOW() - (POWER(2, attempts) * $2::interval)
                        )
                    )
                    OR (
                        status = 'processing'
                        AND (
                            last_attempt_at IS NULL
                            OR last_attempt_at < NOW() - $3::interval
                        )
                    )
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
                rows = await conn.fetch(
                    claim_query,
                    batch_size,
                    f"{RETRY_BACKOFF_BASE_SECONDS} seconds",
                    f"{PROCESSING_LEASE_TIMEOUT_SECONDS} seconds",
                )

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

                guardrail_state = self._observe_guardrail_state(event)
                reconcile_status, reconcile_error = await self._evaluate_reconcile(event)
                compensation_strategy = self._extract_compensation_strategy(reconcile_status)
                should_apply_projection = self._should_apply_projection(
                    reconcile_status, reconcile_error
                )
                next_state = None
                if should_apply_projection:
                    next_state = await self._compute_next_projection_state(conn, event)

                async with conn.transaction():
                    if next_state is not None:
                        await self.repository.upsert_node_projection(conn, tenant_id, next_state)

                    if guardrail_state is not None and should_apply_projection:
                        await self.repository.insert_guardrail_alert(
                            conn=conn,
                            tenant_id=tenant_id,
                            node_id=guardrail_state["node_id"],
                            severity=guardrail_state["severity"],
                            metric_value=guardrail_state["metric_value"],
                            reason=guardrail_state["reason"],
                            timestamp_utc_ms=guardrail_state["timestamp_utc_ms"],
                        )

                    if reconcile_error is None:
                        if compensation_strategy is not None:
                            await self._emit_compensation_followups(
                                conn=conn,
                                source_event=event,
                                strategy_id=compensation_strategy,
                            )
                        await self._mark_processed(conn, event_id, reconcile_status)
                    else:
                        await self._mark_failed(conn, event_id, attempts, reconcile_error)

        except Exception as exc:
            logger.error("Failed processing event %s: %s", event_id, exc)
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await self._mark_failed(conn, event_id, attempts, str(exc))
        finally:
            try:
                await backpressure_manager.record_completion()
            except Exception:
                logger.exception(
                    "Failed to record backpressure completion for event %s", event_id
                )

    async def _compute_next_projection_state(self, conn: asyncpg.Connection, event: EventEnvelope):
        current_state = await self.repository.get_node_projection(
            conn, event.tenant_id, event.aggregate_id
        )
        return reduce_node(current_state, event)

    def _observe_guardrail_state(self, event: EventEnvelope) -> dict | None:
        if isinstance(event.payload, ResourceAllocationRequested):
            current_utilization = event.payload.target_cpu_cores / MAX_CPU_CORES_FOR_GUARDRAIL
            control_signal = pid_controller.observe_resource_change(
                current_utilization=current_utilization,
                aggregate_id=f"{event.tenant_id}:{event.aggregate_id}",
            )
            fuzzy_state = pid_controller.classify_instability(control_signal)
            label = fuzzy_state["label"]
            if fuzzy_state["label"] == "drifting":
                logger.warning(
                    "Guardrail drift detected for aggregate %s: degree=%.2f signal_abs=%.3f",
                    event.aggregate_id,
                    fuzzy_state["degree"],
                    fuzzy_state["control_signal_abs"],
                )
            elif fuzzy_state["label"] == "volatile":
                logger.critical(
                    "Guardrail volatility detected for aggregate %s: degree=%.2f signal_abs=%.3f",
                    event.aggregate_id,
                    fuzzy_state["degree"],
                    fuzzy_state["control_signal_abs"],
                )
            severity = GUARDRAIL_SEVERITY_BY_LABEL.get(label, "warning")
            return {
                "node_id": event.aggregate_id,
                "severity": severity,
                "metric_value": float(fuzzy_state["control_signal_abs"]),
                "reason": (
                    f"pid_state={label} degree={fuzzy_state['degree']:.2f} "
                    f"signal_abs={fuzzy_state['control_signal_abs']:.3f}"
                ),
                "timestamp_utc_ms": int(time.time() * 1000),
            }
        return None

    async def _evaluate_reconcile(self, event: EventEnvelope) -> tuple[str, str | None]:
        if isinstance(event.payload, ResourceAllocationRequested):
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

    def _extract_compensation_strategy(self, reconcile_status: str) -> Optional[str]:
        prefix = "compensating_via_"
        if not reconcile_status.startswith(prefix):
            return None
        strategy = reconcile_status[len(prefix) :].strip()
        return strategy or None

    def _should_apply_projection(
        self, reconcile_status: str, reconcile_error: str | None
    ) -> bool:
        if reconcile_error is not None:
            return False
        return reconcile_status in {"completed", "not_applicable"}

    async def _emit_compensation_followups(
        self,
        conn: asyncpg.Connection,
        source_event: EventEnvelope,
        strategy_id: str,
    ) -> None:
        head_sequence, _ = await self.repository.get_stream_head(
            conn, source_event.tenant_id, source_event.aggregate_id
        )
        base_sequence = max(head_sequence, source_event.sequence_id)
        now_ms = int(time.time() * 1000)

        compensation_event = EventEnvelope(
            event_id=uuid4(),
            tenant_id=source_event.tenant_id,
            aggregate_id=source_event.aggregate_id,
            sequence_id=base_sequence + 1,
            timestamp_utc_ms=now_ms,
            idempotency_key=f"compensation-{source_event.event_id}",
            actor_id="worker-compensator",
            actor_claims=[],
            expected_version=base_sequence,
            payload=CompensationStrategySelected(
                intent_id=str(source_event.event_id),
                aggregate_id=source_event.aggregate_id,
                selected_strategy=strategy_id,
                utility_scores={"selected_strategy_weight": 1.0},
            ),
        )
        await self.repository.append_event_and_enqueue_in_transaction(conn, compensation_event)

        rollback_event = EventEnvelope(
            event_id=uuid4(),
            tenant_id=source_event.tenant_id,
            aggregate_id=source_event.aggregate_id,
            sequence_id=base_sequence + 2,
            timestamp_utc_ms=now_ms + 1,
            idempotency_key=f"rollback-{source_event.event_id}",
            actor_id="worker-compensator",
            actor_claims=[],
            expected_version=base_sequence + 1,
            payload=RollbackInitiated(
                target_node_id=source_event.aggregate_id,
                target_sequence_id=max(0, source_event.sequence_id - 1),
                reason_code=f"auto-compensation:{strategy_id}",
            ),
        )
        await self.repository.append_event_and_enqueue_in_transaction(conn, rollback_event)

    async def _mark_processed(
        self, conn: asyncpg.Connection, event_id: UUID, reconcile_status: str
    ) -> None:
        await conn.execute(
            """
            UPDATE outbox
            SET status = 'processed',
                processed_at = NOW(),
                error_payload = jsonb_build_object('reconcile_status', $2::text)
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
                error_payload = jsonb_build_object('error', $2::text)
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
    _install_shutdown_handlers(stop_event)

    try:
        await worker.run(stop_event=stop_event)
    finally:
        await pool.close()
        logger.info("Worker pool closed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
