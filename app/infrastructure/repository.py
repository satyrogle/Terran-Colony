from __future__ import annotations

from typing import List, Optional
from uuid import UUID

import asyncpg
from asyncpg.exceptions import UniqueViolationError
from pydantic import ValidationError

from app.domain.schemas import EventEnvelope, ResourceNodeSnapshot
from app.security.hash_chain import generate_event_hash, verify_chain


class ConcurrencyConflictError(Exception):
    pass


class IdempotencyKeyInUseError(Exception):
    pass


class DataCorruptionError(Exception):
    pass


class EventRepository:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def _get_latest_hash(
        self, conn: asyncpg.Connection, tenant_id: UUID, aggregate_id: UUID
    ) -> Optional[str]:
        query = """
            SELECT event_hash
            FROM events
            WHERE tenant_id = $1 AND aggregate_id = $2
            ORDER BY sequence_id DESC
            LIMIT 1
        """
        return await conn.fetchval(query, tenant_id, aggregate_id)

    async def get_stream_head(
        self, conn: asyncpg.Connection, tenant_id: UUID, aggregate_id: UUID
    ) -> tuple[int, Optional[str]]:
        query = """
            SELECT sequence_id, event_hash
            FROM events
            WHERE tenant_id = $1 AND aggregate_id = $2
            ORDER BY sequence_id DESC
            LIMIT 1
        """
        record = await conn.fetchrow(query, tenant_id, aggregate_id)
        if record is None:
            return 0, None
        return int(record["sequence_id"]), record["event_hash"]

    async def _append_event_and_enqueue_with_conn(
        self, conn: asyncpg.Connection, envelope: EventEnvelope
    ) -> None:
        append_query = """
            INSERT INTO events (
                event_id, tenant_id, aggregate_id, sequence_id,
                timestamp_utc_ms, idempotency_key, actor_id,
                actor_claims, expected_version, event_type, payload,
                previous_hash, event_hash
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11::jsonb, $12, $13)
        """

        outbox_query = """
            INSERT INTO outbox (event_id, tenant_id, status)
            VALUES ($1, $2, 'pending')
        """

        payload_dict = envelope.payload.model_dump(mode="json")
        previous_hash = await self._get_latest_hash(
            conn, envelope.tenant_id, envelope.aggregate_id
        )
        event_hash = generate_event_hash(
            previous_hash=previous_hash,
            payload=payload_dict,
            timestamp_ms=envelope.timestamp_utc_ms,
            sequence_id=envelope.sequence_id,
            tenant_id=envelope.tenant_id,
            aggregate_id=envelope.aggregate_id,
        )

        await conn.execute(
            append_query,
            envelope.event_id,
            envelope.tenant_id,
            envelope.aggregate_id,
            envelope.sequence_id,
            envelope.timestamp_utc_ms,
            envelope.idempotency_key,
            envelope.actor_id,
            envelope.actor_claims,
            envelope.expected_version,
            envelope.payload.event_type,
            payload_dict,
            previous_hash,
            event_hash,
        )

        await conn.execute(outbox_query, envelope.event_id, envelope.tenant_id)

    async def append_event_and_enqueue_in_transaction(
        self, conn: asyncpg.Connection, envelope: EventEnvelope
    ) -> None:
        await self._append_event_and_enqueue_with_conn(conn, envelope)

    async def append_event_and_enqueue(self, envelope: EventEnvelope) -> None:
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await self._append_event_and_enqueue_with_conn(conn, envelope)

        except UniqueViolationError as e:
            constraint_name = e.constraint_name
            if constraint_name == "uq_aggregate_sequence":
                raise ConcurrencyConflictError(
                    f"Sequence {envelope.sequence_id} already exists for aggregate {envelope.aggregate_id}."
                )
            if constraint_name == "uq_tenant_idempotency":
                raise IdempotencyKeyInUseError(
                    f"Idempotency key {envelope.idempotency_key} is already in use for this tenant."
                )
            raise

    async def get_events(
        self, tenant_id: UUID, aggregate_id: UUID, after_sequence_id: int = 0
    ) -> List[EventEnvelope]:
        query = """
            SELECT event_id, tenant_id, aggregate_id, sequence_id,
                   timestamp_utc_ms, idempotency_key, actor_id, actor_claims,
                   expected_version, payload, previous_hash, event_hash
            FROM events
            WHERE tenant_id = $1 AND aggregate_id = $2 AND sequence_id > $3
            ORDER BY sequence_id ASC
        """
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query, tenant_id, aggregate_id, after_sequence_id)

        for record in records:
            payload_dict = record["payload"]
            verify_chain(
                previous_hash=record["previous_hash"],
                current_hash=record["event_hash"],
                payload=payload_dict,
                timestamp_ms=record["timestamp_utc_ms"],
                sequence_id=record["sequence_id"],
                tenant_id=record["tenant_id"],
                aggregate_id=record["aggregate_id"],
            )

        return [self._map_record_to_envelope(record) for record in records]

    async def get_event_by_id(
        self, conn: asyncpg.Connection, event_id: UUID, tenant_id: UUID
    ) -> Optional[EventEnvelope]:
        query = """
            SELECT event_id, tenant_id, aggregate_id, sequence_id,
                   timestamp_utc_ms, idempotency_key, actor_id, actor_claims,
                   expected_version, payload, previous_hash, event_hash
            FROM events
            WHERE event_id = $1 AND tenant_id = $2
            LIMIT 1
        """
        record = await conn.fetchrow(query, event_id, tenant_id)
        if record is None:
            return None

        verify_chain(
            previous_hash=record["previous_hash"],
            current_hash=record["event_hash"],
            payload=record["payload"],
            timestamp_ms=record["timestamp_utc_ms"],
            sequence_id=record["sequence_id"],
            tenant_id=record["tenant_id"],
            aggregate_id=record["aggregate_id"],
        )

        return self._map_record_to_envelope(record)

    async def get_node_projection(
        self, conn: asyncpg.Connection, tenant_id: UUID, node_id: UUID
    ) -> Optional[ResourceNodeSnapshot]:
        query = """
            SELECT node_id, lifecycle_state, cpu_cores, memory_gb, last_sequence_id, schema_version
            FROM read_model_nodes
            WHERE tenant_id = $1 AND node_id = $2
        """
        record = await conn.fetchrow(query, tenant_id, node_id)
        if record is None:
            return None
        return ResourceNodeSnapshot.model_validate(dict(record))

    async def upsert_node_projection(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        snapshot: ResourceNodeSnapshot,
    ) -> None:
        query = """
            INSERT INTO read_model_nodes (
                tenant_id, node_id, lifecycle_state, cpu_cores, memory_gb, last_sequence_id, schema_version, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (tenant_id, node_id) DO UPDATE
            SET lifecycle_state = EXCLUDED.lifecycle_state,
                cpu_cores = EXCLUDED.cpu_cores,
                memory_gb = EXCLUDED.memory_gb,
                last_sequence_id = EXCLUDED.last_sequence_id,
                schema_version = EXCLUDED.schema_version,
                updated_at = NOW()
            WHERE read_model_nodes.last_sequence_id < EXCLUDED.last_sequence_id
        """
        await conn.execute(
            query,
            tenant_id,
            snapshot.node_id,
            snapshot.lifecycle_state,
            snapshot.cpu_cores,
            snapshot.memory_gb,
            snapshot.last_sequence_id,
            snapshot.schema_version,
        )

    async def insert_guardrail_alert(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        node_id: UUID,
        severity: str,
        metric_value: float,
        reason: str,
        timestamp_utc_ms: int,
    ) -> None:
        query = """
            INSERT INTO read_model_guardrail_alerts (
                tenant_id, node_id, severity, metric_value, reason, timestamp_utc_ms
            ) VALUES ($1, $2, $3, $4, $5, $6)
        """
        await conn.execute(
            query,
            tenant_id,
            node_id,
            severity,
            metric_value,
            reason,
            timestamp_utc_ms,
        )

    def _map_record_to_envelope(self, record: asyncpg.Record) -> EventEnvelope:
        try:
            raw_dict = dict(record)
            raw_dict.pop("previous_hash", None)
            raw_dict.pop("event_hash", None)
            return EventEnvelope.model_validate(raw_dict)
        except ValidationError as e:
            raise DataCorruptionError(
                f"Failed to rehydrate event {record['event_id']}: {str(e)}"
            )
