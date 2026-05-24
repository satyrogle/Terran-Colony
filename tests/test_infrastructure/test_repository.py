from __future__ import annotations

from uuid import uuid4

import pytest

from app.domain.schemas import EventEnvelope, ResourceAllocationRequested
from app.infrastructure.repository import (
    ConcurrencyConflictError,
    EventRepository,
    IdempotencyKeyInUseError,
)


class _UniqueViolationError(Exception):
    def __init__(self, constraint_name: str):
        super().__init__(constraint_name)
        self.constraint_name = constraint_name


class _TxCtx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self, *, latest_hash: str | None = None, execute_side_effect=None):
        self.latest_hash = latest_hash
        self.execute_side_effect = execute_side_effect
        self.executed: list[tuple[str, tuple]] = []

    async def fetchval(self, query, tenant_id, aggregate_id):
        return self.latest_hash

    async def execute(self, query, *args):
        self.executed.append((query, args))
        if self.execute_side_effect is not None:
            raise self.execute_side_effect

    def transaction(self):
        return _TxCtx()


class _AcquireCtx:
    def __init__(self, conn: _FakeConn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def __init__(self, conn: _FakeConn):
        self.conn = conn

    def acquire(self):
        return _AcquireCtx(self.conn)


def _envelope(*, idempotency_key: str = "idemp-1", sequence_id: int = 1) -> EventEnvelope:
    tenant_id = uuid4()
    aggregate_id = uuid4()
    return EventEnvelope(
        event_id=uuid4(),
        tenant_id=tenant_id,
        aggregate_id=aggregate_id,
        sequence_id=sequence_id,
        timestamp_utc_ms=1700000000000,
        idempotency_key=idempotency_key,
        actor_id="tester",
        actor_claims=[f"allocate:node:{aggregate_id}"],
        expected_version=max(0, sequence_id - 1),
        payload=ResourceAllocationRequested(
            node_id=aggregate_id,
            target_cpu_cores=2.0,
            target_memory_gb=4.0,
            reason_code="test",
        ),
    )


@pytest.mark.asyncio
async def test_append_raises_concurrency_conflict_for_sequence_collision(monkeypatch):
    monkeypatch.setattr(
        "app.infrastructure.repository.UniqueViolationError", _UniqueViolationError
    )
    conn = _FakeConn(execute_side_effect=_UniqueViolationError("uq_aggregate_sequence"))
    repo = EventRepository(_FakePool(conn))

    with pytest.raises(ConcurrencyConflictError):
        await repo.append_event_and_enqueue(_envelope())


@pytest.mark.asyncio
async def test_append_raises_idempotency_conflict(monkeypatch):
    monkeypatch.setattr(
        "app.infrastructure.repository.UniqueViolationError", _UniqueViolationError
    )
    conn = _FakeConn(execute_side_effect=_UniqueViolationError("uq_tenant_idempotency"))
    repo = EventRepository(_FakePool(conn))

    with pytest.raises(IdempotencyKeyInUseError):
        await repo.append_event_and_enqueue(_envelope(idempotency_key="dup-key"))


@pytest.mark.asyncio
async def test_append_writes_hash_fields_and_enqueues_outbox(monkeypatch):
    conn = _FakeConn(latest_hash="a" * 64)
    repo = EventRepository(_FakePool(conn))

    captured_hash_args = {}

    def _fake_hash(*, previous_hash, payload, timestamp_ms, sequence_id, tenant_id, aggregate_id):
        captured_hash_args.update(
            {
                "previous_hash": previous_hash,
                "payload": payload,
                "timestamp_ms": timestamp_ms,
                "sequence_id": sequence_id,
                "tenant_id": tenant_id,
                "aggregate_id": aggregate_id,
            }
        )
        return "b" * 64

    monkeypatch.setattr("app.infrastructure.repository.generate_event_hash", _fake_hash)

    envelope = _envelope(sequence_id=2)
    await repo.append_event_and_enqueue(envelope)

    assert captured_hash_args["previous_hash"] == "a" * 64
    assert captured_hash_args["sequence_id"] == 2
    assert captured_hash_args["tenant_id"] == envelope.tenant_id
    assert captured_hash_args["aggregate_id"] == envelope.aggregate_id

    assert len(conn.executed) == 2
    append_query, append_args = conn.executed[0]
    outbox_query, outbox_args = conn.executed[1]

    assert "INSERT INTO events" in append_query
    assert append_args[-2] == "a" * 64
    assert append_args[-1] == "b" * 64

    assert "INSERT INTO outbox" in outbox_query
    assert outbox_args == (envelope.event_id, envelope.tenant_id)
