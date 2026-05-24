from uuid import uuid4

import pytest

from app.domain.schemas import EventEnvelope, ResourceAllocationRequested
from app.worker.projection_worker import OutboxWorker


class _DummyPool:
    pass


class _CaptureRepo:
    def __init__(self):
        self.appended = []

    async def get_stream_head(self, conn, tenant_id, aggregate_id):
        _ = conn, tenant_id, aggregate_id
        return 7, "h" * 64

    async def append_event_and_enqueue_in_transaction(self, conn, envelope):
        _ = conn
        self.appended.append(envelope)


def _source_event() -> EventEnvelope:
    tenant_id = uuid4()
    aggregate_id = uuid4()
    return EventEnvelope(
        event_id=uuid4(),
        tenant_id=tenant_id,
        aggregate_id=aggregate_id,
        sequence_id=7,
        timestamp_utc_ms=1700000000000,
        idempotency_key="idem-source",
        actor_id="operator-1",
        actor_claims=[f"allocate:node:{aggregate_id}"],
        expected_version=6,
        payload=ResourceAllocationRequested(
            node_id=aggregate_id,
            target_cpu_cores=2.0,
            target_memory_gb=4.0,
            reason_code="partial-failure",
        ),
    )


@pytest.mark.asyncio
async def test_emit_compensation_followups_appends_compensation_and_rollback_events():
    worker = OutboxWorker(_DummyPool())
    repo = _CaptureRepo()
    worker.repository = repo
    source_event = _source_event()

    await worker._emit_compensation_followups(
        conn=object(),
        source_event=source_event,
        strategy_id="full_revert",
    )

    assert len(repo.appended) == 2
    compensation_event, rollback_event = repo.appended

    assert compensation_event.sequence_id == 8
    assert compensation_event.expected_version == 7
    assert compensation_event.payload.event_type == "CompensationStrategySelected"
    assert compensation_event.payload.selected_strategy == "full_revert"

    assert rollback_event.sequence_id == 9
    assert rollback_event.expected_version == 8
    assert rollback_event.payload.event_type == "RollbackInitiated"
    assert rollback_event.payload.reason_code == "auto-compensation:full_revert"


def test_extract_compensation_strategy_parses_status():
    worker = OutboxWorker(_DummyPool())

    assert worker._extract_compensation_strategy("compensating_via_full_revert") == "full_revert"
    assert worker._extract_compensation_strategy("completed") is None
