from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.api.routers.commands import _assert_dependency_edge_is_acyclic, validate_and_append
from app.domain.schemas import (
    DependencyEdgeProposed,
    EventEnvelope,
    ResourceAllocationRequested,
)
from app.main import app
from app.security.abac import SubjectContext, SubjectRole


@pytest.fixture(autouse=True)
def _disable_backpressure_and_prepare_state(monkeypatch):
    async def _not_overloaded():
        return False

    async def _record_arrival():
        return None

    monkeypatch.setattr("app.api.middleware.backpressure_manager.is_overloaded", _not_overloaded)
    monkeypatch.setattr("app.api.middleware.backpressure_manager.record_arrival", _record_arrival)
    app.state.db_pool = object()


@pytest.fixture
def client():
    return TestClient(app)


def test_missing_idempotency_key_returns_422(client):
    payload = {
        "node_id": str(uuid4()),
        "target_cpu_cores": 2.0,
        "target_memory_gb": 4.0,
        "reason_code": "scale-up",
    }
    response = client.post(
        "/api/v1/commands/resource-allocation",
        json=payload,
        headers={"x-expected-version": "0", "x-tenant-id": str(uuid4()), "x-actor-id": "u1"},
    )
    assert response.status_code == 422
    assert "x-idempotency-key" in response.text.lower()


def test_dependency_edge_missing_header_returns_422(client):
    payload = {"source_node_id": str(uuid4()), "target_node_id": str(uuid4())}
    response = client.post(
        "/api/v1/commands/dependency-edge",
        json=payload,
        headers={"x-expected-version": "0", "x-tenant-id": str(uuid4()), "x-actor-id": "u1"},
    )
    assert response.status_code == 422


def test_rollback_missing_header_returns_422(client):
    payload = {
        "target_aggregate_id": str(uuid4()),
        "target_sequence_id": 1,
        "reason_code": "emergency-revert",
    }
    response = client.post(
        "/api/v1/commands/rollback",
        json=payload,
        headers={"x-expected-version": "0", "x-tenant-id": str(uuid4()), "x-actor-id": "u1"},
    )
    assert response.status_code == 422


def test_resource_allocation_missing_tenant_header_returns_422(client):
    payload = {
        "node_id": str(uuid4()),
        "target_cpu_cores": 2.0,
        "target_memory_gb": 4.0,
        "reason_code": "scale-up",
    }
    response = client.post(
        "/api/v1/commands/resource-allocation",
        json=payload,
        headers={
            "x-idempotency-key": "idem-1",
            "x-expected-version": "0",
            "x-actor-id": "u1",
        },
    )
    assert response.status_code == 422
    assert "x-tenant-id" in response.text.lower()


def test_invalid_role_claim_returns_401(client):
    payload = {
        "node_id": str(uuid4()),
        "target_cpu_cores": 2.0,
        "target_memory_gb": 4.0,
        "reason_code": "scale-up",
    }
    response = client.post(
        "/api/v1/commands/resource-allocation",
        json=payload,
        headers={
            "x-idempotency-key": "idem-invalid-role",
            "x-expected-version": "0",
            "x-tenant-id": str(uuid4()),
            "x-actor-id": "u1",
            "x-role": "intruder",
        },
    )

    assert response.status_code == 401
    assert "invalid role claim" in response.text.lower()


def test_body_tenant_spoofing_is_rejected_by_ingress_schema(client):
    payload = {
        "tenant_id": str(uuid4()),
        "node_id": str(uuid4()),
        "target_cpu_cores": 2.0,
        "target_memory_gb": 4.0,
        "reason_code": "scale-up",
    }
    response = client.post(
        "/api/v1/commands/resource-allocation",
        json=payload,
        headers={
            "x-idempotency-key": "idem-spoof",
            "x-expected-version": "0",
            "x-tenant-id": str(uuid4()),
            "x-actor-id": "u1",
        },
    )

    assert response.status_code == 422
    assert "tenant_id" in response.text


def test_dependency_edge_cycle_detection_rejects_cycle():
    tenant_id = uuid4()
    node_a = uuid4()
    node_b = uuid4()
    existing = EventEnvelope(
        event_id=uuid4(),
        tenant_id=tenant_id,
        aggregate_id=uuid4(),
        sequence_id=1,
        timestamp_utc_ms=1680000000000,
        idempotency_key="edge-a-b",
        actor_id="u1",
        actor_claims=[f"link:node:{node_a}"],
        expected_version=0,
        payload=DependencyEdgeProposed(
            source_node_id=node_a,
            target_node_id=node_b,
        ),
    )

    with pytest.raises(Exception) as exc_info:
        _assert_dependency_edge_is_acyclic(
            [existing],
            DependencyEdgeProposed(source_node_id=node_b, target_node_id=node_a),
        )

    assert getattr(exc_info.value, "status_code", None) == 422


class _FakeCommandRepository:
    def __init__(self, reconcile_status: str = "not_applicable"):
        self.reconcile_status = reconcile_status
        self.appended = False

    async def get_active_reconcile_status(self, tenant_id, aggregate_id):
        _ = tenant_id, aggregate_id
        return self.reconcile_status

    async def get_events(self, tenant_id, aggregate_id):
        _ = tenant_id, aggregate_id
        return []

    async def append_event_and_enqueue(self, envelope):
        _ = envelope
        self.appended = True


def _allocation_envelope(tenant_id, aggregate_id):
    return EventEnvelope(
        event_id=uuid4(),
        tenant_id=tenant_id,
        aggregate_id=aggregate_id,
        sequence_id=1,
        timestamp_utc_ms=1680000000000,
        idempotency_key="idem-abac",
        actor_id="u1",
        actor_claims=["admin"],
        expected_version=0,
        payload=ResourceAllocationRequested(
            node_id=aggregate_id,
            target_cpu_cores=2.0,
            target_memory_gb=4.0,
            reason_code="scale-up",
        ),
    )


@pytest.mark.asyncio
async def test_user_mutation_on_compensating_aggregate_is_locked():
    tenant_id = uuid4()
    aggregate_id = uuid4()
    repo = _FakeCommandRepository("compensating_via_full_revert")
    subject = SubjectContext("u1", str(tenant_id), SubjectRole.USER)

    with pytest.raises(Exception) as exc_info:
        await validate_and_append(
            repo,
            _allocation_envelope(tenant_id, aggregate_id),
            tenant_id,
            subject,
        )

    assert getattr(exc_info.value, "status_code", None) == 423
    assert repo.appended is False


@pytest.mark.asyncio
async def test_system_mutation_on_compensating_aggregate_is_allowed():
    tenant_id = uuid4()
    aggregate_id = uuid4()
    repo = _FakeCommandRepository("compensating_via_full_revert")
    subject = SubjectContext("worker-1", "system-tenant", SubjectRole.SYSTEM)

    await validate_and_append(
        repo,
        _allocation_envelope(tenant_id, aggregate_id),
        tenant_id,
        subject,
    )

    assert repo.appended is True
