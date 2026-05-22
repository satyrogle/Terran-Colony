from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import app


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
