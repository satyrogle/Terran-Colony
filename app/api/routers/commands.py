from __future__ import annotations

import time
from typing import List
from uuid import NAMESPACE_URL, UUID, uuid3, uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.api.dependencies import (
    get_actor_id,
    get_current_user_claims,
    get_repository,
    get_subject_context,
    get_tenant_id,
)
from app.domain.reducers import InvalidStateTransitionError, reduce_node
from app.domain.schemas import (
    EventEnvelope,
    DependencyEdgeCommandPayload,
    DependencyEdgeProposed,
    ResourceAllocationCommandPayload,
    ResourceAllocationRequested,
    RollbackCommandPayload,
    RollbackInitiated,
)
from app.infrastructure.repository import (
    ConcurrencyConflictError,
    EventRepository,
    IdempotencyKeyInUseError,
)
from app.security.abac import ResourceContext, SubjectContext, evaluate_policy
from app.security.rbac_policy import UnauthorizedStateTransitionError

router = APIRouter(prefix="/api/v1/commands", tags=["Commands"])


def _graph_aggregate_id(tenant_id: UUID) -> UUID:
    return uuid3(NAMESPACE_URL, f"service-graph:{tenant_id}")


def _assert_dependency_edge_is_acyclic(
    existing_events: list[EventEnvelope],
    proposed_edge: DependencyEdgeProposed,
) -> None:
    if proposed_edge.source_node_id == proposed_edge.target_node_id:
        raise HTTPException(
            status_code=422,
            detail="Dependency edge would create a cycle.",
        )

    adjacency: dict[UUID, set[UUID]] = {}
    for event in existing_events:
        if isinstance(event.payload, DependencyEdgeProposed):
            adjacency.setdefault(event.payload.source_node_id, set()).add(
                event.payload.target_node_id
            )

    stack = [proposed_edge.target_node_id]
    visited: set[UUID] = set()
    while stack:
        node_id = stack.pop()
        if node_id == proposed_edge.source_node_id:
            raise HTTPException(
                status_code=422,
                detail="Dependency edge would create a cycle.",
            )
        if node_id in visited:
            continue
        visited.add(node_id)
        stack.extend(adjacency.get(node_id, set()))


async def validate_and_append(
    repository: EventRepository,
    envelope: EventEnvelope,
    tenant_id: UUID,
    subject: SubjectContext,
) -> None:
    reconcile_status = await repository.get_active_reconcile_status(
        tenant_id, envelope.aggregate_id
    )
    decision = evaluate_policy(
        subject,
        "mutate",
        ResourceContext(
            aggregate_id=str(envelope.aggregate_id),
            tenant_id=str(tenant_id),
            reconcile_status=reconcile_status,
        ),
    )
    if not decision.allowed:
        raise HTTPException(status_code=decision.status_code, detail=decision.reason)

    events = await repository.get_events(tenant_id, envelope.aggregate_id)
    current_state = None
    for evt in events:
        current_state = reduce_node(current_state, evt)

    try:
        reduce_node(current_state, envelope)
    except UnauthorizedStateTransitionError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except InvalidStateTransitionError as e:
        if "frozen" in str(e).lower():
            raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(e))
        raise HTTPException(status_code=422, detail=str(e))

    if isinstance(envelope.payload, DependencyEdgeProposed):
        _assert_dependency_edge_is_acyclic(events, envelope.payload)

    try:
        await repository.append_event_and_enqueue(envelope)
    except (ConcurrencyConflictError, IdempotencyKeyInUseError) as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))


@router.post("/resource-allocation", status_code=status.HTTP_202_ACCEPTED)
async def request_resource_allocation(
    payload: ResourceAllocationCommandPayload,
    x_idempotency_key: str = Header(..., min_length=1, max_length=100),
    x_expected_version: int = Header(..., ge=0),
    tenant_id: UUID = Depends(get_tenant_id),
    subject: SubjectContext = Depends(get_subject_context),
    actor_id: str = Depends(get_actor_id),
    actor_claims: List[str] = Depends(get_current_user_claims),
    repository: EventRepository = Depends(get_repository),
):
    event_payload = ResourceAllocationRequested(
        node_id=payload.node_id,
        target_cpu_cores=payload.target_cpu_cores,
        target_memory_gb=payload.target_memory_gb,
        reason_code=payload.reason_code,
    )

    envelope = EventEnvelope(
        event_id=uuid4(),
        tenant_id=tenant_id,
        aggregate_id=payload.node_id,
        sequence_id=x_expected_version + 1,
        timestamp_utc_ms=int(time.time() * 1000),
        idempotency_key=x_idempotency_key,
        actor_id=actor_id,
        actor_claims=actor_claims,
        expected_version=x_expected_version,
        payload=event_payload,
    )

    await validate_and_append(repository, envelope, tenant_id, subject)
    return {"status": "accepted", "event_id": str(envelope.event_id)}


@router.post("/dependency-edge", status_code=status.HTTP_202_ACCEPTED)
async def propose_dependency_edge(
    payload: DependencyEdgeCommandPayload,
    x_idempotency_key: str = Header(..., min_length=1, max_length=100),
    x_expected_version: int = Header(..., ge=0),
    tenant_id: UUID = Depends(get_tenant_id),
    subject: SubjectContext = Depends(get_subject_context),
    actor_id: str = Depends(get_actor_id),
    actor_claims: List[str] = Depends(get_current_user_claims),
    repository: EventRepository = Depends(get_repository),
):
    event_payload = DependencyEdgeProposed(
        source_node_id=payload.source_node_id,
        target_node_id=payload.target_node_id,
    )

    envelope = EventEnvelope(
        event_id=uuid4(),
        tenant_id=tenant_id,
        aggregate_id=_graph_aggregate_id(tenant_id),
        sequence_id=x_expected_version + 1,
        timestamp_utc_ms=int(time.time() * 1000),
        idempotency_key=x_idempotency_key,
        actor_id=actor_id,
        actor_claims=actor_claims,
        expected_version=x_expected_version,
        payload=event_payload,
    )

    await validate_and_append(repository, envelope, tenant_id, subject)
    return {"status": "accepted", "event_id": str(envelope.event_id)}


@router.post("/rollback", status_code=status.HTTP_202_ACCEPTED)
async def initiate_rollback(
    payload: RollbackCommandPayload,
    x_idempotency_key: str = Header(..., min_length=1, max_length=100),
    x_expected_version: int = Header(..., ge=0),
    tenant_id: UUID = Depends(get_tenant_id),
    subject: SubjectContext = Depends(get_subject_context),
    actor_id: str = Depends(get_actor_id),
    actor_claims: List[str] = Depends(get_current_user_claims),
    repository: EventRepository = Depends(get_repository),
):
    event_payload = RollbackInitiated(
        target_node_id=payload.target_aggregate_id,
        target_sequence_id=payload.target_sequence_id,
        reason_code=payload.reason_code,
    )

    envelope = EventEnvelope(
        event_id=uuid4(),
        tenant_id=tenant_id,
        aggregate_id=payload.target_aggregate_id,
        sequence_id=x_expected_version + 1,
        timestamp_utc_ms=int(time.time() * 1000),
        idempotency_key=x_idempotency_key,
        actor_id=actor_id,
        actor_claims=actor_claims,
        expected_version=x_expected_version,
        payload=event_payload,
    )

    await validate_and_append(repository, envelope, tenant_id, subject)
    return {"status": "accepted", "event_id": str(envelope.event_id)}
