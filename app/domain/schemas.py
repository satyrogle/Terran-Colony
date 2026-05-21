from __future__ import annotations

import math
from typing import Any, Dict, Literal, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DomainBaseModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ResourceAllocationRequested(DomainBaseModel):
    event_type: Literal["ResourceAllocationRequested"] = "ResourceAllocationRequested"
    node_id: UUID
    target_cpu_cores: float = Field(ge=0.1, le=128.0)
    target_memory_gb: float = Field(ge=0.5, le=1024.0)
    reason_code: str = Field(min_length=1, max_length=50)

    @field_validator("target_cpu_cores", "target_memory_gb")
    @classmethod
    def check_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("Value must be a finite number")
        return v


class DependencyEdgeProposed(DomainBaseModel):
    event_type: Literal["DependencyEdgeProposed"] = "DependencyEdgeProposed"
    source_node_id: UUID
    target_node_id: UUID


class AggregateFrozen(DomainBaseModel):
    event_type: Literal["AggregateFrozen"] = "AggregateFrozen"
    severity: Literal["Medium", "High"]
    drift_details: Dict[str, Any]




class RollbackInitiated(DomainBaseModel):
    event_type: Literal["RollbackInitiated"] = "RollbackInitiated"
    target_node_id: UUID
    target_sequence_id: int = Field(ge=0)
    reason_code: str = Field(min_length=1, max_length=100)



class GuardrailObservationRecorded(DomainBaseModel):
    event_type: Literal["GuardrailObservationRecorded"] = "GuardrailObservationRecorded"
    aggregate_id: UUID
    utilization: float
    pid_output: float
    timestamp_utc_ms: int


class GuardrailThresholdBreached(DomainBaseModel):
    event_type: Literal["GuardrailThresholdBreached"] = "GuardrailThresholdBreached"
    aggregate_id: UUID
    severity: Literal["warning", "approval_required", "frozen"]
    metric_value: float
    reason: str


class CompensationStrategySelected(DomainBaseModel):
    event_type: Literal["CompensationStrategySelected"] = "CompensationStrategySelected"
    intent_id: str
    aggregate_id: UUID
    selected_strategy: str
    utility_scores: Dict[str, float]

class ExternalDriftResolved(DomainBaseModel):
    event_type: Literal["ExternalDriftResolved"] = "ExternalDriftResolved"
    resolution_mode: Literal["ReapplyInternalState", "AcceptExternalReality"]
    resolved_by: str = Field(min_length=1)


EventPayload = Union[
    ResourceAllocationRequested,
    DependencyEdgeProposed,
    AggregateFrozen,
    ExternalDriftResolved,
    RollbackInitiated,
    GuardrailObservationRecorded,
    GuardrailThresholdBreached,
    CompensationStrategySelected,
]


class EventEnvelope(DomainBaseModel):
    event_id: UUID
    tenant_id: UUID
    aggregate_id: UUID
    sequence_id: int = Field(ge=1)
    timestamp_utc_ms: int = Field(ge=0)
    idempotency_key: str = Field(min_length=1, max_length=100)
    actor_id: str = Field(min_length=1)
    actor_claims: list[str] = Field(default_factory=list)
    expected_version: int = Field(ge=0)
    payload: EventPayload


class ResourceNodeSnapshot(DomainBaseModel):
    node_id: UUID
    lifecycle_state: Literal["active", "orphaned", "tombstoned", "frozen"]
    cpu_cores: float = Field(ge=0.0, le=128.0)
    memory_gb: float = Field(ge=0.0, le=1024.0)
    last_sequence_id: int = Field(ge=0)
    schema_version: int = Field(default=1, ge=1)

    @field_validator("cpu_cores", "memory_gb")
    @classmethod
    def check_snapshot_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("Snapshot value must be finite")
        return v


class ResourceAllocationCommandPayload(DomainBaseModel):
    node_id: UUID
    target_cpu_cores: float = Field(ge=0.1, le=128.0)
    target_memory_gb: float = Field(ge=0.5, le=1024.0)
    reason_code: str = Field(min_length=1, max_length=50)

    @field_validator("target_cpu_cores", "target_memory_gb")
    @classmethod
    def check_command_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("Value must be a finite number")
        return v


class CommandEnvelope(DomainBaseModel):
    tenant_id: UUID
    actor_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1, max_length=100)
    expected_version: int = Field(ge=0)
    payload: ResourceAllocationCommandPayload


class DependencyEdgeCommandPayload(DomainBaseModel):
    source_node_id: UUID
    target_node_id: UUID
    edge_type: Literal["sync", "async"] = "sync"


class RollbackCommandPayload(DomainBaseModel):
    target_aggregate_id: UUID
    target_sequence_id: int = Field(ge=0)
    reason_code: str = Field(min_length=1, max_length=100)
