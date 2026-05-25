from __future__ import annotations

import re
from pathlib import Path


SPEC_PATH = Path(__file__).resolve().parents[2] / "openapi" / "cloudcommander-v1.yaml"


def _read_spec() -> str:
    return SPEC_PATH.read_text(encoding="utf-8")


def _block(spec: str, start_anchor: str, end_anchor: str | None = None) -> str:
    start = spec.index(start_anchor)
    end = spec.index(end_anchor, start) if end_anchor is not None else len(spec)
    return spec[start:end]


def test_openapi_has_single_top_level_paths_and_components_blocks():
    spec = _read_spec()
    assert len(re.findall(r"(?m)^paths:$", spec)) == 1
    assert len(re.findall(r"(?m)^components:$", spec)) == 1


def test_command_paths_require_expected_ingress_headers():
    spec = _read_spec()
    expected_refs = [
        "$ref: '#/components/parameters/XTenantId'",
        "$ref: '#/components/parameters/XActorId'",
        "$ref: '#/components/parameters/XRole'",
        "$ref: '#/components/parameters/XActorClaims'",
        "$ref: '#/components/parameters/XIdempotencyKey'",
        "$ref: '#/components/parameters/XExpectedVersion'",
    ]

    blocks = [
        _block(
            spec,
            "  /api/v1/commands/resource-allocation:",
            "  /api/v1/commands/dependency-edge:",
        ),
        _block(
            spec,
            "  /api/v1/commands/dependency-edge:",
            "  /api/v1/commands/rollback:",
        ),
        _block(
            spec,
            "  /api/v1/commands/rollback:",
            "  /api/v1/projections/service-graph:",
        ),
    ]

    for block in blocks:
        for expected_ref in expected_refs:
            assert expected_ref in block


def test_command_schemas_forbid_extra_properties():
    spec = _read_spec()

    schemas_block = _block(spec, "  schemas:")
    for schema_name in (
        "ResourceAllocationCommand",
        "DependencyEdgeCommand",
        "RollbackCommand",
    ):
        match = re.search(
            rf"(?ms)^    {schema_name}:\n(.*?)(?=^    [A-Za-z][A-Za-z0-9_]*:|\Z)",
            schemas_block,
        )
        assert match is not None
        schema_block = match.group(1)
        assert "additionalProperties: false" in schema_block


def test_actor_claims_header_is_optional_string():
    spec = _read_spec()
    actor_claims_block = _block(spec, "    XActorClaims:", "    XIdempotencyKey:")
    assert "required: false" in actor_claims_block
    assert "type: string" in actor_claims_block


def test_role_header_is_optional_user_or_system_claim():
    spec = _read_spec()
    role_block = _block(spec, "    XRole:", "    XIdempotencyKey:")
    assert "required: false" in role_block
    assert "default: user" in role_block
    assert "- user" in role_block
    assert "- system" in role_block


def test_graph_centrality_route_and_schema_are_in_openapi_contract():
    spec = _read_spec()

    route_block = _block(
        spec,
        "  /api/v1/telemetry/graph/centrality:",
        "  /api/v1/telemetry/nodes/{node_id}/guardrail-state:",
    )
    assert "$ref: '#/components/parameters/XTenantId'" in route_block
    assert "$ref: '#/components/schemas/GraphCentralityNode'" in route_block

    schema_block = _block(spec, "    GraphCentralityNode:", "    GuardrailState:")
    assert "additionalProperties: false" in schema_block
    for field_name in ("node_id", "centrality_score", "rank"):
        assert f"        - {field_name}" in schema_block
        assert f"        {field_name}:" in schema_block


def test_hardening_telemetry_contract_exposes_ema_and_reconciler_state():
    spec = _read_spec()

    backpressure_block = _block(spec, "    BackpressureTelemetry:", "    ReconcilerTelemetry:")
    for field_name in (
        "raw_arrival_rate_hz",
        "raw_service_rate_hz",
        "raw_utilization_rho",
        "ema_arrival_rate_hz",
        "ema_service_rate_hz",
        "ema_utilization_rho",
    ):
        assert f"        - {field_name}" in backpressure_block
        assert f"        {field_name}:" in backpressure_block

    reconciler_route = _block(
        spec,
        "  /api/v1/telemetry/system/reconciler:",
        "  /api/v1/telemetry/graph/centrality:",
    )
    assert "$ref: '#/components/schemas/ReconcilerTelemetry'" in reconciler_route

    reconciler_schema = _block(spec, "    ReconcilerTelemetry:", "    GraphCentralityNode:")
    for field_name in ("state", "recent_failure_count", "opened_at", "next_retry_at"):
        assert f"        - {field_name}" in reconciler_schema
        assert f"        {field_name}:" in reconciler_schema
