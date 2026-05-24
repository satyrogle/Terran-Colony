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
