from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.dependencies import get_db_pool, get_tenant_id

router = APIRouter(prefix="/api/v1/projections", tags=["Projections"])


@router.get("/service-graph")
async def get_service_graph_projection(
    tenant_id: UUID = Depends(get_tenant_id),
    pool: Any = Depends(get_db_pool),
):
    node_query = """
        SELECT node_id
        FROM read_model_nodes
        WHERE tenant_id = $1
        ORDER BY node_id
    """
    edge_query = """
        SELECT source_node_id, target_node_id
        FROM read_model_service_graph_edges
        WHERE tenant_id = $1
        ORDER BY source_node_id, target_node_id
    """
    version_query = """
        SELECT COALESCE(MAX(last_sequence_id), 0)
        FROM read_model_nodes
        WHERE tenant_id = $1
    """
    async with pool.acquire() as conn:
        node_records = await conn.fetch(node_query, tenant_id)
        edge_records = await conn.fetch(edge_query, tenant_id)
        version = await conn.fetchval(version_query, tenant_id)

    return {
        "tenant_id": str(tenant_id),
        "version": int(version or 0),
        "nodes": [str(r["node_id"]) for r in node_records],
        "edges": [
            {"source": str(r["source_node_id"]), "target": str(r["target_node_id"])}
            for r in edge_records
        ],
    }


@router.get("/nodes/{node_id}")
async def get_node_projection(
    node_id: UUID,
    tenant_id: UUID = Depends(get_tenant_id),
    pool: Any = Depends(get_db_pool),
):
    query = """
        SELECT node_id, lifecycle_state, cpu_cores, memory_gb, last_sequence_id, schema_version
        FROM read_model_nodes
        WHERE tenant_id = $1 AND node_id = $2
    """
    async with pool.acquire() as conn:
        record = await conn.fetchrow(query, tenant_id, node_id)

    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Node projection not found")

    return dict(record)
