from __future__ import annotations

from typing import List
from uuid import UUID

import asyncpg
from fastapi import Depends, Header, HTTPException, Request, status

from app.infrastructure.repository import EventRepository
from app.security.abac import SubjectContext, SubjectRole


async def get_db_pool(request: Request) -> asyncpg.Pool:
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database pool is not initialized.",
        )
    return pool


async def get_repository(pool: asyncpg.Pool = Depends(get_db_pool)) -> EventRepository:
    return EventRepository(pool)


def get_tenant_id(x_tenant_id: UUID = Header(...)) -> UUID:
    return x_tenant_id


def get_actor_id(x_actor_id: str = Header(..., min_length=1)) -> str:
    return x_actor_id


def get_subject_context(
    x_tenant_id: UUID = Header(...),
    x_actor_id: str = Header(..., min_length=1),
    x_role: str = Header(default="user"),
) -> SubjectContext:
    try:
        role = SubjectRole(x_role.lower())
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid role claim.",
        )

    return SubjectContext(
        subject_id=x_actor_id,
        tenant_id=str(x_tenant_id),
        role=role,
    )


def get_current_user_claims(x_actor_claims: str = Header(default="")) -> List[str]:
    if not x_actor_claims.strip():
        return []
    return [c.strip() for c in x_actor_claims.split(",") if c.strip()]
