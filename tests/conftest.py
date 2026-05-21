from __future__ import annotations

import os

import asyncpg
import pytest


@pytest.fixture
async def db_pool():
    """Provide an asyncpg pool for worker integration tests."""
    dsn = os.getenv("TEST_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=4)

    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS outbox (
                event_id UUID PRIMARY KEY,
                tenant_id UUID NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await conn.execute("TRUNCATE TABLE outbox")

    try:
        yield pool
    finally:
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE TABLE outbox")
        await pool.close()
