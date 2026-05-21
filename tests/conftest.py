from __future__ import annotations

import os
from pathlib import Path

import asyncpg
import pytest


ROOT_DIR = Path(__file__).resolve().parents[1]
MIGRATIONS = [
    ROOT_DIR / "migrations" / "001_initial_schema.sql",
    ROOT_DIR / "migrations" / "002_outbox_hardening.sql",
    ROOT_DIR / "migrations" / "003_event_hash_chain.sql",
    ROOT_DIR / "migrations" / "004_read_models_and_outbox_consistency.sql",
    ROOT_DIR / "migrations" / "005_event_actor_claims.sql",
]


@pytest.fixture(scope="session")
async def db_pool():
    """Provide an asyncpg pool with migrated schema for integration tests."""
    dsn = os.getenv("TEST_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    try:
        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=4)
    except Exception as exc:
        pytest.skip(f"Postgres is not available for DB-backed tests: {exc}")

    async with pool.acquire() as conn:
        for migration_path in MIGRATIONS:
            await conn.execute(migration_path.read_text(encoding="utf-8"))

    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
async def reset_db(db_pool: asyncpg.Pool):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
                outbox,
                events,
                read_model_nodes,
                read_model_service_graph_edges,
                read_model_guardrail_alerts
            RESTART IDENTITY
            CASCADE
            """
        )
    yield
