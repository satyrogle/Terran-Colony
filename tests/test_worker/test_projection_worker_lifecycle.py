from __future__ import annotations

import pytest

from app.worker.projection_worker import create_pool_from_env


@pytest.mark.asyncio
async def test_create_pool_requires_database_url(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        await create_pool_from_env()


@pytest.mark.asyncio
async def test_create_pool_uses_asyncpg(monkeypatch):
    sentinel_pool = object()

    async def _create_pool(*args, **kwargs):
        _ = args
        assert kwargs["dsn"] == "postgresql://postgres:postgres@localhost:5432/postgres"
        return sentinel_pool

    monkeypatch.setenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")
    monkeypatch.setattr("app.worker.projection_worker.asyncpg.create_pool", _create_pool)
    pool = await create_pool_from_env()
    assert pool is sentinel_pool
