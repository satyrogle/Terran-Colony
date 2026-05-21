from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_lifespan_skips_pool_when_flag_enabled(monkeypatch):
    monkeypatch.setenv("SKIP_DB_POOL_INIT", "1")
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with TestClient(app) as client:
        response = client.get("/docs")
        assert response.status_code == 200
        assert app.state.db_pool is None


def test_lifespan_closes_pool_on_shutdown(monkeypatch):
    class FakePool:
        def __init__(self):
            self.closed = False

        async def close(self):
            self.closed = True

    fake_pool = FakePool()

    async def _create_pool(*args, **kwargs):
        _ = args, kwargs
        return fake_pool

    monkeypatch.setenv("SKIP_DB_POOL_INIT", "0")
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    monkeypatch.setattr("app.main.asyncpg.create_pool", _create_pool)

    with TestClient(app):
        assert app.state.db_pool is fake_pool

    assert fake_pool.closed is True
