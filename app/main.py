from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI

from app.api.middleware import BackpressureMiddleware
from app.api.routers.commands import router as commands_router
from app.api.routers.projections import router as projections_router
from app.api.routers.telemetry import router as telemetry_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    database_url = os.getenv("DATABASE_URL")
    skip_pool = os.getenv("SKIP_DB_POOL_INIT", "0") == "1"
    app.state.db_pool = None

    if database_url and not skip_pool:
        app.state.db_pool = await asyncpg.create_pool(
            dsn=database_url,
            min_size=1,
            max_size=10,
            command_timeout=30,
        )
        logger.info("Database pool initialized.")
    else:
        logger.warning("Database pool not initialized (DATABASE_URL missing or SKIP_DB_POOL_INIT=1).")

    try:
        yield
    finally:
        pool = getattr(app.state, "db_pool", None)
        if pool is not None:
            await pool.close()
            logger.info("Database pool closed.")


app = FastAPI(title="CloudCommander API", lifespan=lifespan)
app.add_middleware(BackpressureMiddleware)
app.include_router(commands_router)
app.include_router(projections_router)

app.include_router(telemetry_router)
