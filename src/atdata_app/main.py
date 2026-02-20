"""FastAPI application factory and lifespan management."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from atdata_app.config import AppConfig
from atdata_app.database import create_pool, run_migrations
from atdata_app.frontend import router as frontend_router
from atdata_app.frontend.routes import _FRONTEND_DIR
from atdata_app.identity import did_json_handler
from atdata_app.ingestion.backfill import backfill_runner
from atdata_app.ingestion.jetstream import jetstream_consumer
from atdata_app.xrpc.router import router as xrpc_router

logger = logging.getLogger(__name__)

# Paths served on both hostnames (everything else is frontend-only)
_SHARED_PATH_PREFIXES = ("/xrpc/", "/.well-known/", "/health")


@asynccontextmanager
async def lifespan(app: FastAPI):
    config: AppConfig = app.state.config
    logger.info("Starting atdata-app (DID: %s)", config.service_did)

    # Database
    pool = await create_pool(config.database_url)
    app.state.db_pool = pool
    await run_migrations(pool)

    # Background tasks
    tasks: list[asyncio.Task] = []
    tasks.append(asyncio.create_task(jetstream_consumer(app), name="jetstream"))
    tasks.append(asyncio.create_task(backfill_runner(app), name="backfill"))

    yield

    # Shutdown
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await pool.close()
    logger.info("Shutdown complete")


def create_app(config: AppConfig | None = None) -> FastAPI:
    if config is None:
        config = AppConfig()

    app = FastAPI(title="atdata AppView", version="0.1.0", lifespan=lifespan)
    app.state.config = config

    # Middleware: block frontend routes on the API hostname
    @app.middleware("http")
    async def gate_frontend_routes(request: Request, call_next) -> Response:
        cfg: AppConfig = request.app.state.config
        if cfg.frontend_hostname:
            host = request.headers.get("host", "").split(":")[0]
            path = request.url.path
            if host != cfg.frontend_hostname and not path.startswith(
                _SHARED_PATH_PREFIXES
            ):
                return JSONResponse(
                    status_code=404, content={"detail": "Not Found"}
                )
        return await call_next(request)

    # Routes
    app.add_api_route("/.well-known/did.json", did_json_handler, methods=["GET"])
    app.include_router(xrpc_router)
    app.include_router(frontend_router)
    app.mount("/static", StaticFiles(directory=str(_FRONTEND_DIR / "static")), name="static")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
