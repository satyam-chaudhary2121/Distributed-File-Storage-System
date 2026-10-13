"""Coordinator entry point (FastAPI + Uvicorn).

    python -m app.main --port 8000
    uvicorn app.main:create_app --factory --port 8000
"""

from __future__ import annotations

import argparse
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app import __version__
from app.api import cluster, files, nodes, recovery
from app.core.config import Settings, load_settings
from app.core.exceptions import DFSError
from app.core.logging_config import setup_logging
from app.runtime import Coordinator

log = logging.getLogger("dfs.api")
_MULTIPART_OVERHEAD = 64 * 1024


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        coordinator = Coordinator(settings)
        coordinator.start()
        app.state.coordinator = coordinator
        try:
            yield
        finally:
            coordinator.stop()

    app = FastAPI(
        title="Distributed File Storage",
        version=__version__,
        description="Coordinator API: chunked, replicated file storage over TCP storage nodes.",
        lifespan=lifespan,
    )

    @app.exception_handler(DFSError)
    async def dfs_error_handler(request: Request, exc: DFSError) -> JSONResponse:
        if exc.status_code >= 500:
            log.warning("%s %s -> %s %s", request.method, request.url.path, exc.code, exc.message)
        return JSONResponse(status_code=exc.status_code, content={"error": exc.to_dict()})

    @app.middleware("http")
    async def limit_body_size(request: Request, call_next):  # type: ignore[no-untyped-def]
        length = request.headers.get("content-length")
        if settings.max_upload_size and length and length.isdigit():
            if int(length) > settings.max_upload_size + _MULTIPART_OVERHEAD:
                return JSONResponse(status_code=413, content={"error": {
                    "code": "PAYLOAD_TOO_LARGE",
                    "message": f"request exceeds max_upload_size of {settings.max_upload_size} bytes",
                    "details": {}}})
        return await call_next(request)

    @app.get("/health", tags=["meta"])
    def health() -> dict:
        return {"status": "ok", "version": __version__}

    app.include_router(files.router)
    app.include_router(nodes.router)
    app.include_router(cluster.router)
    app.include_router(recovery.router)
    return app


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Distributed file storage coordinator")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)
    settings = load_settings(args.config, coordinator_host=args.host, coordinator_port=args.port)
    setup_logging(settings.log_level, component="coordinator")
    uvicorn.run(create_app(settings), host=settings.coordinator_host, port=settings.coordinator_port,
                log_level=settings.log_level.lower(), log_config=None, access_log=False, timeout_graceful_shutdown=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
