"""FastAPI application factory for the control plane."""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ..config import Settings, get_settings
from ..container import Container
from ..domain.exceptions import TranscriptionError
from ..logging_config import configure_logging
from .routes import router

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.json_logs)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.container = Container(settings)
        logger.info("API starting", extra={"environment": settings.environment})
        try:
            yield
        finally:
            await app.state.container.aclose()

    app = FastAPI(
        title="Transcription Service",
        description=(
            "Consumes BullMQ transcription jobs from the shared Redis, "
            "transcribes via Deepgram (chunking long audio), and persists "
            "the transcript."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.exception_handler(TranscriptionError)
    async def _domain_error_handler(
        request: Request, error: TranscriptionError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=400 if not error.retryable else 503,
            content={"message": error.message, "details": error.details},
        )

    return app


app = create_app()
