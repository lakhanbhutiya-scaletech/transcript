"""Process entrypoints for the worker and the API.

`python -m transcriber worker` runs the queue consumer, `python -m
transcriber api` serves the control plane, and `python -m transcriber all`
runs both in one process for local development.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys

from .config import Settings, get_settings
from .container import Container
from .logging_config import configure_logging

logger = logging.getLogger(__name__)


class WorkerRunner:
    """Runs the BullMQ consumer until a shutdown signal arrives."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._container = Container(self._settings)
        self._shutdown = asyncio.Event()

    def request_shutdown(self, reason: str = "signal") -> None:
        if not self._shutdown.is_set():
            logger.info("Shutdown requested", extra={"reason": reason})
            self._shutdown.set()

    async def run(self) -> None:
        settings = self._settings
        if settings.environment != "production":
            # Convenience for local/dev stacks; production uses Alembic.
            await self._container.database.create_all()

        self._container.worker.start()
        logger.info(
            "Transcription service ready",
            extra={
                "queue": settings.queue.name,
                "redis": settings.redis.url,
                "max_chunk_seconds": settings.deepgram.max_chunk_seconds,
            },
        )
        try:
            await self._shutdown.wait()
        finally:
            logger.info("Draining in-flight jobs")
            await self._container.aclose()


async def _serve_api(settings: Settings) -> None:
    import uvicorn

    from .api.app import create_app

    config = uvicorn.Config(
        create_app(settings),
        host=settings.api.host,
        port=settings.api.port,
        log_config=None,
        access_log=False,
    )
    await uvicorn.Server(config).serve()


def _install_signal_handlers(runner: WorkerRunner) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, runner.request_shutdown, sig.name)
        except NotImplementedError:  # pragma: no cover - non-POSIX platforms
            signal.signal(sig, lambda *_: runner.request_shutdown("signal"))


async def _run_worker(settings: Settings) -> None:
    runner = WorkerRunner(settings)
    _install_signal_handlers(runner)
    await runner.run()


async def _run_all(settings: Settings) -> None:
    runner = WorkerRunner(settings)
    _install_signal_handlers(runner)
    await asyncio.gather(runner.run(), _serve_api(settings))


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    mode = (argv[0] if argv else "worker").lower()

    settings = get_settings()
    configure_logging(settings.log_level, settings.json_logs)

    runners = {
        "worker": _run_worker,
        "api": _serve_api,
        "all": _run_all,
    }
    if mode not in runners:
        print(f"Unknown mode {mode!r}. Expected one of: {', '.join(runners)}")
        return 2

    try:
        asyncio.run(runners[mode](settings))
    except KeyboardInterrupt:  # pragma: no cover - interactive use
        logger.info("Interrupted")
    return 0
