"""BullMQ worker: the entry point for jobs produced by the other services."""
from __future__ import annotations

import logging
from typing import Any

from bullmq import Job, UnrecoverableError, Worker

from ...application.payload import JobPayloadParser
from ...application.pipeline import TranscriptionPipeline
from ...config import QueueSettings, RedisSettings
from ...domain.exceptions import TranscriptionError
from ...domain.ports import JobEventPublisher, JobRepository
from .progress import BullMQProgressReporter

logger = logging.getLogger(__name__)

class TranscriptionWorker:
    """Owns the BullMQ consumer loop and maps failures onto retry semantics.

    Responsibilities are deliberately narrow: parse the payload, run the
    pipeline, record the outcome, and decide whether BullMQ should retry.
    All transcription logic lives in `TranscriptionPipeline`.
    """

    def __init__(
        self,
        *,
        pipeline: TranscriptionPipeline,
        parser: JobPayloadParser,
        repository: JobRepository,
        publisher: JobEventPublisher,
        queue_settings: QueueSettings,
        redis_settings: RedisSettings,
    ) -> None:
        self._pipeline = pipeline
        self._parser = parser
        self._repository = repository
        self._publisher = publisher
        self._queue_settings = queue_settings
        self._redis_settings = redis_settings
        self._worker: Worker | None = None

    def start(self) -> Worker:
        """Begin consuming. BullMQ polls in the background from here on."""
        self._worker = Worker(
            self._queue_settings.name,
            self._process,
            {
                "connection": self._redis_settings.url,
                "prefix": self._redis_settings.prefix,
                "concurrency": self._queue_settings.concurrency,
                "lockDuration": self._queue_settings.lock_duration_seconds * 1000,
            },
        )
        logger.info(
            "Worker started",
            extra={
                "queue": self._queue_settings.name,
                "concurrency": self._queue_settings.concurrency,
            },
        )
        return self._worker

    async def aclose(self) -> None:
        if self._worker is not None:
            # force=False lets in-flight jobs finish before we exit.
            await self._worker.close()
            self._worker = None

    # -- job processing ---------------------------------------------------

    async def _process(self, job: Job, job_token: str | None = None) -> dict[str, Any]:
        attempt = self._attempt_number(job)
        log_context = {"queue_job_id": job.id, "attempt": attempt}
        logger.info("Job received", extra=log_context)

        try:
            request = self._parser.parse(job.data, queue_job_id=str(job.id))
        except TranscriptionError as error:
            # No job row can exist yet, so this is logged and reported rather
            # than recorded. A malformed payload will never parse: no retry.
            logger.error(
                "Rejecting unparseable job payload",
                extra={**log_context, "error": error.message, **error.details},
            )
            raise UnrecoverableError(error.message) from error

        log_context["external_id"] = request.external_id

        try:
            outcome = await self._pipeline.run(
                request,
                attempt=attempt,
                progress=BullMQProgressReporter(job),
            )
        except Exception as error:
            retry = await self._handle_failure(
                job, request.external_id, error, attempt
            )
            if not retry:
                # Stop BullMQ burning the remaining attempts on an error that
                # cannot succeed (missing file, bad media, auth failure).
                raise UnrecoverableError(str(error)) from error
            raise

        await self._publisher.publish_completed(request.external_id, outcome.to_dict())
        logger.info("Job finished", extra={**log_context, **outcome.to_dict()})
        return outcome.to_dict()

    async def _handle_failure(
        self, job: Job, external_id: str, error: Exception, attempt: int
    ) -> bool:
        """Record the failure; return whether the job is worth another go."""
        retryable = not isinstance(error, TranscriptionError) or error.retryable
        will_retry = retryable and attempt < self._max_attempts(job)
        message = f"{type(error).__name__}: {error}"

        await self._repository.mark_failed(
            external_id, error=message, will_retry=will_retry
        )
        logger.error(
            "Job failed",
            extra={
                "external_id": external_id,
                "queue_job_id": job.id,
                "attempt": attempt,
                "will_retry": will_retry,
                "error": message,
            },
            exc_info=error,
        )
        if not will_retry:
            await self._publisher.publish_failed(external_id, {"error": message})
        return retryable

    @staticmethod
    def _attempt_number(job: Job) -> int:
        """1-based attempt counter, tolerant of bullmq version differences."""
        made = getattr(job, "attemptsMade", None)
        if made is None:
            made = getattr(job, "attempts_made", 0)
        return int(made or 0) + 1

    @staticmethod
    def _max_attempts(job: Job) -> int:
        opts = getattr(job, "opts", None) or {}
        try:
            return max(1, int(opts.get("attempts", 1)))
        except (AttributeError, TypeError, ValueError):
            return 1
