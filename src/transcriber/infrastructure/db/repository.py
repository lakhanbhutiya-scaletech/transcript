"""SQLAlchemy implementation of the `JobRepository` port."""
from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert

from ...domain.enums import JobStatus
from ...domain.models import Transcript, TranscriptionRequest
from ...domain.ports import JobRepository
from .engine import Database
from .models import TranscriptionJob, TranscriptSegment

logger = logging.getLogger(__name__)


class SqlAlchemyJobRepository(JobRepository):
    """One short transaction per call; nothing is held open across I/O."""

    def __init__(self, database: Database) -> None:
        self._db = database

    async def register(self, request: TranscriptionRequest) -> JobStatus:
        """Upsert the job row, returning the status it had *before* this run."""
        async with self._db.session() as session:
            existing = await session.scalar(
                select(TranscriptionJob).where(
                    TranscriptionJob.external_id == request.external_id
                )
            )
            if existing is not None:
                previous = JobStatus(existing.status)
                if previous is not JobStatus.COMPLETED:
                    existing.audio_path = str(request.audio_path)
                    existing.queue_job_id = request.queue_job_id
                    existing.language = request.language
                    existing.model = request.model
                    if request.metadata:
                        existing.job_metadata = {
                            **(existing.job_metadata or {}),
                            **request.metadata,
                        }
                return previous

            # Insert guarded against a concurrent worker claiming the same
            # external_id: on conflict we simply keep the existing row.
            statement = (
                insert(TranscriptionJob)
                .values(
                    external_id=request.external_id,
                    queue_job_id=request.queue_job_id,
                    audio_path=str(request.audio_path),
                    status=JobStatus.PENDING,
                    language=request.language,
                    model=request.model,
                    job_metadata=request.metadata,
                )
                .on_conflict_do_nothing(index_elements=[TranscriptionJob.external_id])
            )
            await session.execute(statement)
            return JobStatus.PENDING

    async def mark_processing(self, external_id: str, *, attempt: int) -> None:
        async with self._db.session() as session:
            job = await self._require(session, external_id)
            job.status = JobStatus.PROCESSING
            job.attempts = attempt
            job.error = None
            job.started_at = job.started_at or datetime.now(UTC)

    async def save_result(
        self, external_id: str, transcript: Transcript, *, chunk_count: int
    ) -> None:
        async with self._db.session() as session:
            job = await self._require(session, external_id)

            # A retry may be rewriting an earlier partial result.
            await session.execute(
                delete(TranscriptSegment).where(TranscriptSegment.job_id == job.id)
            )

            job.status = JobStatus.COMPLETED
            job.text = transcript.text
            job.confidence = transcript.confidence
            job.duration_seconds = transcript.duration_seconds
            job.language = transcript.language or job.language
            job.model = transcript.model or job.model
            job.provider = transcript.provider
            job.chunk_count = chunk_count
            job.word_count = len(transcript.words)
            job.error = None
            job.completed_at = datetime.now(UTC)

            session.add_all(
                TranscriptSegment(
                    job_id=job.id,
                    index=segment.index,
                    text=segment.text,
                    start_seconds=segment.start,
                    end_seconds=segment.end,
                    confidence=segment.confidence,
                    speaker=segment.speaker,
                )
                for segment in transcript.segments
            )

    async def mark_failed(
        self, external_id: str, *, error: str, will_retry: bool
    ) -> None:
        async with self._db.session() as session:
            job = await self._require(session, external_id)
            # While BullMQ still has attempts left the row stays PROCESSING,
            # so downstream readers don't see a flap of failed -> processing.
            job.status = JobStatus.PROCESSING if will_retry else JobStatus.FAILED
            job.error = error[:4000]
            if not will_retry:
                job.completed_at = datetime.now(UTC)

    async def get(self, external_id: str) -> dict | None:
        async with self._db.session() as session:
            job = await session.scalar(
                select(TranscriptionJob).where(
                    TranscriptionJob.external_id == external_id
                )
            )
            return job.to_dict(include_segments=True) if job else None

    async def list_recent(
        self, *, limit: int = 50, status: JobStatus | None = None
    ) -> list[dict]:
        async with self._db.session() as session:
            query = select(TranscriptionJob).order_by(
                TranscriptionJob.created_at.desc()
            ).limit(limit)
            if status is not None:
                query = query.where(TranscriptionJob.status == status)
            jobs = (await session.scalars(query)).all()
            return [job.to_dict() for job in jobs]

    @staticmethod
    async def _require(session, external_id: str) -> TranscriptionJob:
        job = await session.scalar(
            select(TranscriptionJob).where(TranscriptionJob.external_id == external_id)
        )
        if job is None:
            raise LookupError(f"Transcription job {external_id!r} does not exist")
        return job
