"""SQLAlchemy ORM mapping for jobs and their transcripts."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from ...domain.enums import JobStatus


class Base(DeclarativeBase):
    """Declarative base; `Base.metadata` is what Alembic autogenerates from."""


class TranscriptionJob(Base):
    __tablename__ = "transcription_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    #: Identifier supplied by the producing service. The natural key we
    #: deduplicate on, so a redelivered BullMQ job updates one row.
    external_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    queue_job_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    audio_path: Mapped[str] = mapped_column(Text)
    status: Mapped[JobStatus] = mapped_column(
        String(16), default=JobStatus.PENDING, index=True
    )

    language: Mapped[str | None] = mapped_column(String(16), nullable=True)
    model: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider: Mapped[str | None] = mapped_column(String(32), nullable=True)

    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    chunk_count: Mapped[int] = mapped_column(Integer, default=0)
    word_count: Mapped[int] = mapped_column(Integer, default=0)

    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_metadata: Mapped[dict] = mapped_column("metadata", JSONB, default=dict)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    segments: Mapped[list[TranscriptSegment]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
        order_by="TranscriptSegment.index",
        lazy="selectin",
    )

    __table_args__ = (Index("ix_transcription_jobs_status_created", "status", "created_at"),)

    def to_dict(self, *, include_segments: bool = False) -> dict:
        payload = {
            "id": str(self.id),
            "externalId": self.external_id,
            "queueJobId": self.queue_job_id,
            "audioPath": self.audio_path,
            "status": self.status,
            "language": self.language,
            "model": self.model,
            "provider": self.provider,
            "text": self.text,
            "confidence": self.confidence,
            "durationSeconds": self.duration_seconds,
            "chunkCount": self.chunk_count,
            "wordCount": self.word_count,
            "attempts": self.attempts,
            "error": self.error,
            "metadata": self.job_metadata,
            "createdAt": _iso(self.created_at),
            "updatedAt": _iso(self.updated_at),
            "startedAt": _iso(self.started_at),
            "completedAt": _iso(self.completed_at),
        }
        if include_segments:
            payload["segments"] = [segment.to_dict() for segment in self.segments]
        return payload


class TranscriptSegment(Base):
    __tablename__ = "transcript_segments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("transcription_jobs.id", ondelete="CASCADE"),
        index=True,
    )
    index: Mapped[int] = mapped_column(Integer)
    text: Mapped[str] = mapped_column(Text)
    start_seconds: Mapped[float] = mapped_column(Float)
    end_seconds: Mapped[float] = mapped_column(Float)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    speaker: Mapped[int | None] = mapped_column(Integer, nullable=True)

    job: Mapped[TranscriptionJob] = relationship(back_populates="segments")

    __table_args__ = (Index("ix_transcript_segments_job_index", "job_id", "index"),)

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "text": self.text,
            "start": self.start_seconds,
            "end": self.end_seconds,
            "confidence": self.confidence,
            "speaker": self.speaker,
        }


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None
