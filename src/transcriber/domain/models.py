"""Immutable value objects that flow through the transcription pipeline.

These types are deliberately free of SQLAlchemy, Deepgram and BullMQ
imports: the pipeline is expressed in terms of them, and the adapters in
`infrastructure/` translate to and from the outside world.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True, slots=True)
class AudioMetadata:
    """What ffprobe tells us about a media file."""

    path: Path
    duration_seconds: float
    sample_rate: int | None = None
    channels: int | None = None
    codec: str | None = None
    size_bytes: int = 0


@dataclass(frozen=True, slots=True)
class AudioChunk:
    """One slice of the source audio, ready to be sent to the provider."""

    index: int
    path: Path
    offset_seconds: float
    duration_seconds: float

    @property
    def end_seconds(self) -> float:
        return self.offset_seconds + self.duration_seconds


@dataclass(frozen=True, slots=True)
class Word:
    text: str
    start: float
    end: float
    confidence: float | None = None
    speaker: int | None = None

    def shifted(self, offset: float) -> Word:
        return Word(
            text=self.text,
            start=self.start + offset,
            end=self.end + offset,
            confidence=self.confidence,
            speaker=self.speaker,
        )


@dataclass(frozen=True, slots=True)
class Segment:
    """A contiguous utterance/paragraph of speech."""

    index: int
    text: str
    start: float
    end: float
    confidence: float | None = None
    speaker: int | None = None

    def shifted(self, offset: float, index: int) -> Segment:
        return Segment(
            index=index,
            text=self.text,
            start=self.start + offset,
            end=self.end + offset,
            confidence=self.confidence,
            speaker=self.speaker,
        )


@dataclass(frozen=True, slots=True)
class Transcript:
    """The provider's answer for a single chunk, or the merged whole."""

    text: str
    segments: tuple[Segment, ...] = ()
    words: tuple[Word, ...] = ()
    confidence: float | None = None
    language: str | None = None
    duration_seconds: float = 0.0
    provider: str = ""
    model: str = ""
    raw: dict = field(default_factory=dict, repr=False)


@dataclass(frozen=True, slots=True)
class TranscriptionRequest:
    """A validated, normalised unit of work handed to the pipeline."""

    external_id: str
    audio_path: Path
    language: str | None = None
    model: str | None = None
    diarize: bool = False
    queue_job_id: str | None = None
    metadata: dict = field(default_factory=dict)
