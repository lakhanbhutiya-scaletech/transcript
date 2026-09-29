"""Abstract interfaces (ports) implemented by the infrastructure layer.

The application layer depends only on these, so swapping Deepgram for
another vendor, or ffmpeg for a different splitter, means writing one new
class and changing one line in the composition root.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from pathlib import Path

from .enums import JobStatus
from .models import AudioChunk, AudioMetadata, Transcript, TranscriptionRequest


class AudioProbe(ABC):
    """Reads technical metadata from a media file."""

    @abstractmethod
    async def probe(self, path: Path) -> AudioMetadata:
        """Return duration/codec info, or raise `UnsupportedAudioError`."""


class AudioSplitter(ABC):
    """Cuts a long file into provider-sized pieces."""

    @abstractmethod
    def split(
        self, source: AudioMetadata, chunk_seconds: float
    ) -> AbstractAsyncContextManager[Sequence[AudioChunk]]:
        """Yield chunks for the duration of the context, then clean up.

        Implementations must delete any temporary files they created when
        the context exits, successfully or not.
        """


class TranscriptionProvider(ABC):
    """A speech-to-text vendor."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable identifier persisted alongside the transcript."""

    @property
    @abstractmethod
    def max_chunk_seconds(self) -> float:
        """Longest single request the vendor accepts, in seconds."""

    @abstractmethod
    async def transcribe(
        self,
        path: Path,
        *,
        language: str | None = None,
        model: str | None = None,
        diarize: bool = False,
    ) -> Transcript:
        """Transcribe one file that is already within the vendor's limits."""

    async def aclose(self) -> None:  # pragma: no cover - default no-op
        """Release any held connections."""


class JobRepository(ABC):
    """Persistence for job rows and their transcripts."""

    @abstractmethod
    async def register(self, request: TranscriptionRequest) -> JobStatus:
        """Create or refresh the job row; return its status before this run.

        Used for idempotency: a job already `COMPLETED` is not redone.
        """

    @abstractmethod
    async def mark_processing(self, external_id: str, *, attempt: int) -> None: ...

    @abstractmethod
    async def save_result(
        self,
        external_id: str,
        transcript: Transcript,
        *,
        chunk_count: int,
    ) -> None:
        """Persist the merged transcript and flip the row to COMPLETED."""

    @abstractmethod
    async def mark_failed(
        self, external_id: str, *, error: str, will_retry: bool
    ) -> None: ...

    @abstractmethod
    async def get(self, external_id: str) -> dict | None:
        """Return a serialisable view of the job, or None."""

    @abstractmethod
    async def list_recent(
        self, *, limit: int = 50, status: JobStatus | None = None
    ) -> list[dict]:
        """Most recently created jobs first, optionally filtered by status."""


class JobEventPublisher(ABC):
    """Notifies other services that a job reached a terminal state."""

    @abstractmethod
    async def publish_completed(self, external_id: str, payload: dict) -> None: ...

    @abstractmethod
    async def publish_failed(self, external_id: str, payload: dict) -> None: ...

    async def aclose(self) -> None:  # pragma: no cover - default no-op
        ...


class ProgressReporter(ABC):
    """Feeds progress back to whoever is watching the queue job."""

    @abstractmethod
    async def report(self, percent: float, message: str) -> None: ...


class NullProgressReporter(ProgressReporter):
    """Used when the pipeline runs outside a queue context (tests, CLI)."""

    async def report(self, percent: float, message: str) -> None:
        return None


__all__ = [
    "AudioProbe",
    "AudioSplitter",
    "JobEventPublisher",
    "JobRepository",
    "NullProgressReporter",
    "ProgressReporter",
    "TranscriptionProvider",
]
