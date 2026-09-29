"""Fakes shared between test modules."""
from __future__ import annotations

from transcriber.domain.enums import JobStatus
from transcriber.domain.models import Transcript, TranscriptionRequest
from transcriber.domain.ports import JobEventPublisher, JobRepository


class FakeRepository(JobRepository):
    def __init__(self, initial_status: JobStatus = JobStatus.PENDING) -> None:
        self.initial_status = initial_status
        self.saved: Transcript | None = None
        self.chunk_count = 0
        self.failures: list[tuple[str, bool]] = []
        self.status = initial_status

    async def register(self, request: TranscriptionRequest) -> JobStatus:
        return self.initial_status

    async def mark_processing(self, external_id: str, *, attempt: int) -> None:
        self.status = JobStatus.PROCESSING

    async def save_result(self, external_id, transcript, *, chunk_count) -> None:
        self.saved = transcript
        self.chunk_count = chunk_count
        self.status = JobStatus.COMPLETED

    async def mark_failed(self, external_id, *, error, will_retry) -> None:
        self.failures.append((error, will_retry))

    async def get(self, external_id):
        return None

    async def list_recent(self, *, limit=50, status=None):
        return []


class RecordingPublisher(JobEventPublisher):
    """Captures the events a worker publishes."""

    def __init__(self) -> None:
        self.completed: list[tuple[str, dict]] = []
        self.failed: list[tuple[str, dict]] = []

    async def publish_completed(self, external_id: str, payload: dict) -> None:
        self.completed.append((external_id, payload))

    async def publish_failed(self, external_id: str, payload: dict) -> None:
        self.failed.append((external_id, payload))
