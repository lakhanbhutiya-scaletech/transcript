"""Worker-level retry semantics: what BullMQ is told after a failure."""
from __future__ import annotations

import pytest
from bullmq import UnrecoverableError

from transcriber.application.payload import JobPayloadParser
from transcriber.config import QueueSettings, RedisSettings
from transcriber.domain.enums import JobStatus
from transcriber.domain.exceptions import AudioNotFoundError, ProviderError
from transcriber.infrastructure.queue.worker import TranscriptionWorker

from .fakes import FakeRepository, RecordingPublisher


class FakeJob:
    """Stands in for a bullmq Job."""

    def __init__(self, data, *, job_id="q-1", attempts_made=0, max_attempts=3):
        self.id = job_id
        self.data = data
        self.attemptsMade = attempts_made
        self.opts = {"attempts": max_attempts}
        self.progress_updates = []

    async def updateProgress(self, value):
        self.progress_updates.append(value)


class FakePipeline:
    def __init__(self, error=None):
        self.error = error
        self.runs = 0

    async def run(self, request, *, attempt=1, progress=None):
        self.runs += 1
        if self.error is not None:
            raise self.error
        from transcriber.application.pipeline import TranscriptionOutcome

        return TranscriptionOutcome(
            external_id=request.external_id,
            status=JobStatus.COMPLETED,
            chunk_count=1,
            duration_seconds=10.0,
            word_count=2,
            character_count=8,
        )


def build_worker(audio_settings, *, pipeline, repository=None, publisher=None):
    return TranscriptionWorker(
        pipeline=pipeline,
        parser=JobPayloadParser(audio_settings),
        repository=repository or FakeRepository(),
        publisher=publisher or RecordingPublisher(),
        queue_settings=QueueSettings(),
        redis_settings=RedisSettings(),
    )


@pytest.fixture
def job_data(audio_root):
    (audio_root / "a.mp3").write_bytes(b"audio")
    return {"id": "job-1", "audioPath": "a.mp3"}


async def test_successful_job_publishes_completion(audio_settings, job_data):
    publisher = RecordingPublisher()
    worker = build_worker(audio_settings, pipeline=FakePipeline(), publisher=publisher)

    result = await worker._process(FakeJob(job_data))

    assert result["status"] == "completed"
    assert publisher.completed[0][0] == "job-1"


async def test_unparseable_payload_is_not_retried(audio_settings):
    pipeline = FakePipeline()
    worker = build_worker(audio_settings, pipeline=pipeline)

    with pytest.raises(UnrecoverableError):
        await worker._process(FakeJob({"audioPath": "a.mp3"}))  # no id

    assert pipeline.runs == 0


async def test_non_retryable_failure_stops_bullmq_retrying(audio_settings, job_data):
    repository = FakeRepository()
    publisher = RecordingPublisher()
    worker = build_worker(
        audio_settings,
        pipeline=FakePipeline(AudioNotFoundError("gone")),
        repository=repository,
        publisher=publisher,
    )

    with pytest.raises(UnrecoverableError):
        await worker._process(FakeJob(job_data, attempts_made=0, max_attempts=3))

    # Recorded as terminally failed even though attempts remained.
    assert repository.failures == [("AudioNotFoundError: gone", False)]
    assert publisher.failed[0][0] == "job-1"


async def test_retryable_failure_reraises_so_bullmq_retries(audio_settings, job_data):
    repository = FakeRepository()
    publisher = RecordingPublisher()
    worker = build_worker(
        audio_settings,
        pipeline=FakePipeline(ProviderError("503", retryable=True)),
        repository=repository,
        publisher=publisher,
    )

    with pytest.raises(ProviderError):
        await worker._process(FakeJob(job_data, attempts_made=0, max_attempts=3))

    # Stays "processing" while attempts remain; no premature failure event.
    assert repository.failures[0][1] is True
    assert publisher.failed == []


async def test_last_attempt_of_a_retryable_failure_is_terminal(
    audio_settings, job_data
):
    repository = FakeRepository()
    publisher = RecordingPublisher()
    worker = build_worker(
        audio_settings,
        pipeline=FakePipeline(ProviderError("503", retryable=True)),
        repository=repository,
        publisher=publisher,
    )

    with pytest.raises(ProviderError):
        await worker._process(FakeJob(job_data, attempts_made=2, max_attempts=3))

    assert repository.failures[0][1] is False
    assert publisher.failed[0][0] == "job-1"
