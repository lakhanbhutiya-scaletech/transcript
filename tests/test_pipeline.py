"""Pipeline behaviour, driven entirely through in-memory fakes."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from transcriber.application.merger import TranscriptMerger
from transcriber.application.pipeline import TranscriptionPipeline
from transcriber.domain.enums import JobStatus
from transcriber.domain.exceptions import ProviderError
from transcriber.domain.models import (
    AudioChunk,
    AudioMetadata,
    Segment,
    Transcript,
    TranscriptionRequest,
    Word,
)
from transcriber.domain.ports import (
    AudioProbe,
    AudioSplitter,
    ProgressReporter,
    TranscriptionProvider,
)

from .fakes import FakeRepository


class FakeProbe(AudioProbe):
    def __init__(self, duration: float) -> None:
        self.duration = duration

    async def probe(self, path: Path) -> AudioMetadata:
        return AudioMetadata(path=path, duration_seconds=self.duration, size_bytes=1024)


class FakeSplitter(AudioSplitter):
    """Splits purely arithmetically and records whether it was used."""

    def __init__(self) -> None:
        self.called = False
        self.cleaned_up = False

    @asynccontextmanager
    async def split(self, source: AudioMetadata, chunk_seconds: float):
        self.called = True
        remaining = source.duration_seconds
        chunks: list[AudioChunk] = []
        index = 0
        while remaining > 0:
            duration = min(chunk_seconds, remaining)
            chunks.append(
                AudioChunk(
                    index=index,
                    path=source.path.with_name(f"chunk-{index}.flac"),
                    offset_seconds=index * chunk_seconds,
                    duration_seconds=duration,
                )
            )
            remaining -= duration
            index += 1
        try:
            yield tuple(chunks)
        finally:
            self.cleaned_up = True


class PassthroughSplitter(AudioSplitter):
    def __init__(self) -> None:
        self.called = False

    @asynccontextmanager
    async def split(self, source: AudioMetadata, chunk_seconds: float):
        self.called = True
        yield (
            AudioChunk(
                index=0,
                path=source.path,
                offset_seconds=0.0,
                duration_seconds=source.duration_seconds,
            ),
        )


class FakeProvider(TranscriptionProvider):
    def __init__(self, *, max_chunk_seconds: float = 600.0, fail_on: int | None = None):
        self._max = max_chunk_seconds
        self._fail_on = fail_on
        self.calls: list[Path] = []
        self.peak_concurrency = 0
        self._active = 0

    @property
    def name(self) -> str:
        return "fake"

    @property
    def max_chunk_seconds(self) -> float:
        return self._max

    async def transcribe(self, path, *, language=None, model=None, diarize=False):
        self._active += 1
        self.peak_concurrency = max(self.peak_concurrency, self._active)
        try:
            await asyncio.sleep(0)
            index = len(self.calls)
            self.calls.append(path)
            if self._fail_on is not None and index == self._fail_on:
                raise ProviderError("boom", retryable=True)
            text = f"chunk{index}"
            return Transcript(
                text=text,
                segments=(Segment(index=0, text=text, start=0.0, end=1.0),),
                words=(Word(text=text, start=0.0, end=1.0),),
                confidence=0.9,
                provider="fake",
                model="fake-1",
            )
        finally:
            self._active -= 1


class RecordingReporter(ProgressReporter):
    def __init__(self) -> None:
        self.updates: list[tuple[float, str]] = []

    async def report(self, percent: float, message: str) -> None:
        self.updates.append((percent, message))


def build_pipeline(*, probe, splitter, passthrough, provider, repository, settings):
    return TranscriptionPipeline(
        probe=probe,
        splitter=splitter,
        passthrough_splitter=passthrough,
        provider=provider,
        repository=repository,
        merger=TranscriptMerger(),
        audio_settings=settings,
    )


@pytest.fixture
def request_obj(audio_root):
    path = audio_root / "meeting.mp3"
    path.write_bytes(b"audio")
    return TranscriptionRequest(external_id="job-1", audio_path=path)


async def test_short_audio_skips_chunking(request_obj, audio_settings):
    splitter, passthrough = FakeSplitter(), PassthroughSplitter()
    repository = FakeRepository()
    pipeline = build_pipeline(
        probe=FakeProbe(120.0),
        splitter=splitter,
        passthrough=passthrough,
        provider=FakeProvider(max_chunk_seconds=600.0),
        repository=repository,
        settings=audio_settings,
    )

    outcome = await pipeline.run(request_obj)

    assert splitter.called is False
    assert passthrough.called is True
    assert outcome.chunk_count == 1
    assert repository.status is JobStatus.COMPLETED


async def test_long_audio_is_chunked_and_merged(request_obj, audio_settings):
    splitter = FakeSplitter()
    repository = FakeRepository()
    provider = FakeProvider(max_chunk_seconds=600.0)
    pipeline = build_pipeline(
        probe=FakeProbe(1500.0),  # 25 minutes -> 3 chunks at 10 min
        splitter=splitter,
        passthrough=PassthroughSplitter(),
        provider=provider,
        repository=repository,
        settings=audio_settings,
    )

    outcome = await pipeline.run(request_obj)

    assert splitter.called is True
    assert splitter.cleaned_up is True
    assert outcome.chunk_count == 3
    assert len(provider.calls) == 3
    assert repository.saved is not None
    assert repository.saved.text == "chunk0 chunk1 chunk2"
    # Second and third chunks are re-based onto the original timeline.
    assert [round(s.start) for s in repository.saved.segments] == [0, 600, 1200]
    assert repository.saved.duration_seconds == 1500.0


async def test_chunk_concurrency_is_bounded(request_obj, audio_settings):
    provider = FakeProvider(max_chunk_seconds=60.0)
    pipeline = build_pipeline(
        probe=FakeProbe(600.0),  # 10 chunks
        splitter=FakeSplitter(),
        passthrough=PassthroughSplitter(),
        provider=provider,
        repository=FakeRepository(),
        settings=audio_settings,  # max_parallel_chunks = 2
    )

    await pipeline.run(request_obj)

    assert provider.peak_concurrency <= audio_settings.max_parallel_chunks


async def test_already_completed_job_is_skipped(request_obj, audio_settings):
    provider = FakeProvider()
    repository = FakeRepository(initial_status=JobStatus.COMPLETED)
    pipeline = build_pipeline(
        probe=FakeProbe(1500.0),
        splitter=FakeSplitter(),
        passthrough=PassthroughSplitter(),
        provider=provider,
        repository=repository,
        settings=audio_settings,
    )

    outcome = await pipeline.run(request_obj)

    assert outcome.skipped is True
    assert provider.calls == []
    assert repository.saved is None


async def test_provider_failure_propagates_and_cleans_up(request_obj, audio_settings):
    splitter = FakeSplitter()
    pipeline = build_pipeline(
        probe=FakeProbe(1500.0),
        splitter=splitter,
        passthrough=PassthroughSplitter(),
        provider=FakeProvider(max_chunk_seconds=600.0, fail_on=0),
        repository=FakeRepository(),
        settings=audio_settings,
    )

    with pytest.raises(ProviderError):
        await pipeline.run(request_obj)

    assert splitter.cleaned_up is True


async def test_progress_is_monotonic_and_reaches_100(request_obj, audio_settings):
    reporter = RecordingReporter()
    pipeline = build_pipeline(
        probe=FakeProbe(1800.0),
        splitter=FakeSplitter(),
        passthrough=PassthroughSplitter(),
        provider=FakeProvider(max_chunk_seconds=600.0),
        repository=FakeRepository(),
        settings=audio_settings,
    )

    await pipeline.run(request_obj, progress=reporter)

    percentages = [percent for percent, _ in reporter.updates]
    assert percentages == sorted(percentages)
    assert percentages[-1] == 100
