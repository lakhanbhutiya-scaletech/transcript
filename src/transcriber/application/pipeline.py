"""The transcription use case: probe -> chunk -> transcribe -> merge -> persist."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass

from ..config import AudioSettings
from ..domain.enums import JobStatus
from ..domain.models import AudioChunk, Transcript, TranscriptionRequest
from ..domain.ports import (
    AudioProbe,
    AudioSplitter,
    JobRepository,
    NullProgressReporter,
    ProgressReporter,
    TranscriptionProvider,
)
from .merger import TranscriptMerger

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TranscriptionOutcome:
    """What the worker hands back to BullMQ as the job's return value."""

    external_id: str
    status: JobStatus
    chunk_count: int
    duration_seconds: float
    word_count: int
    character_count: int
    skipped: bool = False

    def to_dict(self) -> dict:
        return {
            "externalId": self.external_id,
            "status": self.status.value,
            "chunkCount": self.chunk_count,
            "durationSeconds": round(self.duration_seconds, 3),
            "wordCount": self.word_count,
            "characterCount": self.character_count,
            "skipped": self.skipped,
        }


class TranscriptionPipeline:
    """Orchestrates one job end to end.

    Knows nothing about Redis, BullMQ or HTTP: it is driven by the worker
    and is equally usable from a test or a CLI.
    """

    def __init__(
        self,
        *,
        probe: AudioProbe,
        splitter: AudioSplitter,
        passthrough_splitter: AudioSplitter,
        provider: TranscriptionProvider,
        repository: JobRepository,
        merger: TranscriptMerger,
        audio_settings: AudioSettings,
    ) -> None:
        self._probe = probe
        self._splitter = splitter
        self._passthrough = passthrough_splitter
        self._provider = provider
        self._repository = repository
        self._merger = merger
        self._settings = audio_settings

    async def run(
        self,
        request: TranscriptionRequest,
        *,
        attempt: int = 1,
        progress: ProgressReporter | None = None,
    ) -> TranscriptionOutcome:
        reporter = progress or NullProgressReporter()
        log = logger.getChild("run")

        previous_status = await self._repository.register(request)
        if previous_status is JobStatus.COMPLETED:
            # A duplicate delivery of an already-finished job. Acknowledge it
            # without burning provider credits.
            log.info(
                "Skipping already completed job",
                extra={"external_id": request.external_id},
            )
            await reporter.report(100, "already completed")
            return TranscriptionOutcome(
                external_id=request.external_id,
                status=JobStatus.COMPLETED,
                chunk_count=0,
                duration_seconds=0.0,
                word_count=0,
                character_count=0,
                skipped=True,
            )

        await self._repository.mark_processing(request.external_id, attempt=attempt)
        await reporter.report(5, "probing audio")

        metadata = await self._probe.probe(request.audio_path)
        log.info(
            "Probed audio",
            extra={
                "external_id": request.external_id,
                "duration": round(metadata.duration_seconds, 2),
                "codec": metadata.codec,
                "size_bytes": metadata.size_bytes,
            },
        )

        chunk_seconds = self._provider.max_chunk_seconds
        needs_chunking = metadata.duration_seconds > chunk_seconds
        splitter = self._splitter if needs_chunking else self._passthrough
        await reporter.report(10, "chunking" if needs_chunking else "preparing audio")

        async with splitter.split(metadata, chunk_seconds) as chunks:
            transcripts = await self._transcribe_chunks(
                request, chunks, reporter=reporter
            )

        merged = self._merger.merge(
            transcripts, total_duration=metadata.duration_seconds
        )
        await reporter.report(95, "saving transcript")
        await self._repository.save_result(
            request.external_id, merged, chunk_count=len(transcripts)
        )
        await reporter.report(100, "completed")

        log.info(
            "Transcription completed",
            extra={
                "external_id": request.external_id,
                "chunks": len(transcripts),
                "words": len(merged.words),
                "characters": len(merged.text),
            },
        )
        return TranscriptionOutcome(
            external_id=request.external_id,
            status=JobStatus.COMPLETED,
            chunk_count=len(transcripts),
            duration_seconds=metadata.duration_seconds,
            word_count=len(merged.words),
            character_count=len(merged.text),
        )

    async def _transcribe_chunks(
        self,
        request: TranscriptionRequest,
        chunks: Sequence[AudioChunk],
        *,
        reporter: ProgressReporter,
    ) -> list[tuple[AudioChunk, Transcript]]:
        """Transcribe chunks concurrently, bounded by a semaphore.

        Progress is reported as chunks land, not in submission order, so the
        percentage moves monotonically even though results arrive unordered.
        """
        limit = asyncio.Semaphore(self._settings.max_parallel_chunks)
        completed = 0
        total = len(chunks)
        lock = asyncio.Lock()

        async def transcribe(chunk: AudioChunk) -> tuple[AudioChunk, Transcript]:
            nonlocal completed
            async with limit:
                transcript = await self._provider.transcribe(
                    chunk.path,
                    language=request.language,
                    model=request.model,
                    diarize=request.diarize,
                )
            async with lock:
                completed += 1
                # Chunk work occupies the 10%..95% band of the job.
                percent = 10 + (completed / total) * 85
                await reporter.report(percent, f"transcribed {completed}/{total}")
            return chunk, transcript

        # gather cancels siblings on the first failure, so a hard provider
        # error does not leave requests running after the job is abandoned.
        return list(await asyncio.gather(*(transcribe(chunk) for chunk in chunks)))


__all__ = ["TranscriptionOutcome", "TranscriptionPipeline"]
