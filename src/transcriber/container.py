"""Composition root.

Every concrete implementation is chosen here and nowhere else. Swapping
Deepgram for another vendor, or Postgres for something else, is a change to
this file plus one new adapter class.
"""
from __future__ import annotations

import logging
from contextlib import AsyncExitStack

from .application.merger import TranscriptMerger
from .application.payload import JobPayloadParser
from .application.pipeline import TranscriptionPipeline
from .config import Settings, get_settings
from .domain.ports import JobEventPublisher, JobRepository
from .infrastructure.audio import (
    FFmpegAudioSplitter,
    FFmpegRunner,
    FFprobeAudioProbe,
    SingleFilePassthroughSplitter,
)
from .infrastructure.db import Database, SqlAlchemyJobRepository
from .infrastructure.providers import DeepgramTranscriptionProvider
from .infrastructure.queue import (
    BullMQEventPublisher,
    NullEventPublisher,
    TranscriptionQueueProducer,
    TranscriptionWorker,
)

logger = logging.getLogger(__name__)


class Container:
    """Lazily builds and owns the object graph, and tears it down in order."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._stack = AsyncExitStack()

        self._database: Database | None = None
        self._provider: DeepgramTranscriptionProvider | None = None
        self._publisher: JobEventPublisher | None = None
        self._producer: TranscriptionQueueProducer | None = None
        self._pipeline: TranscriptionPipeline | None = None
        self._worker: TranscriptionWorker | None = None

    # -- singletons -------------------------------------------------------

    @property
    def database(self) -> Database:
        if self._database is None:
            self._database = Database(self.settings.database)
            self._stack.push_async_callback(self._database.aclose)
        return self._database

    @property
    def repository(self) -> JobRepository:
        return SqlAlchemyJobRepository(self.database)

    @property
    def provider(self) -> DeepgramTranscriptionProvider:
        if self._provider is None:
            self._provider = DeepgramTranscriptionProvider(self.settings.deepgram)
            self._stack.push_async_callback(self._provider.aclose)
        return self._provider

    @property
    def publisher(self) -> JobEventPublisher:
        if self._publisher is None:
            if self.settings.queue.publish_events:
                self._publisher = BullMQEventPublisher(
                    self.settings.queue, self.settings.redis
                )
                self._stack.push_async_callback(self._publisher.aclose)
            else:
                self._publisher = NullEventPublisher()
        return self._publisher

    @property
    def producer(self) -> TranscriptionQueueProducer:
        if self._producer is None:
            self._producer = TranscriptionQueueProducer(
                self.settings.queue, self.settings.redis
            )
            self._stack.push_async_callback(self._producer.aclose)
        return self._producer

    @property
    def payload_parser(self) -> JobPayloadParser:
        return JobPayloadParser(self.settings.audio)

    @property
    def pipeline(self) -> TranscriptionPipeline:
        if self._pipeline is None:
            runner = FFmpegRunner()
            probe = FFprobeAudioProbe(self.settings.audio, runner)
            self._pipeline = TranscriptionPipeline(
                probe=probe,
                splitter=FFmpegAudioSplitter(self.settings.audio, runner, probe),
                passthrough_splitter=SingleFilePassthroughSplitter(),
                provider=self.provider,
                repository=self.repository,
                merger=TranscriptMerger(),
                audio_settings=self.settings.audio,
            )
        return self._pipeline

    @property
    def worker(self) -> TranscriptionWorker:
        if self._worker is None:
            self._worker = TranscriptionWorker(
                pipeline=self.pipeline,
                parser=self.payload_parser,
                repository=self.repository,
                publisher=self.publisher,
                queue_settings=self.settings.queue,
                redis_settings=self.settings.redis,
            )
            self._stack.push_async_callback(self._worker.aclose)
        return self._worker

    # -- lifecycle --------------------------------------------------------

    async def aclose(self) -> None:
        """Close everything that was actually constructed, in reverse order."""
        await self._stack.aclose()
        logger.info("Container closed")
