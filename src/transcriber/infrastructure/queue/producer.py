"""Enqueues jobs onto the shared transcription queue.

Primarily for the API's manual-submit endpoint and for local testing; the
Node services normally produce jobs themselves.
"""
from __future__ import annotations

from bullmq import Queue

from ...config import QueueSettings, RedisSettings


class TranscriptionQueueProducer:
    def __init__(self, queue_settings: QueueSettings, redis: RedisSettings) -> None:
        self._queue = Queue(
            queue_settings.name, {"connection": redis.url, "prefix": redis.prefix}
        )
        self._name = queue_settings.name

    @property
    def name(self) -> str:
        return self._name

    async def enqueue(self, payload: dict, *, attempts: int = 3) -> str:
        opts = {
            "attempts": attempts,
            "backoff": {"type": "exponential", "delay": 5000},
            "removeOnComplete": 1000,
            "removeOnFail": 5000,
        }
        if payload.get("id"):
            # Dedupe: re-submitting the same id maps to one BullMQ job.
            opts["jobId"] = str(payload["id"])
        job = await self._queue.add("transcribe", payload, opts)
        return str(job.id)

    async def counts(self) -> dict:
        return await self._queue.getJobCounts(
            "waiting", "active", "completed", "failed", "delayed"
        )

    async def aclose(self) -> None:
        await self._queue.close()
