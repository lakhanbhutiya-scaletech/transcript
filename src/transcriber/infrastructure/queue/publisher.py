"""Publishes terminal job events back onto a BullMQ queue."""
from __future__ import annotations

import logging

from bullmq import Queue

from ...config import QueueSettings, RedisSettings
from ...domain.ports import JobEventPublisher

logger = logging.getLogger(__name__)


class BullMQEventPublisher(JobEventPublisher):
    """Lets downstream Node services react without polling the database."""

    COMPLETED = "transcription.completed"
    FAILED = "transcription.failed"

    def __init__(self, queue_settings: QueueSettings, redis: RedisSettings) -> None:
        self._queue = Queue(
            queue_settings.events_name,
            {"connection": redis.url, "prefix": redis.prefix},
        )

    async def publish_completed(self, external_id: str, payload: dict) -> None:
        await self._publish(self.COMPLETED, external_id, payload)

    async def publish_failed(self, external_id: str, payload: dict) -> None:
        await self._publish(self.FAILED, external_id, payload)

    async def _publish(self, event: str, external_id: str, payload: dict) -> None:
        try:
            await self._queue.add(
                event,
                {"event": event, "externalId": external_id, **payload},
                {
                    # Keep the event stream from growing without bound.
                    "removeOnComplete": 1000,
                    "removeOnFail": 5000,
                    "attempts": 3,
                    "backoff": {"type": "exponential", "delay": 2000},
                },
            )
        except Exception:
            logger.exception(
                "Failed to publish job event",
                extra={"event": event, "external_id": external_id},
            )

    async def aclose(self) -> None:
        await self._queue.close()


class NullEventPublisher(JobEventPublisher):
    """Used when event fan-out is disabled."""

    async def publish_completed(self, external_id: str, payload: dict) -> None:
        return None

    async def publish_failed(self, external_id: str, payload: dict) -> None:
        return None
