"""Bridges the pipeline's progress reporting onto a BullMQ job."""
from __future__ import annotations

import logging

from ...domain.ports import ProgressReporter

logger = logging.getLogger(__name__)


class BullMQProgressReporter(ProgressReporter):
    """Writes `{percent, message}` to the job so producers can watch it.

    Progress updates are best-effort: a Redis hiccup while reporting must
    never fail an otherwise healthy transcription.
    """

    def __init__(self, job, *, min_delta: float = 1.0) -> None:
        self._job = job
        self._min_delta = min_delta
        self._last = -1.0

    async def report(self, percent: float, message: str) -> None:
        percent = max(0.0, min(100.0, percent))
        if percent < 100 and percent - self._last < self._min_delta:
            return
        self._last = percent
        try:
            await self._job.updateProgress(
                {"percent": round(percent, 2), "message": message}
            )
        except Exception:
            logger.debug("Could not publish job progress", exc_info=True)
