"""Domain-level enumerations shared across layers."""
from __future__ import annotations

from enum import StrEnum


class JobStatus(StrEnum):
    """Lifecycle of a transcription job as persisted in the database."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self in (JobStatus.COMPLETED, JobStatus.FAILED)
