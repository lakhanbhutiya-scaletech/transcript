"""Request/response models for the HTTP control plane."""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class SubmitJobRequest(BaseModel):
    """Manual submission, mirroring what the Node producers put on the queue."""

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(description="Caller-owned identifier; also the dedupe key")
    audio_path: str = Field(alias="audioPath", description="Path on the shared volume")
    language: str | None = None
    model: str | None = None
    diarize: bool = False
    metadata: dict = Field(default_factory=dict)

    def to_job_payload(self) -> dict:
        return {
            "id": self.id,
            "audioPath": self.audio_path,
            "language": self.language,
            "model": self.model,
            "diarize": self.diarize,
            "metadata": self.metadata,
        }


class SubmitJobResponse(BaseModel):
    external_id: str = Field(alias="externalId")
    queue_job_id: str = Field(alias="queueJobId")
    queue: str

    model_config = ConfigDict(populate_by_name=True)


class HealthResponse(BaseModel):
    status: str
    database: bool
    version: str
    environment: str
