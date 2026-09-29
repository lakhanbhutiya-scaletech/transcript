"""Translation of raw BullMQ job data into a validated domain request.

Producers are Node services, so keys arrive camelCased; snake_case is
accepted too so the queue can be driven from Python or curl without
friction.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from ..config import AudioSettings
from ..domain.exceptions import AudioNotFoundError, InvalidJobPayloadError
from ..domain.models import TranscriptionRequest

_ID_KEYS = ("id", "jobId", "job_id", "externalId", "external_id", "transcriptId")
_PATH_KEYS = ("audioPath", "audio_path", "filePath", "file_path", "path", "audioUrl")
_LANGUAGE_KEYS = ("language", "lang")
_MODEL_KEYS = ("model",)
_DIARIZE_KEYS = ("diarize", "diarization")
_METADATA_KEYS = ("metadata", "meta", "context")


class JobPayloadParser:
    """Validates the producer's payload and resolves the audio path safely."""

    def __init__(self, settings: AudioSettings) -> None:
        self._settings = settings

    def parse(self, data: Any, *, queue_job_id: str | None = None) -> TranscriptionRequest:
        if not isinstance(data, dict):
            raise InvalidJobPayloadError(
                f"Job data must be an object, got {type(data).__name__}"
            )

        external_id = self._first_str(data, _ID_KEYS)
        if not external_id:
            raise InvalidJobPayloadError(
                "Job payload is missing an id", details={"expected": list(_ID_KEYS)}
            )

        raw_path = self._first_str(data, _PATH_KEYS)
        if not raw_path:
            raise InvalidJobPayloadError(
                "Job payload is missing an audio path",
                details={"expected": list(_PATH_KEYS), "id": external_id},
            )

        metadata = self._first(data, _METADATA_KEYS)
        return TranscriptionRequest(
            external_id=external_id,
            audio_path=self.resolve_path(raw_path),
            language=self._first_str(data, _LANGUAGE_KEYS),
            model=self._first_str(data, _MODEL_KEYS),
            diarize=bool(self._first(data, _DIARIZE_KEYS) or False),
            queue_job_id=queue_job_id,
            metadata=metadata if isinstance(metadata, dict) else {},
        )

    def resolve_path(self, raw_path: str) -> Path:
        """Resolve to an absolute path, confined to the configured root.

        Relative paths are taken as relative to `AUDIO_ROOT`. Absolute paths
        outside the root are rejected so a compromised or buggy producer
        cannot make us read arbitrary files from the container.

        Deliberately does *not* check that the file exists: parsing stays a
        pure function of the payload, and a missing file is reported by the
        probe, by which point the job row exists to record the failure
        against. Use `ensure_readable` for an up-front check.
        """
        if raw_path.startswith(("http://", "https://")):
            raise InvalidJobPayloadError(
                "Remote URLs are not supported; provide a path on the shared volume",
                details={"path": raw_path},
            )

        candidate = Path(raw_path).expanduser()
        root = self._settings.root.resolve()
        if not candidate.is_absolute():
            candidate = root / candidate

        # Non-strict resolve still normalises `..`, so containment holds for
        # paths that do not exist yet.
        resolved = candidate.resolve()
        if self._settings.enforce_root and not resolved.is_relative_to(root):
            raise InvalidJobPayloadError(
                "Audio path resolves outside the permitted audio root",
                details={"path": str(resolved), "root": str(root)},
            )
        return resolved

    @staticmethod
    def ensure_readable(path: Path) -> None:
        """Fail fast when the caller wants validation before queueing."""
        if not path.is_file():
            raise AudioNotFoundError(
                f"Audio file not found: {path}", details={"path": str(path)}
            )

    @staticmethod
    def _first(data: dict, keys: tuple[str, ...]) -> Any:
        for key in keys:
            if data.get(key) not in (None, ""):
                return data[key]
        return None

    @classmethod
    def _first_str(cls, data: dict, keys: tuple[str, ...]) -> str | None:
        value = cls._first(data, keys)
        return str(value).strip() if value is not None else None
