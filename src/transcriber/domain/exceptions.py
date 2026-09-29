"""Exception hierarchy for the transcription domain.

`TranscriptionError.retryable` tells the queue layer whether it is worth
letting BullMQ schedule another attempt, or whether the job should be
failed permanently (bad payload, missing file, unsupported media).
"""
from __future__ import annotations


class TranscriptionError(Exception):
    """Base class for every error raised by this service."""

    retryable: bool = True

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class InvalidJobPayloadError(TranscriptionError):
    """The producing service sent a payload we cannot act on."""

    retryable = False


class AudioNotFoundError(TranscriptionError):
    """The referenced audio file is missing or unreadable."""

    retryable = False


class UnsupportedAudioError(TranscriptionError):
    """The file exists but ffprobe cannot treat it as decodable audio."""

    retryable = False


class AudioProcessingError(TranscriptionError):
    """ffmpeg/ffprobe failed while probing or splitting the media."""


class ProviderError(TranscriptionError):
    """The speech-to-text provider failed to return a usable response."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retryable: bool = True,
        details: dict | None = None,
    ) -> None:
        super().__init__(message, details=details)
        self.status_code = status_code
        self.retryable = retryable
