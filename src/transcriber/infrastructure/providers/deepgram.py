"""Deepgram pre-recorded transcription adapter.

Talks to the REST API over httpx rather than the vendor SDK: the payload
is a handful of query parameters, and this keeps the dependency surface (and
the blast radius of an SDK major bump) small. Everything Deepgram-specific
lives behind `TranscriptionProvider`.
"""
from __future__ import annotations

import asyncio
import logging
import random
from pathlib import Path

import httpx

from ...config import DeepgramSettings
from ...domain.exceptions import ProviderError
from ...domain.models import Segment, Transcript, Word
from ...domain.ports import TranscriptionProvider

logger = logging.getLogger(__name__)

#: Status codes worth another attempt: rate limiting and transient 5xx.
_RETRYABLE_STATUS = frozenset({408, 409, 429, 500, 502, 503, 504})

_CONTENT_TYPES = {
    ".flac": "audio/flac",
    ".wav": "audio/wav",
    ".mp3": "audio/mpeg",
    ".m4a": "audio/mp4",
    ".mp4": "audio/mp4",
    ".aac": "audio/aac",
    ".ogg": "audio/ogg",
    ".opus": "audio/ogg",
    ".webm": "audio/webm",
    ".amr": "audio/amr",
    ".wma": "audio/x-ms-wma",
}


class DeepgramTranscriptionProvider(TranscriptionProvider):
    """Sends one file per request to `POST /v1/listen`."""

    def __init__(
        self, settings: DeepgramSettings, client: httpx.AsyncClient | None = None
    ) -> None:
        if not settings.api_key.get_secret_value():
            raise ValueError("DEEPGRAM_API_KEY is not configured")
        self._settings = settings
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=settings.base_url,
            timeout=httpx.Timeout(settings.request_timeout_seconds, connect=15.0),
            headers={
                "Authorization": f"Token {settings.api_key.get_secret_value()}",
                "Accept": "application/json",
            },
        )

    @property
    def name(self) -> str:
        return "deepgram"

    @property
    def max_chunk_seconds(self) -> float:
        return self._settings.max_chunk_seconds

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def transcribe(
        self,
        path: Path,
        *,
        language: str | None = None,
        model: str | None = None,
        diarize: bool = False,
    ) -> Transcript:
        model = model or self._settings.model
        payload = path.read_bytes()
        if not payload:
            raise ProviderError(f"Refusing to send empty audio file {path}", retryable=False)

        params = self._build_params(language=language, model=model, diarize=diarize)
        body = await self._post_with_retries(path, payload, params)
        return self._parse(body, model=model)

    # -- request building -------------------------------------------------

    def _build_params(
        self, *, language: str | None, model: str, diarize: bool
    ) -> dict[str, str]:
        params = {
            "model": model,
            "smart_format": _flag(self._settings.smart_format),
            "punctuate": _flag(self._settings.punctuate),
            "diarize": _flag(diarize or self._settings.diarize),
            # Utterance grouping gives us ready-made, timestamped segments.
            "utterances": "true",
        }
        language = language or self._settings.language
        if language:
            params["language"] = language
        return params

    async def _post_with_retries(
        self, path: Path, payload: bytes, params: dict[str, str]
    ) -> dict:
        attempts = self._settings.max_retries + 1
        last_error: ProviderError | None = None

        for attempt in range(1, attempts + 1):
            try:
                response = await self._client.post(
                    "/v1/listen",
                    params=params,
                    content=payload,
                    headers={"Content-Type": _content_type(path)},
                )
            except httpx.HTTPError as exc:
                last_error = ProviderError(f"Deepgram request failed: {exc}")
            else:
                if response.status_code < 400:
                    return response.json()
                last_error = self._error_for(response)

            if not last_error.retryable or attempt == attempts:
                break
            delay = self._backoff(attempt)
            logger.warning(
                "Retrying Deepgram request",
                extra={
                    "attempt": attempt,
                    "of": attempts,
                    "delay": round(delay, 2),
                    "reason": last_error.message,
                    "file": path.name,
                },
            )
            await asyncio.sleep(delay)

        raise last_error or ProviderError("Deepgram request failed")

    def _error_for(self, response: httpx.Response) -> ProviderError:
        body = response.text[:1000]
        return ProviderError(
            f"Deepgram returned {response.status_code}",
            status_code=response.status_code,
            retryable=response.status_code in _RETRYABLE_STATUS,
            details={"body": body},
        )

    @staticmethod
    def _backoff(attempt: int) -> float:
        """Exponential backoff with jitter, capped at 30s."""
        return min(2.0**attempt, 30.0) * (0.5 + random.random() / 2)

    # -- response parsing -------------------------------------------------

    def _parse(self, body: dict, *, model: str) -> Transcript:
        results = body.get("results") or {}
        channels = results.get("channels") or []
        if not channels:
            raise ProviderError(
                "Deepgram response contained no channels", retryable=False
            )

        alternatives = channels[0].get("alternatives") or [{}]
        alternative = alternatives[0]
        metadata = body.get("metadata") or {}

        words = tuple(
            _word_from(raw) for raw in alternative.get("words") or [] if raw.get("word")
        )
        segments = self._segments_from(results, alternative, words)

        return Transcript(
            text=(alternative.get("transcript") or "").strip(),
            segments=segments,
            words=words,
            confidence=_as_float(alternative.get("confidence")),
            language=_detected_language(channels[0], metadata),
            duration_seconds=_as_float(metadata.get("duration")) or 0.0,
            provider=self.name,
            model=self._resolve_model_name(metadata, requested=model),
            raw=body,
        )

    @staticmethod
    def _resolve_model_name(metadata: dict, *, requested: str) -> str:
        """Report a human-readable model name, not Deepgram's internal UUID.

        `metadata.models` lists model ids; `metadata.model_info` maps each to
        a name/arch. The arch ("nova-3") is what callers recognise, so prefer
        it, then the display name, and fall back to what we asked for.
        """
        model_info = metadata.get("model_info") or {}
        for model_id in metadata.get("models") or []:
            info = model_info.get(model_id) or {}
            name = info.get("arch") or info.get("name")
            if name:
                return str(name)
        return requested

    @staticmethod
    def _segments_from(
        results: dict, alternative: dict, words: tuple[Word, ...]
    ) -> tuple[Segment, ...]:
        """Prefer utterances, then paragraphs, then a single whole-file segment."""
        utterances = results.get("utterances") or []
        if utterances:
            return tuple(
                Segment(
                    index=index,
                    text=(item.get("transcript") or "").strip(),
                    start=_as_float(item.get("start")) or 0.0,
                    end=_as_float(item.get("end")) or 0.0,
                    confidence=_as_float(item.get("confidence")),
                    speaker=_as_int(item.get("speaker")),
                )
                for index, item in enumerate(utterances)
                if (item.get("transcript") or "").strip()
            )

        paragraphs = ((alternative.get("paragraphs") or {}).get("paragraphs")) or []
        if paragraphs:
            return tuple(
                Segment(
                    index=index,
                    text=" ".join(
                        (sentence.get("text") or "").strip()
                        for sentence in item.get("sentences") or []
                    ).strip(),
                    start=_as_float(item.get("start")) or 0.0,
                    end=_as_float(item.get("end")) or 0.0,
                    speaker=_as_int(item.get("speaker")),
                )
                for index, item in enumerate(paragraphs)
            )

        text = (alternative.get("transcript") or "").strip()
        if not text:
            return ()
        return (
            Segment(
                index=0,
                text=text,
                start=words[0].start if words else 0.0,
                end=words[-1].end if words else 0.0,
                confidence=_as_float(alternative.get("confidence")),
            ),
        )


def _word_from(raw: dict) -> Word:
    return Word(
        # `punctuated_word` is present when smart_format/punctuate is on.
        text=raw.get("punctuated_word") or raw.get("word") or "",
        start=_as_float(raw.get("start")) or 0.0,
        end=_as_float(raw.get("end")) or 0.0,
        confidence=_as_float(raw.get("confidence")),
        speaker=_as_int(raw.get("speaker")),
    )


def _detected_language(channel: dict, metadata: dict) -> str | None:
    return channel.get("detected_language") or metadata.get("language")


def _flag(value: bool) -> str:
    return "true" if value else "false"


def _content_type(path: Path) -> str:
    return _CONTENT_TYPES.get(path.suffix.lower(), "application/octet-stream")


def _as_float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _as_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
