from __future__ import annotations

import httpx
import pytest
import respx
from pydantic import SecretStr

from transcriber.domain.exceptions import ProviderError
from transcriber.infrastructure.providers.deepgram import (
    DeepgramTranscriptionProvider,
)

RESPONSE = {
    "metadata": {
        "duration": 12.5,
        "models": ["2187e11a-3532-4498-b076-81fa530bdd49"],
        "model_info": {
            "2187e11a-3532-4498-b076-81fa530bdd49": {
                "name": "general-nova-3",
                "arch": "nova-3",
            }
        },
    },
    "results": {
        "channels": [
            {
                "alternatives": [
                    {
                        "transcript": "hello world",
                        "confidence": 0.97,
                        "words": [
                            {
                                "word": "hello",
                                "punctuated_word": "Hello",
                                "start": 0.1,
                                "end": 0.5,
                                "confidence": 0.99,
                            },
                            {
                                "word": "world",
                                "punctuated_word": "world.",
                                "start": 0.6,
                                "end": 1.0,
                                "confidence": 0.95,
                            },
                        ],
                    }
                ]
            }
        ],
        "utterances": [
            {
                "transcript": "Hello world.",
                "start": 0.1,
                "end": 1.0,
                "confidence": 0.97,
                "speaker": 0,
            }
        ],
    },
}


@pytest.fixture
def audio_file(tmp_path):
    path = tmp_path / "chunk-00000.flac"
    path.write_bytes(b"fake-flac-bytes")
    return path


@respx.mock
async def test_successful_transcription_is_parsed(deepgram_settings, audio_file):
    route = respx.post("https://api.deepgram.test/v1/listen").mock(
        return_value=httpx.Response(200, json=RESPONSE)
    )
    provider = DeepgramTranscriptionProvider(deepgram_settings)

    transcript = await provider.transcribe(audio_file, language="en")
    await provider.aclose()

    assert transcript.text == "hello world"
    assert transcript.confidence == 0.97
    assert transcript.duration_seconds == 12.5
    assert transcript.provider == "deepgram"
    # The readable arch, not Deepgram's internal model UUID.
    assert transcript.model == "nova-3"
    assert [w.text for w in transcript.words] == ["Hello", "world."]
    assert len(transcript.segments) == 1
    assert transcript.segments[0].speaker == 0

    request = route.calls.last.request
    assert request.headers["authorization"] == "Token test-key"
    assert request.headers["content-type"] == "audio/flac"
    assert request.url.params["utterances"] == "true"
    assert request.url.params["language"] == "en"


@respx.mock
async def test_server_errors_are_retried(deepgram_settings, audio_file):
    route = respx.post("https://api.deepgram.test/v1/listen").mock(
        side_effect=[
            httpx.Response(503, text="unavailable"),
            httpx.Response(200, json=RESPONSE),
        ]
    )
    provider = DeepgramTranscriptionProvider(deepgram_settings)

    transcript = await provider.transcribe(audio_file)
    await provider.aclose()

    assert route.call_count == 2
    assert transcript.text == "hello world"


@respx.mock
async def test_client_errors_are_not_retried(deepgram_settings, audio_file):
    route = respx.post("https://api.deepgram.test/v1/listen").mock(
        return_value=httpx.Response(401, text="unauthorized")
    )
    provider = DeepgramTranscriptionProvider(deepgram_settings)

    with pytest.raises(ProviderError) as error:
        await provider.transcribe(audio_file)
    await provider.aclose()

    assert route.call_count == 1
    assert error.value.retryable is False
    assert error.value.status_code == 401


@respx.mock
@respx.mock
async def test_model_falls_back_to_the_requested_name(deepgram_settings, audio_file):
    body = {
        "metadata": {"duration": 5.0},
        "results": {"channels": [{"alternatives": [{"transcript": "hi"}]}]},
    }
    respx.post("https://api.deepgram.test/v1/listen").mock(
        return_value=httpx.Response(200, json=body)
    )
    provider = DeepgramTranscriptionProvider(deepgram_settings)

    transcript = await provider.transcribe(audio_file, model="nova-2")
    await provider.aclose()

    assert transcript.model == "nova-2"


@respx.mock
async def test_falls_back_to_whole_transcript_when_no_utterances(
    deepgram_settings, audio_file
):
    body = {
        "metadata": {"duration": 5.0},
        "results": {
            "channels": [
                {"alternatives": [{"transcript": "bare text", "confidence": 0.8}]}
            ]
        },
    }
    respx.post("https://api.deepgram.test/v1/listen").mock(
        return_value=httpx.Response(200, json=body)
    )
    provider = DeepgramTranscriptionProvider(deepgram_settings)

    transcript = await provider.transcribe(audio_file)
    await provider.aclose()

    assert len(transcript.segments) == 1
    assert transcript.segments[0].text == "bare text"


def test_missing_api_key_is_rejected(deepgram_settings):
    with pytest.raises(ValueError, match="DEEPGRAM_API_KEY"):
        DeepgramTranscriptionProvider(
            deepgram_settings.model_copy(update={"api_key": SecretStr("")})
        )
