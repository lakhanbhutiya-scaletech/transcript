from __future__ import annotations

import pytest

from transcriber.application.payload import JobPayloadParser
from transcriber.domain.exceptions import (
    AudioNotFoundError,
    InvalidJobPayloadError,
)


@pytest.fixture
def parser(audio_settings):
    return JobPayloadParser(audio_settings)


def test_parses_camel_case_payload(parser, audio_root):
    (audio_root / "call.mp3").write_bytes(b"audio")

    request = parser.parse(
        {
            "id": "job-1",
            "audioPath": "call.mp3",
            "language": "en",
            "metadata": {"userId": "u1"},
        },
        queue_job_id="42",
    )

    assert request.external_id == "job-1"
    assert request.audio_path == (audio_root / "call.mp3").resolve()
    assert request.queue_job_id == "42"
    assert request.metadata == {"userId": "u1"}


def test_parses_snake_case_aliases(parser, audio_root):
    (audio_root / "call.wav").write_bytes(b"audio")

    request = parser.parse({"job_id": "job-2", "file_path": "call.wav"})

    assert request.external_id == "job-2"


def test_missing_id_is_not_retryable(parser):
    with pytest.raises(InvalidJobPayloadError) as error:
        parser.parse({"audioPath": "x.mp3"})
    assert error.value.retryable is False


def test_parsing_does_not_require_the_file_to_exist(parser, audio_root):
    """Existence is checked by the probe, once a job row exists to record it."""
    request = parser.parse({"id": "job-3", "audioPath": "nope.mp3"})
    assert request.audio_path == (audio_root / "nope.mp3").resolve()


def test_ensure_readable_reports_a_missing_file(parser, audio_root):
    request = parser.parse({"id": "job-3", "audioPath": "nope.mp3"})
    with pytest.raises(AudioNotFoundError):
        parser.ensure_readable(request.audio_path)


def test_traversal_out_of_the_root_is_rejected(parser):
    with pytest.raises(InvalidJobPayloadError, match="outside"):
        parser.parse({"id": "job-6", "audioPath": "../../etc/passwd"})


def test_path_escaping_the_root_is_rejected(parser, audio_root, tmp_path):
    outside = tmp_path / "secret.mp3"
    outside.write_bytes(b"audio")

    with pytest.raises(InvalidJobPayloadError, match="outside"):
        parser.parse({"id": "job-4", "audioPath": str(outside)})


def test_remote_urls_are_rejected(parser):
    with pytest.raises(InvalidJobPayloadError, match="Remote URLs"):
        parser.parse({"id": "job-5", "audioPath": "https://example.com/a.mp3"})
