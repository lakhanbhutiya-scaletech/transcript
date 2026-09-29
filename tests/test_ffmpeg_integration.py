"""Integration tests that exercise the real ffmpeg/ffprobe binaries."""
from __future__ import annotations

import asyncio
import itertools
import shutil
from pathlib import Path

import pytest

from transcriber.config import AudioSettings
from transcriber.domain.exceptions import AudioNotFoundError, UnsupportedAudioError
from transcriber.infrastructure.audio import (
    FFmpegAudioSplitter,
    FFmpegRunner,
    FFprobeAudioProbe,
)

pytestmark = pytest.mark.skipif(
    not (shutil.which("ffmpeg") and shutil.which("ffprobe")),
    reason="ffmpeg/ffprobe not installed",
)

SOURCE_SECONDS = 155.0


@pytest.fixture(scope="module")
def tone(tmp_path_factory) -> Path:
    """A 155 s mp3 tone: long enough to need three 60 s chunks."""
    path = tmp_path_factory.mktemp("audio") / "tone.mp3"
    asyncio.run(
        FFmpegRunner().run(
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-f", "lavfi", "-i", f"sine=frequency=440:duration={SOURCE_SECONDS:.0f}",
            "-ac", "2", "-ar", "44100", str(path),
        )
    )
    return path


@pytest.fixture
def tools(tmp_path, tone):
    settings = AudioSettings(root=tone.parent, work_dir=tmp_path / "work")
    runner = FFmpegRunner()
    probe = FFprobeAudioProbe(settings, runner)
    return probe, FFmpegAudioSplitter(settings, runner, probe)


async def test_probe_reads_source_metadata(tools, tone):
    probe, _ = tools

    metadata = await probe.probe(tone)

    assert metadata.duration_seconds == pytest.approx(SOURCE_SECONDS, abs=0.2)
    assert metadata.codec == "mp3"
    assert metadata.channels == 2
    assert metadata.size_bytes > 0


async def test_probe_rejects_missing_file(tools, tmp_path):
    probe, _ = tools
    with pytest.raises(AudioNotFoundError):
        await probe.probe(tmp_path / "nope.mp3")


async def test_probe_rejects_non_audio(tools, tmp_path):
    probe, _ = tools
    junk = tmp_path / "not-audio.mp3"
    junk.write_bytes(b"definitely not audio")
    with pytest.raises((UnsupportedAudioError, Exception)):
        await probe.probe(junk)


async def test_split_offsets_cover_the_source_without_drift(tools, tone):
    probe, splitter = tools
    metadata = await probe.probe(tone)

    async with splitter.split(metadata, 60.0) as chunks:
        workdir = chunks[0].path.parent

        assert len(chunks) == 3
        assert [c.index for c in chunks] == [0, 1, 2]
        assert chunks[0].offset_seconds == 0.0
        assert all(c.path.stat().st_size > 0 for c in chunks)

        # Chunks must tile the timeline: each starts where the last ended.
        for previous, current in itertools.pairwise(chunks):
            assert current.offset_seconds == pytest.approx(
                previous.end_seconds, abs=0.01
            )

        total = sum(c.duration_seconds for c in chunks)
        assert total == pytest.approx(metadata.duration_seconds, abs=0.1)

    assert not workdir.exists(), "temporary chunk directory must be removed"


async def test_chunks_are_normalised_to_mono_16k(tools, tone):
    probe, splitter = tools
    metadata = await probe.probe(tone)

    async with splitter.split(metadata, 60.0) as chunks:
        # Raw FLAC from the segment muxer has no duration header; this only
        # succeeds because the probe falls back to decoding.
        chunk_meta = await probe.probe(chunks[0].path)

    assert chunk_meta.channels == 1
    assert chunk_meta.sample_rate == 16_000
    assert chunk_meta.codec == "flac"
    assert chunk_meta.duration_seconds == pytest.approx(60.0, abs=0.5)


async def test_workdir_is_cleaned_up_when_the_body_raises(tools, tone):
    probe, splitter = tools
    metadata = await probe.probe(tone)
    captured: list[Path] = []

    with pytest.raises(RuntimeError):
        async with splitter.split(metadata, 60.0) as chunks:
            captured.append(chunks[0].path.parent)
            raise RuntimeError("downstream failure")

    assert not captured[0].exists()
