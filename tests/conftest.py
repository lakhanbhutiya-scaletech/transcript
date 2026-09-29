from __future__ import annotations

from pathlib import Path

import pytest

from transcriber.config import AudioSettings, DeepgramSettings


@pytest.fixture
def audio_root(tmp_path: Path) -> Path:
    root = tmp_path / "audio"
    root.mkdir()
    return root


@pytest.fixture
def audio_settings(audio_root: Path, tmp_path: Path) -> AudioSettings:
    return AudioSettings(
        root=audio_root,
        work_dir=tmp_path / "work",
        max_parallel_chunks=2,
    )


@pytest.fixture
def deepgram_settings() -> DeepgramSettings:
    return DeepgramSettings(
        api_key="test-key",
        base_url="https://api.deepgram.test",
        max_chunk_seconds=600,
        max_retries=1,
    )
