"""ffmpeg/ffprobe adapters for probing and chunking audio.

Both classes shell out asynchronously so a long ffmpeg run never blocks the
worker's event loop.
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import re
import shutil
import tempfile
from collections.abc import Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from ...config import AudioSettings
from ...domain.exceptions import (
    AudioNotFoundError,
    AudioProcessingError,
    UnsupportedAudioError,
)
from ...domain.models import AudioChunk, AudioMetadata
from ...domain.ports import AudioProbe, AudioSplitter

logger = logging.getLogger(__name__)

#: ffmpeg's progress lines, e.g. `size=N/A time=00:01:00.02 bitrate=N/A`.
_TIME_PATTERN = re.compile(r"time=(\d+):(\d{2}):(\d{2}(?:\.\d+)?)")

#: Name of the segment manifest ffmpeg writes alongside the chunks.
_SEGMENT_LIST = "segments.csv"


@dataclass(frozen=True, slots=True)
class CommandResult:
    stdout: str
    stderr: str


class FFmpegRunner:
    """Thin async wrapper around subprocess execution of ffmpeg tooling."""

    def __init__(self, *, timeout_seconds: float = 1800.0) -> None:
        self._timeout = timeout_seconds

    async def run(self, *args: str) -> CommandResult:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=self._timeout
            )
        except TimeoutError as exc:
            process.kill()
            await process.wait()
            raise AudioProcessingError(
                f"{args[0]} timed out after {self._timeout}s"
            ) from exc

        result = CommandResult(
            stdout=stdout.decode(errors="replace"),
            stderr=stderr.decode(errors="replace"),
        )
        if process.returncode != 0:
            raise AudioProcessingError(
                f"{args[0]} exited with {process.returncode}",
                details={"stderr": result.stderr.strip()[-2000:]},
            )
        return result


class FFprobeAudioProbe(AudioProbe):
    """Reads duration and stream info using `ffprobe -print_format json`."""

    def __init__(self, settings: AudioSettings, runner: FFmpegRunner) -> None:
        self._settings = settings
        self._runner = runner

    async def probe(self, path: Path) -> AudioMetadata:
        if not path.is_file():
            raise AudioNotFoundError(f"Audio file not found: {path}")

        result = await self._runner.run(
            self._settings.ffprobe_path,
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            "-select_streams",
            "a:0",
            str(path),
        )
        try:
            probed = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise UnsupportedAudioError(
                f"ffprobe returned invalid JSON for {path}"
            ) from exc

        streams = probed.get("streams") or []
        if not streams:
            raise UnsupportedAudioError(f"No audio stream found in {path}")

        stream = streams[0]
        duration = self._pick_duration(probed.get("format", {}), stream)
        if duration <= 0:
            # Headerless/streamed containers (raw FLAC, ADTS, some MPEG-TS)
            # carry no total length; decoding is the only reliable answer.
            duration = await self._duration_by_decoding(path)
        if duration <= 0:
            raise UnsupportedAudioError(f"Could not determine duration of {path}")

        return AudioMetadata(
            path=path,
            duration_seconds=duration,
            sample_rate=self._as_int(stream.get("sample_rate")),
            channels=self._as_int(stream.get("channels")),
            codec=stream.get("codec_name"),
            size_bytes=path.stat().st_size,
        )

    async def _duration_by_decoding(self, path: Path) -> float:
        """Decode to the null muxer and read the media time ffmpeg reports.

        Hundreds of times faster than realtime for audio, and it is the only
        way to length a file whose container omits the duration.
        """
        try:
            result = await self._runner.run(
                self._settings.ffmpeg_path,
                "-hide_banner",
                "-nostdin",
                "-i",
                str(path),
                "-map",
                "0:a:0",
                "-f",
                "null",
                "-",
            )
        except AudioProcessingError:
            logger.debug("Decode-based duration probe failed", exc_info=True)
            return 0.0

        # ffmpeg emits progress on stderr; the last `time=` is the total.
        matches = _TIME_PATTERN.findall(result.stderr)
        if not matches:
            return 0.0
        hours, minutes, seconds = matches[-1]
        return int(hours) * 3600 + int(minutes) * 60 + float(seconds)

    @staticmethod
    def _pick_duration(container: dict, stream: dict) -> float:
        """Prefer the container duration; fall back to the stream's."""
        for candidate in (container.get("duration"), stream.get("duration")):
            try:
                value = float(candidate)
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value
        return 0.0

    @staticmethod
    def _as_int(value: object) -> int | None:
        try:
            return int(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None


class FFmpegAudioSplitter(AudioSplitter):
    """Splits audio into fixed-length, ASR-normalised chunks.

    ffmpeg cuts on packet boundaries, so a chunk is rarely exactly
    `chunk_seconds` long. Rather than assuming the nominal length -- which
    would drift the timeline of a long file -- we ask the segment muxer for
    a manifest of the cuts it actually made and build the offsets from that.
    """

    def __init__(
        self, settings: AudioSettings, runner: FFmpegRunner, probe: AudioProbe
    ) -> None:
        self._settings = settings
        self._runner = runner
        self._probe = probe

    @asynccontextmanager
    async def split(self, source: AudioMetadata, chunk_seconds: float):
        self._settings.work_dir.mkdir(parents=True, exist_ok=True)
        workdir = Path(
            tempfile.mkdtemp(prefix="chunks-", dir=str(self._settings.work_dir))
        )
        try:
            yield await self._segment(source, chunk_seconds, workdir)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    async def _segment(
        self, source: AudioMetadata, chunk_seconds: float, workdir: Path
    ) -> Sequence[AudioChunk]:
        suffix = self._settings.chunk_format
        manifest = workdir / _SEGMENT_LIST

        await self._runner.run(
            self._settings.ffmpeg_path,
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-i",
            str(source.path),
            "-vn",
            "-map",
            "0:a:0",
            # Mono 16 kHz is what ASR models consume; FLAC keeps it lossless
            # while roughly halving the bytes we upload.
            "-ac",
            "1",
            "-ar",
            str(self._settings.chunk_sample_rate),
            "-f",
            "segment",
            "-segment_time",
            f"{chunk_seconds:.3f}",
            "-reset_timestamps",
            "1",
            "-segment_list",
            str(manifest),
            "-segment_list_type",
            "csv",
            str(workdir / f"chunk-%05d.{suffix}"),
        )

        produced = sorted(workdir.glob(f"chunk-*.{suffix}"))
        if not produced:
            raise AudioProcessingError(
                f"ffmpeg produced no chunks for {source.path}",
                details={"duration": source.duration_seconds},
            )

        chunks = self._chunks_from_manifest(manifest, produced)
        if chunks is None:
            chunks = await self._chunks_by_probing(produced)

        logger.info(
            "Split audio into chunks",
            extra={
                "source": str(source.path),
                "chunks": len(chunks),
                "source_duration": round(source.duration_seconds, 2),
                "chunked_duration": round(
                    sum(chunk.duration_seconds for chunk in chunks), 2
                ),
            },
        )
        return chunks

    @staticmethod
    def _chunks_from_manifest(
        manifest: Path, produced: Sequence[Path]
    ) -> tuple[AudioChunk, ...] | None:
        """Build chunks from ffmpeg's `file,start,end` manifest.

        Returns None if the manifest is missing or disagrees with the files
        on disk, so the caller can fall back to probing.
        """
        if not manifest.is_file():
            return None

        by_name = {path.name: path for path in produced}
        chunks: list[AudioChunk] = []
        try:
            with manifest.open(newline="") as handle:
                for index, row in enumerate(csv.reader(handle)):
                    if len(row) < 3:
                        return None
                    # ffmpeg writes the name relative to the manifest.
                    path = by_name.get(Path(row[0]).name)
                    if path is None:
                        return None
                    start, end = float(row[1]), float(row[2])
                    if end <= start:
                        return None
                    chunks.append(
                        AudioChunk(
                            index=index,
                            path=path,
                            offset_seconds=start,
                            duration_seconds=end - start,
                        )
                    )
        except (OSError, ValueError):
            logger.warning("Unreadable segment manifest, falling back to probing")
            return None

        if len(chunks) != len(produced):
            return None
        return tuple(chunks)

    async def _chunks_by_probing(
        self, produced: Sequence[Path]
    ) -> tuple[AudioChunk, ...]:
        """Fallback: length each chunk and lay them end to end."""
        chunks: list[AudioChunk] = []
        offset = 0.0
        for index, path in enumerate(produced):
            metadata = await self._probe.probe(path)
            chunks.append(
                AudioChunk(
                    index=index,
                    path=path,
                    offset_seconds=offset,
                    duration_seconds=metadata.duration_seconds,
                )
            )
            offset += metadata.duration_seconds
        return tuple(chunks)


class SingleFilePassthroughSplitter(AudioSplitter):
    """Yields the source as one chunk; used when it already fits the limit."""

    @asynccontextmanager
    async def split(self, source: AudioMetadata, chunk_seconds: float):
        yield (
            AudioChunk(
                index=0,
                path=source.path,
                offset_seconds=0.0,
                duration_seconds=source.duration_seconds,
            ),
        )
