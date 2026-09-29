from __future__ import annotations

from pathlib import Path

from transcriber.application.merger import TranscriptMerger
from transcriber.domain.models import AudioChunk, Segment, Transcript, Word


def _chunk(index: int, offset: float, duration: float) -> AudioChunk:
    return AudioChunk(
        index=index,
        path=Path(f"/tmp/chunk-{index}.flac"),
        offset_seconds=offset,
        duration_seconds=duration,
    )


def _transcript(text: str, start: float, end: float, confidence: float) -> Transcript:
    return Transcript(
        text=text,
        segments=(Segment(index=0, text=text, start=start, end=end),),
        words=(Word(text=text, start=start, end=end),),
        confidence=confidence,
        duration_seconds=end,
        provider="deepgram",
        model="nova-3",
    )


def test_offsets_are_applied_to_later_chunks():
    merger = TranscriptMerger()

    merged = merger.merge(
        [
            (_chunk(0, 0.0, 600.0), _transcript("hello there", 1.0, 3.0, 0.9)),
            (_chunk(1, 600.0, 300.0), _transcript("goodbye now", 2.0, 4.0, 0.8)),
        ],
        total_duration=900.0,
    )

    assert merged.text == "hello there goodbye now"
    assert [round(s.start, 2) for s in merged.segments] == [1.0, 602.0]
    assert [round(s.end, 2) for s in merged.segments] == [3.0, 604.0]
    assert [s.index for s in merged.segments] == [0, 1]
    assert merged.duration_seconds == 900.0


def test_confidence_is_weighted_by_chunk_duration():
    merger = TranscriptMerger()

    merged = merger.merge(
        [
            (_chunk(0, 0.0, 600.0), _transcript("a", 0.0, 1.0, 1.0)),
            (_chunk(1, 600.0, 200.0), _transcript("b", 0.0, 1.0, 0.5)),
        ],
        total_duration=800.0,
    )

    # (1.0*600 + 0.5*200) / 800
    assert round(merged.confidence, 4) == 0.875


def test_chunks_are_merged_in_index_order_regardless_of_arrival():
    merger = TranscriptMerger()

    merged = merger.merge(
        [
            (_chunk(1, 10.0, 10.0), _transcript("second", 0.0, 1.0, 0.9)),
            (_chunk(0, 0.0, 10.0), _transcript("first", 0.0, 1.0, 0.9)),
        ],
        total_duration=20.0,
    )

    assert merged.text == "first second"


def test_empty_input_yields_empty_transcript():
    merged = TranscriptMerger().merge([], total_duration=12.0)
    assert merged.text == ""
    assert merged.duration_seconds == 12.0


def test_timestamps_are_clamped_to_the_media_duration():
    """A provider overshooting inside the last chunk must not escape the file."""
    merger = TranscriptMerger()

    merged = merger.merge(
        [
            (_chunk(0, 0.0, 24.0), _transcript("early", 1.0, 2.0, 0.9)),
            # Deepgram reports 3.2s of speech in a chunk only 2.33s long.
            (_chunk(1, 24.0, 2.33), _transcript("late", 0.05, 3.2, 0.9)),
        ],
        total_duration=26.33,
    )

    assert merged.segments[-1].end == 26.33
    assert merged.words[-1].end == 26.33
    assert all(s.end <= merged.duration_seconds for s in merged.segments)
    assert all(s.start <= s.end for s in merged.segments)


def test_clamping_leaves_in_range_timestamps_untouched():
    merger = TranscriptMerger()

    merged = merger.merge(
        [
            (_chunk(0, 0.0, 10.0), _transcript("a", 1.0, 2.0, 0.9)),
            (_chunk(1, 10.0, 10.0), _transcript("b", 1.0, 2.0, 0.9)),
        ],
        total_duration=20.0,
    )

    assert [(s.start, s.end) for s in merged.segments] == [(1.0, 2.0), (11.0, 12.0)]


def test_single_chunk_timestamps_are_clamped_too():
    merger = TranscriptMerger()

    merged = merger.merge(
        [(_chunk(0, 0.0, 5.0), _transcript("only", 0.0, 9.9, 0.9))],
        total_duration=5.0,
    )

    assert merged.segments[0].end == 5.0
    assert merged.words[0].end == 5.0
