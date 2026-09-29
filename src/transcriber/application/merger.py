"""Stitches per-chunk transcripts back into one continuous transcript."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from ..domain.models import AudioChunk, Segment, Transcript, Word


class TranscriptMerger:
    """Re-bases each chunk's timestamps onto the original timeline.

    Deepgram reports times relative to the file it was given, so every chunk
    restarts at 0. Each chunk carries the offset at which it begins in the
    source; adding it back makes the merged segments line up with the
    original audio.
    """

    def merge(
        self,
        pairs: Sequence[tuple[AudioChunk, Transcript]],
        *,
        total_duration: float,
    ) -> Transcript:
        if not pairs:
            return Transcript(text="", duration_seconds=total_duration)

        ordered = sorted(pairs, key=lambda pair: pair[0].index)
        if len(ordered) == 1:
            only = ordered[0][1]
            limit = total_duration or only.duration_seconds
            return Transcript(
                text=only.text,
                segments=tuple(_clamp(s, limit) for s in only.segments),
                words=tuple(_clamp(w, limit) for w in only.words),
                confidence=only.confidence,
                language=only.language,
                duration_seconds=limit,
                provider=only.provider,
                model=only.model,
            )

        segments: list[Segment] = []
        words: list[Word] = []
        texts: list[str] = []
        weighted_confidence = 0.0
        weight_total = 0.0

        for chunk, transcript in ordered:
            offset = chunk.offset_seconds
            for segment in transcript.segments:
                shifted = segment.shifted(offset, index=len(segments))
                segments.append(_clamp(shifted, total_duration))
            words.extend(
                _clamp(word.shifted(offset), total_duration)
                for word in transcript.words
            )
            if transcript.text:
                texts.append(transcript.text)
            if transcript.confidence is not None and chunk.duration_seconds > 0:
                # Longer chunks should weigh more in the overall confidence.
                weighted_confidence += transcript.confidence * chunk.duration_seconds
                weight_total += chunk.duration_seconds

        reference = ordered[0][1]
        return Transcript(
            text=" ".join(texts).strip(),
            segments=tuple(segments),
            words=tuple(words),
            confidence=(weighted_confidence / weight_total) if weight_total else None,
            language=reference.language,
            duration_seconds=total_duration,
            provider=reference.provider,
            model=reference.model,
        )


def _clamp(item: Segment | Word, limit: float) -> Segment | Word:
    """Keep a timestamp inside the media.

    Providers routinely report an end a little past the audio they were given
    -- harmless inside a chunk, but once the chunk's offset is added it can
    push the merged timestamp past the end of the recording, which breaks
    subtitle tracks and seek bars. Bound it here rather than asking every
    consumer to.
    """
    if limit <= 0:
        return item
    start = min(max(item.start, 0.0), limit)
    end = min(max(item.end, start), limit)
    if start == item.start and end == item.end:
        return item
    return replace(item, start=start, end=end)
