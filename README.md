# Transcription Service

A Python worker that consumes **BullMQ** transcription jobs from the shared
Redis, transcribes the referenced audio with **Deepgram**, and writes the
result to Postgres. Audio longer than the provider's per-request limit is
chunked, transcribed in parallel, and stitched back onto a single timeline.

```
 Node service                Redis (shared)              This service
 ────────────                ──────────────              ────────────
 queue.add('transcribe',  →  bull:transcription      →   BullMQ worker
   { id, audioPath, … })                                      │
                                                     probe (ffprobe)
                                                              │
                                          duration > limit? ──┴── no ──┐
                                                     │                 │
                                          split (ffmpeg segment)       │
                                                     │                 │
                                          ┌──────────┴──────────┐      │
                                       chunk 0   chunk 1   chunk N     │
                                          └──────────┬──────────┘      │
                                              Deepgram /v1/listen ◄────┘
                                                     │
                                          merge (re-base timestamps)
                                                     │
                                             Postgres (transcript
                                              + timed segments)
                                                     │
 Node service   ◄──  bull:transcription-events  ◄────┘
```

## Quick start

```bash
cp .env.example .env          # then set DEEPGRAM_API_KEY
docker compose up --build
```

This starts Postgres, Redis, runs the Alembic migrations, then brings up the
`worker` and the `api` (on `http://localhost:8000`, docs at `/docs`).

Point `REDIS_URL` at your existing shared Redis and delete the `redis`
service from `docker-compose.yml` for a real deployment. `REDIS_PREFIX` must
match the BullMQ `prefix` the producing services use (default `bull`), or the
two sides will read different key spaces.

## The job contract

Producers push onto the `transcription` queue (`QUEUE_NAME`):

```js
await transcriptionQueue.add('transcribe', {
  id:        'recording-4f2a',        // your id; also the dedupe key
  audioPath: 'calls/2026/recording-4f2a.mp3',  // relative to AUDIO_ROOT
  language:  'en',                    // optional
  model:     'nova-3',                // optional
  diarize:   false,                   // optional
  metadata:  { userId: 'u_1', meetingId: 'm_9' },  // stored verbatim
}, { attempts: 3, backoff: { type: 'exponential', delay: 5000 } });
```

`id`, `jobId`, `externalId` and their snake_case spellings are all accepted
for the identifier; `audioPath`, `filePath` and `path` for the file.

Audio is passed **by path on a shared volume**, not by URL — mount the same
volume into this service at `AUDIO_ROOT`.

On success the worker returns a summary as the BullMQ job result and, unless
`QUEUE_PUBLISH_EVENTS=false`, publishes a `transcription.completed` job onto
`QUEUE_EVENTS_NAME` so downstream services need not poll:

```json
{ "event": "transcription.completed", "externalId": "recording-4f2a",
  "status": "completed", "chunkCount": 3, "durationSeconds": 1500.0,
  "wordCount": 2841, "characterCount": 15203, "skipped": false }
```

Progress is written to the job as `{ percent, message }` throughout, readable
with BullMQ's `progress` event.

## Chunking

`DEEPGRAM_MAX_CHUNK_SECONDS` (default 600 = 10 minutes) is the ceiling for a
single request. Shorter files are sent as-is; longer ones go through ffmpeg's
segment muxer and are normalised to mono 16 kHz FLAC.

ffmpeg cuts on packet boundaries, so chunks are never exactly the nominal
length. Rather than assuming they are — which would drift the timeline of a
long recording — the splitter reads ffmpeg's own segment manifest for the
exact cut points, and each chunk's transcript is shifted by its real offset
before merging. Over a 155 s file split into three the measured drift is
0.000 s (`tests/test_ffmpeg_integration.py`).

`AUDIO_MAX_PARALLEL_CHUNKS` bounds how many chunks of one job are in flight
at Deepgram at once; `QUEUE_CONCURRENCY` bounds how many jobs a worker runs.
Peak requests per worker is the product of the two.

Chunking is lossless — ffmpeg splits the stream rather than dropping audio —
but the cuts are made at fixed times, not at pauses, so a sentence spanning a
boundary is transcribed as two segments and the model loses its context
across the seam. If boundary accuracy matters more than cost, lower
`DEEPGRAM_MAX_CHUNK_SECONDS` is *not* the fix; overlapping chunks with
word-level de-duplication in `TranscriptMerger` would be.

## Schema

`transcription_jobs` holds one row per `external_id` — status, the full text,
confidence, duration, chunk/word counts, the producer's `metadata` as JSONB,
and error/attempt bookkeeping. `transcript_segments` holds the timed,
speaker-attributed utterances, cascading on delete.

## Failure handling

Errors carry a `retryable` flag. A bad payload, a missing file or unsupported
media fails the job immediately — retrying cannot help. Provider 5xx/429 and
network errors are retried inside the Deepgram client with exponential
backoff and jitter, and beyond that by BullMQ.

While BullMQ still has attempts left the row stays `processing`, so readers
never see a flap of `failed → processing`. Only the final attempt writes
`failed` and emits `transcription.failed`.

Redelivery is safe: a job whose row is already `completed` is acknowledged
without re-transcribing.

## HTTP control plane

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Liveness + database ping |
| `GET` | `/queue/stats` | Waiting/active/completed/failed counts |
| `POST` | `/jobs` | Manually enqueue (validates before queueing) |
| `GET` | `/jobs/{external_id}` | Job status, transcript and segments |
| `GET` | `/jobs?status=&limit=` | Recent jobs |

## Layout

```
src/transcriber/
├── domain/          models, enums, exceptions, ports (ABCs)
├── application/     payload parsing, merging, the pipeline use case
├── infrastructure/  ffmpeg, Deepgram, SQLAlchemy, BullMQ adapters
├── api/             FastAPI app, routes, schemas
├── container.py     composition root — the only place that picks adapters
└── runner.py        `worker` / `api` / `all` entrypoints
```

The pipeline depends only on the abstract ports in `domain/ports.py`, so
swapping Deepgram for another vendor means writing one adapter class and
changing one line in `container.py`.

## Development

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/pytest              # 27 tests; ffmpeg ones skip if it's absent
.venv/bin/ruff check .
PYTHONPATH=src .venv/bin/python -m transcriber worker   # or: api | all
```

Schema changes go through Alembic (`alembic revision --autogenerate`,
`alembic upgrade head`). Outside production the worker calls `create_all()`
on boot as a convenience.

## Security notes

- `DEEPGRAM_API_KEY` lives in `.env`, which is gitignored. Rotate the key if
  it has ever been pasted into a chat, issue or commit.
- Audio paths from the queue are resolved and confined to `AUDIO_ROOT`
  (`AUDIO_ENFORCE_ROOT`), so a buggy or compromised producer cannot make the
  worker read arbitrary files out of the container.
- The container runs as an unprivileged user.
