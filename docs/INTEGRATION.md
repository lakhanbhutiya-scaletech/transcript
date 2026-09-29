# Integration guide

How to send work to the transcription service and get transcripts back.

You do not need to run or read the Python. The whole contract is: **push a job
onto a Redis queue, read the result from Postgres or HTTP.**

---

## 1. Where this service sits

```
 YOUR SERVICE                 REDIS (shared)              TRANSCRIPTION SERVICE
 ────────────                 ──────────────              ─────────────────────
 queue.add('transcribe', {      bull:transcription   →    BullMQ worker
   id, audioPath, … })                                          │
        │                                                  probe (ffprobe)
        │                                                        │
   writes audio to  ──────────────────────────────→   reads from shared volume
   the shared volume                                            │
                                                    longer than the limit?
                                                       │              │
                                                     split          send
                                                  (ffmpeg)          as-is
                                                       │              │
                                                       └──→ Deepgram ←┘
                                                                │
                                                     merge onto one timeline
                                                                │
                                                      ┌─────────┴─────────┐
 read result  ←──────────────────────────────  Postgres            Redis
                                            (transcript +     bull:transcription-events
                                             timed segments)         │
 your listener  ←────────────────────────────────────────────────────┘
```

Three integration points, in the order you will use them:

| You need to | Do this |
| --- | --- |
| Hand over audio | Write the file to the **shared volume** |
| Request a transcript | `queue.add()` on the **`transcription`** queue |
| Get the result | Listen on **`transcription-events`**, or read Postgres / the HTTP API |

---

## 2. Integrate in four steps

### Step 1 — Point at the same Redis, with the same prefix

The service reads the queue your BullMQ client writes to. Both sides must agree
on the Redis instance **and** the key prefix, or you will each be talking to a
queue the other cannot see.

```js
// your service
new Queue('transcription', {
  connection: { host: 'redis', port: 6379 },
  prefix: 'bull',          // must equal the service's REDIS_PREFIX
});
```

`bull` is BullMQ's default and the service's default, so if you have never set
`prefix` anywhere, you are already aligned.

> **The single most common integration failure** is a prefix or database-number
> mismatch: jobs queue successfully, and nothing ever picks them up. If jobs sit
> in `waiting` forever, check this first — see [Troubleshooting](#8-troubleshooting).

### Step 2 — Mount the same audio volume

Audio is passed **by path, not by upload or URL**. Your service writes the file;
the transcription service reads it from the same volume.

```yaml
# your docker-compose.yml
services:
  your-service:
    volumes:
      - audio:/data/audio      # same volume, same mount path
volumes:
  audio:
    external: true
    name: transcription-service_audio
```

Paths in the job payload are resolved against `AUDIO_ROOT` (default
`/data/audio`) and **confined to it**. A path that escapes the root — whether
absolute or via `..` — is rejected. Relative paths are easiest:

```
audioPath: 'calls/2026/09/rec-4f2a.mp3'   →   /data/audio/calls/2026/09/rec-4f2a.mp3
```

Write the file **completely before** you enqueue the job. There is no
coordination between the two — enqueueing first is a race, and the worker may
read a partial file. Write to a temp name and `rename()` into place if your
upload is slow.

### Step 3 — Enqueue the job

```js
import { Queue } from 'bullmq';

const transcription = new Queue('transcription', {
  connection: { host: 'redis', port: 6379 },
  prefix: 'bull',
});

await transcription.add(
  'transcribe',
  {
    id: 'rec-4f2a',                       // required — your id, and the dedupe key
    audioPath: 'calls/2026/09/rec-4f2a.mp3',  // required — relative to AUDIO_ROOT
    language: 'en',                       // optional
    model: 'nova-3',                      // optional
    diarize: true,                        // optional — label speakers
    metadata: { userId: 'u_1', meetingId: 'm_9' },  // optional — stored verbatim
  },
  {
    jobId: 'rec-4f2a',                    // recommended — see "Idempotency"
    attempts: 3,
    backoff: { type: 'exponential', delay: 5000 },
    removeOnComplete: 1000,
    removeOnFail: 5000,
  },
);
```

The job **name** (`'transcribe'`) is not inspected — every job on the queue is
treated as a transcription request. Use whatever name reads well in your
dashboards.

### Step 4 — Receive the result

Pick whichever fits your architecture. All three read the same data.

**Push — listen on the events queue** (recommended; no polling)

```js
import { Worker } from 'bullmq';

new Worker('transcription-events', async (job) => {
  const { event, externalId } = job.data;
  if (event === 'transcription.completed') {
    await onTranscriptReady(externalId);   // then read the text from Postgres
  } else {
    await onTranscriptFailed(externalId, job.data.error);
  }
}, { connection, prefix: 'bull' });
```

**Pull — read the HTTP API**

```js
const res = await fetch(`http://transcription-api:8000/jobs/${id}`);
const job = await res.json();   // status, text, segments, metadata
```

**Direct — query Postgres**

```sql
SELECT status, text, confidence, duration_seconds
FROM transcription_jobs WHERE external_id = $1;
```

---

## 3. The job payload

### Fields

| Field | Required | Type | Notes |
| --- | --- | --- | --- |
| `id` | **yes** | string | Your identifier. Unique per recording; used for dedupe and to look results up. |
| `audioPath` | **yes** | string | Path relative to `AUDIO_ROOT`, or an absolute path inside it. |
| `language` | no | string | BCP-47-ish code, e.g. `en`, `es`, `hi`. Defaults to `DEEPGRAM_LANGUAGE`. |
| `model` | no | string | Deepgram model, e.g. `nova-3`. Defaults to `DEEPGRAM_MODEL`. |
| `diarize` | no | boolean | Label each segment with a speaker number. Default `false`. |
| `metadata` | no | object | Stored verbatim as JSONB and echoed back. Put your foreign keys here. |

### Key aliases

Both camelCase and snake_case are accepted, so you do not have to reshape an
existing payload. The first key present wins, in this order:

| Meaning | Accepted keys |
| --- | --- |
| id | `id`, `jobId`, `job_id`, `externalId`, `external_id`, `transcriptId` |
| path | `audioPath`, `audio_path`, `filePath`, `file_path`, `path`, `audioUrl` |
| language | `language`, `lang` |
| model | `model` |
| diarize | `diarize`, `diarization` |
| metadata | `metadata`, `meta`, `context` |

> `audioUrl` is accepted as an alias for the **path** field for compatibility.
> It is still a path — actual `http://` / `https://` URLs are rejected. Download
> the file to the shared volume yourself.

### What gets rejected immediately

These fail on the first attempt and are **not retried**, because retrying cannot
help:

- no `id` in the payload
- no path in the payload
- an `http://` or `https://` URL
- a path that resolves outside `AUDIO_ROOT`
- a file that does not exist, or is not decodable audio

---

## 4. What you get back

### The BullMQ return value

Available on the `completed` event and as `job.returnvalue`:

```json
{
  "externalId": "rec-4f2a",
  "status": "completed",
  "chunkCount": 3,
  "durationSeconds": 152.633,
  "wordCount": 343,
  "characterCount": 2034,
  "skipped": false
}
```

`skipped: true` means this job was already completed and was acknowledged
without re-transcribing. See [Idempotency](#6-idempotency-retries-and-failures).

### The events queue

Jobs land on `transcription-events` (`QUEUE_EVENTS_NAME`) with the job name set
to the event name:

```json
// job name: "transcription.completed"
{ "event": "transcription.completed", "externalId": "rec-4f2a",
  "status": "completed", "chunkCount": 3, "durationSeconds": 152.633,
  "wordCount": 343, "characterCount": 2034, "skipped": false }

// job name: "transcription.failed"
{ "event": "transcription.failed", "externalId": "rec-4f2a",
  "error": "ProviderError: Deepgram returned 401" }
```

`transcription.failed` is published **once**, on the final attempt only — you
will not get an event per retry. Set `QUEUE_PUBLISH_EVENTS=false` to turn the
fan-out off entirely.

### Progress

Written throughout the job; read it with BullMQ's `progress` event.

```json
{ "percent": 42.5, "message": "transcribed 2/4" }
```

Messages you will see: `probing audio` → `chunking` → `transcribed n/total` →
`saving transcript` → `completed`. Chunk work occupies the 10–95% band.

### The database

**`transcription_jobs`** — one row per `external_id`

| Column | Meaning |
| --- | --- |
| `external_id` | Your `id`. Unique. |
| `status` | `pending` / `processing` / `completed` / `failed` |
| `text` | The full merged transcript |
| `confidence` | 0–1, weighted by chunk duration |
| `duration_seconds` | Length of the source audio |
| `chunk_count` | How many pieces it was split into (`1` = not split) |
| `word_count` | Words in the transcript |
| `language`, `model`, `provider` | What actually ran, e.g. `en`, `nova-3`, `deepgram` |
| `metadata` | Your `metadata` object, as JSONB |
| `attempts` | Attempt number of the last run |
| `error` | Last error message, `NULL` when completed |
| `created_at`, `started_at`, `completed_at`, `updated_at` | Timestamps |

**`transcript_segments`** — timed utterances, `ON DELETE CASCADE`

| Column | Meaning |
| --- | --- |
| `job_id` | FK to `transcription_jobs.id` |
| `index` | Order, 0-based, across the whole recording |
| `text` | The utterance |
| `start_seconds`, `end_seconds` | Offsets **in the original audio**, already re-based across chunks |
| `confidence` | 0–1 |
| `speaker` | Speaker number when `diarize` was set, else `NULL` |

An empty transcript is a **valid completed result**, not a failure. Audio with
no speech — music, silence, hold tone — completes with `text = ''`,
`word_count = 0` and no segments. Check `word_count`, not `status`, if you need
to know whether anything was said.

---

## 5. Reading the HTTP API

Base URL is wherever the `api` container is published (`:8000` by default).

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Liveness + database ping |
| `GET` | `/queue/stats` | `waiting` / `active` / `completed` / `failed` / `delayed` counts |
| `POST` | `/jobs` | Enqueue a job (validates the payload before queueing) |
| `GET` | `/jobs/{external_id}` | Full job: status, transcript, segments, metadata |
| `GET` | `/jobs?status=&limit=` | Recent jobs, newest first |

Interactive docs at `/docs`.

`POST /jobs` is the easy way to test without writing any BullMQ code. It
validates the payload **and** checks the file exists, returning `400` with a
reason instead of queueing something that cannot succeed:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"id":"rec-4f2a","audioPath":"calls/rec-4f2a.mp3","language":"en"}'
```

---

## 6. Idempotency, retries and failures

### Idempotency

Two independent layers protect you, and you should use both:

1. **Pass `jobId` equal to your `id`.** BullMQ then refuses to queue a duplicate
   while the job is still known to Redis.
2. **The service checks the database.** If a row for that `external_id` is
   already `completed`, the job is acknowledged immediately with `skipped: true`
   and no Deepgram credits are spent.

So redelivering a job is always safe.

### Retry semantics

Errors are classified, and the classification decides whether BullMQ retries:

| Kind of failure | Retried? | Row status while retrying |
| --- | --- | --- |
| Deepgram 5xx / 429, network error, timeout | yes — inside the client with backoff, then by BullMQ | `processing` |
| Missing file, undecodable audio, path outside the root | **no** — fails on attempt 1 | `failed` |
| Malformed payload (no `id`, no path, a URL) | **no** — fails on attempt 1 | no row is created |
| Deepgram 401 / 403 (bad key) | **no** | `failed` |

> **Important for your dashboards:** while BullMQ still has attempts left, the
> row stays `processing`, *not* `failed`. This is deliberate — you never see a
> flap of `failed → processing → completed`. A row reads `failed` only when the
> service has genuinely given up, and that is exactly when
> `transcription.failed` is published.

Set `attempts` on your side; the service honours whatever you choose. Three with
exponential backoff is a sensible default.

### Stalled jobs

`QUEUE_LOCK_DURATION_SECONDS` (default 300) is how long a job may run before
BullMQ considers the worker dead and re-queues the job. Transcription of a very
long file with a slow provider can exceed this — if you see jobs restarting
without an error, raise it.

---

## 7. Chunking — what it means for your timestamps

Deepgram is asked for at most `DEEPGRAM_MAX_CHUNK_SECONDS` of audio per request
(default **600 s / 10 min**). You do not need to do anything about this; the
service handles it.

- **Shorter than the limit** → sent as-is, `chunk_count = 1`.
- **Longer** → split with ffmpeg into mono 16 kHz FLAC, transcribed in parallel,
  merged back onto one timeline.

**Timestamps you read are always relative to the original file.** Each chunk's
times are shifted by that chunk's real offset — taken from ffmpeg's own segment
manifest, not assumed from the nominal chunk length — so they do not drift on
long recordings. Measured drift on a 155 s file split in three: 0.000 s.

Chunking also makes long files **faster**, because chunks go to Deepgram in
parallel. A measured 205 s file: 13.2 s as one request, 3.3 s as four chunks.

**The one caveat worth knowing.** Cuts are made at fixed times, not at pauses.
No audio is lost, but a sentence spanning a boundary becomes two segments and
the model loses context across the seam, so wording near a cut can be slightly
worse. Real example from a 4-chunk run, at the 180 s boundary:

```
177.06 -> 179.94   This is an example sound file in OG Corbus
180.00 -> 181.76   Format from Wikipedia,
```

If you are concatenating segment text, join on the text — do not assume one
segment is one sentence.

---

## 8. Troubleshooting

**Jobs sit in `waiting` and nothing picks them up.**
Prefix or Redis database mismatch. Confirm both sides see the same keys:

```bash
redis-cli --scan --pattern 'bull:transcription*'
# expect: bull:transcription:meta, bull:transcription:wait, …
```

If your keys are under a different prefix, align `prefix` in your `Queue`
options with the service's `REDIS_PREFIX`. Also check the database number in the
Redis URL (`redis://redis:6379/0`) — `/0` and `/1` are separate key spaces.

**Every job fails with "Audio file not found".**
The volume is not shared, or not mounted at the same path. Check from inside the
worker:

```bash
docker compose exec worker ls -la /data/audio
```

**Every job fails with "resolves outside the permitted audio root".**
You are sending an absolute path from your own container's filesystem. Send a
path relative to `AUDIO_ROOT` instead.

**`ProviderError: Deepgram returned 401`.**
`DEEPGRAM_API_KEY` is missing or wrong. Not retried — fix the key and re-enqueue.

**Transcript is empty but the status is `completed`.**
Not a bug: there is no speech in the audio. Verify with
`ffmpeg -i file.mp3 -af volumedetect -f null -` — if there is audio but no
speech (music, hold tone), an empty transcript is the correct answer.

**Jobs restart with no error in the logs.**
Exceeding `QUEUE_LOCK_DURATION_SECONDS`. Raise it.

**Where to look.** Logs are one JSON object per line, keyed by `external_id`:

```bash
docker compose logs -f worker | grep rec-4f2a
```

---

## 9. Configuration reference

Everything is environment variables; see `.env.example`. The ones that matter
for integration are marked **★**.

| Variable | Default | Purpose |
| --- | --- | --- |
| ★ `REDIS_URL` | `redis://redis:6379/0` | The shared Redis |
| ★ `REDIS_PREFIX` | `bull` | Must match your BullMQ `prefix` |
| ★ `QUEUE_NAME` | `transcription` | Queue the service consumes |
| ★ `QUEUE_EVENTS_NAME` | `transcription-events` | Queue completion/failure events go to |
| ★ `AUDIO_ROOT` | `/data/audio` | Shared volume; all paths resolve inside it |
| `QUEUE_PUBLISH_EVENTS` | `true` | Set `false` to disable event fan-out |
| `QUEUE_CONCURRENCY` | `2` | Jobs one worker runs at once |
| `QUEUE_LOCK_DURATION_SECONDS` | `300` | Before a job counts as stalled |
| `DEEPGRAM_API_KEY` | — | **Required** |
| `DEEPGRAM_MODEL` | `nova-3` | Default model |
| `DEEPGRAM_LANGUAGE` | `en` | Default language |
| `DEEPGRAM_DIARIZE` | `false` | Default diarization |
| `DEEPGRAM_MAX_CHUNK_SECONDS` | `600` | Chunk above this length |
| `DEEPGRAM_MAX_RETRIES` | `3` | In-client retries per chunk |
| `DEEPGRAM_REQUEST_TIMEOUT_SECONDS` | `300` | Per-request timeout |
| `DATABASE_URL` | `postgresql+asyncpg://…` | Must use the `asyncpg` driver |
| `AUDIO_ENFORCE_ROOT` | `true` | Path confinement. Leave on. |
| `AUDIO_MAX_PARALLEL_CHUNKS` | `3` | Chunks of one job in flight at Deepgram |
| `API_PORT` | `8000` | HTTP control plane |

Peak concurrent Deepgram requests per worker is
`QUEUE_CONCURRENCY × AUDIO_MAX_PARALLEL_CHUNKS` — 6 by default. Raise with an
eye on your Deepgram rate limit.

Scale out by running more `worker` replicas. They coordinate through Redis; no
extra configuration is needed.

---

## 10. Testing your integration locally

```bash
cp .env.example .env        # set DEEPGRAM_API_KEY
docker compose up --build
```

Then, without writing any producer code:

```bash
# 1. put a file on the shared volume
docker compose cp ./sample.mp3 worker:/data/audio/sample.mp3

# 2. enqueue
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"id":"test-1","audioPath":"sample.mp3","language":"en"}'

# 3. watch
docker compose logs -f worker

# 4. read the result
curl -s http://localhost:8000/jobs/test-1 | jq '{status, wordCount, text}'
```

To exercise the chunking path on a short file, set
`DEEPGRAM_MAX_CHUNK_SECONDS=60` and re-run.
