# ChristianTube off-box runners

Standalone GitHub Actions runners that do ChristianTube's heavy AI work off-box.

YouTube blocks Render's datacenter IPs, and a **public** repo gets unlimited free
Actions minutes. So these runners live in their own public repo, do the heavy
lifting (yt-dlp + ffmpeg + Gemini) on GitHub's clean-IP runners, and POST results
back to the private backend over HTTPS. **The database is never exposed and the
transcripts never become public** — only these small scripts are.

There are **two independent runners**, each with its own workflow:

| Script | Workflow | Job | Needs |
|---|---|---|---|
| `transcribe.mjs` | `Transcribe` | transcript → topics → video embedding | yt-dlp + ffmpeg + Gemini |
| `embed.mjs` | `Embed` | embed videos missing a vector (bulk backfill) | Gemini only |

`embed.mjs` exists because bulk embedding — especially after a re-embed migration
nulls every vector — is the heaviest recurring load on the free-tier backend.
Moving it here keeps it off the always-on instance. It needs no yt-dlp/ffmpeg, so
it's fast and its workflow runs more often.

## How it works

The backend is just a queue.

**Transcribe** — each run:

1. `claim` — lease the next pending video.
2. Download audio (yt-dlp), split into clips (ffmpeg), transcribe each clip with
   Gemini → `POST transcript` (text + `[MM:SS]` segment timestamps).
3. Extract topics + per-topic embeddings → `POST topics`.
4. Embed the whole video → `POST embedding` (this marks it `completed`).

Every write carries the lease token. If the backend reaper requeued the video
(this runner was presumed dead), the next POST returns `{ leaseValid: false }`
and the runner abandons that video. One video is transcribed start-to-finish per
attempt; the runner loops through the queue until it's empty or the time/volume
budget is hit.

**Embed** — each run loops: `claim` a batch of videos that **have a transcript
but no embedding** → generate each vector with Gemini → `POST store`. (Videos
without a transcript are not embedded; `content` is null until transcription
fills it, so they're skipped until then. They're still keyword-searchable via
Postgres FTS in the meantime.) Claims are fenced by a
server-side `embeddingClaimedAt` timestamp (not a lease token): re-embedding is
idempotent, so a dead run just leaves claims that go stale and get reclaimed. The
backend's in-process embedding backfill defers to rows this runner has claimed, so
the two never double-embed; it only mops up stragglers when this runner is down.

A video that can *never* be embedded (no text, or content Gemini keeps rejecting
with a 400) would otherwise be reclaimed and re-failed every run forever. So on a
**non-transient, per-video** failure the runner calls `POST fail`, which bumps a
server-side attempt counter; after a few attempts the backend stops handing that
video out. A **transient** 429/5xx outage is *not* reported as a failure — the
claim is just left to go stale and retried later, so a global Gemini hiccup never
poisons good videos. The runner is the only caller of `fail` (it's the only side
that can tell the two apart); the in-process backfill merely respects the cap.

## Setup

1. **Create a new PUBLIC GitHub repo** and push the contents of this directory to it.
2. Add repository **secrets** (Settings → Secrets and variables → Actions → Secrets):
   | Secret | Required | Notes |
   |---|---|---|
   | `BACKEND_URL` | ✅ | e.g. `https://your-backend.onrender.com` (no trailing slash) |
   | `INTERNAL_JOB_SECRET` | ✅ | must match the backend's `INTERNAL_JOB_SECRET` |
   | `GEMINI_API_KEY` | ✅ | **use a dedicated key** with a billing cap, so you can rotate it independently of the app |
   | `YT_DLP_COOKIES` | optional | full Netscape `cookies.txt` contents, if YouTube demands auth |
3. Optionally add repository **variables** (Variables tab) to tune without editing code:
   - Transcribe: `TRANSCRIPTION_MODEL`, `TIME_BUDGET_SECONDS` (default 18000 = 5h), `MAX_VIDEOS`.
   - Embed: `EMBEDDING_MODEL`, `EMBEDDING_BATCH_SIZE` (default 25), `EMBEDDING_TIME_BUDGET_SECONDS` (default 3000), `EMBEDDING_MAX_VIDEOS` (default 500).
4. Trigger them: **Actions → Transcribe → Run workflow** and **Actions → Embed → Run workflow**
   (or wait for their crons — Transcribe every 30 min, Embed every 20 min).

Both runners use the **same three secrets**; `Embed` ignores `YT_DLP_COOKIES`
(it never touches YouTube).

## Security (public repo)

- The workflow triggers **only** on `schedule` and `workflow_dispatch`. Do **not**
  add `pull_request` / `pull_request_target` that checks out and runs PR code —
  on a public repo that would leak the secrets above to fork PRs. Actions secrets
  are otherwise unavailable to fork PRs, which is the protection we rely on.
- Use a **dedicated, capped** `GEMINI_API_KEY`. A billing cap is the real
  blast-radius limiter if the key ever leaks; rotating it never touches the app.

## Environment variables

| Var | Default | Meaning |
|---|---|---|
| `BACKEND_URL` | — | Backend base URL |
| `INTERNAL_JOB_SECRET` | — | Shared secret for the `X-Internal-Secret` header |
| `GEMINI_API_KEY` | — | Gemini API key |
| `TRANSCRIPTION_MODEL` | `gemini-3.1-flash-lite` | Gemini generateContent model (**verify the live id**) |
| `EMBEDDING_MODEL` | `gemini-embedding-001` | 1536-dim embedding model |
| `TRANSCRIPTION_CLIP_DURATION_SECONDS` | `600` | Clip length fed to Gemini |
| `TIME_BUDGET_SECONDS` | `2400` (code) / `18000` (workflow) | Wall-clock budget; the run exits cleanly after this. The workflow passes `18000` (5h) by default; the script's own fallback for local runs is `2400`. |
| `MAX_VIDEOS` | `50` | Max videos per run (transcribe) |
| `YT_DLP_COOKIES` | — | Path to a cookies file (set automatically by the workflow) |
| `EMBEDDING_BATCH_SIZE` | `25` | Videos claimed per `embed.mjs` round-trip |
| `EMBEDDING_TIME_BUDGET_SECONDS` | `2400` (code) / `3000` (workflow) | Wall-clock budget for `embed.mjs` |
| `EMBEDDING_MAX_VIDEOS` | `500` | Max videos per `embed.mjs` run (workflow var; the script reads `MAX_VIDEOS`) |

## Local test

Against a running backend (e.g. the local stack on `:13001`):

```bash
# Transcribe one video
BACKEND_URL=http://localhost:13001 \
INTERNAL_JOB_SECRET=… \
GEMINI_API_KEY=… \
MAX_VIDEOS=1 \
node transcribe.mjs

# Embed a few videos missing a vector
BACKEND_URL=http://localhost:13001 \
INTERNAL_JOB_SECRET=… \
GEMINI_API_KEY=… \
MAX_VIDEOS=5 \
node embed.mjs
```

`transcribe.mjs` requires `node >=20`, `yt-dlp`, and `ffmpeg`/`ffprobe` on PATH.
`embed.mjs` only needs `node >=20`.
