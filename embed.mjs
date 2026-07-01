#!/usr/bin/env node
/**
 * ChristianTube embedding runner.
 *
 * Sibling of transcribe.mjs. Bulk embedding (especially after a re-embed
 * migration nulls every vector) is the heaviest recurring load on the free-tier
 * backend, so it runs here on GitHub Actions instead. The backend is just a
 * queue: this script claims a batch of videos missing an embedding, generates
 * the vectors with Gemini, and POSTs each back over HTTPS. No yt-dlp/ffmpeg — so
 * it's fast and cheap; its own lightweight workflow can run more often than the
 * transcription one. The DB is never exposed.
 *
 * Protocol (all POST, guarded by X-Internal-Secret):
 *   claim  <- { max }                  -> { videos: [{ videoId, text }] }
 *   store  <- { videoId, embedding }   -> { stored }
 *   fail   <- { videoId }              -> { attempts }
 *
 * Fencing is a server-side `embeddingClaimedAt` timestamp; re-embedding is
 * idempotent, so a dead run just leaves claims that go stale and get reclaimed.
 *
 * `fail` is the poison-pill counter and we are its *only* caller: report it only
 * for a non-transient, per-video failure (persistent Gemini 400, no embeddable
 * text). For a 429/5xx outage we do NOT call fail — that's a global hiccup, not a
 * bad video, so we leave the claim to go stale and retry, never poisoning good
 * rows. After MAX_EMBEDDING_ATTEMPTS the backend stops handing the video out.
 */

const BACKEND_URL = required('BACKEND_URL').replace(/\/$/, '');
const SECRET = required('INTERNAL_JOB_SECRET');
const GEMINI_API_KEY = required('GEMINI_API_KEY');
const EMBEDDING_MODEL = process.env.EMBEDDING_MODEL || 'gemini-embedding-001';
const BATCH_SIZE = parseInt(process.env.EMBEDDING_BATCH_SIZE || '25', 10);
const TIME_BUDGET_MS = parseInt(process.env.TIME_BUDGET_SECONDS || '2400', 10) * 1000;
const MAX_VIDEOS = parseInt(process.env.MAX_VIDEOS || '500', 10);

const GEMINI_MAX_RETRIES = 3;

function required(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`Missing required env var: ${name}`);
    process.exit(1);
  }
  return v;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(path, body) {
  const res = await fetch(`${BACKEND_URL}/internal/jobs/embedding/${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Internal-Secret': SECRET },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`API ${path} -> ${res.status} ${text}`);
  }
  return res.json();
}

function geminiUrl(model, method) {
  return `https://generativelanguage.googleapis.com/v1beta/models/${model}:${method}`;
}

// Identical to transcribe.mjs:generateEmbedding so vectors match whichever
// runner produced them (1536-dim, unit-normalised, RETRIEVAL_DOCUMENT).
async function generateEmbedding(text) {
  let input = (text || '').slice(0, 8000).trim();
  if (!input) return [];
  let lastError = null;

  for (let attempt = 0; attempt < GEMINI_MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(geminiUrl(EMBEDDING_MODEL, 'embedContent'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Goog-Api-Key': GEMINI_API_KEY },
        body: JSON.stringify({
          model: `models/${EMBEDDING_MODEL}`,
          content: { parts: [{ text: input }] },
          taskType: 'RETRIEVAL_DOCUMENT',
          outputDimensionality: 1536,
        }),
      });

      if (res.ok) {
        const data = await res.json();
        const values = data?.embedding?.values;
        if (!values || values.length === 0) throw new Error('Gemini embedding returned empty values');
        const magnitude = Math.sqrt(values.reduce((s, v) => s + v * v, 0));
        return magnitude > 0 ? values.map((v) => v / magnitude) : values;
      }

      if (res.status === 400 && input.length > 1000) {
        input = input.slice(0, Math.floor(input.length / 2));
        console.warn(`Embedding 400 — retrying with shorter input (${input.length} chars)`);
        lastError = new Error(`Gemini embedding 400`);
        continue;
      }
      const text = await res.text().catch(() => '');
      const retryable = res.status === 429 || (res.status >= 500 && res.status < 600);
      if (!retryable) {
        // A non-429 4xx that survived the shorten-and-retry above: the content
        // itself is the problem. Tag it poison so main() reports it via `fail`.
        const err = new Error(`Gemini embedding ${res.status}: ${text}`);
        err.poison = true;
        throw err;
      }
      lastError = new Error(`Gemini embedding ${res.status}: ${text}`);
    } catch (err) {
      if (err.message?.startsWith('Gemini embedding ') && !err.message.match(/(429|5\d\d)/)) throw err;
      lastError = err;
    }
    if (attempt < GEMINI_MAX_RETRIES - 1) await sleep(Math.pow(4, attempt) * 1000);
  }
  throw lastError || new Error('Embedding failed after retries');
}

async function main() {
  const deadline = Date.now() + TIME_BUDGET_MS;
  let processed = 0;

  while (Date.now() < deadline && processed < MAX_VIDEOS) {
    const remaining = MAX_VIDEOS - processed;
    let claim;
    try {
      claim = await api('claim', { max: Math.min(BATCH_SIZE, remaining) });
    } catch (err) {
      console.error(`claim failed: ${err.message}`);
      break;
    }
    const videos = claim?.videos || [];
    if (videos.length === 0) {
      console.log('Queue empty — nothing to embed.');
      break;
    }

    for (const { videoId, text } of videos) {
      if (Date.now() >= deadline || processed >= MAX_VIDEOS) break;
      try {
        const embedding = await generateEmbedding(text);
        if (embedding.length === 0) {
          // Nothing embeddable (empty title/description/content) — a permanent,
          // per-video condition, so count it as a failure. After the cap the
          // backend stops handing it out instead of reclaiming it every run.
          console.warn(`[${videoId}] no embeddable text — recording failure`);
          await api('fail', { videoId }).catch((e) =>
            console.error(`[${videoId}] fail report failed: ${e.message}`),
          );
          processed++;
          continue;
        }
        await api('store', { videoId, embedding });
        console.log(`[${videoId}] embedding stored`);
      } catch (err) {
        if (err.poison) {
          // A genuine per-video failure (persistent 400). Count it toward the
          // poison-pill cap so it eventually stops being retried.
          console.error(`[${videoId}] embedding failed (poison): ${err.message}`);
          await api('fail', { videoId }).catch((e) =>
            console.error(`[${videoId}] fail report failed: ${e.message}`),
          );
        } else {
          // Transient (429/5xx outage). Leave the claim to go stale and be
          // retried by a later run / the in-process fallback — do NOT count it,
          // a global hiccup must not poison good videos. Don't abort the batch.
          console.error(`[${videoId}] embedding failed (transient): ${err.message}`);
        }
      }
      processed++;
    }
  }

  console.log(`Run complete. Processed ${processed} video(s).`);
}

main().catch((err) => {
  console.error(`Fatal: ${err.message}`);
  process.exit(1);
});
