#!/usr/bin/env node
/**
 * ChristianTube transcription runner.
 *
 * Runs on a GitHub Actions runner in a *public* repo — real CPU/RAM, unlimited
 * free minutes, and cleaner egress IPs than Render's datacenter ranges (which
 * YouTube frequently blocks). The backend is just a queue: this script claims a
 * video, does the heavy lifting (yt-dlp + ffmpeg + Gemini) off-box, and POSTs
 * the results back over HTTPS in phases. The DB is never exposed; transcripts
 * never become public.
 *
 * Protocol (all POST, guarded by X-Internal-Secret):
 *   claim       -> { videoId, leaseId, resumeFromSeconds, title, description } | { videoId: null }
 *   progress    <- { videoId, leaseId, content?, progressSeconds?, detail? }   -> { leaseValid }
 *   transcript  <- { videoId, leaseId, text, segments }                        -> { leaseValid }
 *   topics      <- { videoId, leaseId, topics[] }                              -> { leaseValid }
 *   embedding   <- { videoId, leaseId, embedding }   (marks completed)         -> { leaseValid }
 *   fail        <- { videoId, leaseId, error }                                 -> { leaseValid }
 *
 * Each video is transcribed start-to-finish in one run. If the run dies, the
 * backend reaper requeues it after 30 min; any later POST whose lease the reaper
 * invalidated returns { leaseValid: false } and we abandon the video.
 */

import { promises as fs, mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

const BACKEND_URL = required('BACKEND_URL').replace(/\/$/, '');
const SECRET = required('INTERNAL_JOB_SECRET');
const GEMINI_API_KEY = required('GEMINI_API_KEY');
const MODEL = process.env.TRANSCRIPTION_MODEL || 'gemini-3.1-flash-lite';
const EMBEDDING_MODEL = process.env.EMBEDDING_MODEL || 'gemini-embedding-001';
const CLIP_SECONDS = parseInt(process.env.TRANSCRIPTION_CLIP_DURATION_SECONDS || '600', 10);
const TIME_BUDGET_MS = parseInt(process.env.TIME_BUDGET_SECONDS || '2400', 10) * 1000;
const MAX_VIDEOS = parseInt(process.env.MAX_VIDEOS || '50', 10);

const CLIP_TIMEOUT_MS = 120_000;
const TOPIC_TIMEOUT_MS = 30_000;
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
  const res = await fetch(`${BACKEND_URL}/internal/jobs/transcription/${path}`, {
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

// Thrown when the backend reports our lease is no longer valid — the reaper
// requeued this video (we were presumed dead). Stop immediately rather than
// racing to overwrite the runner that re-claimed it.
class LeaseLostError extends Error {
  constructor() {
    super('Lease lost — video was requeued by the backend reaper');
    this.name = 'LeaseLostError';
  }
}

function assertLease(res) {
  if (res && res.leaseValid === false) throw new LeaseLostError();
  return res;
}

/* ---- audio helpers ---- */

async function downloadAudio(videoId, outputPath) {
  const url = `https://www.youtube.com/watch?v=${videoId}`;
  const template = outputPath.replace(/\.mp3$/, '.%(ext)s');
  const args = [
    '-f', 'bestaudio',
    '--extract-audio', '--audio-format', 'mp3',
    '--no-part',
    // Node.js is available on Actions runners; register it so yt-dlp can solve
    // YouTube's n-challenge (URL obfuscation) that the web client requires.
    '--js-runtimes', 'nodejs',
    '-o', template,
  ];
  if (process.env.YT_DLP_COOKIES) {
    args.push('--cookies', process.env.YT_DLP_COOKIES);
    // iOS client doesn't support cookie auth — use web client (needs nodejs for n-challenge).
    args.push('--extractor-args', 'youtube:player_client=web,mweb');
  } else {
    // Without cookies, iOS bypasses bot detection and skips the n-challenge entirely.
    args.push('--extractor-args', 'youtube:player_client=ios,web');
  }
  args.push(url);

  // Patterns that mean the video will never be downloadable — no point retrying.
  const PERMANENT_ERRORS = [
    'Requested format is not available',
    'Only images are available',
    'This live event will begin',
    'Private video',
    'Video unavailable',
    'has been removed',
  ];

  let lastError = null;
  for (let attempt = 0; attempt < GEMINI_MAX_RETRIES; attempt++) {
    try {
      await new Promise((resolve, reject) => {
        let stderr = '';
        const proc = spawn('yt-dlp', args, { stdio: ['ignore', 'ignore', 'pipe'] });
        proc.stderr.on('data', (d) => { stderr += d; process.stderr.write(d); });
        proc.on('close', (code) => {
          if (code === 0) return resolve();
          const permanent = PERMANENT_ERRORS.find((p) => stderr.includes(p));
          if (permanent) {
            const e = new Error(`yt-dlp: ${permanent} (permanent, not retrying)`);
            e.permanent = true;
            return reject(e);
          }
          reject(new Error(`yt-dlp exited ${code}`));
        });
        proc.on('error', reject);
      });
      return;
    } catch (err) {
      lastError = err;
      await fs.unlink(outputPath).catch(() => {});
      if (err.permanent) throw err; // skip retries for permanent failures
      if (attempt < GEMINI_MAX_RETRIES - 1) await sleep(Math.pow(2, attempt) * 1000);
    }
  }
  throw lastError || new Error('yt-dlp download failed after retries');
}

async function getAudioDuration(filePath) {
  const { stdout } = await execFileAsync('ffprobe', [
    '-v', 'error', '-show_entries', 'format=duration',
    '-of', 'default=noprint_wrappers=1:nokey=1', filePath,
  ]);
  const duration = parseFloat(stdout.trim());
  // Guard against a missing/zero-length download silently yielding 0 clips and
  // an empty "successful" transcript.
  if (!Number.isFinite(duration) || duration <= 0) {
    throw new Error(`Could not determine audio duration (got "${stdout.trim()}")`);
  }
  return duration;
}

// Re-encode each clip to mono 16 kHz ~32 kbps — ideal for speech ASR, ~5-10x
// smaller than the source, comfortably under Gemini's inline cap.
async function extractClip(inputPath, outputPath, start, duration) {
  await execFileAsync('ffmpeg', [
    '-y', '-ss', String(start), '-t', String(duration),
    '-i', inputPath, '-ac', '1', '-ar', '16000', '-b:a', '32k', outputPath,
  ]);
}

/* ---- Gemini ---- */

function geminiUrl(model, method) {
  return `https://generativelanguage.googleapis.com/v1beta/models/${model}:${method}`;
}

async function transcribeClip(clipPath, clipIndex, clipOffset) {
  const base64Audio = (await fs.readFile(clipPath)).toString('base64');
  let lastError = null;

  for (let attempt = 0; attempt < GEMINI_MAX_RETRIES; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), CLIP_TIMEOUT_MS);
    try {
      const res = await fetch(geminiUrl(MODEL, 'generateContent'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Goog-Api-Key': GEMINI_API_KEY },
        signal: controller.signal,
        body: JSON.stringify({
          contents: [{
            parts: [
              {
                text: 'Transcribe this audio clip word for word, preserving the original language. '
                  + 'At each natural pause or topic change, include the approximate timestamp '
                  + 'in [MM:SS] format relative to the START of this clip. '
                  + "Example:\n[00:00]\nWelcome to today's message.\n\n[00:15]\nLet us turn to Hebrews chapter 10.\n\n"
                  + 'Return only the transcribed text with timestamps. Do not add commentary.',
              },
              { inline_data: { mime_type: 'audio/mpeg', data: base64Audio } },
            ],
          }],
        }),
      });

      if (!res.ok) {
        const text = await res.text().catch(() => '');
        const retryable = res.status === 429 || (res.status >= 500 && res.status < 600);
        if (!retryable) throw new Error(`Gemini ${res.status}: ${text}`);
        lastError = new Error(`Gemini ${res.status}: ${text}`);
      } else {
        const data = await res.json();
        const rawText = data?.candidates?.[0]?.content?.parts?.[0]?.text;
        if (!rawText) throw new Error(`Gemini returned empty response for clip ${clipIndex}`);
        return parseSegments(rawText, clipOffset);
      }
    } catch (err) {
      if (err.message?.startsWith('Gemini ') && !err.message.match(/Gemini (429|5\d\d)/)) throw err;
      lastError = err;
    } finally {
      clearTimeout(timer);
    }
    if (attempt < GEMINI_MAX_RETRIES - 1) {
      const delay = Math.pow(4, attempt) * 1000;
      console.log(`[clip ${clipIndex}] retry ${attempt + 1}/${GEMINI_MAX_RETRIES} after ${delay}ms`);
      await sleep(delay);
    }
  }
  throw lastError || new Error(`Max retries exceeded for clip ${clipIndex}`);
}

function parseSegments(rawText, clipOffset) {
  const tsRe = /\[(\d+):(\d+)\]\s*\n?([\s\S]*?)(?=\n?\[\d+:\d+\]|$)/g;
  const segments = [];
  let match;
  while ((match = tsRe.exec(rawText)) !== null) {
    const text = match[3].trim();
    if (!text) continue;
    const start = Math.round(clipOffset + parseInt(match[1], 10) * 60 + parseInt(match[2], 10));
    segments.push({ start, end: start, text });
  }
  if (segments.length === 0) {
    const cleaned = rawText.replace(/\[\d+:\d+\]\s*/g, '').trim();
    if (cleaned) segments.push({ start: clipOffset, end: clipOffset, text: cleaned });
  }
  for (let i = 0; i < segments.length - 1; i++) segments[i].end = segments[i + 1].start;
  if (segments.length > 0) {
    const last = segments[segments.length - 1];
    last.end = last.start + 30;
  }
  return segments;
}

async function extractTopics(transcript) {
  const trimmed = transcript.slice(0, 30000).trim();
  if (!trimmed) return [];
  let lastError = null;

  for (let attempt = 0; attempt < GEMINI_MAX_RETRIES; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TOPIC_TIMEOUT_MS);
    try {
      const res = await fetch(geminiUrl(MODEL, 'generateContent'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Goog-Api-Key': GEMINI_API_KEY },
        signal: controller.signal,
        body: JSON.stringify({
          contents: [{
            parts: [{
              text: 'Analyze this transcript and identify 3-7 key topics discussed. For each topic provide: '
                + 'topic (short descriptive name, 2-5 words), startSeconds (approximate second when this topic begins), '
                + 'keywords (2-4 related search terms). Return ONLY valid JSON array, no other text:\n'
                + '[\n  {"topic": "Community", "startSeconds": 15, "keywords": ["fellowship", "church"]}\n]\n\n'
                + `Transcript:\n${trimmed}`,
            }],
          }],
        }),
      });

      if (!res.ok) {
        const text = await res.text().catch(() => '');
        const retryable = res.status === 429 || (res.status >= 500 && res.status < 600);
        if (!retryable) throw new Error(`Topic extraction ${res.status}: ${text}`);
        lastError = new Error(`Topic extraction ${res.status}: ${text}`);
      } else {
        const data = await res.json();
        const raw = data?.candidates?.[0]?.content?.parts?.[0]?.text;
        if (!raw) return [];
        const json = raw.replace(/```json\s*|\s*```/g, '').trim();
        let parsed;
        try { parsed = JSON.parse(json); } catch {
          console.warn('Gemini returned malformed JSON for topic extraction');
          return [];
        }
        if (!Array.isArray(parsed)) return [];
        return parsed
          .map((t) => ({
            topic: String(t.topic || '').trim(),
            startSeconds: Number(t.startSeconds) || 0,
            keywords: Array.isArray(t.keywords) ? t.keywords.map(String) : [],
          }))
          .filter((t) => t.topic);
      }
    } catch (err) {
      if (err.message?.startsWith('Topic extraction ') && !err.message.match(/(429|5\d\d)/)) throw err;
      lastError = err;
    } finally {
      clearTimeout(timer);
    }
    if (attempt < GEMINI_MAX_RETRIES - 1) await sleep(Math.pow(4, attempt) * 1000);
  }
  throw lastError || new Error('Topic extraction failed after retries');
}

async function generateEmbedding(text) {
  let input = text.slice(0, 8000).trim();
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
        // Unit-normalise so cosine distance against the HNSW index is consistent.
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
      if (!retryable) throw new Error(`Gemini embedding ${res.status}: ${text}`);
      lastError = new Error(`Gemini embedding ${res.status}: ${text}`);
    } catch (err) {
      if (err.message?.startsWith('Gemini embedding ') && !err.message.match(/(429|5\d\d)/)) throw err;
      lastError = err;
    }
    if (attempt < GEMINI_MAX_RETRIES - 1) await sleep(Math.pow(4, attempt) * 1000);
  }
  throw lastError || new Error('Embedding failed after retries');
}

/* ---- per-video orchestration ---- */

async function processVideo(claim) {
  const { videoId, leaseId, title, description } = claim;

  const reportProgress = (fields) =>
    api('progress', { videoId, leaseId, ...fields }).then(assertLease);

  const tmpDir = mkdtempSync(join(tmpdir(), `ct-${videoId}-`));
  const fullAudioPath = join(tmpDir, `${videoId}.mp3`);
  try {
    // Phase 1: transcript -----------------------------------------------------
    await reportProgress({ detail: 'Downloading audio from YouTube…' });
    console.log(`[${videoId}] downloading audio…`);
    await downloadAudio(videoId, fullAudioPath);

    const totalDuration = await getAudioDuration(fullAudioPath);
    const numClips = Math.max(1, Math.ceil(totalDuration / CLIP_SECONDS));
    console.log(`[${videoId}] duration ${Math.round(totalDuration)}s -> ${numClips} clip(s)`);

    const allSegments = [];
    const textParts = [];
    for (let i = 0; i < numClips; i++) {
      const clipStart = i * CLIP_SECONDS;
      const clipDuration = Math.min(CLIP_SECONDS, totalDuration - clipStart);
      const clipPath = join(tmpDir, `clip-${i}.mp3`);

      await reportProgress({ detail: `Transcribing clip ${i + 1}/${numClips}…` });
      await extractClip(fullAudioPath, clipPath, clipStart, clipDuration);
      try {
        const segs = await transcribeClip(clipPath, i, clipStart);
        for (const s of segs) { allSegments.push(s); textParts.push(s.text); }
      } finally {
        await fs.unlink(clipPath).catch(() => {});
      }
      await reportProgress({ content: textParts.join('\n').trim(), progressSeconds: clipStart + clipDuration });
    }

    allSegments.sort((a, b) => a.start - b.start);
    const transcript = textParts.join('\n').trim();
    assertLease(await api('transcript', { videoId, leaseId, text: transcript, segments: allSegments }));
    console.log(`[${videoId}] transcript posted (${transcript.length} chars, ${allSegments.length} segments)`);

    // Phase 2: topics (+ per-topic embeddings) --------------------------------
    await reportProgress({ detail: 'Extracting topics…' });
    const rawTopics = await extractTopics(transcript);
    const topics = [];
    for (const t of rawTopics) {
      const embedding = await generateEmbedding(`${t.topic} ${t.keywords.join(' ')}`.trim());
      topics.push({ ...t, embedding });
    }
    assertLease(await api('topics', { videoId, leaseId, topics }));
    console.log(`[${videoId}] ${topics.length} topic(s) posted`);

    // Phase 3: video embedding (completes the video) --------------------------
    await reportProgress({ detail: 'Generating embedding…' });
    const embedInput = [title, description || '', transcript].filter(Boolean).join(' ').trim();
    const embedding = await generateEmbedding(embedInput);
    assertLease(await api('embedding', { videoId, leaseId, embedding }));
    console.log(`[${videoId}] done.`);
  } catch (err) {
    if (err instanceof LeaseLostError) {
      console.warn(`[${videoId}] ${err.message} — abandoning`);
      return;
    }
    console.error(`[${videoId}] failed: ${err.message}`);
    await api('fail', { videoId, leaseId, error: err.message }).catch((e) =>
      console.error(`[${videoId}] failed to report failure: ${e.message}`),
    );
  } finally {
    await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
}

/* ---- main loop ---- */

async function main() {
  const deadline = Date.now() + TIME_BUDGET_MS;
  let processed = 0;

  while (Date.now() < deadline && processed < MAX_VIDEOS) {
    let claim;
    try {
      claim = await api('claim');
    } catch (err) {
      console.error(`claim failed: ${err.message}`);
      break;
    }
    if (!claim?.videoId) {
      console.log('Queue empty — nothing to transcribe.');
      break;
    }
    await processVideo(claim);
    processed++;
  }

  console.log(`Run complete. Processed ${processed} video(s).`);
}

main().catch((err) => {
  console.error(`Fatal: ${err.message}`);
  process.exit(1);
});
