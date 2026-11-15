# FINDINGS — Smallest.ai Waves trial (Lightning v3.1 Pro TTS deep, Pulse side checks)

Window: 15 Nov 2026, 05:38–05:56 IST (Asia/Calcutta). Vantage: Mumbai (Hostinger VPS, geo-IP Mumbai). Tools: Python 3.14 + httpx 0.28 + websockets 17.4, curl 8.5. Harness `out/harness/`, raw logs `out/runs/*.jsonl` (redacted, key = `sk_...`). Every run printed its EXPECTED line before sending (first record of each jsonl). `model: lightning_v3.1_pro` explicit on every TTS request except P1 case 1 (deliberately `lightning_v3.1`) and case 3 (deliberately omitted). Max 2 requests in flight, max 3 sockets, enforced by a cross-process lock. 18,406 characters sent in total (`runs/ledger.csv`), about $0.36 at the public Pro list price ($0.195 per 10k characters); the dashboard balance is not readable by us.

Verdict key: **FINDING** (repeated, n≥3, contradicts docs), **CALIBRATION** (docs were right, my expectation was wrong or merely confirmed), **UNRESOLVED** (single run, blocked or unobservable).

---

## F1. A second concurrent TTS request is admitted on all three transports; the documented 1-slot cap and 429 never appear — FINDING (proposed Major)
- **Docs:** `api-reference__concurrency-and-limits.md:21-22` says "Only 1 Text-to-Speech request can be actively processed at a time per account … every Lightning v3.1 TTS endpoint (sync, SSE, WebSocket)". `:30` says a second HTTP request gets `429`, and `:36` says a second WS request "will be rejected with an error". The same docs set also has `lightning-v-3-1-pro.md:34`: "Concurrency (Standard Plan) 10". The pages contradict each other, and neither matches what we saw.
- **EXPECTED (written before the run):** B → 429 within 1 RTT; A completes intact.
- **Observed: 10/10 overlapping second requests were admitted (200 or complete); zero 429s, zero error frames.**
  - Sync B during an SSE A, 3/3. B was sent 21–29 ms after A's first audio and its headers arrived about 4.1 s before A ended (`p3_20261115T053956678IST.jsonl`, `run*_summary.overlap`). B IDs: `a7c04be1-7a9a-414c-bce6-92c24bad9bb8`, `471c7979-cfba-4fcc-af51-c7395685763b`, `7e1431a3-c7e1-4b23-b97b-d05ffb39fa98`. A IDs: `cde34d34-…`, `23ceccd5-…`, `929c58a3-…`.
  - WS A and WS B on two sockets, 1/1. B opened 26 ms after A and both streamed to `complete`. A=`2bcc95d7-b480-4eaa-8985-95907e975aa8`, B=`d99bab72-a699-496f-b9fe-da62034b4a06`, 05:40:24 IST.
  - WS second socket during A's tail (P4 `--tail-probe`), 5/5 admitted. Probe IDs: `214a3f40-…`, `1efebcb4-…`, `53c124f2-…`, `e6cec3fd-…`, `64c00f09-…`, 05:41:14–05:42:40 IST.
  - Sync B during an SSE A (P7 billing slice), 1/1. `f00ce97f-acfe-4a4b-ad17-3df795139f20`, 05:50:51 IST.
  - A always completed intact. A's total time when overlapped was 4.39–4.45 s, against 4.83 s in P7. No measurable degradation.
- **Not tested:** 3 or more in flight, which is forbidden by the rules. The real cap is therefore unknown to us. Rhea said "cap ~5" (`NOTES-docs-issues.md:65`).
- **Impact against the SLO (one turn every 2 s, 99.9% success, p95 first-audio ≤500 ms from Mumbai):** a customer who sizes from the limits page serializes TTS behind a 1-slot queue. Two agents speaking at once then wait for the whole of A (about 4.4 s for 600 chars), which breaks the 500 ms first-audio target even though the server would have served both. The 429/backoff path the error reference tells you to build (`error-reference.md:44`) can't be exercised at the documented limit, so it ships untested.

## F2. The WebSocket `complete` frame does not close the socket; the documented "terminal frame" is wrong and reuse works — FINDING (Minor)
- **Docs:** `api-reference__models__text-to-speech__tts.md:603` says "`complete` — terminal frame; the server closes the WebSocket after this" (the exception is `context_id` only, at `:604-607`). We set no `context_id`. `models__text-to-speech__streaming.md:301` says "Reuse same connection", so the docs contradict each other.
- **EXPECTED:** the server closes after `complete`, and B sent 1 s later fails.
- **Observed, 10/10:** the socket stayed open after `complete`. B sent 1.0 s later was synthesized and got its own `complete` with a new `request_id` on the same `session_id`. The socket was then closed by us (`close.by=client`) with code 1000. Example: run 1 had A `complete` at 4483 ms (`0b61b91c-33d4-4321-ab10-24f4b1280375`) and B `complete` at 10015 ms (`b05e719c-8e4f-4ca6-b736-4044530152ed`), same session `42d41d0f-ea14-4767-aa20-690452cfea23` (`p4_20261115T054112938IST.jsonl`, 05:41:12–05:43:20 IST).
- **Impact:** a client that follows `:603` reconnects on every turn, which here means 21–49 ms of handshake per turn plus a slot re-acquire. A client that treats `complete` as end-of-stream and waits for the server close hangs until the idle timeout (60 s, see F5).

## F3. Error bodies come in four shapes; none matches the documented `{error, message}` — FINDING (Minor)
- **Docs:** `api-reference__models__text-to-speech__synthesize-speech.md:161-180` documents 400, 401 and 500 as `error` (string) plus `message` (string).
- **Observed:**
  - Validation 400, 9/9 sync: `{"error":[{"code":"too_big","maximum":8000,…,"message":"Text exceeds the maximum length of 8000 characters","path":["text"]}]}`. `error` is an array and there is no top-level `message` (`af113645-9c67-4b4b-86e6-f3155399684c`, `f4019c77-…`, `52cd7fa1-…`, P1 `0dd8d135-…`).
  - Content filter 400: `{"status":"error","error_code":"CONTENT_FILTER_BLOCKED","message":…,"language":"en","match_count":1}` (`019760d0-96f4-4f16-ad5d-d5eba479412d`).
  - 401: `{"error":"unauthorized"}`, no `message` (`bb3e2a0a-721f-4635-a772-e0abb0f43d50`, sent once with a fake key).
  - WS error frame: `{"request_id":…,"status":"error","message":"Invalid input data","errors":[…]}` (`714d2afc-bc14-4479-b842-2f0d11864f8c`).
- **Impact:** a client reading the documented `.message` gets `undefined` for every validation 400, which is the most common error a customer hits. Logs then say "TTS failed: undefined", and the failure can't be told apart from a 500 on dashboards.

## F4. `request_id` and `session_id` validation is documented but not enforced; invalid IDs are echoed back — FINDING (Minor)
- **Docs:** `synthesize-speech.md:148-149` (sync) and `tts.md:1262-1279` (WS) say "Only alphanumeric characters, hyphens, underscores, and dots … Max 128 characters" (`pattern ^[a-zA-Z0-9_\-.]+$`, `maxLength: 128`).
- **EXPECTED:** a 129-char ID → 400; an ID with spaces → 400.
- **Observed, 3/3 each, all 200:**
  - The 129-char ID was echoed in full in `X-External-Request-Id` (`1da57b24-5804-4e3f-a9c8-4384763b5acc`, `a600975e-…`, `ea92c015-…`).
  - `"p6 bad id"` was accepted and echoed with spaces (`b751674b-8094-4c8f-a216-2ccbc2a6f4c5`, `972fe9d4-…`, `8521e763-…`). Window 05:49:54–05:54:44 IST.
- **Impact:** low. A customer that relies on the documented contract to sanitize IDs (for example, as a log key) gets the raw value back. The good news is that echo works on sync, SSE and WS (see Done well).

## F5. Idle timeout closes with WebSocket code 1006 (abnormal, no close frame) after the error JSON — FINDING (Minor)
- **Docs:** `tts.md:83-102`: 60 s default, `?timeout=5` honoured as given, larger values clamped to 180, and the documented error JSON. No close code is documented.
- **Observed, 4/4 server-closed sockets:**
  - The error JSON arrived exactly on time (5001.6 / 60002.2 / 180002.4 / 5001.7 ms; `?timeout=999` → 180 s clamp ✓).
  - The TCP connection then dropped without a close frame. The client saw **1006** (`p5_20261115T054414788IST.jsonl`, 05:44:14–05:49:47 IST).
- **Impact:** most WS libraries surface 1006 as a network failure. An agent can't tell "idle, reconnect quietly" from "network died, page someone" without parsing the text frame before it. The timer values themselves are exact (Done well).

## F6. The Pro model card contradicts the API reference on voice/model mismatch and on the default model; the API reference is right — FINDING (Minor, docs)
- **Docs:**
  - `model-cards__text-to-speech__lightning-v-3-1-pro.md:496` says "The API does not currently reject mismatched pairings … can produce wrong or hallucinated audio".
  - `:537` says "it defaults to standard Lightning v3.1".
  - `synthesize-speech.md:106` says a mismatch → 400, and `:131` says "If you omit `model`, it's picked from the voice: Pro catalog voices use `lightning_v3.1_pro`".
- **Observed, 3/3 each:**
  - Pro voice `rhea` + `lightning_v3.1` → 400 `Invalid Voice ID` (`0dd8d135-…`, `4bddf066-…`, `bf418bdb-…`).
  - Standard voice `magnus` + Pro → 400 `Voice 'magnus' is not available on the lightning_v3.1_pro model` (`0a11a899-…`, `0af78dee-…`, `2ced6edf-…`).
  - `rhea` with no model → 200, 24 kHz mono WAV (`b10decd2-…`, `eb9ef6f6-…`, `66de20fe-…`). Window 05:38:43–05:39:10 IST.
  - If the omitted model defaulted to v3.1, case 3 would have failed like case 1. It succeeded, so `:537` is wrong and `:131` is right.
- **Impact:** a customer who follows the card adds client-side pairing validation and pins `model` defensively. That's harmless, but the card scares people off about hallucinated audio when the server already protects them. Case 1's message ("Invalid Voice ID") is also less helpful than case 2's.

---

## Calibration: docs were right, I expected wrong (or merely confirmed)
- **C1, P2 limits (n=1 each, all matched docs):**
  - 8001 non-space chars → 400 `too_big` max 8000 (`synthesize-speech.md:129`).
  - Whitespace-only → 400 `Text cannot be empty`.
  - 22050 Hz → 400 sync and WS error frame (`:134`, `tts.md:1256`).
  - 251 and 1000 chars → 200. The card's "max chunk 250" (`lightning-v-3-1-pro.md:39`) is a throughput hint, not a limit.
  - `word_timestamps` on sync → 200, no timing (`:147`).
  - Docs right.
- **C2, `complete_backoff_ms`:**
  - Default last-chunk → `complete` = 4019.6–4025.9 ms (n=5); with `0` it was 21–31 ms (n=5). That is exactly `tts.md:1032-1040`.
  - A side note on the SLO: a client that waits for `complete` before the next turn adds 4 s of dead air at default settings. It's documented, so not a defect, but it's worth a callout in the voice-agent guide.
- **C3, keep-alive:**
  - JSON `{"type":"ping"}` every 3 s kept a `?timeout=5` socket alive for 150 s, with 49 pongs (`tts.md:109-117`).
  - Protocol-level pings did not reset the timer (closed at 5.0 s), which matches `keep-alive.md:37`.
  - I had marked this UNDOCUMENTED for TTS. `tts.md:104` "every message you send" turns out to mean application messages.
- **C4, region (P9, Mumbai leg):**
  - `api.us` + `multi-indic` → 400 `LANGUAGE_NOT_ENABLED_IN_REGION`, `region: us-west-2` (`ebeed2b1-7f31-4a87-8ddd-9ac319a08a06`), exactly `transcribe.md:164-169`.
  - `api.india` → 200 (`84705ff6-…`); the generic host from Mumbai → 200 (`143c60cb-…`).
- **C5, TTFB, descriptive (n=20 per host, sequential SSE, 20 chars):**

  | Host | First audio p50 | First audio p95 |
  |---|---|---|
  | generic | 162 ms | 177 ms |
  | api.india | 160 ms | 202 ms |

  DNS, TCP and TLS together come to under 12 ms. Well inside the 500 ms SLO (`pttfb_20261115T055141163IST.jsonl`).

## UNRESOLVED
- **U1, P7 billing on failure:**
  - We sent a 1000-char success, a filter reject, a 400, an overlapped pair, an SSE abort after the first chunk and a WS close after the first chunk. That's 4,674 chars, 05:50:36–05:51:05 IST.
  - There is no usage API in `pages/`; only the dashboard link is at `concurrency-and-limits.md:48`.
  - Billing on failure can't be observed without Sanket's dashboard reads, so I'm not claiming anything.
- **U2, P8 Pulse webhook:**
  - Not run. Exposing a public HTTPS route meant a Caddy config change on the host, which needs the owner's approval and didn't get it overnight.
  - Harness, receiver and an 8 s own-audio clip are ready (`out/fixtures/clip.wav`).
- **U3, P9 US leg and the india/us variant of P3:**
  - Not run. There is no US box: the Linode token isn't present on this host.
  - The generic-host-from-US question stays open.
- **U4, real concurrency ceiling:** not measurable within the "≤2 in flight" rule.
