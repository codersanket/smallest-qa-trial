# QA report — Smallest.ai Waves APIs (Lightning v3.1 Pro TTS, Pulse side checks)

## Scope

**Area: APIs.** Lightning v3.1 Pro TTS over sync HTTP, SSE and WebSocket, tested against the API reference, the model card and the limits page. I went deep on one model because a voice agent depends on exactly these contracts: concurrency, error shape, streaming lifecycle, correlation.

The docs contradict each other on concurrency, voice/model pairing, the default model and whether `complete` closes the socket. Where they conflicted, I tested which side the API follows.

**In:** error semantics, limits, streaming lifecycle, idle timers, correlation IDs, billing of rejected requests, Pulse region gating. **Out:** voice quality, Atoms, cloning, phone numbers, load/security testing, webhook delivery (receiver built, not run), US-vantage routing.

**Impact baseline:** a voice agent taking one TTS turn every 2 s, 99.9% turn success, p95 first audio ≤ 500 ms from Mumbai. Test targets, not production SLOs.

## Method

- Harness: Hostinger VPS, Mumbai, 05:38–05:56 IST (Python 3.14, httpx 0.28, websockets 17.4). Hand checks of the two key probes: my laptop on home broadband, Mumbai, 13:07–13:11 IST (curl 8.x, Python 3.12). All times Asia/Calcutta, 15 Nov 2026.
- `model: lightning_v3.1_pro` explicit on every request except the deliberate mismatch/default cases. 20-character texts unless the probe needed otherwise.
- Expected result written before each run (first record of every log). At most two requests in flight and three sockets open, enforced by a lock. 18,406 characters submitted by the harness. Dashboard read at sign-up ($10.00, fresh account) and after the runs ($9.80).
- Every run logged status, headers, `x-request-id`, external IDs, bytes, timings, WebSocket frames and close codes. Logs and scripts linked below; key redacted as `sk_...`.

## Findings

### 1. The published one-request TTS concurrency limit is not what the API enforces
**Area / severity:** APIs / Major (incorrect published concurrency contract).

**Expected:** `api-reference/concurrency-and-limits`: "Only 1 Text-to-Speech request can be actively processed at a time per account" across sync, SSE and WebSocket; a second HTTP request gets `429`. The Pro model card says standard-plan concurrency is 10. I expected the limits page to be right: B → 429 within one RTT.

**Observed:** all 13 deliberate second requests were accepted; none returned 429 or a WebSocket error. Harness: 4 sync during an SSE stream, 1 WebSocket during another, 5 WebSocket during another request's post-audio tail (05:39:56–05:50:52). Hand re-run: 3/3 sync B returned 200 in 0.57–0.74 s while a 600-character SSE A was still streaming (A total 5.7–6.1 s); A completed every time; no `Retry-After` anywhere. I tested no more than two in flight, so I cannot state the real ceiling or confirm the card's 10.

**Evidence:** `p3_20261115T053956678IST.jsonl` summaries: B sent 21–29 ms after A's first audio, 4.13–4.27 s before A ended; B's headers arrived ~3.6 s before A ended. Hand B IDs `b7f4367f-b264-46a3-90da-711a515da357`, `ccc48534-…`, `c280bd29-…`; A `57bf13f8-…`, `29e49dde-…`, `312b1bc3-…`; 13:10:28–13:10:53 IST.

**Customer impact:** a customer who trusts the one-slot page serialises TTS behind a queue and builds the 429/backoff path the error reference tells them to build. Here B would have waited 4–6 s behind A despite being accepted concurrently, missing the 500 ms first-audio target for nothing, and the backoff path ships untested because it cannot be triggered at the stated limit. Major because capacity planning and retry logic are built on this page; not a runtime failure.

### 2. WebSocket `complete` does not close the socket; connection reuse works
**Area / severity:** APIs / Minor.

**Expected:** `tts.md`: "`complete` — terminal frame; the server closes the WebSocket after this" (exception only with `context_id`). `streaming.md` says "reuse same connection". I expected the reference to win: B sent 1 s after `complete` fails.

**Observed:** 14/14 sockets stayed open after `complete`; B sent 1 s later was synthesised with a new `request_id` on the same `session_id`. Harness 10/10 (5 default backoff, 5 at `complete_backoff_ms: 0`); hand 4/4. The socket closed only when I closed it (1000).

**Evidence:** `p4_20261115T054112938IST.jsonl` session `42d41d0f-ea14-4767-aa20-690452cfea23`, A complete 4483 ms, B complete 10015 ms. Hand: A `311a5173-812b-436d-bdef-b50d2fc698e7` complete 4441 ms, B `1fc3b036-…` 9859 ms, 13:10:55 IST.

**Customer impact:** a client that follows `tts.md` reconnects every turn, paying the measured 21–49 ms handshake plus TLS for nothing; one that explicitly waits for the promised server close can wait until the 60 s idle timeout. Reuse works and `streaming.md` says so; the reference needs to match.

### 3. Validation errors do not match the documented error body
**Area / severity:** APIs / Minor.

**Expected:** `synthesize-speech` documents 400/401/500 as `{ "error": string, "message": string }`.

**Observed:** all 9 validation 400s returned `error` as an **array** of issue objects with the message nested inside and no top-level `message`. Four shapes in these tests: validation `{"error":[{code,maximum,message,path}]}`; content filter `{"status":"error","error_code":"CONTENT_FILTER_BLOCKED","message",…}`; 401 `{"error":"unauthorized"}`; WS `{"status":"error","message","errors":[…]}`.

**Evidence:** `p2_20261115T053916358IST.jsonl:5`, `af113645-9c67-4b4b-86e6-f3155399684c`, 05:39:33: 8001 chars → `{"error":[{"code":"too_big","maximum":8000,…,"message":"Text exceeds the maximum length of 8000 characters","path":["text"]}]}`. Filter `019760d0-…`; 401 `bb3e2a0a-…`.

**Customer impact:** a parser written to the documented `.message` gets nothing for all nine validation 400s and logs "TTS failed: undefined"; it has to branch on shape. I did not test a 500, so cannot compare its body.

### 4. Idle timeout drops the connection without a close frame (client sees 1006)
**Area / severity:** APIs / Minor.

**Expected:** `tts.md`: 60 s default, `?timeout=5` honoured, values above 180 clamped, the timeout JSON precedes the close. No close code is promised.

**Observed:** 4/4 timers exact — JSON at 5001.6, 60002.2, 180002.4 (clamped from 999) and 5001.7 ms. Then the TCP connection ended with no WebSocket close frame; the client reported 1006. Server vs intermediary is not visible from the client.

**Evidence:** `p5_20261115T054414788IST.jsonl:2–5`, 05:44:19–05:47:22 IST.

**Customer impact:** libraries report 1006 as a network failure, so an agent must keep the preceding text frame to tell "idle, reconnect quietly" from "network died, alert". A 1000/1001 close with a reason fixes this.

### 5. Sync `request_id` validation is documented but not enforced
**Area / severity:** APIs / Minor.

**Expected:** `synthesize-speech`: alphanumerics, hyphens, underscores, dots; max 128 characters.

**Observed:** a 129-character ID and `"p6 bad id"` (spaces) both returned 200 and were echoed unchanged in `X-External-Request-Id`, 3/3 each. Sync only; WebSocket field not tested.

**Evidence:** `1da57b24-5804-4e3f-a9c8-4384763b5acc` (length), `b751674b-8094-4c8f-a216-2ccbc2a6f4c5` (spaces), 05:49:54–05:54:44 IST.

**Customer impact:** low. Correlation works; the contract isn't the one written down.

### 6. The Pro model card is wrong about voice/model mismatch and the default model
**Area / severity:** APIs / Minor (docs).

**Expected:** model card: "The API does not currently reject mismatched pairings … can produce wrong or hallucinated audio"; omitted `model` "defaults to standard Lightning v3.1". Reference: mismatch → 400; omitted model is picked from the voice.

**Observed, 3/3 each:** `rhea` (Pro) + `lightning_v3.1` → 400 `Invalid Voice ID`; `magnus` (standard) + Pro → 400 `Voice 'magnus' is not available on the lightning_v3.1_pro model`; `rhea` with no `model` → 200. Contradicts the card, supports the reference's routing description; does not directly prove which model served case 3.

**Evidence:** `0dd8d135-7a06-4919-8b66-2120650c1b27`, `0a11a899-…`, `b10decd2-…`, 05:38:43–05:39:10 IST.

**Customer impact:** customers add redundant pairing checks or pin `model` because the card says mismatches can succeed. Documentation risk only. Case 1's message is also less helpful than case 2's.

## What is done well

- **Rejected requests appear not to be billed.** Fresh account $10.00 → $9.80 after 18,406 submitted characters, of which ~8,100 were deliberately rejected (one 8001-char `too_big`, mismatches, whitespace, filter); the ~10,300 successful × $0.195/10k = $0.20. Had the 8001-char reject been billed, the balance would read ~$9.64. Two-decimal precision cannot resolve the two aborted streams.
- **Timers exact, keep-alive as documented:** `?timeout=5/60/180` closed within 3 ms; `{"type":"ping"}` every 3 s held a 5 s socket for 150 s (49 pongs); protocol pings did not reset the timer, as `keep-alive.md` says.
- **`complete_backoff_ms` does what it says:** 4018–4026 ms tail by default, 26–31 ms at 0 (n=14).
- **Correlation:** `x-request-id` on 40/40 HTTP responses including all 9 validation 400s and the 401; client IDs echoed on sync, SSE and WebSocket.
- **Input validation:** 8001 chars, whitespace-only and 22050 Hz → 400; 251 and 1000 chars → 200, so the card's "max chunk size 250" is not an API limit and should say what it is.
- **Region gating is explicit:** `api.us` + `multi-indic` → 400 `LANGUAGE_NOT_ENABLED_IN_REGION`, `region: us-west-2` (`ebeed2b1-7f31-4a87-8ddd-9ac319a08a06`).
- **Latency from Mumbai:** first audio p50/p95 162/177 ms (generic), 160/202 ms (`api.india`), n=20 each.

Five of my pre-written expectations were wrong and the docs were right (length and sample-rate limits, backoff, keep-alive, region gating).

## Top 3 fixes

1. **Publish the real concurrency contract per model and plan, and make rejection observable.** First because capacity planning, queueing and retry code are built on this page, and today it describes a limit the API doesn't apply. Check: the documented second-request behaviour reproduces on a fresh standard-plan key.
2. **One error envelope across sync, SSE, WS and the filter** (`{error_code, message, details[]}`), documented once. Cheap, and it fixes the most common path a customer's error handler sees. Check: a schema test over every 4xx/5xx body.
3. **Make the WS lifecycle match reality:** document `complete` as non-terminal and reuse as supported; close idle sockets with a proper close frame (1000/1001 + reason) after the timeout JSON. Removes a 60 s wait for reference-following clients and a false "network failure" on every idle timeout.

## Time spent

**6 hours**: docs and planning, harness work, scripted probes, hand re-runs, writing.

Links: https://github.com/codersanket/smallest-qa-trial/tree/main/runs (raw logs, redacted) · https://github.com/codersanket/smallest-qa-trial/tree/main/harness (scripts) · https://github.com/codersanket/smallest-qa-trial/blob/main/FINDINGS.md (full evidence, calibrations, unresolved items).

## Private: security concern

(none)
