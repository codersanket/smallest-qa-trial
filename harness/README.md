# Smallest.ai probe harness

Python 3 scripts for PLAN.md probes 1-9 and the TTFB breakdown. Use the interpreter
`/root/smallest-review/out/.venv/bin/python`. It has websockets 17.4, httpx 0.28.1 and
requests 2.34.2. websockets >= 12 is required for `websockets.asyncio.client`.

```
PY=/root/smallest-review/out/.venv/bin/python
cd /root/smallest-review/out/harness
$PY p1_mismatch.py --dry-run         # every script accepts --dry-run
```

## Safety rails (all in `common.py`)

| Rail | Mechanism |
|---|---|
| API key | Read from `/root/smallest-review/.env` (`SMALLEST_API_KEY=`) only on the first real request. It is never printed. Every log record and console line goes through `redact()` (`sk_[A-Za-z0-9_-]+` → `sk_...`, Authorization → `Bearer sk_...`, plus the literal key and any registered sensitive word) |
| ≤2 HTTP in flight, ≤3 WebSockets open, across all scripts | `fcntl` lock files `out/.locks/http.{0,1}.lock` and `ws.{0,1,2}.lock`. A slot is taken before connect and released after close. The kernel frees it if a process dies. No slot within 120 s → the script aborts (exit 3) |
| Deliberate 429s ≤10 | `out/.locks/429count`. Each intentional over-limit attempt runs in an `async with reserve_429(case)` window: the counter is incremented on entry (counted even if never sent), windows of all scripts are serialized by `out/.locks/deliberate.lock`, and the ≥5 s gap is enforced against the **actual dispatch** time (`dispatch()` right before the bytes go out, persisted as `last_dispatch_epoch`). A corrupt/out-of-range counter file aborts instead of resetting. The script refuses once the count reaches 10 |
| Fake-key 401 ≤1, ever | `out/.locks/401count`. P6 skips the case if it has already been used; a corrupt file aborts |
| Character budget | `out/runs/ledger.csv` (ist, probe, case, chars, status, reservation). Before every send (HTTP and each WS frame) the budget check and a `reserved` row are written in one critical section under `ledger.lock`, fsynced, so concurrent scripts see it and a crash still leaves it counted. Reserved chars are never refunded; the outcome is appended as a 0-char row. Aborts before any send that would take the ledger past 60 000 chars |
| `--dry-run` | Loads no key, takes no locks, writes no ledger or log files. `socket.connect`/`create_connection`/`getaddrinfo` are patched to raise, and WebSockets use an offline stub |

Logs go to `out/runs/<probe>_<IST start stamp>.jsonl`. The first record is always the
EXPECTED line, which is printed before any network I/O. Each request record holds:

- IST time (ms), case, method, redacted URL
- the body (text cut to 80 chars, plus `text_len`)
- status and all response headers
- `x_request_id` and external IDs
- byte count and audio sha256
- the WAV header, when there is one
- timings in ms (dns via a separate `getaddrinfo`; connect, tls and ttfb via httpx trace hooks, `perf_counter`; first body byte, first audio, total)
- the resolved IP

WebSocket records hold every frame with `t_ms`, the close code, reason and who closed, the last text frame, and any `external_*` IDs echoed back.

P1 and P2 save their sync TTS audio bodies to `out/audio/`; P8 and P9 reuse them.

## Running order and what each run costs against the limits

Run the probes one at a time. The limiter enforces the socket and request caps anyway.

| Script | What it does | Chars (approx.) | Deliberate 429s | Max concurrency |
|---|---|---|---|---|
| `p1_mismatch.py` | 3 sequential sync calls: Pro voice + v3.1, standard voice + Pro, Pro voice with no `model` | 60 | 0 | 1 HTTP |
| `p2_params.py [--with-8000]` | Sync calls with 251 / 1000 / whitespace-only / 8001 non-space / sr 22050 / word_timestamps, plus a WS call at 22050. `--with-8000` adds 8000 chars + 5 trailing spaces | ~9.3k (+8k) | 0 | 1 HTTP / 1 WS |
| `p3_admission.py [--runs 3] [--ws-runs 1] [--stt-addon] [--hostA/--hostB generic\|india\|us]` | SSE A (600 chars). On A's first chunk, a 20-char sync B. B is retried at A_end+0.2 s and A_end+1 s. WS-A/WS-B on 2 sockets. Optional STT POST during A | ~2.6k at the defaults | **runs + ws-runs (+1 with `--stt-addon`)**; default 4–5 | 2 HTTP / 2 WS |
| `p4_complete_tail.py [--n 5] [--tail-probe]` | One WS per run: A, then B 1 s after `complete`. n runs with the default backoff and n with `complete_backoff_ms: 0`. `--tail-probe` opens a second socket inside the tail | ~400 (+100) | **`--tail-probe`: n** | 2 WS |
| `p5_idle.py [--json-ping-hold 150]` | Idle sockets. Wave 1: default, `?timeout=5`, `?timeout=999`. Wave 2: protocol pings vs `{"type":"ping"}` | 0 | 0 | 3 WS (wave 1) |
| `p6_correlation.py [--id-transport body\|header\|both] [--matrix-only]` | 401 with a fake key (once ever), a 129-char id, an id containing whitespace, sync and SSE with both ids, WS with both ids. Then writes `runs/p6_matrix.md` from the P1/P2/P3/P6 logs | 120 | 0 (**uses the single 401**) | 1 HTTP / 1 WS |
| `p7_billing.py` | 1000 chars OK; filter rejection; bad voice → 400; 429 (SSE A 600 chars + sync B on A's first chunk); SSE of 1500 chars aborted after the first chunk; WS of 1500 chars aborted after the first chunk. **Sanket reads the dashboard balance before and after** | ~4.7k | **1** | 2 HTTP / 1 WS |
| `p8_receiver.py` | Webhook receiver on 127.0.0.1:8765 (Caddy proxies to it). Logs to `runs/p8_hooks.jsonl`. Returns 500 once for token `r2-500`, 204 for everything else. `--reset-500` re-arms the 500 | 0 | – | – |
| `p8_webhook.py --hook-host HOST [--runs 1,2,3] [--prepare-clip-only] [--clip F] [--clip-seconds 8]` | Trims the newest ≥5 s P2 WAV to 5–10 s (default 8), validates it and writes `fixtures/clip.wav`. Then STT POSTs with `webhook_url`: pulse-pro r1, pulse-pro r2-500, pulse r3 | 0 (STT audio) | 0 | 1 HTTP |
| `p9_region.py --vantage mumbai\|us` | Cuts a 1.5 s clip from `clip.wav`, then sends `?model=pulse&language=multi-indic` to the generic, india and us hosts | 0 | 0 | 1 HTTP |
| `p_ttfb.py [--n 20] [--gap 1.0]` | Sequential SSE calls alternating between the generic host and api.india; reports dns/connect/tls/ttfb/first-audio p50 and p95 | 20 × 2n = 800 | 0 | 1 HTTP |

`p3`, `p4 --tail-probe`, `p6` (the 401) and `p7` (one 429 + the failure cases) are the scripts
that spend the scarce counters. `p3`, `p4 --tail-probe` and `p7` check up front that the whole
invocation fits under the 429 cap. For the india/us variant of P3, run from the US box:
`p3_admission.py --runs 3 --ws-runs 0 --hostA india --hostB us`.

## Choices and differences from the brief (please review)

1. **Voices.** I used Pro `rhea` (lightning-v-3-1-pro.md:348,354), which is not in the v3.1
   catalog. The standard voice is `magnus` (lightning-v-3-1.md:218). I avoided the docs'
   example `meher`, because lightning-v-3-1-pro.md:553 lists it as a voice shared with the
   base queue, which would blur P1.
2. **External IDs.** On sync and SSE, the docs define the request fields as the **body**
   fields `session_id` and `request_id`. `X-External-Session-Id` and `X-External-Request-Id`
   are the response headers that echo them (synthesize-speech.md:148-149,
   synthesize-speech-sse.md:91-92). On WS, the request fields are also `session_id` and
   `request_id`; they come back as `external_session_id` and `external_request_id`
   (tts.md:645-660,1264-1279). P6 therefore sends body fields by default.
   `--id-transport header|both` also sends `X-External-*` request headers, which are
   undocumented.
3. **P7 filter word.** content-filter.md:89 says mild expletives are deliberately not on the
   list. The only term the page shows being filtered is the one at :59 and :121, so that is
   the word I used. It appears in logs as `<filtered-word>`.
4. **P5 JSON ping.** `{"type":"ping"}` → `{"type":"pong"}` comes from tts.md:112-117. The
   socket is held for 150 s by default (PLAN.md:27; the brief's 30 s was overridden). S4 (protocol pings) is closed by us after 40 s
   if the server hasn't closed it first.
5. **P7 sequence** follows PLAN.md:29 (completed 1000 chars, billing-slice 429 via
   `reserve_429`) **except** the aborted SSE/WS stay at 1500 chars instead of 4000: a
   deliberate credit cut (plan-check-sara.md:68), stated in the EXPECTED line and in each
   abort record's `meta`.
6. **Output formats.** The sync default is `pcm` (synthesize-speech.md:144), so sync calls
   set `output_format: wav` and `sample_rate: 24000` to make the WAV header parseable. SSE
   uses `pcm`. The WS schema (tts.md:957-1339) has no `output_format` field.
7. **TTFB.** I used httpx trace hooks rather than `curl -w`. DNS is timed with a separate
   `getaddrinfo` just before the request; the connect span may include a second, usually
   cached, lookup.
8. **P8 clip.** P8 takes the newest P2 PCM WAV of at least 5 s (in practice `len251`,
   about 15 s), trims it to `--clip-seconds` (default 8; must be 5–10 s per PLAN.md:30),
   writes a fresh WAV header and validates the result (PCM, 5–10 s, consistent data size),
   also right before each submission. No fallback: if no source qualifies it aborts.
   `--clip` overrides the source.
