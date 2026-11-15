#!/usr/bin/env python3
"""P2 - parameter contracts (PLAN.md:24, plan-check-sara.md:39-42,76).

Sync POST /waves/v1/tts, output_format=wav so the WAV header can be parsed:
  len251, len1000                 -> 200 (card :39 "max chunk 250" is a throughput hint)
  whitespace_only                 -> 400 (synthesize-speech.md:129)
  len8001_nonspace                -> 4xx inferred from "Max 8000 characters after trim"
  sr22050_sync                    -> 4xx inferred from enum (synthesize-speech.md:134)
  word_timestamps_sync            -> 200, no timing data (synthesize-speech.md:147)
  [--with-8000] len8000_plus5sp   -> 200 ("after trim", plan-check addition 2)
WS /waves/v1/tts/live:
  sr22050_ws                      -> 4xx/error frame inferred from tts.md:1256
Audio bodies saved to out/audio/ (P8 copies the newest P2 WAV into fixtures/clip.wav).
"""
import asyncio

import common as C

EXPECTED = (
    "251 & 1000 -> 200; 8001 non-space -> 4xx (inferred from \"max\", code undocumented); "
    "whitespace-only -> 400 (:129); 22050 -> 4xx sync+WS (inferred from enum); "
    "word_timestamps on sync -> 200, no timing data (:147)")

FMT = {"sample_rate": C.SAMPLE_RATE, "output_format": "wav"}


def _case(name, text, **extra):
    body = C.tts_body(text, **dict(FMT, **extra))
    return name, body


async def main(args):
    C.expected(EXPECTED + (" | --with-8000: 8000 chars + 5 trailing spaces -> 200 "
                           "(\"after trim\", synthesize-speech.md:129)" if args.with_8000 else ""))
    cases = [
        _case("len251", C.prose(251)),
        _case("len1000", C.prose(1000)),
        _case("whitespace_only", " " * 20),
        _case("len8001_nonspace", C.prose(8001)),
        _case("sr22050_sync", C.text20("p2 sr"), sample_rate=22050),
        _case("word_timestamps_sync", C.text20("p2 wt"), word_timestamps=True),
    ]
    if args.with_8000:
        cases.append(_case("len8000_plus5sp", C.prose(8000) + " " * 5))
    for i, (case, body) in enumerate(cases):
        if i:
            await C.sleep(2, "(spacing)")
        rec = await C.http(case, "POST", C.tts_sync_url(), json_body=body, mode="sync",
                           chars=len(body["text"]), save_audio=True,
                           meta={"text_len": len(body["text"]),
                                 "text_len_stripped": len(body["text"].strip()),
                                 "requested_sample_rate": body.get("sample_rate")})
        if rec.get("wav"):
            w = rec["wav"]
            C.info(f"    WAV header: sr={w.get('sample_rate')} ch={w.get('channels')} "
                   f"bits={w.get('bits_per_sample')} dur={w.get('duration_s')} s")
        if case == "word_timestamps_sync" and rec.get("status") == 200:
            C.note(case, content_type=rec.get("content_type"),
                   timing_data_present=bool(rec.get("body_json")),
                   observation="sync body is audio; any timing data would need a JSON body")
        if case == "len8001_nonspace" and rec.get("status") == 200:
            C.note(case, warning="8001 non-space chars were ACCEPTED and billed")

    await C.sleep(2, "(spacing)")
    ws = C.WS("sr22050_ws", C.tts_ws_url())
    try:
        if await ws.open():
            text = C.text20("p2 ws")
            await ws.send_json(C.tts_body(text, sample_rate=22050), chars=len(text), label="A")
            done = asyncio.ensure_future(ws.complete.wait())
            err = asyncio.ensure_future(ws.error.wait())
            cls = asyncio.ensure_future(ws.closed.wait())
            await asyncio.wait({done, err, cls}, timeout=30 if not C.RUN.dry else 1,
                               return_when=asyncio.FIRST_COMPLETED)
            for f in (done, err, cls):
                f.cancel()
            await ws.wait(ws.closed, 6)   # give the server a chance to close after complete
    finally:
        await ws.close()
        ws.finish(requested_sample_rate=22050,
                  note="WS chunks are raw PCM; no WAV header to inspect - compare audio_bytes "
                       "per second of speech against 22050 vs 24000 if accepted")


if __name__ == "__main__":
    p = C.parser("p2", "P2 parameter contracts")
    p.add_argument("--with-8000", action="store_true",
                   help="also send 8000 chars + 5 trailing spaces (costs 8000 chars)")
    a = p.parse_args()
    C.run("p2", a, main)
