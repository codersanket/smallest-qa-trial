#!/usr/bin/env python3
"""P8 - Pulse pre-recorded STT webhook (side probe) (PLAN.md:30, plan-check-sara.md:51-56,71).

POST https://api.smallest.ai/waves/v1/stt/?model=<m>&language=en&webhook_url=...&webhook_extra=...
  (transcribe.md:11-12,44-50,120-123; webhooks.md:25,42). Body = raw WAV bytes,
  Content-Type application/octet-stream (transcribe.md:12,27).
webhook_url = https://<--hook-host>/smallest-hook/<run-token>; p8_receiver.py on
127.0.0.1:8765 (behind Caddy) logs arrivals and returns 500 exactly once for token r2-500.
Runs (default all):
  1  pulse-pro  token r1-<stamp>   receiver -> 204
  2  pulse-pro  token r2-500       receiver -> 500 once, then 204 (expect >=1 retry, :33)
  3  pulse      token r3-<stamp>   receiver -> 204 (pulse may omit request_id, transcribe.md:305)
webhook_extra = run:p8-<n> (comma-separated key:value pairs, webhooks.md:25).
Audio: the newest >= 5 s PCM WAV saved by P2 in out/audio/ is TRIMMED to --clip-seconds
(default 8, must be 5-10 s per PLAN.md:30), re-headered, validated (PCM, 5-10 s, consistent
data size) and written to out/fixtures/clip.wav; it is re-validated before every submission
(own Lightning audio, plan-check-sara.md:71). Use --clip to pick another source file.
"""
import hashlib
import re
import struct
from pathlib import Path

import common as C

EXPECTED = (
    "200 `processing` <1 s (transcribe.md:50,120); pulse-pro callback carries request_id "
    "(`webhooks.md:64` sample); plain `pulse` may omit it (`transcribe.md:305`); webhook_extra "
    "verbatim (:25, field location undocumented - log where it appears); one 500 -> >=1 retry "
    "(:33); <30 s = our threshold, not theirs. Do NOT claim 10 retries")

HOST_RE = re.compile(r"^[A-Za-z0-9.-]+(:[0-9]{1,5})?$")


CLIP_MIN_S, CLIP_MAX_S = 5.0, 10.0          # PLAN.md:30 "5-10 s clip"


def _usable(w: dict | None) -> bool:
    return bool(w) and w.get("format_tag") == 1 and bool(w.get("byte_rate")) and \
        bool(w.get("block_align")) and w.get("data_offset") is not None and \
        (w.get("duration_s") or 0) >= CLIP_MIN_S


def pick_clip(explicit: str | None) -> Path | None:
    """Newest P2 PCM WAV that is >= 5 s long (it is trimmed to --clip-seconds afterwards).
    The 20-char P2 cases are ~1.5 s (too short); 251/1000-char ones are long enough."""
    if explicit:
        return Path(explicit)
    cands = sorted(C.AUDIO.glob("p2_*.wav"), key=lambda p: p.stat().st_mtime, reverse=True)
    for p in cands:
        if _usable(C.parse_wav(p.read_bytes())):
            return p
    return None


def trim_wav(data: bytes, seconds: float) -> tuple[bytes, dict]:
    """First `seconds` of a PCM WAV, re-wrapped with a fresh canonical 44-byte header."""
    w =C.parse_wav(data)
    if not _usable(w):
        raise C.Abort(f"clip source is not a >= {CLIP_MIN_S:.0f} s PCM WAV: {w}")
    start = w["data_offset"]
    avail = min(w["data_size_header"], w["data_size_actual"])
    n = int(seconds * w["byte_rate"])
    n -= n % w["block_align"]
    pcm = data[start:start + min(n, avail - avail % w["block_align"])]
    fmt = struct.pack("<HHIIHH", 1, w["channels"], w["sample_rate"], w["byte_rate"],
                      w["block_align"], w["bits_per_sample"])
    out = (b"RIFF" + struct.pack("<I", 36 + len(pcm)) + b"WAVE" + b"fmt " +
           struct.pack("<I", 16) + fmt + b"data" + struct.pack("<I", len(pcm)) + pcm)
    return out, w


def validate_clip(data: bytes) -> dict:
    w = C.parse_wav(data)
    dur = (w or {}).get("duration_s") or 0
    if not w or w.get("format_tag") != 1 or not CLIP_MIN_S <= dur <= CLIP_MAX_S or \
            w.get("data_size_header") != w.get("data_size_actual"):
        raise C.Abort(f"clip failed validation (need PCM WAV {CLIP_MIN_S:.0f}-{CLIP_MAX_S:.0f} s,"
                      f" consistent data size): {w}")
    return w


def prepare_clip(explicit: str | None, seconds: float) -> Path:
    if not CLIP_MIN_S <= seconds <= CLIP_MAX_S:
        raise C.Abort(f"--clip-seconds must be {CLIP_MIN_S:.0f}-{CLIP_MAX_S:.0f} (PLAN.md:30)")
    src = pick_clip(explicit)
    dst = C.FIXTURES / "clip.wav"
    if src is None:
        if C.RUN.dry:
            C.info(f"[dry-run] no >= {CLIP_MIN_S:.0f} s P2 WAV in {C.AUDIO} yet; real run would "
                   f"abort until P2 has run")
            return dst
        raise C.Abort(f"no >= {CLIP_MIN_S:.0f} s PCM P2 WAV in {C.AUDIO}; run p2_params.py "
                      f"first or pass --clip")
    data, src_w = trim_wav(src.read_bytes(), seconds)
    w = validate_clip(data)
    C.info(f"clip source: {src.name} sr={src_w.get('sample_rate')} src_dur="
           f"{src_w.get('duration_s')} s -> trimmed {w['duration_s']} s (validated "
           f"{CLIP_MIN_S:.0f}-{CLIP_MAX_S:.0f} s)")
    if C.RUN.dry:
        C.info(f"[dry-run] would write trimmed clip {src} -> {dst}")
        return dst
    C.FIXTURES.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(".tmp")
    tmp.write_bytes(data)
    validate_clip(tmp.read_bytes())     # validate what is actually on disk
    tmp.replace(dst)
    C.note("clip", source=str(src), dest=str(dst), source_wav=src_w, clip_wav=w,
           clip_sha256=hashlib.sha256(data).hexdigest())
    return dst


async def main(args):
    C.expected(EXPECTED)
    clip = prepare_clip(args.clip, args.clip_seconds)
    if args.prepare_clip_only:
        return
    if not args.hook_host or not HOST_RE.match(args.hook_host):
        raise C.Abort("--hook-host <public host[:port]> is required (no scheme, no path)")
    if clip.exists():
        data = clip.read_bytes()
        validate_clip(data)                 # re-validate right before submission
    elif C.RUN.dry:
        data = b"RIFF-dry-run-placeholder"
    else:
        raise C.Abort(f"{clip} missing")
    short = C.RUN.stamp[-12:-3]
    plan = {1: ("pulse-pro", f"r1-{short}"), 2: ("pulse-pro", "r2-500"),
            3: ("pulse", f"r3-{short}")}
    for n in [int(x) for x in args.runs.split(",")]:
        model, token = plan[n]
        hook = f"https://{args.hook_host}/smallest-hook/{token}"
        u = C.stt_url("generic", model=model, language="en", webhook_url=hook,
                      webhook_extra=f"run:p8-{n}")
        rec = await C.http(f"run{n}_{model}", "POST", u, content=data,
                           content_type="application/octet-stream", mode="stt", chars=0,
                           meta={"model": model, "run_token": token, "webhook_url": hook,
                                 "webhook_extra": f"run:p8-{n}", "clip": str(clip),
                                 "receiver_behaviour": "500 once then 204" if token == "r2-500"
                                 else "204"})
        bj = rec.get("body_json") or {}
        C.info(f"    immediate: status={rec.get('status')} body.status={bj.get('status')} "
               f"request_id={bj.get('request_id')} in {rec.get('timings_ms', {}).get('total')} ms")
        await C.sleep(3)
    C.info("Now watch out/runs/p8_hooks.jsonl (p8_receiver.py) for callbacks; match request_id "
           "and run token, note where `run:p8-<n>` appears in the payload.")


if __name__ == "__main__":
    p = C.parser("p8", "P8 Pulse webhook")
    p.add_argument("--hook-host", help="public HTTPS host that Caddy proxies to 127.0.0.1:8765")
    p.add_argument("--runs", default="1,2,3", help="comma list of runs to do (default 1,2,3)")
    p.add_argument("--clip", help="explicit PCM WAV (>= 5 s) to trim instead of the newest P2 WAV")
    p.add_argument("--clip-seconds", type=float, default=8.0,
                   help="trim the clip to this many seconds; must be 5-10 (default 8)")
    p.add_argument("--prepare-clip-only", action="store_true",
                   help="only write the trimmed, validated clip to out/fixtures/clip.wav "
                        "(no network)")
    a = p.parse_args()
    C.run("p8", a, main)
