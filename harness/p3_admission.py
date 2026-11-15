#!/usr/bin/env python3
"""P3 - admission control / the 429 (PLAN.md:25, plan-check-sara.md:43,61-66,75).

Per run (--runs N, default 3):
  A = SSE POST /waves/v1/tts/live, ~600 chars, on --hostA.
  On A's FIRST audio chunk (asyncio barrier) fire B = sync POST /waves/v1/tts, 20 chars,
  on --hostB. Log B's status, headers, body. Wait for A to end (byte count intact?),
  then retry B at A_end+0.2 s and A_end+1.0 s.
WS variant (--ws-runs, default 1): two sockets pre-opened; send WS-A on socket 1, on its
  first chunk send WS-B on socket 2; record every frame / close on both.
--stt-addon: one extra run where, instead of B, a Pulse STT POST (out/fixtures/clip.wav)
  fires on A's first chunk (does STT share the 1-slot? limits :21 vs :73).
Every deliberate over-limit attempt (B, WS-B, STT add-on) runs inside a reserve_429 window
(<=10 total; >=5 s between ACTUAL dispatches, checked at dispatch). The first-chunk barrier
is awaited: A is not consumed further until B's request body is on the wire; real overlap is
logged from perf_counter marks. Retries fire at absolute A_end+0.2/+1.0 s deadlines (lateness
logged). Max 2 HTTP in flight / 2 sockets.
"""
import asyncio
import time

import common as C

EXPECTED = (
    "B -> 429 (limits :30) within 1 RTT; 429 body shape UNDOCUMENTED for TTS (only "
    "400/401/500 `{error,message}` at `synthesize-speech.md:159-180`); Retry-After optional; "
    "A completes intact; WS B -> \"rejected with an error\" (:36), socket state UNKNOWN; "
    "retry after A ends -> 200")

A_CHARS = 600


RETRY_OFFSETS_S = (0.2, 1.0)


async def sse_run(run_idx: int, args, kind: str):
    """kind 'tts' -> B is sync TTS; 'stt' -> B is a Pulse STT POST."""
    tag = f"run{run_idx}" if kind == "tts" else f"stt_addon{run_idx}"
    async with C.reserve_429(f"{tag}_B") as dw:   # 5 s gap waited here, before A starts
        await _sse_run(tag, run_idx, args, kind, dw)


async def _sse_run(tag, run_idx, args, kind, dw):
    a_text = C.prose(A_CHARS)
    b_text = C.text20(f"p3 r{run_idx}")
    clip = None
    if kind == "stt":
        clip = C.FIXTURES / "clip.wav"
        if not C.RUN.dry and not clip.exists():
            raise C.Abort(f"--stt-addon needs {clip} (run p8 --prepare-clip-only or P1/P2 first)")
        clip_bytes = clip.read_bytes() if clip.exists() else b"RIFF-dry"

    async def start_b(dispatched):
        if kind == "tts":
            body = C.tts_body(b_text, sample_rate=C.SAMPLE_RATE, output_format="wav")
            return await C.http(f"{tag}_B_during_A", "POST", C.tts_sync_url(args.hostB),
                                json_body=body, mode="sync", chars=len(b_text),
                                deliberate=dw, dispatched=dispatched,
                                meta={"deliberate_over_limit": True, "barrier": "A first chunk"})
        return await C.http(f"{tag}_STT_during_A", "POST",
                            C.stt_url(args.hostB, model="pulse", language="en"),
                            content=clip_bytes, content_type="application/octet-stream",
                            mode="stt", chars=0, deliberate=dw, dispatched=dispatched,
                            meta={"deliberate_over_limit": True, "clip": str(clip)})

    # Awaited barrier: A's reader stops at its first chunk until B's request is on the wire.
    barrier, bst = C.dispatch_barrier(start_b)
    a_body = C.tts_body(a_text, sample_rate=C.SAMPLE_RATE, output_format="pcm")
    a_rec = await C.http(f"{tag}_A_sse", "POST", C.tts_sse_url(args.hostA), json_body=a_body,
                         mode="sse", chars=len(a_text), on_first_chunk=barrier,
                         meta={"hostA": args.hostA, "hostB": args.hostB})
    a_end = (a_rec.get("perf_s") or {}).get("end")       # A's completion clock (absolute)
    b_task = bst["task"]
    if b_task is None:
        if not C.RUN.dry:
            C.note(tag, warning="A produced no audio chunk; B never fired",
                   a_status=a_rec.get("status"))
        return
    retries = []
    if kind == "tts" and a_end is not None:
        # slot-release lag: retries at ABSOLUTE deadlines A_end+0.2 s / A_end+1.0 s. A retry
        # never fires while B (or the previous retry) is still in flight - that would be an
        # unreserved over-limit request - it waits and the lateness is recorded instead.
        for k, off in enumerate(RETRY_OFFSETS_S):
            deadline = a_end + off
            wait = deadline - time.perf_counter()
            delayed_by = None
            if wait > 0:
                await C.sleep(wait, f"(retry B at A_end+{off} s)")
            elif k:
                delayed_by = "previous retry still in flight at deadline"
            if not b_task.done():
                delayed_by = "B still in flight"
                await b_task
            late_ms = round((time.perf_counter() - deadline) * 1000, 1)
            body = C.tts_body(C.text20(f"p3 r{run_idx}{'bc'[k]}"), sample_rate=C.SAMPLE_RATE,
                              output_format="wav")
            r = await C.http(f"{tag}_B_retry_+{off}s", "POST", C.tts_sync_url(args.hostB),
                             json_body=body, mode="sync", chars=len(body["text"]),
                             meta={"scheduled_after_A_end_s": off,
                                   "lateness_at_call_ms": late_ms, "delayed_by": delayed_by})
            sent = (r.get("perf_s") or {}).get("request_sent") or (r.get("perf_s") or {}).get(
                "send_start")
            actual = round(sent - a_end, 3) if sent is not None else None
            C.note(f"{tag}_retry_+{off}s_timing", scheduled_after_A_end_s=off,
                   actual_after_A_end_s=actual,
                   lateness_ms=round((actual - off) * 1000, 1) if actual is not None else None,
                   delayed_by=delayed_by, status=r.get("status"))
            retries.append(r)
    b_rec = await b_task
    sse = a_rec.get("sse") or {}
    C.note(f"{tag}_summary", A_status=a_rec.get("status"), A_audio_bytes=sse.get("audio_bytes"),
           A_chunks=sse.get("audio_chunks"), A_done=bool(sse.get("done_frame")),
           B_status=(b_rec or {}).get("status"),
           B_retry_after=_hdr(b_rec, "retry-after"), B_body=(b_rec or {}).get("body_text"),
           barrier_timeout=bst["barrier_timeout"],
           barrier_hold_ms=round((bst["t_released"] - bst["t_first_chunk"]) * 1000, 2)
           if bst["t_released"] and bst["t_first_chunk"] else None,
           overlap=C.overlap(a_rec, b_rec),
           retry_status=[r.get("status") for r in retries])


def _hdr(rec, name):
    for k, v in (rec or {}).get("resp_headers") or []:
        if k.lower() == name:
            return v
    return None


async def ws_run(run_idx: int, args):
    tag = f"ws_run{run_idx}"
    async with C.reserve_429(f"{tag}_WS_B") as dw:
        await _ws_run(tag, run_idx, args, dw)


async def _ws_run(tag, run_idx, args, dw):
    wa = C.WS(f"{tag}_WS_A", C.tts_ws_url(args.hostA))
    wb = C.WS(f"{tag}_WS_B", C.tts_ws_url(args.hostB))
    try:
        if not await wa.open() or not await wb.open():
            return
        a_text = C.prose(A_CHARS)
        await wa.send_json(C.tts_body(a_text, sample_rate=C.SAMPLE_RATE), chars=len(a_text),
                           label="A")
        if not await wa.wait(wa.first_chunk, 30):
            C.note(tag, warning="WS-A produced no chunk within 30 s; WS-B not sent")
            return
        b_text = C.text20(f"p3 ws{run_idx}")
        wb.rec["deliberate_dispatch_epoch"] = dw.dispatch()
        b_sent_a_ms = wa.now_ms()
        a_done_before_b = wa.complete.is_set() or wa.closed.is_set()
        await wb.send_json(C.tts_body(b_text, sample_rate=C.SAMPLE_RATE), chars=len(b_text),
                           label="B_during_A")
        await wa.wait(wa.complete, 60)
        await wa.wait(wa.closed, 6)
        # give B time to show either rejection, delayed service, or nothing
        await wb.wait(wb.closed, 8)
        C.note(tag, ws_b_socket_open_after_A=wb.is_open, ws_b_frames=len(wb.frames),
               ws_b_last=wb.last_text, B_sent_at_A_ms=b_sent_a_ms,
               A_complete_ms=wa.t_complete_ms, A_complete_before_B_sent=a_done_before_b,
               B_sent_before_A_complete=(wa.t_complete_ms is None
                                         or b_sent_a_ms < wa.t_complete_ms))
    finally:
        await wb.close()
        await wa.close()
        wa.finish(role="A")
        wb.finish(role="B", deliberate_over_limit=True)


async def main(args):
    C.expected(EXPECTED)
    C.info(f"hostA={args.hostA} hostB={args.hostB} runs={args.runs} ws_runs={args.ws_runs} "
           f"stt_addon={args.stt_addon}; 429 counter now {C.count_429()}/{C.MAX_DELIBERATE_429}")
    needed = args.runs + args.ws_runs + (1 if args.stt_addon else 0)
    if not C.RUN.dry and C.count_429() + needed > C.MAX_DELIBERATE_429:
        raise C.Abort(f"this invocation needs {needed} deliberate-429 attempts; counter is "
                      f"{C.count_429()}/{C.MAX_DELIBERATE_429}")
    for r in range(1, args.runs + 1):
        await sse_run(r, args, "tts")
        await C.sleep(3, "(between runs)")
    for r in range(1, args.ws_runs + 1):
        await ws_run(r, args)
        await C.sleep(3, "(between runs)")
    if args.stt_addon:
        await sse_run(1, args, "stt")


if __name__ == "__main__":
    p = C.parser("p3", "P3 admission control (deliberate 429s)")
    p.add_argument("--runs", type=int, default=3, help="SSE-A + sync-B runs (default 3)")
    p.add_argument("--ws-runs", type=int, default=1, help="WS-A/WS-B runs (default 1)")
    p.add_argument("--stt-addon", action="store_true", help="one STT POST during A")
    p.add_argument("--hostA", choices=sorted(C.HOSTS), default="generic")
    p.add_argument("--hostB", choices=sorted(C.HOSTS), default="generic")
    a = p.parse_args()
    C.run("p3", a, main)
