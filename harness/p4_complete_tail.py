#!/usr/bin/env python3
"""P4 - `complete` tail + socket reuse (PLAN.md:26, plan-check-sara.md:20-21,44).

One WS per run, NO context_id (tts.md:603-607 exception must not apply):
  send A (20 chars); timestamp every chunk, the last chunk, `complete`, close code;
  1 s after `complete`, try sending B on the same socket and log what happens.
n runs with default complete_backoff_ms (field omitted -> 4000, tts.md:1032-1040),
n runs with complete_backoff_ms=0.
--tail-probe: in default-backoff runs, once A has gone quiet for --tail-quiet-ms after its
  last chunk (i.e. inside the tail), open a SECOND socket and send a request; log whether
  it is admitted (slot freed at last chunk) or rejected (slot held until `complete`).
  A's state is RE-CHECKED right before the tail request is sent (after the second socket's
  slot wait + handshake); if `complete`/close already arrived the request is not sent and the
  run is classified `missed_tail` (never counted as a slot-release result).
  Each tail probe is a potential over-limit request -> reserve_429 window, dispatch at open.
"""
import asyncio

import common as C

EXPECTED = (
    "last-chunk->complete ~ 4000 ms default, ~ 0 with `0`; server closes after complete; "
    "B sent 1 s later fails. Open question: does the SLOT free at last chunk or at `complete`? "
    "(second socket request inside the tail)")


async def one_run(i: int, backoff, args):
    tag = f"{'default' if backoff is None else 'backoff0'}_run{i}"
    if args.tail_probe and backoff is None:
        async with C.reserve_429(f"{tag}_tail_probe") as dw:
            await _one_run(tag, i, backoff, args, dw)
    else:
        await _one_run(tag, i, backoff, args, None)


async def _one_run(tag, i, backoff, args, dw):
    probe_ws = None
    tail_probe = dw is not None
    tail_outcome = None       # in_tail | missed_tail | send_failed | probe_handshake_failed
    tail_sent_at = None
    ws = C.WS(tag, C.tts_ws_url(), meta={"complete_backoff_ms": backoff if backoff is not None
                                         else "<omitted: default 4000>"})
    try:
        if not await ws.open():
            return
        text = C.text20(f"p4 {i}")
        extra = {"sample_rate": C.SAMPLE_RATE}
        if backoff is not None:
            extra["complete_backoff_ms"] = backoff
        await ws.send_json(C.tts_body(text, **extra), chars=len(text), label="A")
        if not await ws.wait(ws.first_chunk, 30):
            C.note(tag, warning="no chunk within 30 s")
        if tail_probe and ws.first_chunk.is_set():
            # detect the tail: no new chunk for tail_quiet_ms and no complete yet
            while not ws.complete.is_set() and not ws.closed.is_set():
                quiet = (ws.now_ms() or 0) - (ws.t_last_chunk_ms or 0)
                if quiet >= args.tail_quiet_ms or C.RUN.dry:
                    break
                await asyncio.sleep(0.05)
            if not ws.complete.is_set() and not ws.closed.is_set():
                probe_ws = C.WS(f"{tag}_tail_probe", C.tts_ws_url())
                if await probe_ws.open(deliberate=dw):
                    # slot wait + handshake may have outlasted A's tail: RE-CHECK A right
                    # before sending; a request after `complete` says nothing about the slot
                    if ws.complete.is_set() or ws.closed.is_set():
                        tail_outcome = "missed_tail"
                        C.note(tag, tail_probe="missed_tail: A's complete/close arrived during "
                               "the second socket's slot wait/handshake; request NOT sent",
                               A_complete_ms=ws.t_complete_ms, A_now_ms=ws.now_ms())
                    else:
                        t2 = C.text20(f"p4 tail{i}")
                        tail_sent_at = ws.now_ms()
                        ev = await probe_ws.send_json(C.tts_body(t2, sample_rate=C.SAMPLE_RATE),
                                                      chars=len(t2), label="tail_request")
                        tail_outcome = "in_tail" if ev.get("ok") else "send_failed"
                        C.note(tag, tail_probe_sent_at_A_ms=tail_sent_at,
                               A_last_chunk_ms=ws.t_last_chunk_ms,
                               A_complete_already_at_send_return=ws.complete.is_set())
                else:
                    tail_outcome = "probe_handshake_failed"
            else:
                tail_outcome = "missed_tail"
                C.note(tag, tail_probe="missed_tail: complete arrived before tail was detected")
        await ws.wait(ws.complete, 15)
        if ws.complete.is_set():
            # B exactly 1 s after `complete` (keep reading meanwhile: close is recorded)
            delay = max(0.0, 1.0 - ((ws.now_ms() or 0) - ws.t_complete_ms) / 1000)
            await C.sleep(delay, "(B at complete+1 s)")
            b = C.text20(f"p4 {i} B")
            ev = await ws.send_json(C.tts_body(b, sample_rate=C.SAMPLE_RATE, **(
                {"complete_backoff_ms": backoff} if backoff is not None else {})),
                chars=len(b), label="B_after_complete+1s")
            if ev.get("ok"):
                await ws.wait(ws.closed, 8)
        else:
            await ws.wait(ws.closed, 5)
        if probe_ws is not None:
            await probe_ws.wait(probe_ws.complete, 15)
            await probe_ws.wait(probe_ws.closed, 6)
    finally:
        if probe_ws is not None:
            await probe_ws.close()
        await ws.close()
        tail = (ws.t_complete_ms - ws.t_last_chunk_ms) if (
            ws.t_complete_ms is not None and ws.t_last_chunk_ms is not None) else None
        ws.finish(complete_backoff_ms=backoff, last_chunk_to_complete_ms=tail,
                  tail_probe_outcome=tail_outcome,
                  complete_to_close_ms=(ws.close_info.get("t_ms") - ws.t_complete_ms) if (
                      ws.close_info.get("t_ms") is not None and ws.t_complete_ms is not None)
                  else None)
        if probe_ws is not None:
            # admitted/rejected only mean something for a request actually sent in the tail
            in_tail = tail_outcome == "in_tail"
            probe_ws.finish(role="tail_probe", deliberate_over_limit=True,
                            tail_probe_outcome=tail_outcome,
                            sent_at_A_ms=tail_sent_at,
                            A_complete_ms=ws.t_complete_ms,
                            sent_before_A_complete=None if not in_tail else (
                                ws.t_complete_ms is None or tail_sent_at < ws.t_complete_ms),
                            admitted=probe_ws.first_chunk.is_set() if in_tail else None,
                            rejected=probe_ws.error.is_set() if in_tail else None)
        C.info(f"    {tag}: last_chunk->complete = {tail} ms")


async def main(args):
    C.expected(EXPECTED)
    if args.tail_probe and not C.RUN.dry and C.count_429() + args.n > C.MAX_DELIBERATE_429:
        raise C.Abort(f"--tail-probe needs {args.n} deliberate attempts; counter "
                      f"{C.count_429()}/{C.MAX_DELIBERATE_429}")
    for backoff in (None, 0):
        for i in range(1, args.n + 1):
            await one_run(i, backoff, args)
            await C.sleep(2, "(between runs)")


if __name__ == "__main__":
    p = C.parser("p4", "P4 complete tail + socket reuse")
    p.add_argument("--n", type=int, default=5, help="runs per backoff setting (default 5)")
    p.add_argument("--tail-probe", action="store_true",
                   help="second socket request inside the default 4 s tail (<=2 sockets)")
    p.add_argument("--tail-quiet-ms", type=int, default=600,
                   help="no-chunk gap that marks the tail (default 600 ms)")
    a = p.parse_args()
    C.run("p4", a, main)
