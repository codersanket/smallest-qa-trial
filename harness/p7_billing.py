#!/usr/bin/env python3
"""P7 - billing on failure / abort (PLAN.md:29, plan-check-sara.md:50,64-69,77).

Sequence per PLAN.md:29 (chars logged per request in out/runs/ledger.csv):
  success_1000      sync, 1000 chars                        -> billed (baseline)
  filter_reject     sync, content_filter reject, one listed term -> 400 CONTENT_FILTER_BLOCKED,
                    "costs no synthesis time" (content-filter.md:40-50,91). The term is logged
                    as <filtered-word>.
  bad_voice_400     sync, nonexistent voice                 -> 400 (billing undocumented)
  over_limit_429    A = SSE 600 chars in flight; on A's first chunk (awaited barrier) B = sync
                    20 chars -> 429. Deliberate: reserve_429 window (counter <=10, >=5 s apart).
  sse_abort         SSE 1500 chars, client aborts after the first chunk (billing undocumented)
  ws_abort          WS 1500 chars, client closes after the first chunk (billing undocumented)
DELIBERATE DEVIATION: PLAN.md:29 says 4000-char aborts; we send 1500 (credit cut,
plan-check-sara.md:68). Recorded in EXPECTED and in each abort record's meta.
There is no usage/balance API in pages/ (only the dashboard link,
concurrency-and-limits.md:48), so balance must be read by Sanket before and after.
"""
import common as C

ABORT_CHARS = 1500           # PLAN says 4000; cut per plan-check-sara.md:68
SUCCESS_CHARS = 1000         # PLAN.md:29 "completed 1000-char bills"
A429_CHARS = 600             # same in-flight A as P3 (plan-check-sara.md:67)
DEVIATION = (f"aborted SSE/WS use {ABORT_CHARS} chars, not PLAN's 4000 "
             "(deliberate credit cut, plan-check-sara.md:68)")

# PLAN.md:29 EXPECTED, verbatim, plus the harness deviation note
EXPECTED = (
    "filter reject = no synthesis time (:91); 400/429 billing = hypothesis 0; SSE aborted after "
    "first chunk of 4000 chars = billed in full (guess); completed 1000-char bills"
    f" [HARNESS DEVIATION: {DEVIATION}]")

# The only term the docs themselves show being filtered (content-filter.md:59,121).
# content-filter.md:89 says mild expletives are deliberately NOT listed, so a milder word
# would not trigger the filter. Logged/printed only as <filtered-word>.
FILTER_TERM = "fuck" + "er"
C.add_sensitive(FILTER_TERM, "<filtered-word>")

REMINDER = ("REMINDER: Sanket must read the dashboard balance BEFORE and AFTER this script "
            "(https://app.smallest.ai/dashboard/developers/usage - concurrency-and-limits.md:48); "
            "there is no usage API in pages/. Allow for reporting lag.")


async def over_limit_429(fmt):
    async with C.reserve_429("over_limit_429_B") as dw:
        b_text = C.text20("p7 429")

        async def start_b(dispatched):
            return await C.http("over_limit_429_B", "POST", C.tts_sync_url(),
                                json_body=C.tts_body(b_text, **fmt), mode="sync",
                                chars=len(b_text), deliberate=dw, dispatched=dispatched,
                                meta={"deliberate_over_limit": True, "barrier": "A first chunk"})

        barrier, bst = C.dispatch_barrier(start_b)
        a_text = C.prose(A429_CHARS)
        a_rec = await C.http("over_limit_429_A_sse", "POST", C.tts_sse_url(),
                             json_body=C.tts_body(a_text, sample_rate=C.SAMPLE_RATE,
                                                  output_format="pcm"),
                             mode="sse", chars=len(a_text), on_first_chunk=barrier,
                             meta={"role": "in-flight A for the billing-slice 429"})
        if bst["task"] is None:
            C.note("over_limit_429", warning="A produced no audio chunk; B never fired",
                   a_status=a_rec.get("status"))
            return
        b_rec = await bst["task"]
        C.note("over_limit_429_summary", A_status=a_rec.get("status"),
               B_status=b_rec.get("status"), barrier_timeout=bst["barrier_timeout"],
               overlap=C.overlap(a_rec, b_rec))


async def main(args):
    C.expected(EXPECTED)
    C.info(REMINDER)
    if not C.RUN.dry and C.count_429() + 1 > C.MAX_DELIBERATE_429:
        raise C.Abort(f"P7 needs 1 deliberate-429 attempt; counter is "
                      f"{C.count_429()}/{C.MAX_DELIBERATE_429}")
    fmt = {"sample_rate": C.SAMPLE_RATE, "output_format": "wav"}

    t = C.prose(SUCCESS_CHARS)
    await C.http("success_1000", "POST", C.tts_sync_url(), json_body=C.tts_body(t, **fmt),
                 mode="sync", chars=len(t))
    await C.sleep(2)

    t = f"Hello {FILTER_TERM}, your order is ready."
    await C.http("filter_reject", "POST", C.tts_sync_url(),
                 json_body=C.tts_body(t, content_filter={"enabled": True, "action": "reject"},
                                      **fmt),
                 mode="sync", chars=len(t), meta={"content_filter": "reject", "terms": 1})
    await C.sleep(2)

    t = C.text20("p7 badvoice")
    await C.http("bad_voice_400", "POST", C.tts_sync_url(),
                 json_body=C.tts_body(t, voice="nonexistent_voice_p7", **fmt),
                 mode="sync", chars=len(t))
    await C.sleep(2)

    await over_limit_429(fmt)
    await C.sleep(5, "(let the 429 pair settle before the aborts)")

    t = C.prose(ABORT_CHARS)
    rec = await C.http("sse_abort", "POST", C.tts_sse_url(),
                       json_body=C.tts_body(t, sample_rate=C.SAMPLE_RATE, output_format="pcm"),
                       mode="sse", chars=len(t), abort_after_first_chunk=True,
                       meta={"plan_chars": 4000, "deviation": DEVIATION})
    if not C.RUN.dry and not (rec.get("sse") or {}).get("aborted_by_client"):
        C.note("sse_abort", warning="no first chunk observed: stream was NOT aborted by us",
               status=rec.get("status"))
    await C.sleep(5, "(let the aborted SSE drain server-side before the WS)")

    t = C.prose(ABORT_CHARS)
    ws = C.WS("ws_abort", C.tts_ws_url(), meta={"plan_chars": 4000, "deviation": DEVIATION})
    try:
        if await ws.open():
            await ws.send_json(C.tts_body(t, sample_rate=C.SAMPLE_RATE), chars=len(t), label="A")
            got = await ws.wait(ws.first_chunk, 30)
            # only an OBSERVED chunk makes this an abort-after-first-chunk
            ws.rec["client_abort_after_first_chunk"] = got
            if not got:
                ws.rec["first_chunk_outcome"] = (
                    "closed_before_chunk" if ws.closed.is_set() else "timeout_no_chunk_30s")
    finally:
        await ws.close("client abort after first chunk"
                       if ws.rec.get("client_abort_after_first_chunk") else "client close: no chunk")
        ws.finish()
    C.info(REMINDER)


if __name__ == "__main__":
    a = C.parser("p7", "P7 billing on failure/abort").parse_args()
    C.run("p7", a, main)
