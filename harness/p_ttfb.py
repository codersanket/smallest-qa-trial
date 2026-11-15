#!/usr/bin/env python3
"""TTFB decomposition (descriptive only; PLAN.md:40,44). n sequential SSE requests per host,
interleaved generic / api.india (concurrency-and-limits.md:80-83), 20 chars, Pro model.
Per request: dns, connect, tls, ttfb (response headers), first audio chunk, total.
"""
import statistics

import common as C

EXPECTED = (
    "Descriptive only (no docs promise tested): p95 first-audio <= 500 ms from Mumbai is our "
    "SLO persona's threshold, not a documented SLA; TTFB-vs-200 ms is NOT a defect "
    "(PLAN.md:40)")


def _pct(xs, q):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    k = max(0, min(len(xs) - 1, round(q * (len(xs) - 1))))
    return round(xs[k], 1)


async def main(args):
    C.expected(EXPECTED + f" | n={args.n} per host")
    hosts = ["generic", "india"]
    res = {h: [] for h in hosts}
    for i in range(args.n):
        for h in hosts:
            t = C.text20(f"ttfb {i:02d}")
            rec = await C.http(f"{h}_{i:02d}", "POST", C.tts_sse_url(h),
                               json_body=C.tts_body(t, sample_rate=C.SAMPLE_RATE,
                                                    output_format="pcm"),
                               mode="sse", chars=len(t), meta={"host_alias": h, "i": i})
            tm = dict(rec.get("timings_ms") or {})
            tm["first_audio"] = (rec.get("sse") or {}).get("first_audio_ms")
            tm["status"] = rec.get("status")
            res[h].append(tm)
            await C.sleep(args.gap, "(gap)")
    if C.RUN.dry:
        return
    summary = {}
    for h in hosts:
        ok = [r for r in res[h] if r.get("status") == 200]
        summary[h] = {"n_ok": len(ok), "n": len(res[h])}
        for f in ("dns", "connect", "tls", "ttfb_headers", "first_audio", "total"):
            xs = [r.get(f) for r in ok]
            summary[h][f] = {"p50": _pct(xs, 0.5), "p95": _pct(xs, 0.95),
                             "mean": round(statistics.fmean([x for x in xs if x is not None]), 1)
                             if any(x is not None for x in xs) else None}
    C.log(C.RUN.probe, {"case": "_summary", "summary_ms": summary})
    for h, s in summary.items():
        C.info(f"{h}: n_ok={s['n_ok']}/{s['n']} " + " ".join(
            f"{f}=p50 {s[f]['p50']}/p95 {s[f]['p95']}" for f in
            ("dns", "connect", "tls", "ttfb_headers", "first_audio", "total")))


if __name__ == "__main__":
    p = C.parser("pttfb", "TTFB decomposition (SSE, generic vs api.india)")
    p.add_argument("--n", type=int, default=20, help="requests per host (default 20)")
    p.add_argument("--gap", type=float, default=1.0, help="seconds between requests (1.0)")
    a = p.parse_args()
    C.run("pttfb", a, main)
