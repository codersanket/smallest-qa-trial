#!/usr/bin/env python3
"""P6 - correlation-ID matrix (PLAN.md:28, plan-check-sara.md:25-28,46-49).

1. New requests (all lightning_v3.1_pro, 20 chars):
   fake401           sync with obviously fake key sk_invalid_probe - ONCE EVER (401 counter)
   id129             sync, 129-char request_id (max 128, synthesize-speech.md:149)
   id_whitespace     sync, request_id containing spaces (pattern, synthesize-speech.md:149)
   ok_sync_ids       sync 200 with session_id + request_id (echo as X-External-Session-Id /
                     X-External-Request-Id response headers, synthesize-speech.md:148-149)
   ok_sse_ids        SSE with both (synthesize-speech-sse.md:91-92)
   ws_ids            WS with session_id/request_id message fields, echoed as
                     external_session_id/external_request_id (tts.md:645-660,1264-1279)
   NOTE: the docs define the *request* side as body fields `session_id`/`request_id`;
   `X-External-*` are the *response* headers. --id-transport header|both also sends them
   as X-External-* request headers (undocumented) for comparison.
2. Re-read out/runs/p1_*, p2_*, p3_*, p6_* logs into an endpoint x status matrix ->
   out/runs/p6_matrix.md.
"""
import glob
import json
from collections import defaultdict
from pathlib import Path

import common as C

EXPECTED = (
    "`x-request-id` on failing responses (error-reference :50,70; 200/429 = our SLO "
    "expectation); `X-External-*` echoed on 200 and 400; 129-char id -> 400 (inferred from "
    "maxLength/pattern, synthesize-speech.md:149)")

FAKE_KEY = "sk_invalid_probe"   # obviously fake; redact() logs it as sk_...


def _ids(args, session_id=None, request_id=None):
    body, hdrs = {}, {}
    if args.id_transport in ("body", "both"):
        if session_id is not None:
            body["session_id"] = session_id
        if request_id is not None:
            body["request_id"] = request_id
    if args.id_transport in ("header", "both"):
        if session_id is not None:
            hdrs["X-External-Session-Id"] = session_id
        if request_id is not None:
            hdrs["X-External-Request-Id"] = request_id
    return body, hdrs


async def sync_case(case, args, *, session_id=None, request_id=None, key=None, sse=False):
    t = C.text20(f"p6 {case[:9]}")
    extra, hdrs = _ids(args, session_id, request_id)
    body = C.tts_body(t, sample_rate=C.SAMPLE_RATE,
                      output_format="pcm" if sse else "wav", **extra)
    return await C.http(case, "POST", C.tts_sse_url() if sse else C.tts_sync_url(),
                        json_body=body, mode="sse" if sse else "sync", chars=len(t),
                        key_override=key, extra_headers=hdrs or None,
                        meta={"sent_session_id": session_id, "sent_request_id": request_id,
                              "request_id_len": len(request_id) if request_id else None,
                              "id_transport": args.id_transport})


async def main(args):
    C.expected(EXPECTED)
    stamp = C.RUN.stamp
    if C.reserve_fake_401("fake401"):
        await sync_case("fake401", args, key=FAKE_KEY)
    else:
        C.info("  fake401: skipped (the single fake-key 401 was already used)")
    await C.sleep(2)
    await sync_case("id129", args, request_id="r" * 129)
    await C.sleep(2)
    await sync_case("id_whitespace", args, request_id="p6 bad id")
    await C.sleep(2)
    await sync_case("ok_sync_ids", args, session_id=f"p6-sess-{stamp}",
                    request_id=f"p6-req-sync-{stamp}")
    await C.sleep(2)
    await sync_case("ok_sse_ids", args, session_id=f"p6-sess-{stamp}",
                    request_id=f"p6-req-sse-{stamp}", sse=True)
    await C.sleep(2)
    ws = C.WS("ws_ids", C.tts_ws_url())
    try:
        if await ws.open():
            t = C.text20("p6 ws ids")
            await ws.send_json(C.tts_body(t, sample_rate=C.SAMPLE_RATE,
                                          session_id=f"p6-sess-{stamp}",
                                          request_id=f"p6-req-ws-{stamp}"),
                               chars=len(t), label="A")
            await ws.wait(ws.complete, 30)
            await ws.wait(ws.closed, 6)
    finally:
        await ws.close()
        ws.finish(sent_session_id=f"p6-sess-{stamp}", sent_request_id=f"p6-req-ws-{stamp}")
    if not C.RUN.dry:
        write_matrix()
    else:
        C.info(f"[dry-run] would write matrix of p1/p2/p3/p6 logs to {C.RUNS / 'p6_matrix.md'}")


def _endpoint(rec):
    kind, u = rec.get("kind"), rec.get("url") or ""
    host = u.split("://", 1)[-1].split("/", 1)[0]
    path = "/" + u.split("://", 1)[-1].split("/", 1)[-1].split("?", 1)[0] if "://" in u else u
    return f"{kind} {host}{path}"


def _status(rec):
    if rec.get("kind") == "ws":
        if rec.get("status") != 101:
            return f"handshake {rec.get('status') or rec.get('error')}"
        fc = rec.get("frame_counts") or {}
        tag = "complete" if "complete" in fc else ("error" if "error" in fc else "no-complete")
        return f"101+{tag}"
    return str(rec.get("status") if rec.get("status") is not None else rec.get("error", "?"))


def write_matrix():
    files = sorted(glob.glob(str(C.RUNS / "p[1236]_*.jsonl")))
    cells = defaultdict(lambda: {"n": 0, "xrid": 0, "ext_hdr": 0, "ext_ws": 0, "ids": [],
                                 "files": set()})
    for f in files:
        for line in Path(f).read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if rec.get("kind") not in ("sync", "sse", "ws", "stt"):
                continue
            c = cells[(_endpoint(rec), _status(rec))]
            c["n"] += 1
            c["files"].add(Path(f).name.split("_")[0])
            if rec.get("x_request_id"):
                c["xrid"] += 1
                if len(c["ids"]) < 3:
                    c["ids"].append(rec["x_request_id"])
            if any(k.lower().startswith("x-external") for k in (rec.get("external_ids") or {})):
                c["ext_hdr"] += 1
            if any(k.startswith("external_") for k in (rec.get("external_ids_in_frames") or {})):
                c["ext_ws"] += 1
    lines = [
        "# P6 correlation-ID matrix",
        "",
        f"Generated {C.ist_now()} from {len(files)} log files (p1/p2/p3/p6). "
        "Columns count records; `ext hdr` = any X-External-* response header; "
        "`ext ws` = external_* field in WS frames.",
        "",
        "| endpoint | status | n | x-request-id present | ext hdr echoed | ext ws echoed | "
        "probes | sample x-request-id |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for (ep, st), c in sorted(cells.items()):
        lines.append(f"| `{ep}` | {st} | {c['n']} | {c['xrid']}/{c['n']} | {c['ext_hdr']} | "
                     f"{c['ext_ws']} | {','.join(sorted(c['files']))} | "
                     f"{'<br>'.join(c['ids']) or '-'} |")
    out = C.RUNS / "p6_matrix.md"
    out.write_text(C.redact("\n".join(lines)) + "\n", encoding="utf-8")
    C.info(f"matrix written: {out}")


if __name__ == "__main__":
    p = C.parser("p6", "P6 correlation-ID matrix")
    p.add_argument("--id-transport", choices=["body", "header", "both"], default="body",
                   help="how to send session/request ids on sync/SSE (docs: body)")
    p.add_argument("--matrix-only", action="store_true",
                   help="only rebuild p6_matrix.md from existing logs (no network)")
    a = p.parse_args()
    if a.matrix_only:
        C.init("p6", True if a.dry_run else False)
        C.expected(EXPECTED)        # first record, even for a matrix-only rebuild
        if a.dry_run:
            print("[dry-run] would rebuild p6_matrix.md")
        else:
            write_matrix()
        raise SystemExit(0)
    C.run("p6", a, main)
