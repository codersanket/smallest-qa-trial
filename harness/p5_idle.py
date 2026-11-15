#!/usr/bin/env python3
"""P5 - idle-timer ownership (PLAN.md:27, plan-check-sara.md:23-24,45). No synthesis, $0.

Sockets (automatic websockets pings disabled on all of them):
  S1 default            no query; wait for the server close (tts.md:84-89: 60 s)
  S2 timeout5           ?timeout=5  (tts.md:97-98 verbatim)
  S3 timeout999         ?timeout=999 (clamped to 180 s, tts.md:98-99)
  S4 protoping_t5       ?timeout=5 + protocol (RFC 6455) pings every 3 s only
  S5 jsonping_t5        ?timeout=5 + {"type":"ping"} every 3 s (tts.md:109-117), held
                        --json-ping-hold s (default 150, PLAN.md:27) then closed by us
Wave 1 = S1, S2, S3 (3 sockets); wave 2 = S4, S5 (2 sockets). Never more than 3 open.
"""
import asyncio
import json

import common as C

# PLAN.md:27 EXPECTED, verbatim
EXPECTED = (
    "silent → closes 60 s with documented error JSON; `?timeout=5` → 5 s; `?timeout=999` → "
    "180 s; protocol-ping socket → UNDOCUMENTED for TTS (report observed); JSON-ping socket → "
    "survives 150 s")

DOC_TIMEOUT_MSG = "Connection timed out after 60 seconds of inactivity"   # tts.md:88


async def idle(case, timeout_q, expect_s, cap_s, ping=None, hold_s=None):
    ws = C.WS(case, C.tts_ws_url(timeout=timeout_q),
              meta={"timeout_query": timeout_q, "expected_close_s": expect_s, "ping": ping})
    pings = 0
    try:
        if not await ws.open():
            return
        limit = hold_s if hold_s is not None else cap_s
        if ping is None:
            await ws.wait(ws.closed, limit)
        else:
            elapsed = 0.0
            while elapsed < limit and not ws.closed.is_set():
                if await ws.wait(ws.closed, 3.0) or C.RUN.dry and pings >= 2:
                    break
                elapsed += 3.0
                if ping == "protocol":
                    await ws.protocol_ping()
                else:
                    await ws.send_json({"type": "ping"}, label="json-ping")
                pings += 1
    finally:
        closed_by_server = ws.closed.is_set()
        await ws.close("probe hold finished" if hold_s else "cap reached")
        ci = ws.close_info
        ws.finish(pings_sent=pings, closed_by_server_before_cap=closed_by_server,
                  close_s=round(ci["t_ms"] / 1000, 2) if ci.get("t_ms") is not None else None,
                  documented_error_frame=bool(ws.last_text) and
                  "Connection timed out" in json.dumps(ws.last_text),
                  documented_message_tts_md_88=DOC_TIMEOUT_MSG)


async def main(args):
    C.expected(EXPECTED)
    C.info("wave 1: default / timeout=5 / timeout=999 (3 sockets)")
    await asyncio.gather(
        idle("S1_default", None, 60, cap_s=75),
        idle("S2_timeout5", 5, 5, cap_s=20),
        idle("S3_timeout999", 999, 180, cap_s=200),
    )
    await C.sleep(2, "(between waves)")
    C.info("wave 2: protocol pings / JSON pings at timeout=5 (2 sockets)")
    await asyncio.gather(
        idle("S4_protoping_t5", 5, None, cap_s=args.protoping_hold, ping="protocol"),
        idle("S5_jsonping_t5", 5, None, cap_s=args.json_ping_hold, ping="json",
             hold_s=args.json_ping_hold),
    )


if __name__ == "__main__":
    p = C.parser("p5", "P5 idle-timer ownership (no synthesis)")
    p.add_argument("--json-ping-hold", type=int, default=150,
                   help="seconds to hold S5 (150, PLAN.md:27)")
    p.add_argument("--protoping-hold", type=int, default=40,
                   help="max seconds to hold S4 if the server does not close it (40)")
    a = p.parse_args()
    C.run("p5", a, main)
