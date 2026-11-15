#!/usr/bin/env python3
"""P4 by hand: does `complete` close the WebSocket, and how long is the tail?
Docs: tts.md:603 "complete — terminal frame; the server closes the WebSocket after this".
      tts.md:1032-1040 complete_backoff_ms default 4000 (wait after last chunk before `complete`).
EXPECTED (write before running): last chunk -> complete ~4000 ms; server closes after complete;
                                 B sent 1 s later fails.
Usage: python3 p4_hand.py [complete_backoff_ms]   (omit = default 4000)
"""
import asyncio, json, os, ssl, sys, time
from datetime import datetime
from websockets.asyncio.client import connect

KEY = dict(l.strip().split("=", 1) for l in open(os.path.join(os.path.dirname(__file__), "..", ".env")) if "=" in l)["SMALLEST_API_KEY"]
URL = "wss://api.smallest.ai/waves/v1/tts/live"
backoff = int(sys.argv[1]) if len(sys.argv) > 1 else None

def body(text):
    b = {"text": text, "voice_id": "rhea", "model": "lightning_v3.1_pro"}
    if backoff is not None: b["complete_backoff_ms"] = backoff
    return json.dumps(b)

async def one_request(ws, label, text, t0):
    await ws.send(body(text))
    sent = time.perf_counter() - t0
    chunks, last_chunk, req_id = 0, None, None
    async for m in ws:
        t = (time.perf_counter() - t0) * 1000
        f = json.loads(m)
        if f.get("status") == "chunk":
            chunks += 1; last_chunk = t; req_id = f.get("request_id", req_id)
        elif f.get("status") == "complete":
            print(f"  {label}: request_id={f.get('request_id', req_id)} chunks={chunks} "
                  f"last_chunk={last_chunk:.0f}ms complete={t:.0f}ms  tail={t - last_chunk:.0f}ms")
            return
        elif f.get("status") == "error":
            print(f"  {label}: ERROR frame {m[:200]}"); return

async def main():
    print(f"EXPECTED: tail ~{backoff if backoff is not None else 4000} ms; server closes after complete; "
          f"B fails.  ({datetime.now():%F %T} IST)")
    sslctx = ssl.create_default_context(cafile="/etc/ssl/cert.pem")  # python.org builds lack CA roots
    async with connect(URL, additional_headers={"Authorization": f"Bearer {KEY}"}, ssl=sslctx) as ws:
        t0 = time.perf_counter()
        await one_request(ws, "A", "Hand run A, twenty.", t0)
        await asyncio.sleep(1.0)
        try:
            await one_request(ws, "B (1 s after A complete, same socket)", "Hand run B, twenty.", t0)
            print("  => socket was REUSABLE after complete (docs tts.md:603 say it closes)")
        except Exception as e:
            print(f"  => B failed: {type(e).__name__}: {e}  (socket closed after complete, as documented)")
        await ws.close()
        print(f"  close: code={ws.close_code} reason={ws.close_reason!r} (closed by client)")

asyncio.run(main())
