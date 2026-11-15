#!/usr/bin/env python3
"""P8 webhook receiver. Listens on 127.0.0.1:8765 (Caddy reverse-proxies the public HTTPS
route to it). Makes NO outbound calls.

Path /smallest-hook/<run-token>:
  * token `r2-500` -> 500 exactly once (state persisted in out/.locks/p8_r2_500_done), then 204
  * anything else (any path, any method) -> 204
Every arrival is appended to out/runs/p8_hooks.jsonl: IST arrival time (ms), method, path,
token, client address, all headers (redacted), body sha256, body length and the body itself
(the transcript of our own synthetic audio), and the status we returned.
"""
import argparse
import hashlib
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import common as C

HOOKS_LOG = C.RUNS / "p8_hooks.jsonl"
STATE = C.LOCKS / "p8_r2_500_done"
FAIL_TOKEN = "r2-500"
MAX_BODY = 20 * 1024 * 1024
_lock = threading.Lock()


def _decide(token: str) -> int:
    with _lock:
        if token == FAIL_TOKEN and not STATE.exists():
            STATE.write_text(C.ist_now())
            return 500
    return 204


class Hook(BaseHTTPRequestHandler):
    server_version = "p8-receiver"

    def _handle(self):
        arrived = C.ist_now()
        try:  # malformed header must not reach the default (unredacted) error handler
            length = int(self.headers.get("Content-Length") or 0)
        except (TypeError, ValueError):
            length = 0
        body = self.rfile.read(min(length, MAX_BODY)) if length > 0 else b""
        path = self.path.split("?", 1)[0]
        parts = path.strip("/").split("/")
        token = parts[1] if len(parts) >= 2 and parts[0] == "smallest-hook" else None
        status = _decide(token) if token else 204
        text = body.decode("utf-8", "replace")
        rec = {"t_ist": arrived, "probe": "p8_receiver", "method": self.command,
               "path": self.path, "token": token, "client": self.client_address[0],
               "headers": list(self.headers.items()), "body_len": len(body),
               "body_sha256": hashlib.sha256(body).hexdigest(), "body": text[:200000],
               "returned_status": status}
        try:
            rec["body_json"] = json.loads(text) if text else None
        except ValueError:
            pass
        with _lock:
            C.RUNS.mkdir(parents=True, exist_ok=True)
            with open(HOOKS_LOG, "a", encoding="utf-8") as fh:
                fh.write(C.dumps_redacted(rec) + "\n")
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()
        # path/token are attacker-controlled (could carry a key): redacted via C.info
        C.info(f"{arrived} {self.command} {path} token={token} -> {status} "
               f"({len(body)} B, sha256 {rec['body_sha256'][:12]})")

    def _safe(self):
        try:
            self._handle()
        except Exception as e:  # never let a traceback print raw request data
            C.info(f"p8_receiver handler error: {type(e).__name__}")
            try:
                self.send_response(204); self.send_header("Content-Length", "0"); self.end_headers()
            except Exception:
                pass

    do_POST = do_GET = do_PUT = _safe

    def log_message(self, *_a):  # silence default stderr access log
        pass


def main():
    p = argparse.ArgumentParser(description="P8 webhook receiver (127.0.0.1:8765)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--reset-500", action="store_true",
                   help="forget that r2-500 already got its 500 (for a fresh P8 run 2)")
    p.add_argument("--dry-run", action="store_true", help="print config and exit; no bind")
    a = p.parse_args()
    if a.dry_run:
        print(f"[dry-run] would listen on {a.host}:{a.port}; log -> {HOOKS_LOG}; "
              f"500 once for token {FAIL_TOKEN} (already used: {STATE.exists()}); else 204")
        return 0
    C.LOCKS.mkdir(parents=True, exist_ok=True)
    if a.reset_500 and STATE.exists():
        STATE.unlink()
    srv = ThreadingHTTPServer((a.host, a.port), Hook)
    print(f"p8_receiver listening on {a.host}:{a.port} -> {HOOKS_LOG}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
