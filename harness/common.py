"""Shared harness plumbing for the Smallest.ai probe scripts (p1..p9, p_ttfb).

Everything a probe needs goes through here so the safety rules live in ONE place:
  * the API key is loaded lazily from /root/smallest-review/.env, never printed, and
    every logged string passes through redact();
  * a cross-process limiter (fcntl lock files in out/.locks/) caps the whole harness at
    2 HTTP requests in flight and 3 WebSockets open; no slot within 120 s -> abort;
  * deliberate-429 counter (<=10, >=5 s apart) and fake-key-401 counter (<=1 ever);
  * a character ledger (out/runs/ledger.csv) with a hard 60 000-char budget;
  * --dry-run: no key, no locks, no ledger, no files, and socket connect/DNS are
    monkeypatched to raise, so a dry run cannot touch the network.

Doc references (pages/ = saved docs, cited as file:line):
  sync  POST https://api.smallest.ai/waves/v1/tts            synthesize-speech.md:11
  SSE   POST https://api.smallest.ai/waves/v1/tts/live       synthesize-speech-sse.md:11
        frames `data: {"audio": ..., "done": false}` / `{"done": true}`  synthesize-speech-sse.md:29-31
  WS    wss://api.smallest.ai/waves/v1/tts/live              tts.md:11,94,128
        auth = Authorization: Bearer header                   models__text-to-speech__streaming.md:78
        frames status chunk|word_timestamp|complete           tts.md:588-607
  STT   POST https://api.smallest.ai/waves/v1/stt/?model=...  speech-to-text__transcribe.md:11-12
  Regions api.india.smallest.ai / api.us.smallest.ai         api-reference__concurrency-and-limits.md:80-83
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import csv
import datetime as _dt
import errno
import fcntl
import hashlib
import json
import os
import re
import socket
import struct
import sys
import time
from pathlib import Path

ROOT = Path("/root/smallest-review")
OUT = ROOT / "out"
LOCKS = OUT / ".locks"
RUNS = OUT / "runs"
AUDIO = OUT / "audio"
FIXTURES = OUT / "fixtures"
ENV_FILE = ROOT / ".env"
LEDGER = RUNS / "ledger.csv"

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover - tzdata missing
    IST = _dt.timezone(_dt.timedelta(hours=5, minutes=30), "IST")

# ---------------------------------------------------------------- endpoints
HOSTS = {
    "generic": "api.smallest.ai",       # synthesize-speech.md:11
    "india": "api.india.smallest.ai",   # concurrency-and-limits.md:82
    "us": "api.us.smallest.ai",         # concurrency-and-limits.md:83; stt overview.md:101
}
TTS_SYNC_PATH = "/waves/v1/tts"         # synthesize-speech.md:11
TTS_LIVE_PATH = "/waves/v1/tts/live"    # SSE: synthesize-speech-sse.md:11 ; WS: tts.md:11
STT_PATH = "/waves/v1/stt/"             # transcribe.md:11 (trailing slash as documented)

MODEL_PRO = "lightning_v3.1_pro"        # synthesize-speech.md:131-132
MODEL_STD = "lightning_v3.1"            # synthesize-speech.md:132 (retiring 30 Nov 2026)
# Pro voice: `rhea` is in the Pro catalog (lightning-v-3-1-pro.md:348,354) and NOT in the
# v3.1 catalog (lightning-v-3-1.md:214-314). We avoid the docs' example `meher`
# (synthesize-speech.md:63) because lightning-v-3-1-pro.md:553 says meher is a shared
# base-queue voice, which would blur the P1 mismatch test.
PRO_VOICE = "rhea"
# Standard voice: `magnus` (lightning-v-3-1.md:218; API default synthesize-speech.md:130),
# absent from the Pro catalog (lightning-v-3-1-pro.md:335-420).
STD_VOICE = "magnus"
SAMPLE_RATE = 24000                     # allowed 8000/16000/24000/44100 synthesize-speech.md:134

# ---------------------------------------------------------------- limits
MAX_HTTP_IN_FLIGHT = 2
MAX_WS_OPEN = 3
SLOT_TIMEOUT_S = 120.0
MAX_DELIBERATE_429 = 10
MIN_GAP_429_S = 5.0                     # plan-check-sara.md:62
MAX_FAKE_401 = 1
CHAR_BUDGET = 60000

_KEY_RE = re.compile(r"sk_[A-Za-z0-9_\-]+")


class Abort(Exception):
    """Raised to stop a probe script cleanly (logged, exit code 3)."""


# ---------------------------------------------------------------- run state
class _Run:
    probe = "p?"
    dry = False
    start = None
    stamp = ""
    log_path: Path | None = None
    chars_sent = 0
    argv: list[str] = []
    n_records = 0


RUN = _Run()
_SECRET_LITERALS: dict[str, str] = {}   # literal -> replacement (api key, filtered word)


def ist_now() -> str:
    return _dt.datetime.now(IST).isoformat(timespec="milliseconds")


def _stamp(dt: _dt.datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%S") + f"{dt.microsecond // 1000:03d}IST"


def add_sensitive(literal: str, replacement: str) -> None:
    """Register a literal that must never appear in logs (e.g. the filter word)."""
    if literal:
        _SECRET_LITERALS[literal] = replacement


def redact(obj):
    """Recursively scrub keys/sensitive literals. Anything that is not str/dict/list/tuple/
    int/float/bool/None is converted to str FIRST, so no object repr escapes redaction."""
    if obj is None or isinstance(obj, (bool, int, float)):
        return obj
    if isinstance(obj, (bytes, bytearray)):
        obj = bytes(obj).decode("utf-8", "replace")
    elif isinstance(obj, (set, frozenset)):
        obj = sorted(obj, key=str)
    elif not isinstance(obj, (str, dict, list, tuple)):
        obj = str(obj)
    if isinstance(obj, str):
        s = obj
        for lit, rep in _SECRET_LITERALS.items():
            s = s.replace(lit, rep)
        return _KEY_RE.sub("sk_...", s)
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if isinstance(k, str) and k.lower() in ("authorization", "proxy-authorization"):
                out[k] = "Bearer sk_..." if v else v
            else:
                out[redact(k) if not isinstance(k, (bool, int, float)) else k] = redact(v)
        return out
    if isinstance(obj, (list, tuple)):
        items = [redact(x) for x in obj]
        # header pair ["Authorization", "..."]
        if len(items) == 2 and isinstance(items[0], str) and items[0].lower() in (
                "authorization", "proxy-authorization"):
            items[1] = "Bearer sk_..."
        return items
    return obj


def dumps_redacted(obj, **kw) -> str:
    """json.dumps(redact(obj)) and then redact the serialized text once more."""
    return redact(json.dumps(redact(obj), ensure_ascii=False, default=str, **kw))


def _install_network_guard() -> None:
    """Make any socket connect / DNS lookup raise. Used for --dry-run."""
    def _blocked(*_a, **_k):
        raise RuntimeError("network access is disabled in --dry-run")
    socket.socket.connect = _blocked          # type: ignore[assignment]
    socket.socket.connect_ex = _blocked       # type: ignore[assignment]
    socket.create_connection = _blocked       # type: ignore[assignment]
    socket.getaddrinfo = _blocked             # type: ignore[assignment]
    socket.gethostbyname = _blocked           # type: ignore[assignment]


def parser(probe: str, description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog=f"{probe}", description=description)
    p.add_argument("--dry-run", action="store_true",
                   help="print the (redacted) requests that would be made and exit; no network")
    return p


def init(probe: str, dry_run: bool) -> None:
    RUN.probe = probe
    RUN.dry = dry_run
    now = _dt.datetime.now(IST)
    RUN.start = now
    RUN.stamp = _stamp(now)
    RUN.argv = list(sys.argv)
    RUN.chars_sent = 0
    if dry_run:
        _install_network_guard()
        RUN.log_path = None
    else:
        for d in (LOCKS, RUNS, AUDIO, FIXTURES):
            d.mkdir(parents=True, exist_ok=True)
        RUN.log_path = RUNS / f"{probe}_{RUN.stamp}.jsonl"


# ---------------------------------------------------------------- key
_KEY: str | None = None


def api_key() -> str:
    """Load SMALLEST_API_KEY from .env at runtime. Never call in --dry-run."""
    global _KEY
    if RUN.dry:
        raise RuntimeError("api_key() must not be called in --dry-run")
    if _KEY is None:
        key = None
        with open(ENV_FILE, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line.startswith("export "):
                    line = line[7:].strip()
                if line.startswith("SMALLEST_API_KEY="):
                    key = line.split("=", 1)[1].strip().strip('"').strip("'")
        if not key:
            raise Abort("SMALLEST_API_KEY not found in .env")
        _KEY = key
        add_sensitive(key, "sk_...")      # even if the key does not look like sk_...
    return _KEY


# ---------------------------------------------------------------- logging
def log(probe: str, record: dict) -> None:
    """Append one redacted JSON line to out/runs/<probe>_<IST-start>.jsonl."""
    rec = {"t_ist": ist_now(), "probe": probe}
    rec.update(record)
    RUN.n_records += 1
    line = dumps_redacted(rec)
    if RUN.dry:
        print(f"[dry-run log] {line[:400]}")
        return
    with open(RUN.log_path, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def expected(text: str) -> None:
    """Print + log the pre-written EXPECTED line. Must be called before any network I/O."""
    print(f"EXPECTED: {text}", flush=True)
    log(RUN.probe, {"case": "_expected", "expected": text,
                    "command": " ".join(RUN.argv), "dry_run": RUN.dry,
                    "script_start_ist": RUN.start.isoformat(timespec="milliseconds")})


def note(case: str, **fields) -> None:
    log(RUN.probe, {"case": case, "event": "note", **fields})


def info(msg: str) -> None:
    print(redact(msg), flush=True)


# ---------------------------------------------------------------- file locks
@contextlib.contextmanager
def _flock(name: str):
    LOCKS.mkdir(parents=True, exist_ok=True)
    fd = os.open(LOCKS / name, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


class Slot:
    """Global cross-process slot: kind 'http' (2) or 'ws' (3). Async context manager.

    Each slot is a lock file out/.locks/<kind>.<i>.lock held with flock(LOCK_EX|LOCK_NB);
    the kernel releases it if the process dies, so slots cannot leak.
    """

    def __init__(self, kind: str, label: str = ""):
        assert kind in ("http", "ws")
        self.kind = kind
        self.n = MAX_HTTP_IN_FLIGHT if kind == "http" else MAX_WS_OPEN
        self.label = label
        self.fd = None
        self.index = None
        self.waited_ms = 0.0

    async def acquire(self):
        if RUN.dry:
            info(f"[dry-run] would acquire global {self.kind} slot ({self.label})")
            return self
        LOCKS.mkdir(parents=True, exist_ok=True)
        t0 = time.perf_counter()
        deadline = t0 + SLOT_TIMEOUT_S
        while True:
            for i in range(self.n):
                fd = os.open(LOCKS / f"{self.kind}.{i}.lock", os.O_RDWR | os.O_CREAT, 0o600)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as e:
                    os.close(fd)
                    if e.errno not in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                        raise
                    continue
                os.ftruncate(fd, 0)
                os.write(fd, f"{os.getpid()} {RUN.probe} {self.label} {ist_now()}\n".encode())
                self.fd, self.index = fd, i
                self.waited_ms = (time.perf_counter() - t0) * 1000
                return self
            if time.perf_counter() > deadline:
                raise Abort(f"no global {self.kind} slot free within {SLOT_TIMEOUT_S:.0f} s "
                            f"({self.label}); another harness script is running?")
            await asyncio.sleep(0.05)

    def release(self):
        if self.fd is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None

    async def __aenter__(self):
        return await self.acquire()

    async def __aexit__(self, *exc):
        self.release()
        return False


# ---------------------------------------------------------------- counters
_COUNTER_MAX = {"429count": MAX_DELIBERATE_429, "401count": MAX_FAKE_401}


def _read_counter(name: str) -> dict:
    """Counter state. Missing file -> fresh counter. An existing file that is empty,
    unparsable or out of range ABORTS: silently resetting would re-enable spent attempts."""
    p = LOCKS / name
    try:
        raw = p.read_text()
    except FileNotFoundError:
        return {"count": 0}
    try:
        st = json.loads(raw)
    except ValueError:
        raise Abort(f"{p} is corrupt (not JSON); fix it by hand, refusing to guess") from None
    if not isinstance(st, dict):
        raise Abort(f"{p} is corrupt (not an object)")
    count = st.get("count")
    if type(count) is not int or not 0 <= count <= _COUNTER_MAX[name]:
        raise Abort(f"{p} has invalid count {count!r} (expected int 0..{_COUNTER_MAX[name]})")
    for k in ("last_epoch", "last_dispatch_epoch"):
        v = st.get(k, 0.0)
        if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0 or v > time.time() + 60:
            raise Abort(f"{p} has invalid {k} {v!r}")
    return st


def _write_counter(name: str, data: dict) -> None:
    p = LOCKS / name
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    os.replace(tmp, p)


class Deliberate:
    """One deliberate over-limit attempt (a 'window'), returned by reserve_429().

    async with C.reserve_429(case) as dw:
        ... start the in-flight request A ...
        ... pass deliberate=dw to the over-limit http()/WS.open(), or call dw.dispatch()
            immediately before an over-limit WS send ...

    * Entering takes out/.locks/deliberate.lock (cross-process, polled, 120 s -> Abort), so
      deliberate windows of different scripts never interleave; the counter is incremented
      then (reservation = counted, even if the attempt never happens: conservative).
    * Before returning it waits until >= 5 s have passed since the last ACTUAL dispatch.
    * dispatch() runs right before the over-limit bytes go out; it re-checks the 5 s gap
      against the persisted last_dispatch_epoch and records the actual dispatch time.
      Slot waits/DNS/handshakes between entry and dispatch only widen the gap.
    """

    def __init__(self, case: str):
        self.case = case
        self.fd = None
        self.count = None
        self.dispatched_at = None

    async def __aenter__(self):
        if RUN.dry:
            cur = _read_counter("429count")["count"]
            info(f"[dry-run] would reserve deliberate-429 window for {self.case} (counter now "
                 f"{cur}/{MAX_DELIBERATE_429}; >= {MIN_GAP_429_S:.0f} s after last dispatch)")
            self.count = cur
            return self
        LOCKS.mkdir(parents=True, exist_ok=True)
        fd = os.open(LOCKS / "deliberate.lock", os.O_RDWR | os.O_CREAT, 0o600)
        deadline = time.perf_counter() + SLOT_TIMEOUT_S
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as e:
                if e.errno not in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                    os.close(fd)
                    raise
            if time.perf_counter() > deadline:
                os.close(fd)
                raise Abort(f"deliberate-429 window busy for {SLOT_TIMEOUT_S:.0f} s ({self.case})")
            await asyncio.sleep(0.1)
        self.fd = fd
        try:
            with _flock("counters.lock"):
                st = _read_counter("429count")
                if st["count"] >= MAX_DELIBERATE_429:
                    raise Abort(f"deliberate-429 counter is {st['count']} (max "
                                f"{MAX_DELIBERATE_429}); refusing {self.case}")
                st.update(count=st["count"] + 1, last_epoch=time.time(), last_ist=ist_now(),
                          last_case=f"{RUN.probe}:{self.case}")
                _write_counter("429count", st)
                self.count = st["count"]
                last = st.get("last_dispatch_epoch", 0.0)
            note(self.case, counter_429=self.count, event="deliberate_reserved")
            wait = MIN_GAP_429_S - (time.time() - last)
            if wait > 0:
                await asyncio.sleep(wait)
        except BaseException:
            self._unlock()
            raise
        return self

    def dispatch(self) -> float:
        """Call immediately before the over-limit request is written. Returns epoch."""
        if RUN.dry:
            info(f"[dry-run] deliberate dispatch {self.case}")
            self.dispatched_at = time.time()
            return self.dispatched_at
        if self.fd is None:
            raise Abort(f"deliberate dispatch outside its window ({self.case})")
        with _flock("counters.lock"):
            st = _read_counter("429count")
            gap = time.time() - st.get("last_dispatch_epoch", 0.0)
            if gap < MIN_GAP_429_S:   # impossible while we hold deliberate.lock; belt+braces
                raise Abort(f"deliberate dispatch {self.case} only {gap:.2f} s after the last one")
            now = time.time()
            st.update(last_dispatch_epoch=now, last_dispatch_ist=ist_now(),
                      last_dispatch_case=f"{RUN.probe}:{self.case}")
            _write_counter("429count", st)
        self.dispatched_at = now
        return now

    def _unlock(self):
        if self.fd is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None

    async def __aexit__(self, *exc):
        note(self.case, event="deliberate_window_closed", counter_429=self.count,
             dispatched_epoch=self.dispatched_at, dispatched=self.dispatched_at is not None)
        self._unlock()
        return False


def reserve_429(case: str) -> Deliberate:
    """Deliberate over-limit window (see Deliberate). Use as `async with`."""
    return Deliberate(case)


def count_429() -> int:
    return _read_counter("429count")["count"]


def reserve_fake_401(case: str) -> bool:
    """True if the single, ever, fake-key 401 may be sent now (and records it)."""
    if RUN.dry:
        used = _read_counter("401count")["count"]
        info(f"[dry-run] fake-key 401 counter is {used}/{MAX_FAKE_401}")
        return used < MAX_FAKE_401
    with _flock("counters.lock"):
        st = _read_counter("401count")
        if st["count"] >= MAX_FAKE_401:
            note(case, skipped="fake-key 401 already used once; never repeated")
            return False
        _write_counter("401count", {"count": st["count"] + 1, "ist": ist_now(),
                                    "case": f"{RUN.probe}:{case}"})
        return True


# ---------------------------------------------------------------- ledger / budget
LEDGER_FIELDS = ["ist", "probe", "case", "chars", "status", "reservation"]


def ledger_total() -> int:
    """Sum of `chars` over ledger.csv. Unparsable chars ABORT (never undercount)."""
    if not LEDGER.exists():
        return 0
    total = 0
    with open(LEDGER, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            try:
                total += int(row.get("chars") or 0)
            except (ValueError, TypeError):
                raise Abort(f"{LEDGER} has an unparsable chars value {row!r}") from None
    return total


def _ledger_write(row: list) -> None:
    new = not LEDGER.exists()
    with open(LEDGER, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(LEDGER_FIELDS)
        w.writerow(row)
        fh.flush()
        os.fsync(fh.fileno())


def reserve_chars(case: str, chars: int) -> str | None:
    """Budget check + reservation in ONE critical section under ledger.lock, persisted
    BEFORE anything is sent: the row (status 'reserved') already counts the chars, so a
    concurrent script sees them and a crash after this point still leaves them counted.
    Reserved chars are never refunded (a send whose outcome is uncertain counts in full);
    settle() only appends a 0-char status row."""
    if chars <= 0:
        return None
    RUN.chars_sent += chars
    if RUN.dry:
        return None
    RUNS.mkdir(parents=True, exist_ok=True)
    rid = f"{os.getpid()}-{time.time_ns()}"
    with _flock("ledger.lock"):
        total = ledger_total()
        if total + chars > CHAR_BUDGET:
            RUN.chars_sent -= chars
            raise Abort(f"character budget: ledger {total} + {chars} ({case}) > {CHAR_BUDGET}")
        _ledger_write([ist_now(), RUN.probe, case, chars, "reserved", rid])
    return rid


def settle(rid: str | None, case: str, status) -> None:
    """Record the outcome of a reserved send (0 chars; the reservation row already counts)."""
    if RUN.dry or rid is None:
        return
    with _flock("ledger.lock"):
        _ledger_write([ist_now(), RUN.probe, case, 0, status, rid])


# ---------------------------------------------------------------- helpers
def url(host: str, path: str, scheme: str = "https", query: str = "") -> str:
    h = HOSTS.get(host, None)
    if h is None:
        raise ValueError(f"unknown host alias {host!r}; use one of {sorted(HOSTS)}")
    return f"{scheme}://{h}{path}" + (f"?{query}" if query else "")


def tts_sync_url(host="generic"):
    return url(host, TTS_SYNC_PATH)


def tts_sse_url(host="generic"):
    return url(host, TTS_LIVE_PATH)


def tts_ws_url(host="generic", timeout=None):
    return url(host, TTS_LIVE_PATH, scheme="wss",
               query=f"timeout={timeout}" if timeout is not None else "")


def stt_url(host="generic", **params):
    from urllib.parse import urlencode
    return url(host, STT_PATH, query=urlencode(params))


_PROSE = ("Our order desk confirms your parcel left the warehouse this morning. "
          "It should reach you within two working days, and we will text you "
          "when the courier is nearby. ")


def prose(n: int) -> str:
    """Exactly n chars of plain English, first/last char non-whitespace."""
    s = (_PROSE * (n // len(_PROSE) + 2))[:n]
    if n >= 1 and s[-1].isspace():
        s = s[:-1] + "."
    assert len(s) == n and not s[0].isspace()
    return s


def text20(tag: str = "") -> str:
    """Exactly 20 chars, e.g. 'Probe p1 case a now.'"""
    s = f"Probe {tag} ok now."
    s = (s + " " * 20)[:20] if len(s) < 20 else s[:20]
    if s[-1].isspace():
        s = s.rstrip()
        s = s + "." * (20 - len(s))
    assert len(s) == 20
    return s


def tts_body(text: str, *, voice=PRO_VOICE, model=MODEL_PRO, **extra) -> dict:
    body = {"text": text, "voice_id": voice}
    if model is not None:
        body["model"] = model
    body.update(extra)
    return body


def body_summary(body) -> dict | None:
    """Request body for the log: text truncated to 80 chars + its length; never the key."""
    if body is None:
        return None
    b = dict(body)
    if "text" in b and isinstance(b["text"], str):
        t = b.pop("text")
        b = {"text": t[:80], "text_len": len(t), **b}
    return b


def parse_wav(data: bytes) -> dict | None:
    if len(data) < 12 or data[:4] != b"RIFF" or data[8:12] != b"WAVE":
        return None
    out = {"riff_size": int.from_bytes(data[4:8], "little")}
    i = 12
    while i + 8 <= len(data):
        cid = data[i:i + 4]
        size = int.from_bytes(data[i + 4:i + 8], "little")
        if cid == b"fmt " and i + 24 <= len(data):
            fmt, ch, sr, brate, align, bits = struct.unpack("<HHIIHH", data[i + 8:i + 24])
            out.update(format_tag=fmt, channels=ch, sample_rate=sr, byte_rate=brate,
                       block_align=align, bits_per_sample=bits)
        elif cid == b"data":
            actual = len(data) - (i + 8)
            out["data_offset"] = i + 8
            out["data_size_header"] = size
            out["data_size_actual"] = actual
            if out.get("byte_rate"):
                out["duration_s"] = round(min(size, actual) / out["byte_rate"], 3)
            break
        i += 8 + size + (size & 1)
    return out


def _ext_ids_from_headers(headers) -> dict:
    ids = {}
    for k, v in headers:
        kl = k.lower()
        if "external" in kl or kl in ("x-request-id", "x-session-id", "request-id",
                                      "x-amzn-requestid", "x-amz-cf-id", "cf-ray"):
            ids[k] = v
    return ids


def _resolve(hostname: str, port: int = 443) -> tuple[float, list[str]]:
    t0 = time.perf_counter()
    infos = socket.getaddrinfo(hostname, port, proto=socket.IPPROTO_TCP)
    return (time.perf_counter() - t0) * 1000, sorted({i[4][0] for i in infos})


async def sleep(seconds: float, why: str = "") -> None:
    if RUN.dry:
        info(f"[dry-run] would sleep {seconds:.1f} s {why}")
        return
    await asyncio.sleep(seconds)


def save_audio_file(case: str, data: bytes, ext: str) -> str:
    AUDIO.mkdir(parents=True, exist_ok=True)
    p = AUDIO / f"{RUN.probe}_{case}_{_stamp(_dt.datetime.now(IST))}.{ext}"
    p.write_bytes(data)
    return str(p)


# ---------------------------------------------------------------- HTTP / SSE
async def http(case: str, method: str, target_url: str, *, json_body: dict | None = None,
               content: bytes | None = None, content_type: str | None = None,
               mode: str = "sync", chars: int = 0, key_override: str | None = None,
               extra_headers: dict | None = None, on_first_chunk=None,
               abort_after_first_chunk: bool = False, save_audio: bool = False,
               audio_ext: str = "wav", timeout_s: float = 120.0, meta: dict | None = None,
               deliberate: "Deliberate | None" = None,
               dispatched: asyncio.Event | None = None) -> dict:
    """One HTTP request (mode: 'sync' TTS, 'sse' TTS, 'stt', or 'raw'), fully logged.

    Returns the log record (dict). on_first_chunk: async callable AWAITED on the first SSE
    audio chunk; the SSE reader does not consume further until it returns (see
    dispatch_barrier). deliberate: Deliberate window -> dispatch() right before send.
    dispatched: Event set once the request body is on the wire (httpx trace) or the
    request ended without getting there. rec['perf_s'] holds absolute time.perf_counter()
    marks (comparable across requests of one process) for overlap analysis.
    In --dry-run prints the request and returns a stub record."""
    headers = {"Content-Type": content_type or ("application/json" if json_body is not None
                                                 else "application/octet-stream")}
    if extra_headers:
        headers.update(extra_headers)
    req_summary = body_summary(json_body) if json_body is not None else (
        {"bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()} if content else None)

    if RUN.dry:
        shown = dict(headers, Authorization="Bearer sk_..." if not key_override else
                     f"Bearer {key_override}")
        info(f"[dry-run] {case}: {method} {target_url} mode={mode} chars={chars}\n"
             f"           headers={json.dumps(redact(shown))}\n"
             f"           body={json.dumps(redact(req_summary), ensure_ascii=False)[:300]}"
             + (f"\n           meta={json.dumps(redact(meta))}" if meta else ""))
        reserve_chars(case, chars)
        if deliberate is not None:
            deliberate.dispatch()
        if dispatched is not None:
            dispatched.set()
        if on_first_chunk is not None:
            await on_first_chunk()
        p = time.perf_counter()
        return {"case": case, "status": None, "dry_run": True,
                "perf_s": {"send_start": p, "request_sent": p, "headers": p, "end": p}}

    import httpx  # imported lazily so --dry-run works even without httpx

    headers["Authorization"] = f"Bearer {key_override or api_key()}"
    if key_override:
        add_sensitive(key_override, "sk_...")
    from urllib.parse import urlsplit
    hostname = urlsplit(target_url).hostname

    rec: dict = {"case": case, "kind": mode, "method": method, "url": target_url,
                 "req_headers": {k: v for k, v in headers.items()},
                 "req_body": req_summary, "chars_sent": chars, "meta": meta or {}}
    marks: dict[str, float] = {}
    perf: dict[str, float] = {}
    rec["perf_s"] = perf
    peer = {}
    slot = Slot("http", case)
    try:
        await slot.acquire()
    except BaseException:
        if dispatched is not None:
            dispatched.set()      # never leave a barrier hanging
        raise
    rec["slot_wait_ms"] = round(slot.waited_ms, 1)
    try:
        rid = reserve_chars(case, chars)      # budget check + persisted reservation
    except BaseException:
        slot.release()
        if dispatched is not None:
            dispatched.set()
        raise
    status_for_ledger = "exc"
    t0 = time.perf_counter()

    async def trace(name, inf):
        now = time.perf_counter()
        marks.setdefault(name, round((now - t0) * 1000, 2))
        if name.endswith("connect_tcp.complete"):
            with contextlib.suppress(Exception):
                peer["addr"] = inf["return_value"].get_extra_info("server_addr")
        if name.endswith("send_request_body.complete"):
            perf.setdefault("request_sent", now)
            rec.setdefault("t_request_sent_ist", ist_now())
            if dispatched is not None:
                dispatched.set()

    try:
        try:
            dns_ms, ips = await asyncio.get_running_loop().run_in_executor(
                None, _resolve, hostname)
            rec["dns_answers"] = ips
        except OSError as e:
            dns_ms, rec["dns_error"] = None, repr(e)
        if deliberate is not None:
            rec["deliberate_dispatch_epoch"] = deliberate.dispatch()
            rec["deliberate_counter"] = deliberate.count
        t0 = time.perf_counter()
        perf["send_start"] = t0
        rec["t_send_ist"] = ist_now()
        async with httpx.AsyncClient(http2=False, trust_env=False,
                                     timeout=httpx.Timeout(timeout_s, connect=15.0)) as client:
            req = client.build_request(method, target_url, headers=headers,
                                       json=json_body if json_body is not None else None,
                                       content=content if json_body is None else None,
                                       extensions={"trace": trace})
            resp = await client.send(req, stream=True)
            perf["headers"] = time.perf_counter()
            perf.setdefault("request_sent", perf["headers"])   # trace hook missing: upper bound
            if dispatched is not None:
                dispatched.set()
            t_headers = (perf["headers"] - t0) * 1000
            rec["status"] = resp.status_code
            status_for_ledger = resp.status_code
            rec["http_version"] = resp.http_version
            hdrs = list(resp.headers.multi_items())
            rec["resp_headers"] = hdrs
            rec["x_request_id"] = resp.headers.get("x-request-id")
            rec["external_ids"] = _ext_ids_from_headers(hdrs)
            ctype = resp.headers.get("content-type", "")
            rec["content_type"] = ctype
            sha = hashlib.sha256()
            nbytes = 0
            first_body_ms = None
            if mode == "sse" and resp.status_code == 200 and "event-stream" in ctype:
                sse = {"events": 0, "audio_chunks": 0, "audio_bytes": 0, "first_audio_ms": None,
                       "done_frame": None, "last_data": None, "event_names": {},
                       "aborted_by_client": False, "non_audio_frames": []}
                async for line in resp.aiter_lines():
                    if first_body_ms is None:
                        first_body_ms = (time.perf_counter() - t0) * 1000
                    nbytes += len(line.encode()) + 1
                    if line.startswith("event:"):
                        ev = line[6:].strip()
                        sse["event_names"][ev] = sse["event_names"].get(ev, 0) + 1
                        continue
                    if not line.startswith("data:"):
                        continue
                    sse["events"] += 1
                    raw = line[5:].strip()
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        sse["last_data"] = raw[:500]
                        sse["non_audio_frames"].append(raw[:500])
                        continue
                    audio = msg.get("audio") if isinstance(msg, dict) else None
                    if audio:
                        pcm = base64.b64decode(audio)
                        sha.update(pcm)
                        sse["audio_chunks"] += 1
                        sse["audio_bytes"] += len(pcm)
                        if sse["first_audio_ms"] is None:
                            sse["first_audio_ms"] = round((time.perf_counter() - t0) * 1000, 2)
                            sse["first_audio_ist"] = ist_now()
                            perf["first_audio"] = time.perf_counter()
                            if on_first_chunk is not None:
                                # awaited: A is not consumed further until the callback
                                # (e.g. dispatch_barrier) returns
                                await on_first_chunk()
                                perf["barrier_released"] = time.perf_counter()
                            if abort_after_first_chunk:
                                sse["aborted_by_client"] = True
                                sse["aborted_ist"] = ist_now()
                                break
                        msg = dict(msg, audio=f"<{len(audio)} b64 chars>")
                    else:
                        sse["non_audio_frames"].append(msg)
                    sse["last_data"] = msg
                    if isinstance(msg, dict) and msg.get("done") is True:
                        sse["done_frame"] = msg
                sse["non_audio_frames"] = sse["non_audio_frames"][:20]
                rec["sse"] = sse
                rec["audio_bytes"] = sse["audio_bytes"]
                rec["audio_sha256"] = sha.hexdigest() if sse["audio_chunks"] else None
            else:
                chunks = []
                async for chunk in resp.aiter_bytes():
                    if first_body_ms is None:
                        first_body_ms = (time.perf_counter() - t0) * 1000
                    chunks.append(chunk)
                body = b"".join(chunks)
                nbytes = len(body)
                is_audio = resp.status_code == 200 and mode == "sync" and (
                    ctype.startswith("audio/") or body[:4] == b"RIFF"
                    or "octet-stream" in ctype)
                if is_audio:
                    rec["audio_sha256"] = hashlib.sha256(body).hexdigest()
                    rec["audio_bytes"] = nbytes
                    rec["wav"] = parse_wav(body)
                    if save_audio:
                        rec["audio_file"] = save_audio_file(case, body, audio_ext)
                else:
                    text = body.decode("utf-8", "replace")
                    rec["body_text"] = text[:8000]
                    rec["body_truncated"] = len(text) > 8000
                    with contextlib.suppress(ValueError):
                        rec["body_json"] = json.loads(text)
                    rec["body_sha256"] = hashlib.sha256(body).hexdigest()
            await resp.aclose()
            perf["end"] = time.perf_counter()
            t_end = (perf["end"] - t0) * 1000
            rec["resp_bytes"] = nbytes
            addr = peer.get("addr")
            rec["resolved_ip"] = addr[0] if isinstance(addr, (tuple, list)) and addr else (
                rec.get("dns_answers") or [None])[0]
            rec["timings_ms"] = {
                "dns": round(dns_ms, 2) if dns_ms is not None else None,
                "connect": _span(marks, "connection.connect_tcp"),
                "tls": _span(marks, "connection.start_tls"),
                "ttfb_headers": round(t_headers, 2),
                "first_body_byte": round(first_body_ms, 2) if first_body_ms else None,
                "total": round(t_end, 2),
                "trace_marks": marks,
                "clock": "time.perf_counter from just before client.send (dns measured separately)",
            }
    except Abort:
        raise
    except Exception as e:  # network/protocol errors are data, not crashes
        rec["error"] = f"{type(e).__name__}: {e}"
        rec["timings_ms"] = {"total": round((time.perf_counter() - t0) * 1000, 2),
                             "trace_marks": marks}
        status_for_ledger = f"exc:{type(e).__name__}"
    finally:
        perf.setdefault("end", time.perf_counter())
        if dispatched is not None:
            dispatched.set()
        slot.release()
        settle(rid, case, status_for_ledger)
    log(RUN.probe, rec)
    info(f"  {case}: status={rec.get('status')} bytes={rec.get('resp_bytes')} "
         f"x-request-id={rec.get('x_request_id')} total_ms="
         f"{rec.get('timings_ms', {}).get('total')}"
         + (f" error={rec['error']}" if rec.get("error") else ""))
    return rec


def _span(marks, prefix):
    a, b = marks.get(prefix + ".started"), marks.get(prefix + ".complete")
    return round(b - a, 2) if a is not None and b is not None else None


def dispatch_barrier(start_b, timeout_s: float = 20.0):
    """Awaited first-chunk barrier for 'B during A' probes.

    Returns (callback, state). Pass callback as http(on_first_chunk=...) for A. On A's first
    audio chunk it starts start_b(dispatched_event) as a task and BLOCKS A's reader until B's
    request body is on the wire (dispatched set by http()) or B has finished/failed, or
    timeout_s passes. state: task (B), perf marks, barrier_timeout."""
    state = {"task": None, "dispatched": asyncio.Event(), "t_first_chunk": None,
             "t_released": None, "barrier_timeout": False}

    async def cb():
        state["t_first_chunk"] = time.perf_counter()
        state["task"] = asyncio.ensure_future(start_b(state["dispatched"]))
        waiter = asyncio.ensure_future(state["dispatched"].wait())
        done, _ = await asyncio.wait({waiter, state["task"]}, timeout=timeout_s,
                                     return_when=asyncio.FIRST_COMPLETED)
        if waiter not in done:
            waiter.cancel()
        state["barrier_timeout"] = not done
        state["t_released"] = time.perf_counter()
    return cb, state


def overlap(a_rec: dict, b_rec: dict | None) -> dict:
    """Did B actually overlap A in flight? Uses absolute perf_counter marks (same process)."""
    ap, bp = (a_rec or {}).get("perf_s") or {}, (b_rec or {}).get("perf_s") or {}
    a_end, b_sent, b_hdr = ap.get("end"), bp.get("request_sent"), bp.get("headers")
    out = {"A_first_audio_to_B_sent_ms": None, "B_sent_before_A_end": None,
           "B_headers_before_A_end": None, "B_sent_to_A_end_ms": None}
    if ap.get("first_audio") is not None and b_sent is not None:
        out["A_first_audio_to_B_sent_ms"] = round((b_sent - ap["first_audio"]) * 1000, 2)
    if a_end is not None and b_sent is not None:
        out["B_sent_before_A_end"] = b_sent < a_end
        out["B_sent_to_A_end_ms"] = round((a_end - b_sent) * 1000, 2)
    if a_end is not None and b_hdr is not None:
        out["B_headers_before_A_end"] = b_hdr < a_end
    return out


# ---------------------------------------------------------------- WebSocket
class _Closed(Exception):
    def __init__(self, code, reason, by):
        self.code, self.reason, self.by = code, reason, by


class _DryConn:
    """Offline stand-in used only by --dry-run: answers requests with a fake chunk+complete."""

    def __init__(self):
        self.q: asyncio.Queue = asyncio.Queue()
        self.remote_address = ("0.0.0.0", 0)
        self.response = None

    async def send(self, msg):
        m = json.loads(msg)
        if m.get("type") in ("ping", "keepalive", "keep_alive", "keep-alive"):
            await self.q.put(json.dumps({"type": "pong"}))
        elif "text" in m:
            await self.q.put(json.dumps({"status": "chunk", "data": {"audio": "AAAA"}}))
            asyncio.ensure_future(self._tail())     # short fake tail so tail paths run

    async def _tail(self):
        await asyncio.sleep(0.2)
        await self.q.put(json.dumps({"status": "complete"}))
        await self.q.put(_Closed(1000, "dry-run", "server"))

    async def recv(self):
        item = await self.q.get()
        if isinstance(item, _Closed):
            raise item
        return item

    async def ping(self):
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(0.0)
        return fut

    async def close(self, code=1000, reason=""):
        await self.q.put(_Closed(code, reason, "client"))


class WS:
    """One logged TTS WebSocket. Holds a global 'ws' slot from open() until close().

    Records: handshake status/headers (or the rejection response), resolved IP, timings,
    every frame (t_ms since open, opcode, status/type, bytes), audio sha256, close
    code/reason/by, last text frame, external IDs echoed in frames."""

    def __init__(self, case: str, target_url: str, *, meta: dict | None = None):
        self.case, self.url, self.meta = case, target_url, meta or {}
        self.conn = None
        self.slot = Slot("ws", case)
        self.frames: list[dict] = []
        self.sent: list[dict] = []
        self.rec: dict = {"case": case, "kind": "ws", "method": "GET(upgrade)", "url": target_url,
                          "meta": self.meta}
        self.t_open = None
        self.first_chunk = asyncio.Event()
        self.complete = asyncio.Event()
        self.error = asyncio.Event()
        self.closed = asyncio.Event()       # set only once the CONNECTION is closed
        self.reader_done = asyncio.Event()  # reader task finished (any reason)
        self.reader_error = None
        self.any_text = asyncio.Event()
        self.last_text = None
        self.close_info: dict = {}
        self.ext_ids: dict = {}
        self._sha = hashlib.sha256()
        self.audio_bytes = 0
        self.audio_chunks = 0
        self.t_last_chunk_ms = None
        self.t_complete_ms = None
        self._reader = None
        self.rejected = False
        self._logged = False

    def now_ms(self):
        return round((time.perf_counter() - self.t_open) * 1000, 2) if self.t_open else None

    async def open(self, deliberate: "Deliberate | None" = None) -> bool:
        await self.slot.acquire()
        self.rec["slot_wait_ms"] = round(self.slot.waited_ms, 1)
        try:
            return await self._open(deliberate)
        except BaseException:
            await self.close("open failed")      # never leak the slot on an exception
            raise

    async def _open(self, deliberate) -> bool:
        if deliberate is not None:
            self.rec["deliberate_dispatch_epoch"] = deliberate.dispatch()
            self.rec["deliberate_counter"] = deliberate.count
        if RUN.dry:
            info(f"[dry-run] {self.case}: WS connect {self.url} headers="
                 f"{{'Authorization': 'Bearer sk_...'}} ping_interval=None")
            self.conn = _DryConn()
            self.t_open = time.perf_counter()
            self._reader = asyncio.ensure_future(self._read())
            return True
        from urllib.parse import urlsplit
        from websockets.asyncio.client import connect
        from websockets.exceptions import InvalidStatus, InvalidHandshake
        host = urlsplit(self.url).hostname
        try:
            dns_ms, ips = await asyncio.get_running_loop().run_in_executor(None, _resolve, host)
        except OSError as e:
            dns_ms, ips = None, []
            self.rec["dns_error"] = repr(e)
        self.rec["dns_answers"] = ips
        t0 = time.perf_counter()
        self.rec["t_connect_ist"] = ist_now()
        try:
            self.conn = await connect(
                self.url, additional_headers={"Authorization": f"Bearer {api_key()}"},
                ping_interval=None,      # no automatic pings: they would reset idle timers
                compression=None, proxy=None, open_timeout=20, close_timeout=5,
                max_size=None)
        except InvalidStatus as e:
            r = e.response
            self.rejected = True
            self.rec.update(status=r.status_code, resp_headers=list(r.headers.raw_items()),
                            x_request_id=r.headers.get("x-request-id"),
                            body_text=(r.body or b"").decode("utf-8", "replace")[:4000],
                            handshake_ms=round((time.perf_counter() - t0) * 1000, 2),
                            error=f"InvalidStatus {r.status_code}")
            self.rec["external_ids"] = _ext_ids_from_headers(r.headers.raw_items())
            self.rec["dns_ms"] = round(dns_ms, 2) if dns_ms is not None else None
            info(f"  {self.case}: WS handshake rejected {r.status_code}")
            return False
        except (InvalidHandshake, OSError, asyncio.TimeoutError) as e:
            self.rejected = True
            self.rec.update(error=f"{type(e).__name__}: {e}",
                            handshake_ms=round((time.perf_counter() - t0) * 1000, 2))
            info(f"  {self.case}: WS connect failed {type(e).__name__}")
            return False
        self.t_open = time.perf_counter()
        resp = getattr(self.conn, "response", None)
        if resp is not None:
            hdrs = list(resp.headers.raw_items())
            self.rec.update(status=resp.status_code, resp_headers=hdrs,
                            x_request_id=resp.headers.get("x-request-id"),
                            external_ids=_ext_ids_from_headers(hdrs))
        ra = getattr(self.conn, "remote_address", None)
        self.rec["resolved_ip"] = ra[0] if ra else (ips[0] if ips else None)
        self.rec["timings_ms"] = {"dns": round(dns_ms, 2) if dns_ms is not None else None,
                                  "handshake_tcp_tls_upgrade": round((self.t_open - t0) * 1000, 2)}
        self.rec["t_open_ist"] = ist_now()
        self._reader = asyncio.ensure_future(self._read())
        return True

    async def _recv(self):
        if RUN.dry:
            return await self.conn.recv()
        from websockets.exceptions import ConnectionClosed
        try:
            return await self.conn.recv()
        except ConnectionClosed as e:
            rc, sc = e.rcvd, e.sent
            by = "server" if (rc is not None and (sc is None or e.rcvd_then_sent)) else (
                "client" if sc is not None else "abnormal")
            code = rc.code if rc is not None else (sc.code if sc is not None else 1006)
            reason = rc.reason if rc is not None else (sc.reason if sc is not None else "")
            raise _Closed(code, reason, by)

    async def _read(self):
        try:
            while True:
                msg = await self._recv()
                t = self.now_ms()
                if isinstance(msg, bytes):
                    self.frames.append({"t_ms": t, "op": "binary", "bytes": len(msg)})
                    continue
                fr = {"t_ms": t, "op": "text", "bytes": len(msg)}
                try:
                    m = json.loads(msg)
                except ValueError:
                    m = None
                if isinstance(m, dict):
                    st = m.get("status")
                    fr["status"] = st
                    if m.get("type"):
                        fr["type"] = m.get("type")
                    for k in ("request_id", "session_id", "external_request_id",
                              "external_session_id"):
                        if k in m:
                            self.ext_ids.setdefault(k, set()).add(str(m[k]))
                    audio = (m.get("data") or {}).get("audio") if isinstance(
                        m.get("data"), dict) else None
                    if st == "chunk" and audio:
                        pcm = base64.b64decode(audio)
                        self._sha.update(pcm)
                        self.audio_bytes += len(pcm)
                        self.audio_chunks += 1
                        self.t_last_chunk_ms = t
                        fr["audio_bytes"] = len(pcm)
                        if not self.first_chunk.is_set():
                            self.rec["first_chunk_ist"] = ist_now()
                            self.first_chunk.set()
                        m = dict(m, data=dict(m["data"], audio=f"<{len(audio)} b64 chars>"))
                    elif st == "complete":
                        self.t_complete_ms = t
                        self.rec["complete_ist"] = ist_now()
                        self.complete.set()
                    elif st == "error" or "error" in m or m.get("error_code"):
                        fr["error"] = {k: m.get(k) for k in ("message", "error", "error_code",
                                                             "code") if k in m}
                        self.error.set()
                    if st != "chunk":
                        fr["payload"] = m
                    self.last_text = m
                else:
                    fr["raw"] = msg[:500]
                    self.last_text = msg[:2000]
                self.frames.append(fr)
                self.any_text.set()
        except _Closed as c:
            # the connection itself is closed: `closed` means exactly that
            self.close_info = {"code": c.code, "reason": c.reason, "by": c.by,
                               "t_ms": self.now_ms(), "ist": ist_now()}
            self.closed.set()
        except Exception as e:  # reader died, transport may still be OPEN
            self.reader_error = f"{type(e).__name__}: {e}"
            self.close_info = {"code": None, "error": self.reader_error,
                               "t_ms": self.now_ms(), "ist": ist_now(), "by": "reader-error"}
            # actually close the transport before anyone treats this socket as gone
            try:
                await self._shutdown_transport("reader error")
                self.close_info["transport_closed_t_ms"] = self.now_ms()
                self.closed.set()
            except Abort:
                pass        # close() retries and raises; slot stays held meanwhile
        finally:
            self.reader_done.set()

    def _transport_closed(self) -> bool:
        if self.conn is None:
            return True
        if RUN.dry:
            return self.closed.is_set()
        from websockets.protocol import State
        return self.conn.state is State.CLOSED

    async def _shutdown_transport(self, reason: str) -> None:
        """Close handshake (bounded), then hard-abort the TCP transport if still not CLOSED,
        then wait for the closure to be observed. Idempotent."""
        if self.conn is None:
            return
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.conn.close(1000, reason), 6)
        if RUN.dry:
            return
        if not self._transport_closed():
            tr = getattr(self.conn, "transport", None)
            if tr is not None:
                self.rec["transport_aborted"] = True
                tr.abort()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.conn.wait_closed(), 5)
        if not self._transport_closed():
            # should be impossible after transport.abort(); refuse to free the slot
            self.rec["transport_still_open"] = True
            raise Abort(f"{self.case}: WS transport did not close; not releasing the slot")

    async def send_json(self, obj: dict, chars: int = 0, label: str = "") -> dict:
        """Send one JSON frame; returns {'ok':bool,'t_ms':..,'error':..}.

        chars are reserved in the ledger (budget-checked under ledger.lock) BEFORE the send
        and are never refunded once a send was attempted (outcome uncertain -> counted)."""
        ev = {"t_ms": self.now_ms(), "ist": ist_now(), "label": label,
              "frame": body_summary(obj), "chars": chars}
        if self.conn is None or self.closed.is_set():
            ev.update(ok=False, error="not connected" if self.conn is None else "already closed",
                      chars=0)
            self.sent.append(ev)
            return ev
        lcase = f"{self.case}:{label}" if label else self.case
        rid = reserve_chars(lcase, chars)
        if RUN.dry:
            info(f"[dry-run] {self.case}: WS send {label} "
                 f"{json.dumps(redact(body_summary(obj)), ensure_ascii=False)[:300]}")
        try:
            await self.conn.send(json.dumps(obj))
            ev["ok"] = True
        except Exception as e:
            ev.update(ok=False, error=f"{type(e).__name__}: {e}")
        self.sent.append(ev)
        settle(rid, lcase, "ws-sent" if ev["ok"] else "ws-send-failed(counted)")
        return ev

    async def protocol_ping(self) -> dict:
        ev = {"t_ms": self.now_ms(), "label": "protocol-ping"}
        try:
            waiter = await self.conn.ping()
            lat = await asyncio.wait_for(waiter, 5)
            ev.update(ok=True, pong_latency_s=lat)
        except Exception as e:
            ev.update(ok=False, error=f"{type(e).__name__}: {e}")
        self.sent.append(ev)
        return ev

    async def wait(self, event: asyncio.Event, timeout: float) -> bool:
        if RUN.dry:
            timeout = min(timeout, 0.5)
        try:
            await asyncio.wait_for(event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    @property
    def is_open(self) -> bool:
        return self.conn is not None and not self.closed.is_set() and \
            not self._transport_closed()

    async def close(self, reason: str = "client done"):
        """Close/abort the transport, AWAIT its closure, stop the reader, then release the
        slot. The slot is released only once the socket is really gone (item: no >3 live
        sockets even if the reader died early)."""
        if self.conn is not None and not self._transport_closed():
            self.rec.setdefault("client_close_ist", ist_now())
            self.rec.setdefault("client_close_t_ms", self.now_ms())
            await self._shutdown_transport(reason)
        if self._reader is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._reader), 10)
            except Exception:
                self._reader.cancel()
                with contextlib.suppress(BaseException):
                    await self._reader
        self.slot.release()

    def finish(self, **extra) -> dict:
        """Log the WS record once (after close)."""
        if self._logged:
            return self.rec
        self._logged = True
        self.rec.update({
            "frames": self.frames,
            "frame_counts": _count_frames(self.frames),
            "sent": self.sent,
            "close": self.close_info,
            "last_text_frame": self.last_text,
            "external_ids_in_frames": {k: sorted(v) for k, v in self.ext_ids.items()},
            "audio_bytes": self.audio_bytes,
            "audio_chunks": self.audio_chunks,
            "audio_sha256": self._sha.hexdigest() if self.audio_chunks else None,
            "t_last_chunk_ms": self.t_last_chunk_ms,
            "t_complete_ms": self.t_complete_ms,
            "resp_bytes": sum(f.get("bytes", 0) for f in self.frames),
            "chars_sent": sum(s.get("chars", 0) for s in self.sent if s.get("ok")),
        })
        self.rec.update(extra)
        log(RUN.probe, self.rec)
        info(f"  {self.case}: ws status={self.rec.get('status')} frames={len(self.frames)} "
             f"chunks={self.audio_chunks} complete_ms={self.t_complete_ms} "
             f"close={self.close_info.get('code')}/{self.close_info.get('by')}@"
             f"{self.close_info.get('t_ms')}ms")
        return self.rec


def _count_frames(frames):
    c: dict[str, int] = {}
    for f in frames:
        k = f.get("status") or f.get("type") or f.get("op")
        c[str(k)] = c.get(str(k), 0) + 1
    return c


# ---------------------------------------------------------------- main wrapper
def run(probe: str, args, coro_fn) -> None:
    """init + asyncio.run(coro_fn(args)); Abort -> logged, exit 3."""
    init(probe, args.dry_run)
    code = 0
    try:
        asyncio.run(coro_fn(args))
    except Abort as e:
        info(f"ABORT: {e}")
        with contextlib.suppress(Exception):
            log(probe, {"case": "_abort", "event": "abort", "reason": str(e)})
        code = 3
    finally:
        if RUN.dry:
            info(f"[dry-run] done: {probe} would send {RUN.chars_sent} chars; no network used.")
        else:
            info(f"{probe}: chars_sent this run = {RUN.chars_sent}; ledger total = "
                 f"{ledger_total()} / {CHAR_BUDGET}; log = {RUN.log_path}")
    sys.exit(code)
