# Smallest.ai Waves API — QA trial evidence (Sanket Babar, 15 Nov 2026)

Evidence for the product-trial QA report (area: APIs, Lightning v3.1 Pro TTS deep, Pulse STT side checks).

- `REPORT.md` — the report as submitted.
- `FINDINGS.md` — full evidence: every probe's expected vs observed, calibrations (docs were right), unresolved items.
- `runs/` — raw JSONL logs from the scripted harness (Mumbai VPS, 05:38–05:56 IST). First record of each file is the EXPECTED line written before the run. API key redacted as `sk_...`. `ledger.csv` is the character budget.
- `harness/` — the probe scripts (Python 3.14, httpx, websockets) with rate/concurrency rails in `common.py`.
- `hand/` — the two key probes re-run by hand from a laptop on home broadband (13:07–13:11 IST): `p3_hand.sh` (curl; concurrency), `p4_hand.py` (websockets; `complete` tail and reuse), response headers in `hand/runs/`, output in `HAND-RUNS.md`. SSE audio bodies removed to keep the repo small; byte counts are in `HAND-RUNS.md`.

Rules followed: ≤2 requests in flight, ≤3 sockets open, no load or security testing, own account only.
