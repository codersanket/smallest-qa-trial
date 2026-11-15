Hand re-runs by Sanket, Mac on home broadband (Mumbai), 15 Nov 2026 13:10–13:11 IST.
Scripts: hand/p3_hand.sh, hand/p4_hand.py. Raw headers/bodies in hand/runs/.

EXPECTED: B -> 429 while A is streaming; A completes intact.  (2026-11-15 13:10:28 IST)
B run1: status=200 ttfb=0.737207s sent_at=13:10:29.014
A run1: status=200 ttfb=0.273770s total=6.128260s
  B x-request-id: b7f4367f-b264-46a3-90da-711a515da357
  B retry-after : (none)
  A x-request-id: 57bf13f8-883a-4110-8cd7-67a93333b8ad  A bytes=4169850
B run2: status=200 ttfb=0.571417s sent_at=13:10:38.188
A run2: status=200 ttfb=0.246783s total=5.696306s
  B x-request-id: ccc48534-e933-40bc-a920-59ca4138ecc8
  B retry-after : (none)
  A x-request-id: 29e49dde-c8de-474f-add8-d430f27002d8  A bytes=3924032
B run3: status=200 ttfb=0.704909s sent_at=13:10:46.932
A run3: status=200 ttfb=0.627786s total=6.130320s
  B x-request-id: c280bd29-a78a-47c5-a2d4-fc3f430d9a39
  B retry-after : (none)
  A x-request-id: 312b1bc3-dfa3-48d6-bcc6-5ec78f6ae37f  A bytes=3840492

EXPECTED: tail ~4000 ms; server closes after complete; B fails.  (2026-11-15 13:10:55 IST)
  A: request_id=311a5173-812b-436d-bdef-b50d2fc698e7 chunks=13 last_chunk=424ms complete=4441ms  tail=4018ms
  B (1 s after A complete, same socket): request_id=1fc3b036-0cf1-49d2-9fc5-bb839157a978 chunks=11 last_chunk=5835ms complete=9859ms  tail=4024ms
  => socket was REUSABLE after complete (docs tts.md:603 say it closes)
  close: code=1000 reason='' (closed by client)
EXPECTED: tail ~0 ms; server closes after complete; B fails.  (2026-11-15 13:11:05 IST)
  A: request_id=35791004-a355-448d-bab2-348bb96d8aaa chunks=15 last_chunk=589ms complete=619ms  tail=31ms
  B (1 s after A complete, same socket): request_id=78d15bf4-2a16-4aa8-8187-7993872435d1 chunks=13 last_chunk=2052ms complete=2080ms  tail=28ms
  => socket was REUSABLE after complete (docs tts.md:603 say it closes)
  close: code=1000 reason='' (closed by client)

Earlier Hermes verification runs (same scripts, same network) 13:07–13:08 IST: identical outcomes
(P3 3/3 B=200 at 0.55–0.75 s; P4 tails 4025/4024 ms default, 26/28 ms at 0).

Dashboard balance read by Sanket ~12:45 IST: $9.80 after Sara's 18,406 submitted chars, of which
~8,100 were deliberately rejected (8001-char too_big, mismatches, whitespace, filter). ~10,300 successful
chars × $0.195/10k = $0.20 → rejected requests are not billed (two-decimal dashboard precision).
