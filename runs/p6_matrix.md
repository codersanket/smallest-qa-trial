# P6 correlation-ID matrix

Generated 2026-11-15T05:54:55.592+05:30 from 8 log files (p1/p2/p3/p6). Columns count records; `ext hdr` = any X-External-* response header; `ext ws` = external_* field in WS frames.

| endpoint | status | n | x-request-id present | ext hdr echoed | ext ws echoed | probes | sample x-request-id |
|---|---|---|---|---|---|---|---|
| `sse api.smallest.ai/waves/v1/tts/live` | 200 | 6 | 6/6 | 6 | 0 | p3,p6 | cde34d34-d593-4ca0-b6d5-61f81033851f<br>23ceccd5-6bb8-442c-bd3e-8101fd8e9535<br>929c58a3-2120-4db8-8470-8868cbc9e61e |
| `sync api.smallest.ai/waves/v1/tts` | 200 | 24 | 24/24 | 24 | 0 | p1,p2,p3,p6 | b10decd2-360b-485c-8e27-1b9905206476<br>eb9ef6f6-c37e-4291-a54b-d4acef65f28b<br>66de20fe-9db9-47e2-8859-62ad7489ab50 |
| `sync api.smallest.ai/waves/v1/tts` | 400 | 9 | 9/9 | 0 | 0 | p1,p2 | 0dd8d135-7a06-4919-8b66-2120650c1b27<br>0a11a899-72ad-47d7-be0b-fe1476afecb4<br>4bddf066-c054-4b00-91e6-1bd5001a1f82 |
| `sync api.smallest.ai/waves/v1/tts` | 401 | 1 | 1/1 | 0 | 0 | p6 | bb3e2a0a-721f-4635-a772-e0abb0f43d50 |
| `ws api.smallest.ai/waves/v1/tts/live` | 101+complete | 5 | 0/5 | 0 | 3 | p3,p6 | - |
| `ws api.smallest.ai/waves/v1/tts/live` | 101+error | 1 | 0/1 | 0 | 0 | p2 | - |
