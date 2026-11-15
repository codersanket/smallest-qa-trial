#!/bin/bash
# P3 by hand: does a 2nd TTS request get a 429 while one is in flight?
# Docs: concurrency-and-limits.md says 1 active TTS request per account, 2nd HTTP -> 429.
# EXPECTED (write before running): B -> 429 while A streams; A completes intact.
set -u
source "$(dirname "$0")/../.env"
H="Authorization: Bearer $SMALLEST_API_KEY"
A_TEXT=$(python3 -c "print(('The quick brown fox jumps over the lazy dog near the riverbank at dawn. ' * 9)[:600])")
OUT=$(dirname "$0")/runs; mkdir -p "$OUT"; TS=$(date +%Y%m%dT%H%M%S)IST
echo "EXPECTED: B -> 429 while A is streaming; A completes intact.  ($(date '+%F %T IST'))"
for run in 1 2 3; do
  # A: 600-char SSE stream, headers+body to file, in background
  curl -s -D "$OUT/p3h_${TS}_r${run}_A.hdr" -o "$OUT/p3h_${TS}_r${run}_A.sse" \
    -w "A run$run: status=%{http_code} ttfb=%{time_starttransfer}s total=%{time_total}s\n" \
    -H "$H" -H 'content-type: application/json' \
    -d "{\"text\":\"$A_TEXT\",\"voice_id\":\"rhea\",\"model\":\"lightning_v3.1_pro\"}" \
    https://api.smallest.ai/waves/v1/tts/live &
  sleep 0.5   # A has started streaming (its TTFB is ~160 ms)
  # B: 20-char sync request while A is in flight
  curl -s -D "$OUT/p3h_${TS}_r${run}_B.hdr" -o /dev/null \
    -w "B run$run: status=%{http_code} ttfb=%{time_starttransfer}s sent_at=$(date +%T.%N | cut -c1-12)\n" \
    -H "$H" -H 'content-type: application/json' \
    -d '{"text":"Hand run B, twenty.","voice_id":"rhea","model":"lightning_v3.1_pro"}' \
    https://api.smallest.ai/waves/v1/tts
  wait
  echo "  B x-request-id: $(grep -i '^x-request-id' "$OUT/p3h_${TS}_r${run}_B.hdr" | tr -d '\r')"
  echo "  B retry-after : $(grep -i '^retry-after' "$OUT/p3h_${TS}_r${run}_B.hdr" | tr -d '\r' || echo '(none)')"
  echo "  A x-request-id: $(grep -i '^x-request-id' "$OUT/p3h_${TS}_r${run}_A.hdr" | tr -d '\r')  A bytes=$(wc -c < "$OUT/p3h_${TS}_r${run}_A.sse")"
  sleep 3
done
