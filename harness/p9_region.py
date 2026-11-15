#!/usr/bin/env python3
"""P9 - Pulse region oracle (side probe) (PLAN.md:31, plan-check-sara.md:32-34,57).

POST /waves/v1/stt/?model=pulse&language=multi-indic with a 1.5 s clip to 3 hosts:
  generic api.smallest.ai, api.india.smallest.ai, api.us.smallest.ai
  (concurrency-and-limits.md:80-83). multi-indic is "India region only"
  (language-detection.md:29, stt overview.md:99); the 400 body carries error_code,
  language, region (transcribe.md:158-170). Logs resolved IP + full body.
Run once per vantage: --vantage mumbai (Hostinger VPS) and --vantage us (Linode Fremont).
The 1.5 s clip is cut from out/fixtures/clip.wav (written by p8 --prepare-clip-only).
"""
import io
import wave

import common as C

EXPECTED = (
    "`api.us` -> 400 region us-west-2 (LANGUAGE_NOT_ENABLED_IN_REGION, transcribe.md:164-169); "
    "`api.india` -> 200; generic host from Mumbai -> India; generic host from US box -> "
    "us-west-2 (limits :78 nearest-cluster routing) - contradicts overview.md:95,107 / "
    "pulse.md:372 which equate India-only with the generic host")


def short_clip(seconds: float = 1.5) -> bytes:
    src = C.FIXTURES / "clip.wav"
    if not src.exists():
        if C.RUN.dry:
            C.info(f"[dry-run] {src} missing; a real run would abort (run p8 --prepare-clip-only)")
            return b"RIFF-dry-run-placeholder"
        raise C.Abort(f"{src} missing: run `p8_webhook.py --prepare-clip-only` after P2")
    with wave.open(str(src), "rb") as r:
        params = r.getparams()
        frames = r.readframes(int(params.framerate * seconds))
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setparams(params)
        w.writeframes(frames)
    data = buf.getvalue()
    if not C.RUN.dry:
        (C.FIXTURES / "clip_1500ms.wav").write_bytes(data)
    return data


async def main(args):
    C.expected(EXPECTED + f" | vantage={args.vantage}")
    data = short_clip()
    for i, host in enumerate(("generic", "india", "us")):
        if i:
            await C.sleep(2)
        rec = await C.http(f"{args.vantage}_{host}", "POST",
                           C.stt_url(host, model="pulse", language="multi-indic"),
                           content=data, content_type="application/octet-stream", mode="stt",
                           chars=0, meta={"vantage": args.vantage, "host_alias": host})
        bj = rec.get("body_json") or {}
        C.info(f"    {host}: ip={rec.get('resolved_ip')} status={rec.get('status')} "
               f"error_code={bj.get('error_code')} region={bj.get('region')}")


if __name__ == "__main__":
    p = C.parser("p9", "P9 Pulse region oracle")
    p.add_argument("--vantage", choices=["mumbai", "us"], required=True,
                   help="label for where this box is")
    a = p.parse_args()
    C.run("p9", a, main)
