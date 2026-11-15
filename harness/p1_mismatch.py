#!/usr/bin/env python3
"""P1 - voice/model mismatch (PLAN.md:23). 3 sequential sync POSTs to /waves/v1/tts.

case1 Pro voice + model lightning_v3.1 (deliberate; the only P1 request with v3.1)
case2 standard voice + model lightning_v3.1_pro
case3 Pro voice, `model` omitted (deliberate; the only harness request without model)
Audio bodies are saved to out/audio/ (P8 reuses them).
"""
import common as C

EXPECTED = (
    "case1 (Pro voice+v3.1) -> 400 (`synthesize-speech.md:106`); case2 (std voice+pro) -> 400; "
    "case3 (Pro voice, no model) -> 200 Pro audio per `synthesize-speech.md:131` (\"Pro catalog "
    "voices use lightning_v3.1_pro\") - contradicts card :496. Zero spend on 400 = hypothesis "
    "(undocumented)")

FMT = {"sample_rate": C.SAMPLE_RATE, "output_format": "wav"}   # synthesize-speech.md:134,144


async def main(args):
    C.expected(EXPECTED)
    C.info(f"voices: pro={C.PRO_VOICE} (lightning-v-3-1-pro.md:354), std={C.STD_VOICE} "
           f"(lightning-v-3-1.md:218)")
    cases = [
        ("case1_pro_voice_v3.1", C.tts_body(C.text20("p1 c1"), voice=C.PRO_VOICE,
                                            model=C.MODEL_STD, **FMT)),
        ("case2_std_voice_pro", C.tts_body(C.text20("p1 c2"), voice=C.STD_VOICE,
                                           model=C.MODEL_PRO, **FMT)),
        ("case3_pro_voice_no_model", C.tts_body(C.text20("p1 c3"), voice=C.PRO_VOICE,
                                                model=None, **FMT)),
    ]
    for i, (case, body) in enumerate(cases):
        if i:
            await C.sleep(2, "(spacing between sequential calls)")
        await C.http(case, "POST", C.tts_sync_url(), json_body=body, mode="sync",
                     chars=len(body["text"]), save_audio=True,
                     meta={"model_in_body": body.get("model", "<omitted>")})
    C.info("REMINDER: Sanket reads the dashboard balance before P1 / after P2 / after P7 "
           "(no usage API in pages/; concurrency-and-limits.md:48 links the dashboard only).")


if __name__ == "__main__":
    a = C.parser("p1", "P1 voice/model mismatch").parse_args()
    C.run("p1", a, main)
