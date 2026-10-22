#!/usr/bin/env python3
"""Throwaway test: build judge x doctrine reasoning profiles.

Stage 1: Sonnet 5.5 (medium effort) writes per-doctrine reasoning notes for
each opinion, with every quoted passage verified against the PDF and pinned
to a page.
Stage 2: Opus 5.5 synthesizes a profile per (judge, doctrine) from only that
judge's own notes. It cites evidence by ID; quotes are rendered from the
verified passages, never written by the model. Not part of the pipeline."""

import json
import sys
import time
from pathlib import Path

import anthropic
from pydantic import BaseModel

from test_reasoning_notes import PROMPT, OpinionNotes, fetch_pages, locate, split_page

SONNET = ("claude-sonnet-5-5", "medium", 2.00, 10.00)
OPUS = ("claude-opus-5-5", "high", 4.00, 20.00)

# (judge, doctrine, [(case, date, url), ...]) — filled from the precedential scan
PAIRS = json.loads(Path(__file__).with_name("test_profile_pairs.json").read_text())


class Point(BaseModel):
    heading: str
    analysis: str
    evidence: list[str]


class Profile(BaseModel):
    overview: str
    approach: list[Point]
    close_cases: list[Point]
    separate_writings: str | None = None
    limits: str


SYNTH_PROMPT = """You are writing a research memo for an incoming Federal Circuit law clerk who wants to understand how Judge {judge} reasons about {doctrine} — especially in close cases — so they can anticipate how the judge thinks, not just how cases came out.

Below are notes on every opinion in our corpus (2020-present) where Judge {judge} personally wrote on {doctrine}: majority opinions they authored and any concurrences or dissents they wrote. Each note has an ID (e.g. N3) and verified verbatim passages with IDs (e.g. N3.P2).

Write the profile:
- overview: 3-5 sentences on what is distinctive about how this judge approaches {doctrine} — not a description of the doctrine itself. If the evidence shows no distinctive approach beyond faithful application of settled law, say so.
- approach: 2-5 points on recurring analytical moves — which version of the test they reach for, which precedents they lean on, what kinds of evidence or claim language they treat as decisive, how they handle procedural posture. Each point must cite the note and passage IDs that support it in "evidence".
- close_cases: 1-4 points on what tips the judge in close or contested cases (notes marked close/contested), with evidence IDs. If there are none, return an empty list and say so in limits.
- separate_writings: if the judge wrote separately on this doctrine, how those writings diverge from their colleagues and what that reveals; otherwise null.
- limits: 1-3 sentences on the limits of this evidence — how many opinions, how many were majority opinions written for a panel (which may reflect the panel's view more than the author's), date range, and anything the clerk should not over-read.

Rules:
- Every claim must be supported by the cited notes. Do not use outside knowledge of the judge or of cases not in the notes.
- Do not quote; refer to evidence by ID only. Quotes are rendered from the IDs.
- Distinguish writing for the court from writing separately.
- Prefer concrete observations ("treats the absence of a disclosed common structural feature as dispositive") over adjectives ("rigorous", "textualist") unless the notes clearly support the label.

Notes:
{notes}"""


def cite(case: str, writer: str, part: str, loc: dict) -> str:
    page = loc.get("printed_page") or f"PDF p. {loc.get('pdf_page')}"
    sep = "" if part in ("majority", "per curiam") else f" ({writer}, J., {part})"
    return f"{case}, slip op. at {page}{sep}"


def main() -> int:
    client = anthropic.Anthropic()
    out = Path("profile-test")
    out.mkdir(exist_ok=True)
    cost = 0.0

    # ---- stage 1: notes per unique opinion
    opinions = {}
    for _, _, ops in PAIRS:
        for case, date, url in ops:
            opinions[url] = (case, date)
    notes_by_url = {}
    for url, (case, date) in opinions.items():
        pages = fetch_pages(url)
        marked = "\n".join(f"[[PDF page {i + 1}]]\n{split_page(t)[1]}" for i, t in enumerate(pages))
        model, effort, pin, pout = SONNET
        try:
            r = client.with_options(timeout=900).messages.parse(
                model=model, max_tokens=16000,
                messages=[{"role": "user", "content": PROMPT.format(case=case, text=marked)}],
                output_format=OpinionNotes, output_config={"effort": effort},
            )
            c = r.usage.input_tokens * pin / 1e6 + r.usage.output_tokens * pout / 1e6
            cost += c
            notes = r.parsed_output.model_dump()["notes"] if r.parsed_output else []
            for n in notes:
                n["passages"] = [{"text": p, **locate(p, pages)} for p in n["passages"]]
            notes_by_url[url] = notes
            nv = sum(p["verified"] for n in notes for p in n["passages"])
            nq = sum(len(n["passages"]) for n in notes)
            print(f"notes  {case[:55]:55} {len(notes)} notes {nv}/{nq} verified ${c:.3f}", flush=True)
        except Exception as e:  # noqa: BLE001
            notes_by_url[url] = []
            print(f"notes  {case[:55]:55} FAILED {type(e).__name__}: {e}", flush=True)
    (out / "notes.json").write_text(json.dumps(
        {u: {"case": opinions[u][0], "date": opinions[u][1], "notes": n} for u, n in notes_by_url.items()},
        indent=1, ensure_ascii=False))

    # ---- stage 2: one profile per (judge, doctrine)
    profiles = []
    for judge, doctrine, ops in PAIRS:
        evidence, blocks, nid = {}, [], 0
        for case, date, url in sorted(ops, key=lambda o: o[1]):
            for n in notes_by_url.get(url, []):
                if n["writer"].strip().lower() != judge.lower() or n["doctrine"] != doctrine:
                    continue
                nid += 1
                key = f"N{nid}"
                evidence[key] = {"case": case, "date": date, "url": url, "opinion_part": n["opinion_part"],
                                 "closeness": n["closeness"]}
                lines = [f"{key} — {case} ({date}); {judge} writing {n['opinion_part']}; closeness: "
                         f"{n['closeness']} ({n['closeness_basis']})",
                         f"  Framing: {n['framing']}",
                         f"  Decisive: {n['decisive_considerations']}"]
                pid = 0
                for p in n["passages"]:
                    if not p["verified"]:
                        continue
                    pid += 1
                    pkey = f"{key}.P{pid}"
                    evidence[pkey] = {"case": case, "quote": p["text"],
                                      "cite": cite(case, judge, n["opinion_part"], p), "url": url,
                                      "pdf_page": p["pdf_page"]}
                    lines.append(f"  {pkey}: \"{' '.join(p['text'].split())}\"")
                blocks.append("\n".join(lines))
        entry = {"judge": judge, "doctrine": doctrine, "opinions_considered": len(ops),
                 "notes_used": nid, "evidence": evidence}
        if not blocks:
            entry["error"] = "no notes written by this judge on this doctrine"
            profiles.append(entry)
            print(f"profile {judge} / {doctrine}: no notes", flush=True)
            continue
        model, effort, pin, pout = OPUS
        t0 = time.time()
        try:
            r = client.with_options(timeout=900).beta.messages.parse(
                model=model, max_tokens=32000,
                betas=["server-side-fallback-2026-07-01"], fallbacks="default",
                messages=[{"role": "user", "content": SYNTH_PROMPT.format(
                    judge=judge, doctrine=doctrine, notes="\n\n".join(blocks))}],
                output_format=Profile, output_config={"effort": effort},
            )
            c = r.usage.input_tokens * pin / 1e6 + r.usage.output_tokens * pout / 1e6
            cost += c
            prof = r.parsed_output.model_dump() if r.parsed_output else None
            bad = sorted({e for sec in ("approach", "close_cases") for pt in (prof or {}).get(sec, [])
                          for e in pt["evidence"] if e not in evidence})
            entry.update({"profile": prof, "served_by": r.model, "stop_reason": r.stop_reason,
                          "unknown_evidence_ids": bad, "cost_usd": round(c, 4),
                          "seconds": round(time.time() - t0, 1)})
            print(f"profile {judge} / {doctrine}: {nid} notes, model={r.model}, "
                  f"bad ids={bad}, ${c:.3f}", flush=True)
        except Exception as e:  # noqa: BLE001
            entry["error"] = f"{type(e).__name__}: {e}"
            print(f"profile {judge} / {doctrine}: FAILED {entry['error']}", flush=True)
        profiles.append(entry)

    (out / "profiles.json").write_text(json.dumps(profiles, indent=1, ensure_ascii=False))
    print(f"\nTotal cost: ${cost:.3f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
