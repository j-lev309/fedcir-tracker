#!/usr/bin/env python3
"""Throwaway comparison test: extract per-doctrine reasoning notes from three
real opinions with Sonnet 5.5 and Haiku 5.5, verify every quoted passage
against the PDF text, and write a side-by-side report. Not part of the
pipeline."""

import json
import re
import sys
import time
from io import BytesIO
from pathlib import Path
from typing import Literal

import anthropic
import pdfplumber
import requests
import urllib3
from pydantic import BaseModel

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

OPINIONS = [
    ("USAA v. PNC Bank (Hughes, §101)",
     "https://cafc.uscourts.gov/opinions-orders/23-1639.OPINION.6-12-2025_2529490.pdf"),
    ("Otsuka v. Hetero Labs (Bryson; Dyk concurring/dissenting in part)",
     "https://cafc.uscourts.gov/opinions-orders/25-2016.OPINION.7-1-2026_2717310.pdf"),
    ("Seagen v. Daiichi Sankyo (Lourie, written description/enablement)",
     "https://cafc.uscourts.gov/opinions-orders/23-2424.OPINION.12-2-2025_2612088.pdf"),
]

MODELS = {
    "claude-sonnet-5-5": (2.00, 10.00),
    "claude-haiku-5-5": (0.10, 0.50),
}

Doctrine = Literal[
    "§ 101 eligibility", "anticipation", "obviousness", "written description",
    "enablement", "indefiniteness", "claim construction", "infringement",
    "doctrine of equivalents", "inequitable conduct", "damages",
    "injunctions & other remedies", "attorney fees & sanctions",
    "IPR / PTAB procedure", "standing & jurisdiction", "trademark",
    "government contracts & Tucker Act", "veterans benefits",
    "federal employment", "trade & customs", "takings", "other",
]


class ReasoningNote(BaseModel):
    doctrine: Doctrine
    writer: str
    opinion_part: Literal["majority", "per curiam", "concurrence", "dissent",
                          "concurrence in part / dissent in part"]
    framing: str
    decisive_considerations: str
    closeness: Literal["clear", "close", "contested"]
    closeness_basis: str
    passages: list[str]


class OpinionNotes(BaseModel):
    notes: list[ReasoningNote]


PROMPT = """You are building a research file for an incoming Federal Circuit law clerk who wants to understand how individual judges reason about specific doctrines, especially in close cases.

Read the full opinion below (including any concurrences or dissents) and write one note for each doctrine the court or a separate writer actually ANALYZES — not doctrines merely mentioned or recited in background. If a concurrence or dissent addresses a doctrine, write a separate note for that separate opinion, attributed to its writer.

For each note:
- writer: surname of the judge who wrote that text (the authoring judge for the majority; "Per Curiam" if unsigned).
- framing: 2-4 sentences on how the writer framed and applied the governing test — which precedents and which version of the test they emphasized, and any interpretive choices that a different judge might have made differently.
- decisive_considerations: 1-3 sentences on the specific facts or arguments that drove the result on this doctrine.
- closeness: "contested" if another panel member disagreed on this doctrine; "close" if the opinion itself signals the question was close (acknowledging contrary authority, strong arguments on both sides, a reversal of a fact finding, language like "close question"); otherwise "clear".
- closeness_basis: one sentence citing what in the opinion supports that closeness rating.
- passages: 1-3 passages that best capture the writer's reasoning on this doctrine, each 1-3 sentences, copied EXACTLY character for character from the text below. No ellipses, no brackets, no paraphrase, no stitching non-adjacent sentences together. Omit internal citations only if they cannot be avoided — prefer passages without citations.

Record what the text does. Do not characterize the judge's general philosophy; that comes later from many opinions.

Case: {case}

Opinion text (page markers like [[PDF page 3]] are inserted by us, not part of the opinion — never include them in passages):
{text}"""


def fetch_pages(url: str) -> list[str]:
    r = requests.get(url, timeout=60, verify=False,
                     headers={"User-Agent": "fedcir-tracker (personal research dashboard)"})
    r.raise_for_status()
    with pdfplumber.open(BytesIO(r.content)) as pdf:
        return [(p.extract_text() or "") for p in pdf.pages]


ECF_HEADER = re.compile(r"^Case:\s*\S+\s+Document:\s*\d+\s+Page:\s*\d+\s+Filed:\s*\S+\s*$")
RUNNING_HEAD = re.compile(r"^(\d{1,3})\s+\S.*\bv\.|\bv\.(?:.*\S)?\s+(\d{1,3})$|^(\d{1,3})\s+IN RE\b|\bIN RE\b.*\s(\d{1,3})$")


def split_page(page_text: str) -> tuple[str | None, str]:
    """Strip the ECF filing header and the running head; return
    (printed slip-opinion page number, body text)."""
    lines = page_text.strip().splitlines()
    printed = None
    if lines and ECF_HEADER.match(lines[0].strip()):
        lines = lines[1:]
    if lines:
        m = RUNNING_HEAD.search(lines[0].strip())
        if m:
            printed = next(g for g in m.groups() if g)
            wrapped = re.search(r"\bv\.(\s+\d{1,3})?$", lines[0].strip())
            lines = lines[1:]
            # long captions wrap the running head onto an all-caps second line
            if wrapped and lines and lines[0].strip() and not re.search(r"[a-z]", lines[0]):
                lines = lines[1:]
    return printed, "\n".join(lines)


def norm(s: str) -> str:
    s = s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    s = re.sub(r"(\w)-\s+(\w)", r"\1\2", s)   # rejoin words hyphenated across lines
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def locate(passage: str, pages: list[str]) -> dict:
    q = norm(passage)
    if len(q) < 20:
        return {"verified": False, "reason": "too short"}
    split = [split_page(p) for p in pages]
    normed = [norm(body) for _, body in split]
    for i, p in enumerate(normed):
        if q in p:
            return {"verified": True, "pdf_page": i + 1, "printed_page": split[i][0]}
    for i in range(len(normed) - 1):        # quote spanning a page break
        if q in normed[i] + " " + normed[i + 1]:
            return {"verified": True, "pdf_page": i + 1, "printed_page": split[i][0],
                    "spans_pages": True}
    return {"verified": False, "reason": "not found verbatim"}


def main() -> int:
    client = anthropic.Anthropic()
    results = []
    for case, url in OPINIONS:
        pages = fetch_pages(url)
        marked = "\n".join(f"[[PDF page {i + 1}]]\n{split_page(t)[1]}" for i, t in enumerate(pages))
        print(f"\n### {case}: {len(pages)} pages, {len(marked):,} chars", flush=True)
        for model, (pin, pout) in MODELS.items():
            t0 = time.time()
            entry = {"case": case, "url": url, "model": model, "pages": len(pages)}
            try:
                r = client.with_options(timeout=900).messages.parse(
                    model=model, max_tokens=16000,
                    messages=[{"role": "user", "content": PROMPT.format(case=case, text=marked)}],
                    output_format=OpinionNotes,
                )
                u = r.usage
                entry.update({
                    "stop_reason": r.stop_reason,
                    "input_tokens": u.input_tokens, "output_tokens": u.output_tokens,
                    "cost_usd": round(u.input_tokens * pin / 1e6 + u.output_tokens * pout / 1e6, 4),
                    "seconds": round(time.time() - t0, 1),
                })
                if r.parsed_output is None:
                    entry["error"] = f"no parsed output (stop_reason={r.stop_reason})"
                else:
                    notes = r.parsed_output.model_dump()["notes"]
                    for n in notes:
                        n["passages"] = [{"text": p, **locate(p, pages)} for p in n["passages"]]
                    entry["notes"] = notes
            except Exception as e:  # noqa: BLE001
                entry["error"] = f"{type(e).__name__}: {e}"
            results.append(entry)
            nq = sum(len(n["passages"]) for n in entry.get("notes", []))
            nv = sum(p["verified"] for n in entry.get("notes", []) for p in n["passages"])
            print(f"  {model}: {len(entry.get('notes', []))} notes, {nv}/{nq} quotes verified, "
                  f"${entry.get('cost_usd')}, {entry.get('seconds')}s {entry.get('error', '')}", flush=True)

    out = Path("reasoning-test")
    out.mkdir(exist_ok=True)
    (out / "results.json").write_text(json.dumps(results, indent=1, ensure_ascii=False))
    print("\nTotal cost: $%.4f" % sum(e.get("cost_usd") or 0 for e in results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
