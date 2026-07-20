#!/usr/bin/env python3
"""
Federal Circuit Tracker — daily data pipeline.

Sources:
  1. CourtListener REST API v4 (dockets, clusters, opinions, oral-argument audio)
     - requires a free token: https://www.courtlistener.com/profile/api-token/
     - set as env var COURTLISTENER_TOKEN
  2. CAFC "Scheduled Cases" monthly PDFs (upcoming argument dates)
  3. CAFC Opinions & Orders RSS feed (same-day releases, incl. Rule 36 judgments)

Optional:
  ANTHROPIC_API_KEY — if set, cases the keyword classifier can't bucket are
  classified with Claude (claude-haiku-4-5). Results are cached in
  data/issue_cache.json so each opinion is only classified once, ever.

Output: data/cases.json consumed by index.html.

Every source is wrapped in try/except: one flaky feed never kills the run.
"""

import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import requests

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

SCRIPT_VERSION = "v9-incremental (2026-07-20)"
CL_BASE = "https://www.courtlistener.com/api/rest/v4"
CAFC_SCHEDULED_URL = "https://www.cafc.uscourts.gov/home/oral-argument/scheduled-cases/"
CAFC_OPINION_RSS = "https://www.cafc.uscourts.gov/category/opinion-order/feed/"

CL_TOKEN = os.environ.get("COURTLISTENER_TOKEN", "").strip()
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()

WINDOW_MONTHS = int(os.environ.get("WINDOW_MONTHS", "18"))   # rolling window of filings
MAX_DOCKETS = int(os.environ.get("MAX_DOCKETS", "4000"))
MAX_OPINION_TEXT_FETCHES = int(os.environ.get("MAX_OPINION_TEXT_FETCHES", "120"))
REQUEST_TIMEOUT = 90
RL_BUDGET_SECONDS = int(os.environ.get("RL_BUDGET_SECONDS", "240"))  # stop a source after ~4 min throttled
QUOTA_EXHAUSTED = False  # set True once a source gives up on rate limiting
FIRST_SOURCE = True      # only the run's first source treats instant throttling as fatal

# --- incremental fetching -----------------------------------------------
# Re-pulling the whole window every run starved the later sources (opinions,
# audio) of quota. Instead each source records how far it got, and subsequent
# runs fetch only what's new — with a per-source page budget so no single
# source can consume the entire hourly quota, and rotating priority so every
# source gets to go first regularly.
INCREMENTAL = os.environ.get("INCREMENTAL", "1") != "0"
PAGE_BUDGET = int(os.environ.get("PAGE_BUDGET", "12"))       # max pages per source per run
BACKFILL_OVERLAP_DAYS = 3   # re-scan a few days back to catch late-posted records

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUT_PATH = DATA_DIR / "cases.json"
CACHE_PATH = DATA_DIR / "issue_cache.json"
STATE_PATH = DATA_DIR / "state.json"      # per-source high-water marks
STORE_PATH = DATA_DIR / "store.json"      # accumulated raw records across runs

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "fedcir-tracker (personal research dashboard)"})
if CL_TOKEN:
    SESSION.headers.update({"Authorization": f"Token {CL_TOKEN}"})


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


# ----------------------------------------------------------------------------
# CourtListener helpers
# ----------------------------------------------------------------------------

def cl_paginate(url: str, params: dict, cap: int, meta: dict = None,
                page_budget: int = None) -> list:
    """Follow v4 cursor pagination until cap items, page_budget pages, or no `next`.
    If `meta` dict is passed, records {'complete': bool, 'reason': str} describing
    whether the full result set was retrieved or the run was cut short.
    page_budget prevents one source from consuming the entire hourly quota."""
    global QUOTA_EXHAUSTED, FIRST_SOURCE
    if QUOTA_EXHAUSTED:  # a prior source already hit the wall this run; don't re-grind
        if meta is not None:
            meta["complete"], meta["reason"], meta["count"] = False, "quota exhausted", 0
        return []
    pages = 0
    items, next_url, first, backoff, strikes, got_any = [], url, True, 30, 0, False
    complete, reason, timeouts, rl_waited = True, "ok", 0, 0
    while next_url and len(items) < cap:
        try:
            r = SESSION.get(next_url, params=params if first else None, timeout=REQUEST_TIMEOUT)
            if r.status_code == 401:
                raise RuntimeError(
                    "CourtListener returned 401 Unauthorized. The COURTLISTENER_TOKEN "
                    "secret is missing, misspelled, or has a stray space. Re-copy it "
                    "from https://www.courtlistener.com/profile/api-token/ and update "
                    "the repo secret exactly.")
            if r.status_code == 429:
                strikes += 1
                if not got_any and strikes >= 3:
                    if FIRST_SOURCE:
                        # Nothing has succeeded all run — likely a bad token or a
                        # fully spent quota. Worth failing loudly with a diagnosis.
                        raise RuntimeError(
                            "Rate limited on the very first requests with no success. "
                            "Either the token is invalid (treated as anonymous) or this "
                            "token's hourly quota is exhausted from prior runs. Verify with:\n"
                            "  curl -s -H 'Authorization: Token <TOKEN>' "
                            "'https://www.courtlistener.com/api/rest/v4/dockets/?court=cafc&fields=id' "
                            "-o /dev/null -w 'HTTP %{http_code}\\n'\n"
                            "401 = bad token; 429 = wait one hour for the quota to reset.")
                    # A later source: earlier data is good, so keep it and move on
                    # rather than throwing the whole run away.
                    log("  throttled before any results; skipping this source "
                        "(earlier data retained) — it goes first next run")
                    QUOTA_EXHAUSTED = True
                    complete, reason = False, "quota exhausted"
                    break
                if rl_waited >= RL_BUDGET_SECONDS:
                    log(f"  quota exhausted — spent {rl_waited}s throttled with no "
                        f"progress; stopping this source. Wait ~1 hour and re-run; "
                        f"the cache keeps what's already been fetched.")
                    QUOTA_EXHAUSTED = True
                    complete, reason = False, "quota exhausted"
                    break
                log(f"  rate limited; sleeping {backoff}s")
                time.sleep(backoff)
                rl_waited += backoff
                backoff = min(backoff + 30, 120)
                continue
            backoff, strikes = 30, 0
            r.raise_for_status()
            payload = r.json()
        except RuntimeError:
            raise  # fatal auth/quota diagnosis — stop the whole run loudly
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            timeouts += 1
            if timeouts <= 3:
                wait = 15 * timeouts
                log(f"  network timeout ({timeouts}/3); retrying same page in {wait}s")
                time.sleep(wait)
                continue  # retry the SAME page — cursor unchanged
            log(f"  giving up on this source after 3 timeouts: {e}")
            complete, reason = False, "network timeouts"
            break
        except Exception as e:  # noqa: BLE001 — unexpected; keep what we have
            log(f"  pagination stopped: {e}")
            complete, reason = False, f"interrupted ({type(e).__name__})"
            break
        items.extend(payload.get("results", []))
        got_any = True
        pages += 1
        timeouts = 0  # reset per-page timeout counter after a success
        next_url, first = payload.get("next"), False
        if page_budget and pages >= page_budget and next_url:
            complete, reason = False, "page budget reached"
            log(f"  page budget ({page_budget}) reached — resuming next run")
            break
        time.sleep(0.9)  # stay well under 5,000 req/hr
    if len(items) >= cap and next_url:
        complete, reason = False, "hit cap"
    if meta is not None:
        meta["complete"], meta["reason"], meta["count"] = complete, reason, len(items)
    FIRST_SOURCE = False  # subsequent sources degrade gracefully instead of raising
    return items[:cap]


def fetch_dockets(since: str, meta: dict = None) -> list:
    log(f"Fetching CAFC dockets filed since {since} …")
    fields = ",".join([
        "id", "docket_number", "case_name", "case_name_short", "date_filed",
        "date_argued", "date_terminated", "appeal_from_str", "appeal_from",
        "absolute_url", "nature_of_suit", "panel_str",
    ])
    items = cl_paginate(
        f"{CL_BASE}/dockets/",
        {"court": "cafc", "date_filed__gte": since, "order_by": "-date_filed", "fields": fields},
        MAX_DOCKETS, meta, PAGE_BUDGET,
    )
    log(f"  {len(items)} dockets")
    return items


def fetch_clusters(since: str, meta: dict = None) -> list:
    """Decisions (opinion clusters) — carries precedential status + panel judges."""
    log(f"Fetching CAFC opinion clusters since {since} …")
    fields = ",".join([
        "id", "absolute_url", "case_name", "date_filed", "precedential_status",
        "judges", "docket_id", "nature_of_suit", "syllabus", "headnotes",
        "disposition", "sub_opinions",
    ])
    items = cl_paginate(
        f"{CL_BASE}/clusters/",
        {"docket__court": "cafc", "date_filed__gte": since,
         "order_by": "-date_filed", "fields": fields},
        MAX_DOCKETS, meta, PAGE_BUDGET,
    )
    log(f"  {len(items)} clusters")
    return items


def fetch_opinions(since: str, meta: dict = None, after_id: int = 0) -> list:
    """Individual opinions — author, joined-by, and lead/concurrence/dissent type."""
    log("Fetching CAFC opinions (authorship / roles) …")
    fields = ",".join([
        "id", "cluster_id", "author_str", "joined_by_str", "type",
        "download_url", "absolute_url",
    ])
    items = cl_paginate(
        f"{CL_BASE}/opinions/",
        {"cluster__docket__court": "cafc", "cluster__date_filed__gte": since,
         "order_by": "id" if INCREMENTAL else "-id", "fields": fields,
         **({"id__gt": after_id} if (INCREMENTAL and after_id) else {})},
        MAX_DOCKETS, meta, PAGE_BUDGET,
    )
    log(f"  {len(items)} opinions")
    return items


def fetch_audio(since: str, meta: dict = None, after_id: int = 0) -> list:
    """Oral-argument recordings — confirms argued dates and panel names.
    Note: the v4 audio endpoint doesn't support docket__date_argued__gte,
    so we filter by the recording's own date_created (argument day)."""
    log("Fetching CAFC oral-argument audio metadata …")
    fields = ",".join(["id", "docket", "case_name", "judges", "absolute_url"])
    items = cl_paginate(
        f"{CL_BASE}/audio/",
        {"docket__court": "cafc", "date_created__gte": since,
         "order_by": "id" if INCREMENTAL else "-date_created", "fields": fields,
         **({"id__gt": after_id} if (INCREMENTAL and after_id) else {})},
        2000, meta, PAGE_BUDGET,
    )
    log(f"  {len(items)} recordings")
    return items


def fetch_opinion_text(opinion_id: int) -> str:
    """Plain text of one opinion (for issue classification). Capped by caller."""
    try:
        time.sleep(0.9)  # pacing: these add up during the summary backfill
        r = SESSION.get(f"{CL_BASE}/opinions/{opinion_id}/",
                        params={"fields": "plain_text,html_with_citations"},
                        timeout=REQUEST_TIMEOUT)
        if r.status_code == 429:
            log("  rate limited on opinion text; sleeping 60s")
            time.sleep(60)
            r = SESSION.get(f"{CL_BASE}/opinions/{opinion_id}/",
                            params={"fields": "plain_text,html_with_citations"},
                            timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        d = r.json()
        text = d.get("plain_text") or ""
        if not text and d.get("html_with_citations"):
            text = re.sub(r"<[^>]+>", " ", d["html_with_citations"])
        return text[:60000]
    except Exception as e:  # noqa: BLE001
        log(f"  opinion text {opinion_id} failed: {e}")
        return ""


# ----------------------------------------------------------------------------
# CAFC website: scheduled-cases PDFs + opinion RSS
# ----------------------------------------------------------------------------

DOCKET_RE = re.compile(r"\b((?:20)?\d{2}-\d{3,5})\b")


def norm_dn(dn: str) -> str:
    """Normalize docket numbers to 4-digit-year form: '25-1444' -> '2025-1444'."""
    m = re.match(r"^(\d{2})-(\d{3,5})$", dn or "")
    return f"20{m.group(1)}-{m.group(2)}" if m else (dn or "")
DATE_LINE_RE = re.compile(
    r"(Monday|Tuesday|Wednesday|Thursday|Friday),?\s+"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2}),?\s+(20\d{2})", re.I)
TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?))", re.I)
COURTROOM_RE = re.compile(r"courtroom\s+(\w+)", re.I)


def fetch_scheduled_arguments() -> dict:
    """Parse the monthly Scheduled Cases PDFs → {docket_number: {date,time,courtroom}}."""
    log("Fetching CAFC scheduled-cases PDFs …")
    out: dict = {}
    try:
        import pdfplumber  # imported lazily so a broken install degrades gracefully
    except Exception:
        log("  pdfplumber unavailable; skipping argument schedule")
        return out
    try:
        page = SESSION.get(CAFC_SCHEDULED_URL, timeout=REQUEST_TIMEOUT)
        page.raise_for_status()
        pdf_urls = list(dict.fromkeys(re.findall(
            r'href="(https?://[^"]*?\.pdf)"', page.text, re.I)))
        pdf_urls = [u for u in pdf_urls if "sched" in u.lower() or "cases" in u.lower()][:4]
    except Exception as e:  # noqa: BLE001
        log(f"  could not load scheduled-cases page: {e}")
        return out

    for url in pdf_urls:
        try:
            r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            with pdfplumber.open(BytesIO(r.content)) as pdf:
                text = "\n".join((p.extract_text() or "") for p in pdf.pages)
        except Exception as e:  # noqa: BLE001
            log(f"  PDF failed {url}: {e}")
            continue

        cur_date, cur_time, cur_room = None, None, None
        for line in text.splitlines():
            m = DATE_LINE_RE.search(line)
            if m:
                try:
                    cur_date = datetime.strptime(
                        f"{m.group(2)} {m.group(3)} {m.group(4)}", "%B %d %Y"
                    ).date().isoformat()
                except ValueError:
                    pass
            t = TIME_RE.search(line)
            if t:
                cur_time = re.sub(r"\s+", " ", t.group(1)).lower().replace(".", "")
            c = COURTROOM_RE.search(line)
            if c:
                cur_room = f"Courtroom {c.group(1)}"
            for dn in DOCKET_RE.findall(line):
                dn = norm_dn(dn)
                if cur_date:
                    caption = DOCKET_RE.sub("", line).strip(" -–—\t")
                    out.setdefault(dn, {
                        "date": cur_date, "time": cur_time,
                        "courtroom": cur_room, "caption": caption[:160],
                    })
        log(f"  parsed {url.rsplit('/', 1)[-1]}")
    log(f"  {len(out)} scheduled arguments")
    return out


DISPO_TAG_RE = re.compile(r"\[([^\]]+)\]")


def fetch_opinion_rss() -> dict:
    """CAFC opinion/order RSS → {docket_number: {disposition, url, date}}.
    Catches same-day releases (incl. Rule 36) before CourtListener ingests them."""
    log("Fetching CAFC opinion RSS …")
    out: dict = {}
    try:
        r = SESSION.get(CAFC_OPINION_RSS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.content)
    except Exception as e:  # noqa: BLE001
        log(f"  RSS failed: {e}")
        return out
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        try:
            pub_date = datetime.strptime(pub[:16], "%a, %d %b %Y").date().isoformat()
        except ValueError:
            pub_date = None
        tag = DISPO_TAG_RE.search(title)
        dispo = tag.group(1).title() if tag else "Opinion/Order"
        for dn in map(norm_dn, DOCKET_RE.findall(title)):
            out.setdefault(dn, {"disposition": dispo, "url": link, "date": pub_date,
                                "title": title[:200]})
    log(f"  {len(out)} RSS items")
    return out


# ----------------------------------------------------------------------------
# Classification
# ----------------------------------------------------------------------------

CASE_TYPE_RULES = [
    ("Patent (PTAB)",        r"patent trial|ptab|patent and trademark|uspto|trademark trial"),
    ("Veterans",             r"veterans claims|cavc|veterans appeals"),
    ("Trade (CIT)",          r"international trade court|court of international trade|\bcit\b"),
    ("ITC / § 337",          r"international trade commission|\bitc\b"),
    ("Federal Claims",       r"federal claims"),
    ("Federal Employment",   r"merit systems|mspb|personnel"),
    ("Patent (District Ct.)", r"district"),
]

VETERANS_HINT = re.compile(r"\bv\.?\s+(mcdonough|collins|wilkie|shulkin)\b", re.I)
PTO_HINT = re.compile(r"\bv\.?\s+(vidal|stewart|squires|hirshfeld|iancu)\b|in re\b", re.I)
MSPB_HINT = re.compile(r"\bv\.?\s+(merit systems|mspb|office of personnel|opm)\b", re.I)

ISSUE_RULES = {
    "§ 101 Eligibility": [
        r"§+\s*101", r"section 101", r"patent[- ]eligib", r"abstract idea",
        r"\balice\b", r"\bmayo\b.{0,40}collaborative", r"inventive concept",
    ],
    "§§ 102/103 Prior Art": [
        r"§+\s*10[23]", r"section 10[23]", r"anticipat(?:ion|ed|es)", r"obviousness",
        r"prior art", r"motivation to combine", r"\bksr\b", r"secondary considerations",
        r"reasonable expectation of success",
    ],
    "§ 112 / Claim Construction": [
        r"§+\s*112", r"section 112", r"written description", r"enablement",
        r"indefinite", r"claim construction", r"\bmarkman\b", r"\bphillips\b.{0,30}awh",
        r"plain and ordinary meaning", r"means[- ]plus[- ]function",
    ],
    "PTAB Procedure & Standing": [
        r"inter partes review", r"\bipr\b", r"institution decision", r"§+\s*31[4-8]",
        r"estoppel", r"real part(?:y|ies) in interest", r"\bfintiv\b",
        r"appointments clause", r"article iii standing", r"director review",
        r"discretionary denial",
    ],
    "Damages & Remedies": [
        r"reasonable royalty", r"lost profits", r"willful", r"enhanced damages",
        r"injunction", r"apportionment", r"\bebay\b.{0,30}mercexchange",
    ],
}


def classify_case_type(docket: dict) -> str:
    origin = " ".join(str(docket.get(k) or "") for k in
                      ("appeal_from_str", "appeal_from", "nature_of_suit")).lower()
    for label, pat in CASE_TYPE_RULES:
        if re.search(pat, origin):
            return label
    name = docket.get("case_name") or ""
    if VETERANS_HINT.search(name):
        return "Veterans"
    if PTO_HINT.search(name):
        return "Patent (PTAB)"
    if MSPB_HINT.search(name):
        return "Federal Employment"
    return "Other / Unclassified"


def classify_issues_keywords(text: str) -> list:
    found = []
    low = text.lower()
    for label, pats in ISSUE_RULES.items():
        hits = sum(len(re.findall(p, low)) for p in pats)
        if hits >= 2:  # require 2+ hits to avoid stray-cite noise
            found.append((hits, label))
    return [lbl for _, lbl in sorted(found, reverse=True)]


def classify_issues_claude(case_name: str, text: str) -> list:
    """Optional fallback via Claude for opinions the keyword pass missed."""
    if not ANTHROPIC_KEY or not text:
        return []
    buckets = list(ISSUE_RULES.keys()) + ["Other"]
    prompt = (
        "You are classifying a Federal Circuit patent decision by primary legal "
        f"issue. Case: {case_name}\n\nOpinion excerpt:\n{text[:6000]}\n\n"
        f"Choose up to 2 labels from this list only: {json.dumps(buckets)}. "
        'Respond with ONLY a JSON array of strings, e.g. ["§ 101 Eligibility"].'
    )
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5", "max_tokens": 100,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=60,
        )
        r.raise_for_status()
        raw = "".join(b.get("text", "") for b in r.json().get("content", []))
        labels = [x for x in json.loads(re.sub(r"```(json)?", "", raw).strip())
                  if x in buckets and x != "Other"]
    except Exception as e:  # noqa: BLE001
        log(f"  Claude classify failed: {e}")
        labels = []
    return labels


def summarize_claude(case_name: str, text: str) -> str | None:
    """2–3 sentence neutral summary of a decision. Requires ANTHROPIC_API_KEY."""
    if not ANTHROPIC_KEY or not text or len(text) < 400:
        return None
    prompt = (
        "Summarize this Federal Circuit decision in 2-3 sentences for a law-student "
        "reader: state the disposition/holding and the key reasoning. Be neutral, "
        "specific, and concrete (name the statute/doctrine at issue). No preamble, "
        "no markdown — just the sentences.\n\n"
        f"Case: {case_name}\n\nOpinion text:\n{text[:14000]}"
    )
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            json={"model": "claude-haiku-4-5", "max_tokens": 300,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=90,
        )
        r.raise_for_status()
        out = "".join(b.get("text", "") for b in r.json().get("content", [])).strip()
        return out[:1200] or None
    except Exception as e:  # noqa: BLE001
        log(f"  Claude summary failed: {e}")
        return None


# ----------------------------------------------------------------------------
# Panel formatting
# ----------------------------------------------------------------------------

OPINION_TYPE_MAP = {
    "lead": "Author", "combined": "Author", "majority": "Author",
    "concur": "Concurrence", "concurrence": "Concurrence",
    "dissent": "Dissent", "concur-in-part": "Concur/Dissent in part",
    "concurring-in-part-and-dissenting-in-part": "Concur/Dissent in part",
    "per-curiam": "Per Curiam",
}


def norm_type(t: str) -> str:
    t = re.sub(r"^\d+", "", (t or "")).strip().lower()
    for k, v in OPINION_TYPE_MAP.items():
        if k in t:
            return v
    return "Opinion"


def split_judges(s: str) -> list:
    parts = re.split(r",| and |;|\band\b", s or "")
    return [p.strip() for p in parts if p.strip() and len(p.strip()) > 2]


# ----------------------------------------------------------------------------
# Assembly
# ----------------------------------------------------------------------------

def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text())
    except Exception:  # noqa: BLE001 — missing or corrupt: start fresh
        return default


def _merge_store(store: dict, key: str, new_records: list, id_field: str = "id") -> list:
    """Merge newly fetched records into the persisted store, de-duped by id.
    Newer records replace older ones with the same id. Returns the full set."""
    existing = {str(r.get(id_field)): r for r in store.get(key, [])
                if r.get(id_field) is not None}
    for r in new_records:
        if r.get(id_field) is not None:
            existing[str(r[id_field])] = r
    merged = list(existing.values())
    store[key] = merged
    return merged


def build() -> dict:
    since = (date.today() - timedelta(days=30 * WINDOW_MONTHS)).isoformat()
    cache = {}
    if CACHE_PATH.exists():
        try:
            cache = json.loads(CACHE_PATH.read_text())
        except Exception:  # noqa: BLE001
            cache = {}

    cov = {k: {} for k in ("dockets", "clusters", "opinions", "audio")}

    # ---- load persisted state and accumulated raw records ------------------
    state = _load_json(STATE_PATH, {})
    store = _load_json(STORE_PATH, {})
    run_no = int(state.get("run_no", 0)) + 1
    full_since = since

    def _since_for(key: str) -> str:
        """Incremental: only fetch what's new since this source last succeeded."""
        if not INCREMENTAL:
            return full_since
        last = (state.get(key) or {}).get("last_date")
        if not last:
            return full_since
        try:
            d = datetime.strptime(last, "%Y-%m-%d").date() - timedelta(days=BACKFILL_OVERLAP_DAYS)
            return max(d.isoformat(), full_since)
        except ValueError:
            return full_since

    # Priority by staleness: the source that has gone longest without a clean
    # finish goes first. A simple rotation could starve whichever source sat
    # last in the list, so we sort by (caught_up, last_ok) instead — never
    # caught up sorts first, then oldest success.
    order = ["dockets", "clusters", "opinions", "audio"]

    def _staleness(k):
        st = state.get(k) or {}
        # Never-caught-up sources come first; among those, the one attempted
        # longest ago goes first, so a source that fails repeatedly still yields
        # its turn instead of blocking the queue every run.
        return (1 if st.get("caught_up") else 0, st.get("last_attempt") or "")

    order.sort(key=_staleness)
    log(f"Run #{run_no} — source order: {', '.join(order)}"
        + (f" (incremental)" if INCREMENTAL else " (full window)"))

    results: dict = {}
    for key in order:
        if key == "dockets":
            results[key] = fetch_dockets(_since_for(key), cov[key])
        elif key == "clusters":
            results[key] = fetch_clusters(_since_for(key), cov[key])
        elif key == "opinions":
            results[key] = fetch_opinions(_since_for(key), cov[key],
                                          (state.get(key) or {}).get("last_id", 0))
        elif key == "audio":
            results[key] = fetch_audio(_since_for(key), cov[key],
                                       (state.get(key) or {}).get("last_id", 0))

    # ---- merge this run's records into the accumulated store ---------------
    dockets = _merge_store(store, "dockets", results.get("dockets", []), "id")
    clusters = _merge_store(store, "clusters", results.get("clusters", []), "id")
    opinions = _merge_store(store, "opinions", results.get("opinions", []), "id")
    audio = _merge_store(store, "audio", results.get("audio", []), "id")
    log(f"Store totals — dockets {len(dockets)}, clusters {len(clusters)}, "
        f"opinions {len(opinions)}, audio {len(audio)}")

    # ---- advance high-water marks for sources that finished cleanly --------
    for key, recs in (("dockets", results.get("dockets", [])),
                      ("clusters", results.get("clusters", [])),
                      ("opinions", results.get("opinions", [])),
                      ("audio", results.get("audio", []))):
        st = state.setdefault(key, {})
        ok = cov[key].get("complete", False)
        if recs:
            ids = [r.get("id") for r in recs if isinstance(r.get("id"), int)]
            if ids:
                st["last_id"] = max(st.get("last_id", 0), max(ids))
            dates = [r.get("date_filed") or r.get("date_created") for r in recs]
            dates = [d for d in dates if d]
            if dates:
                st["last_date"] = max(st.get("last_date", ""), max(dates))[:10]
        if ok:
            st["caught_up"] = True
            st["last_ok"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        elif cov[key].get("reason") in ("quota exhausted", "network timeouts"):
            st["caught_up"] = False
        # Always advance the attempt clock so a source that keeps failing still
        # rotates out of first place and doesn't monopolize the front of the queue.
        st["last_attempt"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["run_no"] = run_no

    scheduled = fetch_scheduled_arguments()
    rss = fetch_opinion_rss()

    ops_by_cluster: dict = {}
    for op in opinions:
        cid = op.get("cluster_id")
        if cid:
            ops_by_cluster.setdefault(cid, []).append(op)

    clusters_by_docket: dict = {}
    for cl in clusters:
        did = cl.get("docket_id")
        if did:
            clusters_by_docket.setdefault(did, []).append(cl)

    audio_by_docket: dict = {}
    for a in audio:
        m = re.search(r"/dockets?/(\d+)", str(a.get("docket") or ""))
        did = int(m.group(1)) if m else a.get("docket") if isinstance(a.get("docket"), int) else None
        if did:
            audio_by_docket[did] = a

    cases, text_fetches = [], 0
    for d in dockets:
        did = d["id"]
        dn = norm_dn((d.get("docket_number") or "").strip())
        case_type = classify_case_type(d)
        d_clusters = sorted(clusters_by_docket.get(did, []),
                            key=lambda c: c.get("date_filed") or "", reverse=True)
        lead_cluster = d_clusters[0] if d_clusters else None

        # ---- status -----------------------------------------------------
        decided = bool(lead_cluster) or dn in rss
        argued_date = d.get("date_argued")
        sched = scheduled.get(dn)
        if decided:
            status = "Decided"
        elif argued_date:
            status = "Argued — Awaiting Decision"
        elif sched:
            status = "Argument Scheduled"
        else:
            status = "Pending / Briefing"

        # ---- decision block ---------------------------------------------
        decision, panel = None, []
        if lead_cluster:
            prec = (lead_cluster.get("precedential_status") or "").title()
            cluster_ops = ops_by_cluster.get(lead_cluster["id"], [])
            for op in cluster_ops:
                role = norm_type(op.get("type"))
                author = (op.get("author_str") or "").strip()
                if author:
                    panel.append({"name": author, "role": role})
                for j in split_judges(op.get("joined_by_str") or ""):
                    panel.append({"name": j, "role": "Joined"})
            for j in split_judges(lead_cluster.get("judges") or ""):
                if not any(p["name"].lower() == j.lower() for p in panel):
                    panel.append({"name": j, "role": "Panel"})
            rss_hit = rss.get(dn) or {}
            dispo = (lead_cluster.get("disposition") or rss_hit.get("disposition") or "").strip()
            if prec.startswith("Unpub") and re.search(r"rule\s*36", dispo, re.I):
                dispo = "Rule 36 Judgment"
            pdf = ""
            for op in cluster_ops:
                if op.get("download_url"):
                    pdf = op["download_url"]
                    break
            decision = {
                "date": lead_cluster.get("date_filed"),
                "precedential_status": prec or None,
                "disposition": dispo or None,
                "url_cl": ("https://www.courtlistener.com" + lead_cluster["absolute_url"])
                          if lead_cluster.get("absolute_url") else None,
                "url_pdf": pdf or (rss_hit.get("url") or None),
            }
        elif dn in rss:  # released today; CL hasn't ingested yet
            h = rss[dn]
            is_r36 = bool(re.search(r"rule\s*36", h.get("disposition") or "", re.I))
            decision = {
                "date": h.get("date"),
                "precedential_status": "Unpublished" if is_r36 else None,
                "disposition": h.get("disposition"),
                "url_cl": None, "url_pdf": h.get("url"),
            }

        # ---- panel from audio if not decided ------------------------------
        if not panel and did in audio_by_docket:
            for j in split_judges(audio_by_docket[did].get("judges") or ""):
                panel.append({"name": j, "role": "Panel"})

        # ---- opinion enrichment: patent issues + case summary --------------
        issues, summary = [], None
        is_patent = case_type.startswith(("Patent", "ITC"))
        is_r36 = bool(decision and re.search(
            r"rule\s*36", str(decision.get("disposition") or ""), re.I))
        lead_ops = ops_by_cluster.get(lead_cluster["id"], []) if lead_cluster else []
        cache_key = f"op-{lead_ops[0]['id']}" if lead_ops else None

        cached = cache.get(cache_key) if cache_key else None
        if isinstance(cached, list):          # migrate pre-summary cache format
            cached = {"issues": cached, "summary": None}
        if cached:
            issues = cached.get("issues") or []
            summary = cached.get("summary")

        if is_patent and not issues:          # cheap pass before spending a fetch
            seed_text = " ".join(str(x or "") for x in (
                (lead_cluster or {}).get("syllabus"), (lead_cluster or {}).get("headnotes"),
                (lead_cluster or {}).get("disposition"), d.get("case_name")))
            issues = classify_issues_keywords(seed_text)

        need_issues = is_patent and not issues and bool(lead_ops)
        need_summary = (bool(ANTHROPIC_KEY) and decision is not None
                        and not summary and not is_r36 and bool(lead_ops))
        if cache_key and (need_issues or need_summary) \
                and text_fetches < MAX_OPINION_TEXT_FETCHES:
            opinion_text = fetch_opinion_text(lead_ops[0]["id"])
            text_fetches += 1
            if need_issues and opinion_text:
                issues = (classify_issues_keywords(opinion_text)
                          or classify_issues_claude(d.get("case_name") or dn, opinion_text))
            if need_summary:
                summary = summarize_claude(d.get("case_name") or dn, opinion_text)
            cache[cache_key] = {"issues": issues, "summary": summary}

        if is_r36 and not summary:
            summary = ("Judgment of the tribunal below summarily affirmed without "
                       "opinion under Federal Circuit Rule 36.")
        if is_patent and not issues and status == "Decided":
            issues = ["Other / Procedural"]

        cases.append({
            "docket_number": dn,
            "case_name": d.get("case_name") or d.get("case_name_short") or dn,
            "case_type": case_type,
            "origin": d.get("appeal_from_str") or None,
            "status": status,
            "date_filed": d.get("date_filed"),
            "date_argued": argued_date,
            "argument": sched,
            "decision": decision,
            "panel": panel,
            "patent_issues": issues,
            "summary": summary,
            "url_cl": ("https://www.courtlistener.com" + d["absolute_url"])
                      if d.get("absolute_url") else None,
            "url_audio": ("https://www.courtlistener.com" +
                          audio_by_docket[did]["absolute_url"])
                         if did in audio_by_docket and audio_by_docket[did].get("absolute_url")
                         else None,
        })

    # Upcoming arguments that never matched a CourtListener docket still matter
    known = {c["docket_number"] for c in cases}
    for dn, s in scheduled.items():
        if dn not in known and s.get("date") and s["date"] >= date.today().isoformat():
            cases.append({
                "docket_number": dn, "case_name": s.get("caption") or dn,
                "case_type": "Other / Unclassified", "origin": None,
                "status": "Argument Scheduled", "date_filed": None,
                "date_argued": None, "argument": s, "decision": None,
                "panel": [], "patent_issues": [], "summary": None,
                "url_cl": None, "url_audio": None,
            })

    cases.sort(key=lambda c: (c.get("decision") or {}).get("date")
               or (c.get("argument") or {}).get("date")
               or c.get("date_filed") or "", reverse=True)

    today = date.today().isoformat()
    upcoming = sorted(
        [c for c in cases if (c.get("argument") or {}).get("date", "") >= today
         and c["status"] != "Decided"],
        key=lambda c: c["argument"]["date"])

    stats = {
        "total": len(cases),
        "pending": sum(c["status"].startswith("Pending") for c in cases),
        "scheduled": sum(c["status"] == "Argument Scheduled" for c in cases),
        "awaiting": sum(c["status"].startswith("Argued") for c in cases),
        "decided": sum(c["status"] == "Decided" for c in cases),
        "precedential": sum(1 for c in cases if str((c.get("decision") or {})
                            .get("precedential_status") or "").startswith("Pub")),
        "rule36": sum(1 for c in cases if re.search(
            r"rule\s*36", str((c.get("decision") or {}).get("disposition") or ""), re.I)),
    }

    DATA_DIR.mkdir(exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, indent=1))
    STATE_PATH.write_text(json.dumps(state, indent=1))
    STORE_PATH.write_text(json.dumps(store, separators=(",", ":")))

    # ---- coverage: what this file actually contains, and how complete -----
    summarized = sum(1 for c in cases if c.get("summary"))
    decided_summarizable = sum(
        1 for c in cases if c["status"] == "Decided"
        and not re.search(r"rule\s*36",
                          str((c.get("decision") or {}).get("disposition") or ""), re.I))
    all_complete = all(cov[k].get("complete", True) for k in cov)
    latest_decision = max((c["decision"]["date"] for c in cases
                           if (c.get("decision") or {}).get("date")), default=None)
    next_arg = min((c["argument"]["date"] for c in cases
                    if (c.get("argument") or {}).get("date", "") >= today
                    and c["status"] != "Decided"), default=None)
    coverage = {
        "complete": all_complete,
        "window_since": since,
        "window_months": WINDOW_MONTHS,
        "sources": {
            "dockets": cov["dockets"], "clusters": cov["clusters"],
            "opinions": cov["opinions"], "audio": cov["audio"],
            "scheduled_arguments": {"count": len(scheduled), "complete": True,
                                    "reason": "ok"},
            "opinion_rss": {"count": len(rss), "complete": True, "reason": "ok"},
        },
        "summaries": {
            "enabled": bool(ANTHROPIC_KEY),
            "have": summarized,
            "expected": decided_summarizable,
            "complete": (not ANTHROPIC_KEY) or summarized >= decided_summarizable,
        },
        "opinion_text_fetches_this_run": text_fetches,
        "opinion_text_fetch_cap": MAX_OPINION_TEXT_FETCHES,
        "latest_decision_date": latest_decision,
        "next_argument_date": next_arg,
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window_since": since,
        "coverage": coverage,
        "stats": stats,
        "upcoming_argument_docket_numbers": [c["docket_number"] for c in upcoming],
        "cases": cases,
    }


def main() -> int:
    log(f"Federal Circuit Tracker pipeline {SCRIPT_VERSION}")
    if not CL_TOKEN:
        log("ERROR: COURTLISTENER_TOKEN is not set. Get a free token at "
            "https://www.courtlistener.com/profile/api-token/ and add it as a "
            "GitHub Actions secret named COURTLISTENER_TOKEN.")
        return 1
    data = build()
    OUT_PATH.write_text(json.dumps(data, indent=1))
    cov = data["coverage"]
    log(f"Wrote {OUT_PATH} — {data['stats']['total']} cases "
        f"({data['stats']['decided']} decided, {data['stats']['scheduled']} scheduled).")
    log(f"Coverage: {'COMPLETE' if cov['complete'] else 'PARTIAL — a source was cut short; next run will fill in'}"
        f" | window since {cov['window_since']}"
        f" | summaries {cov['summaries']['have']}/{cov['summaries']['expected']}"
        f"{' (disabled)' if not cov['summaries']['enabled'] else ''}")
    if not cov["complete"]:
        for name, m in cov["sources"].items():
            if not m.get("complete", True):
                log(f"  partial source: {name} — {m.get('reason')} ({m.get('count')} items)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
