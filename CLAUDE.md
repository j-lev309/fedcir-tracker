# Federal Circuit Tracker

A self-updating dashboard tracking all Federal Circuit appeals from January 1, 2020 to present. Hosted on GitHub Pages, refreshed by a Python pipeline running via GitHub Actions.

## Architecture

- **Pipeline**: `scripts/update.py` — fetches data from CourtListener API + CAFC website, enriches cases, writes `data/cases.json`
- **Dashboard**: `index.html` — static JS app that reads `cases.json` and renders case cards with filters
- **Schedule**: GitHub Actions runs the pipeline 5x/day weekdays (dial back to 2-3x once backfill completes)
- **Hosting**: GitHub Pages serves the site from the `main` branch root

## Data Flow

1. **CourtListener API** provides dockets, decision clusters, opinion records, and argument audio metadata
2. **Cases are built from clusters (decisions), NOT dockets** — this is critical. CourtListener's CAFC docket coverage is sparse (~371 via RECAP), but opinion clusters are scraped directly from the court (~1,890+). Phase 1 of assembly iterates clusters; Phase 2 adds undecided dockets.
3. **Opinion text comes from CAFC PDFs** (`cafc.uscourts.gov`), NOT the CourtListener API — this avoids the rate limit entirely. The `download_url` on each opinion record points to the court's own PDF.
4. **Panel parsing** extracts judges and roles from the PDF cover page. Two formats:
   - Third-person: "Opinion for the court filed by Circuit Judge BRYSON."
   - First-person: "CUNNINGHAM, Circuit Judge."
   - The "Before X, Y, and Z, Circuit Judges." line identifies the full panel.
5. **Patent issue classification** uses keyword matching on the same PDF text (§101, §§102/103, §112, PTAB procedure, damages)
6. **Case summaries** (optional, requires ANTHROPIC_API_KEY) use Claude Haiku on the PDF text
7. Results are cached in `data/issue_cache.json` — keyed by cluster ID (`cl-{id}` for issues/summaries, `panel-cl-{id}` for panels)

## Key Files

- `scripts/update.py` — the entire pipeline (single file)
- `scripts/requirements.txt` — Python dependencies (requests, pdfplumber)
- `index.html` — the dashboard (single file, vanilla JS)
- `data/cases.json` — the output consumed by the dashboard
- `data/store.json` — accumulated raw records across runs (dockets, clusters, opinions, audio)
- `data/state.json` — per-source backfill progress, high-water marks, cursor positions
- `data/issue_cache.json` — cached panel parses, issue classifications, and summaries
- `.github/workflows/update.yml` — the GitHub Actions schedule

## Rate Limit Context

The owner has a CourtListener academic membership. Limits are still tight enough that:
- Each run can't finish all sources — they rotate by staleness priority
- Cursor positions persist in `state.json` so backfill advances across runs
- PDF fetches from `cafc.uscourts.gov` are free (not rate-limited)
- `SSL verify=False` is required for CAFC PDFs (their cert chain isn't in GitHub runners' trust store)
- Failed opinion-text fetches are tracked per-session in `FAILED_TEXT_IDS` to prevent retry loops

## Backfill State

Sources track `backfill_done` in `state.json`. A source only earns incremental mode after completing the full window in one clean pass. Until then it re-scans from `WINDOW_START` (2020-01-01). The scope panel on the dashboard shows backfill progress honestly.

## Common Issues

- **Empty panels**: Usually means the PDF fetch failed (check for SSL errors or 404s in the log) or the panel cache has an empty list from a prior failed attempt (empty caches are retried, not treated as valid)
- **Missing cases**: If a case exists on CourtListener but not on the dashboard, check whether it has a cluster record. If only a docket exists, it won't appear until decided.
- **30,000+ tracked items**: A past cursor-resume bug dropped the `court=cafc` filter, accumulating non-CAFC dockets. v22 includes a migration that purges and rebuilds the docket store.
- **Quota exhaustion**: The circuit breaker stops after ~4 minutes of throttling. Cursor position is saved. Next run resumes.
- **"page budget reached"**: Normal during backfill — each source gets a capped number of API pages per run to prevent one source monopolizing quota.

## Panel Parser Formats

The Federal Circuit uses two attribution styles on opinion cover pages:

```
# Third-person (most common in recent opinions):
Before DYK, BRYSON, and STOLL, Circuit Judges.
Opinion for the court filed by Circuit Judge BRYSON.
Opinion dissenting-in-part and concurring-in-part filed by Circuit Judge DYK.

# First-person (older style, still appears):
Before PROST, MAYER, and CUNNINGHAM, Circuit Judges.
CUNNINGHAM, Circuit Judge.
```

The parser handles both, including line-wrapped attributions in PDF-extracted text. Roles captured: Author, Dissent, Concurrence, Dissent in part, Concurrence in part, Concurrence/Dissent in part, Per Curiam, Panel (default for Before-line members with no separate opinion).

## Testing

The pipeline has no formal test suite. Verification has been done through:
- `python3 -m py_compile scripts/update.py` for syntax
- Mock-based integration tests in ad-hoc Python scripts
- Spot-checking dashboard output against real court documents
- The scope panel's honest reporting of what each run accomplished

## Environment Variables

- `COURTLISTENER_TOKEN` (required) — API authentication
- `ANTHROPIC_API_KEY` (optional) — enables AI summaries on decided cases
- `WINDOW_START` (default: "2020-01-01") — fixed floor for the data window
- `WINDOW_MONTHS` (default: 18) — rolling window fallback if WINDOW_START is empty
- `PAGE_BUDGET` / `PAGE_BUDGET_STEADY` — per-source page caps during backfill vs steady state
- `MAX_OPINION_TEXT_FETCHES` (default: 600) — PDF fetches per run for enrichment
- `RL_BUDGET_SECONDS` (default: 240) — circuit breaker threshold for rate-limit sleep time
