# Federal Circuit Tracker

A self-updating dashboard of Federal Circuit appeals: docketed → argument scheduled → argued → decided, with panel composition and roles, case-type sorting (Patent, Trade, Veterans, etc.), and patent-issue sub-classification (§ 101, §§ 102/103, § 112/claim construction, PTAB procedure & standing).

Live data sources:

- **CourtListener v4 API** (Free Law Project) — dockets, opinion clusters, opinion authorship/dissents, argument audio
- **cafc.uscourts.gov** — monthly *Scheduled Cases* PDFs (upcoming argument dates/times/courtrooms) and the *Opinions & Orders* RSS feed (same-day releases, including Rule 36 judgments)

The pipeline runs twice each weekday via GitHub Actions and commits a fresh `data/cases.json`, which the static dashboard (`index.html`) reads. GitHub Pages serves the site.

## Setup (~10 minutes, all free)

1. **Create the repo.** Make a new GitHub repository (public is easiest for Pages) and push these files to `main`.

2. **Get a CourtListener token.** Create a free account at courtlistener.com, then copy your API token from <https://www.courtlistener.com/profile/api-token/>.

3. **Add secrets.** In the repo: *Settings → Secrets and variables → Actions → New repository secret*:
   - `COURTLISTENER_TOKEN` — required.
   - `ANTHROPIC_API_KEY` — optional. If present, patent cases the keyword classifier can't bucket get classified by Claude (claude-haiku, ~fractions of a cent per case, cached forever in `data/issue_cache.json`).

4. **Enable Pages.** *Settings → Pages → Source: Deploy from a branch → Branch: `main` / (root)*. Your site will be at `https://<username>.github.io/<repo>/`.

5. **Run it once.** *Actions → Update Federal Circuit data → Run workflow*. When it finishes, the sample data is replaced with real cases and the yellow banner disappears.

That's it — it now refreshes itself at 11:45 a.m. and 5:00 p.m. ET every weekday.

## Configuration

Environment variables (set in the workflow or repo variables):

| Variable | Default | Meaning |
|---|---|---|
| `WINDOW_MONTHS` | 18 | Rolling window of docketed cases to track |
| `MAX_DOCKETS` | 4000 | Hard cap on cases per source |
| `MAX_OPINION_TEXT_FETCHES` | 120 | Opinion full-texts fetched per run for issue classification (cached, so the backlog clears within a few runs) |

## Known limitations (by design of the court, not the code)

- **Panels are not knowable in advance.** The Federal Circuit posts panel names one hour before argument. Panels appear here once a case is argued (from argument audio metadata) or decided (from the opinion, with author / joined / dissent / concurrence roles).
- **Pending-appeal coverage is partial.** CourtListener's docket database is built from its scrapers plus RECAP contributions; a newly docketed appeal that nobody has pulled from PACER may not appear until it shows up on an argument calendar or in an opinion. Everything argued or decided is captured.
- **Scheduled Cases PDFs vary in format.** The parser is deliberately tolerant, but if the Clerk's Office changes the layout, argument-date extraction may need a regex tweak in `scripts/update.py` (`fetch_scheduled_arguments`).

## Local development

```bash
pip install -r scripts/requirements.txt
export COURTLISTENER_TOKEN=...   # your token
python scripts/update.py
python -m http.server 8000       # then open http://localhost:8000
```
