# sports — Sofascore football archive fetcher

Bulk-downloads daily Sofascore football schedules from **1930-07-13** (first
World Cup match — earliest data available) through **2026-12-31** and stores
each day as `football/YYYY.MM.DD.json`.

Designed to run on **GitHub Actions** because Sofascore's Cloudflare blocks
most home/datacenter IPs. The script itself is resumable, threaded, and
gracefully handles SIGINT/SIGTERM.

## Layout

```
.
├── fetch_sofa.py              # the fetcher
├── requirements.txt
├── .gitignore
└── .github/workflows/fetch.yml
```

Fetched JSON files **do not** live on `main`. They land on a separate orphan
branch called `data` (auto-created on first run) at `data:football/*.json`.
This keeps `main` clean for code review and lets you mirror just the archive.

## First-time setup

```bash
git init -b main
git add .
git commit -m "Initial: fetcher + workflow"
git remote add origin git@github.com:<you>/sports.git
git push -u origin main
```

No secrets to configure — the workflow uses the auto-provided `GITHUB_TOKEN`.

## Running it

1. Open the repo on GitHub → **Actions** tab.
2. Pick **Fetch Sofascore football archive** in the sidebar.
3. Click **Run workflow** (top-right). Defaults already point at
   `1930-07-13 → 2026-12-31` with 4 workers.
4. A single run is capped at ~5h 50m. Click **Run workflow** again to
   continue — already-fetched days are skipped instantly.

To make it self-continue every 6 hours until done, uncomment the `schedule:`
block in `.github/workflows/fetch.yml`.

## Pulling the data locally

```bash
git fetch origin data
git worktree add ../sports-data data
ls ../sports-data/football | wc -l   # number of fetched days
```

## Running locally (when your IP isn't Cloudflare-flagged)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
./fetch_sofa.py --start 1930-07-13 --end 2026-12-31 --workers 4
```

## Notes

- **Workers ≤ 4.** Higher concurrency triggers Cloudflare's bot detection
  during the JS challenge phase. The default is correct.
- **Empty days are not saved.** Sofascore returns `{"events": []}` for most
  pre-1970 dates; the script counts them but writes nothing. They'll be
  re-checked on every run (cheap — one HTTP call each).
- **Failures** are logged to `fetch_sofa.failures.txt` on the `data` branch.
  To retry only those, feed them back into the script day by day.
