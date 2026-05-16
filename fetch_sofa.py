#!/usr/bin/env python3
"""
Bulk-fetch Sofascore scheduled football events for a date range.

Each day is saved to `<out-dir>/YYYY.MM.DD.json`. The script is resumable
(skips files that already exist), threaded, retries transient failures
with exponential backoff, and shuts down cleanly on SIGINT/SIGTERM.

Usage:
    python3 fetch_sofa.py --start 1900-01-01 --end 2026-12-31 --workers 6

Run inside the venv that has cloudscraper installed, e.g.:
    /tmp/sofa_venv/bin/python3 fetch_sofa.py ...
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from curl_cffi import requests as cffi_requests
from curl_cffi.requests.exceptions import RequestException

API = "https://api.sofascore.com/api/v1/sport/football/scheduled-events/{date}"

# ---------- shutdown coordination ----------

_shutdown = threading.Event()


def _install_signal_handlers() -> None:
    def handler(signum, _frame):
        if not _shutdown.is_set():
            logging.warning("Signal %s received — finishing in-flight work and stopping…", signum)
            _shutdown.set()
        else:
            logging.error("Second signal received — exiting immediately.")
            os._exit(130)

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


# ---------- shared scraper session ----------
#
# curl_cffi impersonates real Chrome at the TLS/JA3 layer, which is what
# Cloudflare actually fingerprints. cloudscraper (pure Python) failed on
# GitHub-hosted runners because its TLS signature is recognisably non-Chrome.
#
# One Session object shared across all workers — curl_cffi sessions are safe
# for concurrent GETs.

IMPERSONATE = "chrome"

_scraper_lock = threading.Lock()
_scraper_obj: Optional[cffi_requests.Session] = None


def _build_scraper() -> cffi_requests.Session:
    s = cffi_requests.Session(impersonate=IMPERSONATE)
    s.headers.update({
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.sofascore.com/",
        "Origin": "https://www.sofascore.com",
    })
    return s


def _scraper() -> cffi_requests.Session:
    global _scraper_obj
    s = _scraper_obj
    if s is None:
        with _scraper_lock:
            if _scraper_obj is None:
                _scraper_obj = _build_scraper()
            s = _scraper_obj
    return s


def _reset_scraper() -> None:
    """Rebuild the shared session after a stretch of 403/429 responses.

    Serialized so only one thread rebuilds while others wait."""
    global _scraper_obj
    with _scraper_lock:
        _scraper_obj = _build_scraper()


def _warmup(timeout: float) -> bool:
    """Prime the Cloudflare challenge once, single-threaded.

    Returns True on a 2xx. We hit a date well-inside the known data range so
    a 200 is the expected outcome — this makes any failure here unambiguous."""
    url = API.format(date="2025-01-15")
    try:
        r = _scraper().get(url, timeout=timeout)
        return r.status_code == 200
    except RequestException:
        return False


# ---------- fetch logic ----------

@dataclass
class Stats:
    saved: int = 0
    skipped: int = 0
    failed: int = 0
    empty: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def bump(self, key: str, n: int = 1) -> None:
        with self.lock:
            setattr(self, key, getattr(self, key) + n)

    def snapshot(self) -> dict[str, int]:
        with self.lock:
            return {"saved": self.saved, "skipped": self.skipped, "failed": self.failed, "empty": self.empty}


def _backoff(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter."""
    return random.uniform(0, min(cap, base * (2 ** attempt)))


def fetch_one(
    d: date,
    out_dir: Path,
    *,
    max_attempts: int,
    base_backoff: float,
    cap_backoff: float,
    timeout: float,
    stats: Stats,
) -> Optional[str]:
    """Download a single day. Returns None on success, otherwise an error string."""
    iso = d.strftime("%Y-%m-%d")
    fname = out_dir / f"{d.strftime('%Y.%m.%d')}.json"

    if fname.exists() and fname.stat().st_size > 0:
        stats.bump("skipped")
        return None

    url = API.format(date=iso)
    last_err = "unknown"

    for attempt in range(max_attempts):
        if _shutdown.is_set():
            return "shutdown"
        try:
            r = _scraper().get(url, timeout=timeout)
        except RequestException as e:
            last_err = f"net: {type(e).__name__}: {e}"
            time.sleep(_backoff(attempt, base_backoff, cap_backoff))
            continue

        sc = r.status_code
        if sc == 200:
            try:
                data = r.json()
            except ValueError:
                last_err = "non-JSON body"
                _reset_scraper()
                time.sleep(_backoff(attempt, base_backoff, cap_backoff))
                continue

            if not (data.get("events") or []):
                stats.bump("empty")
                return None

            tmp = fname.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, fname)
            stats.bump("saved")
            return None

        if sc == 404:
            stats.bump("empty")
            return None

        # 403 / 429 / 5xx — back off, and on 403/429 drop the session
        last_err = f"http {sc}"
        if sc in (403, 429):
            _reset_scraper()
            # 429: respect Retry-After if present
            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    time.sleep(min(float(ra), cap_backoff))
                    continue
                except ValueError:
                    pass
        time.sleep(_backoff(attempt, base_backoff, cap_backoff))

    stats.bump("failed")
    return last_err


# ---------- driver ----------

def daterange(start: date, end: date):
    cur = start
    one = timedelta(days=1)
    while cur <= end:
        yield cur
        cur += one


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=parse_date, default=date(1930, 7, 13))
    ap.add_argument("--end", type=parse_date, default=date(2026, 12, 31))
    ap.add_argument("--out-dir", type=Path, default=Path(__file__).parent / "football")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--max-attempts", type=int, default=5)
    ap.add_argument("--base-backoff", type=float, default=1.5, help="Base seconds for exponential backoff.")
    ap.add_argument("--cap-backoff", type=float, default=60.0, help="Max backoff seconds.")
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--log-file", type=Path, default=Path(__file__).parent / "fetch_sofa.log")
    ap.add_argument("--failures-file", type=Path, default=Path(__file__).parent / "fetch_sofa.failures.txt")
    ap.add_argument("--progress-every", type=int, default=50, help="Log stats every N completed days.")
    ap.add_argument("--reverse", action="store_true", help="Iterate newest-first instead of oldest-first.")
    args = ap.parse_args()

    if args.end < args.start:
        print("error: --end is before --start", file=sys.stderr)
        return 2

    args.out_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(args.log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    _install_signal_handlers()

    logging.info("Warming up Cloudflare session…")
    for attempt in range(5):
        if _warmup(args.timeout):
            logging.info("Warmup OK")
            break
        wait = 2 ** attempt
        logging.warning("Warmup failed (attempt %d/5), retrying in %ds…", attempt + 1, wait)
        time.sleep(wait)
        _reset_scraper()
    else:
        logging.error("Could not pass Cloudflare challenge after 5 attempts — aborting.")
        return 3

    dates = list(daterange(args.start, args.end))
    if args.reverse:
        dates.reverse()
    total = len(dates)
    logging.info("Fetching %d days [%s … %s] into %s with %d workers",
                 total, args.start, args.end, args.out_dir, args.workers)

    stats = Stats()
    failures: list[tuple[str, str]] = []
    failures_lock = threading.Lock()
    started = time.monotonic()
    completed = 0

    with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="fetch") as pool:
        futures: dict[Future, date] = {}
        submit_iter = iter(dates)

        # Prime the pool
        for _ in range(min(args.workers * 4, total)):
            try:
                d = next(submit_iter)
            except StopIteration:
                break
            futures[pool.submit(
                fetch_one, d, args.out_dir,
                max_attempts=args.max_attempts,
                base_backoff=args.base_backoff,
                cap_backoff=args.cap_backoff,
                timeout=args.timeout,
                stats=stats,
            )] = d

        while futures:
            done = next(as_completed(futures))
            d = futures.pop(done)
            try:
                err = done.result()
            except Exception as e:  # pragma: no cover — defensive
                err = f"exc: {type(e).__name__}: {e}"
                stats.bump("failed")

            completed += 1
            if err and err != "shutdown":
                with failures_lock:
                    failures.append((d.isoformat(), err))
                logging.warning("FAIL %s: %s", d, err)

            if completed % args.progress_every == 0 or completed == total:
                snap = stats.snapshot()
                elapsed = time.monotonic() - started
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = (total - completed) / rate if rate > 0 else float("inf")
                logging.info(
                    "progress %d/%d (%.1f%%) saved=%d skipped=%d empty=%d failed=%d rate=%.2f/s eta=%.0fs",
                    completed, total, 100 * completed / total,
                    snap["saved"], snap["skipped"], snap["empty"], snap["failed"],
                    rate, remaining,
                )

            if _shutdown.is_set():
                continue  # drain in-flight without submitting more

            try:
                nd = next(submit_iter)
            except StopIteration:
                continue
            futures[pool.submit(
                fetch_one, nd, args.out_dir,
                max_attempts=args.max_attempts,
                base_backoff=args.base_backoff,
                cap_backoff=args.cap_backoff,
                timeout=args.timeout,
                stats=stats,
            )] = nd

    if failures:
        with open(args.failures_file, "w", encoding="utf-8") as f:
            for iso, err in failures:
                f.write(f"{iso}\t{err}\n")
        logging.info("Wrote %d failures to %s", len(failures), args.failures_file)

    snap = stats.snapshot()
    elapsed = time.monotonic() - started
    logging.info("Done in %.1fs — saved=%d skipped=%d empty=%d failed=%d",
                 elapsed, snap["saved"], snap["skipped"], snap["empty"], snap["failed"])
    return 0 if snap["failed"] == 0 and not _shutdown.is_set() else 1


if __name__ == "__main__":
    sys.exit(main())
