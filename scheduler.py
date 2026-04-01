#!/usr/bin/env python3
"""
Engrain Scraper — Nightly Scheduler
=====================================
Runs run.py once per night at a random time between 11:00 PM and 3:00 AM.

Usage (keep running in background):
    python3 scheduler.py                        # uses default args
    python3 scheduler.py --workers 3            # pass extra args to run.py
    nohup python3 scheduler.py > scheduler.log 2>&1 &   # detach from terminal

Cloud / systemd:  see README for service file example.

Environment variables read by this script:
    SCRAPER_WORKERS     number of workers to pass to run.py (default: auto)
    SCRAPER_HEADLESS    set to "1" to force headless mode
    SCRAPER_RETRIES     number of retries (default: 2)
    SCRAPER_DELAY       retry delay in seconds (default: 300)
"""

import subprocess, time, random, sys, os, argparse
from datetime import datetime, timedelta

# ── Window: 11:00 PM to 3:00 AM (4 hours = 240 minutes) ─────────────────────
WINDOW_START_HOUR  = 23   # 11 PM
WINDOW_MINUTES     = 240  # 4 hours → ends at 3:00 AM

# ── Path to run.py (same folder as this script) ───────────────────────────────
SCRAPER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run.py')


def _build_scraper_cmd(extra_args):
    """Build the command list to invoke run.py with all configured options."""
    workers  = os.environ.get('SCRAPER_WORKERS', 'auto')
    headless = os.environ.get('SCRAPER_HEADLESS', '0') == '1'
    retries  = os.environ.get('SCRAPER_RETRIES', '2')
    delay    = os.environ.get('SCRAPER_DELAY',   '300')

    cmd = [sys.executable, SCRAPER,
           '--workers', workers,
           '--retries', retries,
           '--retry-delay', delay]
    if headless:
        cmd.append('--headless')
    cmd.extend(extra_args)
    return cmd


def next_run_time():
    """Return a datetime for the next scheduled run inside the 11 PM – 3 AM window."""
    now = datetime.now()

    # Random offset within the 4-hour window
    offset = timedelta(
        minutes=random.randint(0, WINDOW_MINUTES - 1),
        seconds=random.randint(0, 59),
    )

    # Tonight's window start (today at 23:00)
    tonight = now.replace(hour=WINDOW_START_HOUR, minute=0, second=0, microsecond=0)
    candidate = tonight + offset

    # If that slot is already in the past (or within the next 60 s), move to tomorrow
    if candidate <= now + timedelta(seconds=60):
        candidate = (tonight + timedelta(days=1)) + offset

    return candidate


def run_scraper(extra_args):
    """Invoke run.py and return the exit code."""
    cmd = _build_scraper_cmd(extra_args)
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'\n{"=" * 60}', flush=True)
    print(f'[{ts}] Starting scraper', flush=True)
    print(f'  Command: {" ".join(cmd)}', flush=True)
    result = subprocess.run(cmd)
    ts2 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{ts2}] Scraper finished — exit code {result.returncode}', flush=True)
    return result.returncode


def _hms(seconds):
    """Format seconds as Xh Ym Zs."""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    parts = []
    if h: parts.append(f'{h}h')
    if m: parts.append(f'{m}m')
    parts.append(f'{s}s')
    return ' '.join(parts)


def main():
    ap = argparse.ArgumentParser(
        description='Nightly scheduler for Engrain Scraper (11 PM – 3 AM window)',
        epilog='Any unknown arguments are forwarded directly to run.py.',
    )
    ap.add_argument('--run-now', action='store_true',
                    help='Run the scraper immediately once, then start the normal schedule.')
    args, extra = ap.parse_known_args()

    end_hour = (WINDOW_START_HOUR + WINDOW_MINUTES // 60) % 24
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Engrain Scheduler v27.0 started.',
          flush=True)
    print(f'  Nightly window: {WINDOW_START_HOUR:02d}:00 – {end_hour:02d}:00  '
          f'(random each night)', flush=True)
    print(f'  Scraper:        {SCRAPER}', flush=True)

    # Optional immediate run before entering the sleep loop
    if args.run_now:
        print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] --run-now: starting immediately.',
              flush=True)
        run_scraper(extra)

    # Main loop — sleep until next window, run, repeat
    while True:
        run_at    = next_run_time()
        wait_secs = max(0, (run_at - datetime.now()).total_seconds())
        print(f'\n[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] '
              f'Next run: {run_at.strftime("%Y-%m-%d %H:%M:%S")}  '
              f'(sleeping {_hms(wait_secs)})', flush=True)

        # Sleep in chunks so Ctrl+C / SIGTERM are responsive
        deadline = time.time() + wait_secs
        while time.time() < deadline:
            time.sleep(min(60, deadline - time.time()))

        run_scraper(extra)


if __name__ == '__main__':
    main()
