# Engrain Scraper v27.0

Automated apartment unit scraper for Engrain / SightMap-powered property websites.
Extracts unit numbers, floor plans, rent, beds/baths, sqft, availability dates, and promotions.

---

## Table of Contents

1. [System Requirements](#1-system-requirements)
2. [Installation](#2-installation)
3. [Configuration](#3-configuration)
4. [Running the Scraper](#4-running-the-scraper)
5. [All Command-Line Options](#5-all-command-line-options)
6. [Adding / Changing URLs](#6-adding--changing-urls)
7. [Output Files](#7-output-files)
8. [Nightly Scheduler](#8-nightly-scheduler)
9. [Failure Tracking and Email Alerts](#9-failure-tracking-and-email-alerts)
10. [Cloud / Docker Deployment](#10-cloud--docker-deployment)
11. [Headless vs Headed Mode](#11-headless-vs-headed-mode)
12. [Troubleshooting](#12-troubleshooting)
13. [Quick Reference](#13-quick-reference)

---

## 1. System Requirements

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.10 or newer | 3.13 tested and confirmed working |
| Google Chrome | 115 or newer | Must be installed system-wide |
| macOS / Linux | any recent | Windows works but is untested |
| RAM | 2 GB minimum | ~500 MB per parallel worker |
| Disk | 500 MB free | Chrome profile cache + output files |

---

## 2. Installation

### Step 1 — Enter the project folder

```bash
cd /path/to/all_scraper_files_final
```

### Step 2 — Create a Python virtual environment

```bash
python3 -m venv venv
```

### Step 3 — Activate the virtual environment

```bash
# macOS / Linux:
source venv/bin/activate

# Windows:
venv\Scripts\activate
```

You will see `(venv)` at the start of your terminal prompt once it is active.

### Step 4 — Install dependencies

```bash
pip install --upgrade pip
pip install seleniumbase pandas requests
```

| Package | Purpose |
|---|---|
| `seleniumbase` | Browser automation — wraps Selenium with undetected-chromedriver |
| `pandas` | Writes CSV and JSON output files |
| `requests` | Capsolver API calls and Brookfield REST fallback |

### Step 5 — macOS Apple Silicon only: fix the ChromeDriver signature

This is a one-time step. Re-run it any time you upgrade `seleniumbase`.

```bash
codesign --force --sign - \
  venv/lib/python3.*/site-packages/seleniumbase/drivers/chromedriver
```

Skip this on Linux servers and Docker. See [Troubleshooting](#10-troubleshooting) for why this is needed.

### Step 6 — Confirm the install worked

```bash
python3 run.py --help
```

Expected output:
```
usage: run.py [-h] [--file FILE] [--workers WORKERS] [--headless]
              [--retries RETRIES] [--retry-delay RETRY_DELAY] [urls ...]
Engrain Scraper v26.0
...
```

---

## 3. Configuration

### 3a. Capsolver API Key (optional)

Only needed for properties protected by Cloudflare Turnstile. Most properties do not require it.

**Option A — Environment variable (recommended):**
```bash
export CAPSOLVER_API_KEY="your_key_here"
python3 run.py
```

**Option B — .env file:**

Create a file named `.env` in the project folder:
```
CAPSOLVER_API_KEY=your_key_here
```
Then load it before running:
```bash
source .env && python3 run.py
```

**Option C — Hard-code directly in run.py (not recommended for shared or cloud environments):**

Around line 28 in `run.py`, change:
```python
CAPSOLVER_API_KEY = os.environ.get("CAPSOLVER_API_KEY", "")
```
to:
```python
CAPSOLVER_API_KEY = "your_actual_key_here"
```

### 3b. Debug output

Debug logging is on by default. To disable it, set `DEBUG = False` near the top of `run.py`:
```python
DEBUG = False
```

---

## 4. Running the Scraper

### Interactive mode — recommended for local use

Run with no arguments. The scraper will ask how many workers you want:

```bash
python3 run.py
```

You will see a prompt like this:

```
No URLs — using defaults.

  This machine has 10 CPU cores.
  Each worker opens one Chrome browser and processes URLs in parallel.
  More workers = faster, but uses more RAM (~500 MB per worker).

  Recommended: 3  |  Max sensible: 10

  Enter number of workers [press Enter for 3]:
```

Press **Enter** to use the recommended number, or type any integer (e.g. `5`) and press Enter.

Chrome windows will open and scraping will begin.

### Scrape specific URLs on the command line

```bash
python3 run.py https://example.com/floorplans/ https://another.com/floorplans/
```

### Scrape from a text file

```bash
python3 run.py --file my_urls.txt
```

Format of `my_urls.txt` (one URL per line, lines starting with `#` are ignored):
```
# This is a comment
https://callowayatlascolinas.com/floorplans/
https://soleacopperfield.com/floorplans/
```

### Skip the interactive prompt by specifying workers explicitly

```bash
python3 run.py --workers 4
```

---

## 5. All Command-Line Options

| Flag | Short | Default | Description |
|---|---|---|---|
| `--workers` | `-w` | `auto` | Number of parallel Chrome workers. `auto` uses CPU count. In a terminal you are prompted if not set. On cloud / non-interactive it defaults to 3. |
| `--headless` | | off | Run Chrome without a visible window. Required on servers with no display. |
| `--file` | `-f` | | Path to a text file containing one URL per line. |
| `--retries` | | `2` | How many times to retry URLs that returned 0 units. |
| `--retry-delay` | | `300` | Seconds to wait between retry passes (300 = 5 minutes). |

### Examples

```bash
# Local — interactive worker prompt, scrape all default URLs
python3 run.py

# Local — skip prompt, use 4 workers
python3 run.py --workers 4

# Cloud server — headless, 3 workers
python3 run.py --workers 3 --headless

# Retry up to 3 times, wait 10 minutes between each retry
python3 run.py --retries 3 --retry-delay 600

# Scrape a custom URL list headlessly with retries
python3 run.py --file urls.txt --workers 3 --headless --retries 2

# Pass Capsolver key at runtime
CAPSOLVER_API_KEY=abc123 python3 run.py --workers 3 --headless
```

---

## 6. Adding / Changing URLs

All default URLs and property display names live in **`properties.py`**.

### Add a new property

Open `properties.py` and add to `DEFAULT_URLS`:
```python
DEFAULT_URLS = [
    ...
    "https://yournewproperty.com/floorplans/",   # add here
]
```

Then add a friendly display name to `PROPERTY_NAMES`:
```python
PROPERTY_NAMES = {
    ...
    "yournewproperty.com": "YOUR PROPERTY DISPLAY NAME",
}
```

The key in `PROPERTY_NAMES` is a **substring** of the URL — it does not need to be the full domain. More specific substrings take priority. Example of two properties on the same domain:

```python
"irtliving.com/Apartments-In/San-Antonio-TX/Los-Robles": "LOS ROBLES",
"irtliving.com/Apartments-In/Tampa-FL/Vantage":          "VANTAGE ON HILLSBOROUGH",
```

### Remove a property

Delete or comment out its entry from both `DEFAULT_URLS` and `PROPERTY_NAMES`.

### One-off URL without editing any files

```bash
python3 run.py "https://yournewproperty.com/floorplans/"
```

---

## 7. Output Files

| File | Description |
|---|---|
| `engrain_units_YYYYMMDD_HHMMSS.csv` | Main output — one row per unit |
| `engrain_units_YYYYMMDD_HHMMSS.json` | Same data as a JSON array |
| `engrain_results_latest.json` | Live progress file, updated after each URL finishes. Useful for monitoring long runs. |
| `extracted_units_combined.csv` | Cumulative — if a previous run's CSV exists, new results are automatically appended to it. |

### CSV columns

| Column | Description |
|---|---|
| `property_name` | Display name from `PROPERTY_NAMES` |
| `floor` | Floor number |
| `apt_number` | Unit number (e.g. `1204`, `2-203`) |
| `model_number` | Floor plan or model name (e.g. `B2`, `Agean`) |
| `unit_model_combined` | `apt_number` + `model_number` combined |
| `garage` | `✓` if unit has a garage |
| `renovated` | `✓` if unit is renovated |
| `townhome` | `✓` if unit is a townhome |
| `rent` | Monthly rent (e.g. `$1,450`) |
| `sqft` | Square footage |
| `beds` | Bedroom count |
| `baths` | Bathroom count |
| `available_date` | Move-in date in `MM/DD/YYYY` format |
| `special_promotion` | Promo banners or specials found on the page |
| `source_url` | The URL that was scraped |
| `scraped_at` | ISO 8601 timestamp of when the unit was scraped |

---

## 8. Nightly Scheduler

`scheduler.py` keeps running in the background and launches `run.py` once per night at a
random time between **11:00 PM and 3:00 AM**.

### Start the scheduler

```bash
# Foreground (good for testing)
python3 scheduler.py

# Run the scraper immediately right now, then enter the nightly schedule
python3 scheduler.py --run-now

# Background / detached (production)
nohup python3 scheduler.py > scheduler.log 2>&1 &

# Check it is running
ps aux | grep scheduler.py

# View live log
tail -f scheduler.log
```

### Configure via environment variables

| Variable | Default | Description |
|---|---|---|
| `SCRAPER_WORKERS` | `auto` | Workers passed to `run.py --workers` |
| `SCRAPER_HEADLESS` | `0` | Set to `1` to pass `--headless` |
| `SCRAPER_RETRIES` | `2` | Retry count passed to `run.py --retries` |
| `SCRAPER_DELAY` | `300` | Retry delay in seconds |

Example for a cloud server:
```bash
export SCRAPER_HEADLESS=1
export SCRAPER_WORKERS=3
nohup python3 scheduler.py > scheduler.log 2>&1 &
```

### systemd service (Linux cloud servers)

Create `/etc/systemd/system/engrain-scraper.service`:

```ini
[Unit]
Description=Engrain Nightly Scraper Scheduler
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/all_scraper_files_final
ExecStart=/home/ubuntu/all_scraper_files_final/venv/bin/python3 scheduler.py
Restart=on-failure
RestartSec=30
Environment=SCRAPER_HEADLESS=1
Environment=SCRAPER_WORKERS=3
Environment=PYTHONUNBUFFERED=1
StandardOutput=append:/home/ubuntu/all_scraper_files_final/scheduler.log
StandardError=append:/home/ubuntu/all_scraper_files_final/scheduler.log

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable engrain-scraper
sudo systemctl start engrain-scraper
sudo systemctl status engrain-scraper
```

### Alternative: cron (simpler, no persistent process)

If you prefer cron over a long-running process, add this to your crontab (`crontab -e`):

```cron
# Run at 11:00 PM every night, random delay added by scheduler.py --run-now
0 23 * * * cd /path/to/all_scraper_files_final && \
  SCRAPER_HEADLESS=1 venv/bin/python3 scheduler.py --run-now >> scheduler.log 2>&1
```

With `--run-now`, the scheduler picks a random delay within the window, then runs once and exits.
This is simpler than a persistent service but requires cron to be available.

---

## 9. Failure Tracking and Email Alerts

The scraper keeps a persistent record of which URLs are failing across runs in **`failure_history.json`**.

### How it works

1. After every run (including all retries), each URL is marked as succeeded or failed for today.
2. The **consecutive failure day count** increments once per calendar day — retrying the same URL
   multiple times in one day only counts as one failure day.
3. If a URL reaches **3 or more consecutive failure days**, an alert is triggered.
4. Once alerted, the same URL will not trigger another alert until it succeeds and fails again.

### failure_history.json format

```json
{
  "https://example.com/floorplans/": {
    "consecutive_fail_days": 3,
    "last_fail_date": "2026-03-17",
    "last_success_date": "2026-03-14",
    "notified_on": "2026-03-17"
  }
}
```

### Setting up email alerts

Set these environment variables before running (or in your `.env` file):

```bash
export NOTIFY_EMAIL_TO="you@example.com"
export SMTP_USER="sender@gmail.com"
export SMTP_PASS="your_app_password"
# Optional — defaults shown:
export SMTP_HOST="smtp.gmail.com"
export SMTP_PORT="587"
```

For Gmail, `SMTP_PASS` must be an **App Password**, not your regular password.
To create one: Google Account → Security → 2-Step Verification → App Passwords.

### Setting up webhook alerts (Slack / Discord / Teams)

Set a single environment variable:

```bash
# Slack incoming webhook:
export NOTIFY_WEBHOOK_URL="https://hooks.slack.com/services/XXX/YYY/ZZZ"

# Discord webhook:
export NOTIFY_WEBHOOK_URL="https://discord.com/api/webhooks/XXX/YYY"
```

The scraper will POST a JSON payload `{"text": "..."}` to the URL, which works for Slack,
Discord, and most other services out of the box.

### If neither email nor webhook is configured

The alert is printed to the terminal/log instead — no silent failures:

```
ALERT: 2 URL(s) failing 3+ days — no email/webhook configured.
   ❌ https://example.com/floorplans/  (3 days)
```

### Manually resetting a URL's failure count

Edit `failure_history.json` and set `consecutive_fail_days` to `0` for the URL,
or delete its entry entirely:

```bash
# Remove one URL's history entry (using Python):
python3 -c "
import json
h = json.load(open('failure_history.json'))
h.pop('https://example.com/floorplans/', None)
json.dump(h, open('failure_history.json', 'w'), indent=2)
print('Done')
"
```

---

## 10. Cloud / Docker Deployment

### Chrome flags applied automatically

These flags are set inside `make_driver()` — you do not need to add them manually:

| Flag | Why it is needed |
|---|---|
| `--no-sandbox` | Chrome cannot create kernel sandboxes when running as root in a container |
| `--disable-gpu` | No GPU is available or needed in headless mode |
| `--disable-dev-shm-usage` | Prevents Chrome OOM crashes caused by Docker's small `/dev/shm` (64 MB default) |

### Dockerfile example

```dockerfile
FROM python:3.13-slim

RUN apt-get update && apt-get install -y wget gnupg \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" \
       > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir seleniumbase pandas requests
RUN seleniumbase install chromedriver

ENV PYTHONUNBUFFERED=1

CMD ["python3", "run.py", "--headless", "--workers", "3"]
```

Build and run:
```bash
docker build -t engrain-scraper .
docker run \
  -e CAPSOLVER_API_KEY=your_key \
  -v $(pwd)/output:/app \
  engrain-scraper
```

### Environment variables for cloud

| Variable | Required | Description |
|---|---|---|
| `CAPSOLVER_API_KEY` | Optional | Capsolver key for Cloudflare Turnstile bypass |
| `PYTHONUNBUFFERED` | Recommended | Set to `1` so logs appear in real time in cloud log viewers |

### Cloud platform quick-start

**AWS EC2 / any Linux VM:**
```bash
python3 -m venv venv && source venv/bin/activate
pip install seleniumbase pandas requests
CAPSOLVER_API_KEY=xxx python3 run.py --headless --workers 3
```

**GitHub Actions:**
```yaml
- name: Run scraper
  env:
    CAPSOLVER_API_KEY: ${{ secrets.CAPSOLVER_API_KEY }}
    PYTHONUNBUFFERED: "1"
  run: |
    source venv/bin/activate
    python3 run.py --headless --workers 2 --retries 2
```

---

## 11. Headless vs Headed Mode

| | Headed (default, no flag) | Headless (--headless flag) |
|---|---|---|
| Best for | Local development and debugging | Cloud servers, Docker, CI/CD |
| Display required | Yes — opens Chrome windows | No |
| Bot detection risk | Lower | Slightly higher (mitigated by uc=True) |
| RAM per worker | ~500 MB | ~400 MB |
| Debug-ability | Watch the browser live | Rely on log output only |
| Cloudflare handling | Better pass rate | Good — uc=True handles most cases |

**Rule of thumb:**
- On your **laptop or desktop** → use the default (headed). Optionally add `--workers N`.
- On a **server, cloud VM, or inside Docker** → always add `--headless`.

---

## 12. Troubleshooting

### ChromeDriver hangs silently on macOS (Apple Silicon / arm64)

**Symptom:** Script prints the URL list then freezes indefinitely. Many `chromedriver --version` zombie processes visible in Activity Monitor.

**Cause:** The arm64 ChromeDriver shipped by seleniumbase carries a stale adhoc linker signature. macOS Sequoia (15.x) triggers an online Gatekeeper verification check for such binaries, which hangs indefinitely when the binary is not in Apple's notarization database.

**Fix — run this once after every seleniumbase upgrade:**
```bash
codesign --force --sign - \
  venv/lib/python3.*/site-packages/seleniumbase/drivers/chromedriver
```

To automate this, add it to a `Makefile`:
```makefile
install:
	pip install seleniumbase pandas requests
	codesign --force --sign - venv/lib/python3.*/site-packages/seleniumbase/drivers/chromedriver
```

---

### Chrome version and ChromeDriver version mismatch

**Symptom:** Error: `This version of ChromeDriver only supports Chrome version 114`

**Fix:**
```bash
seleniumbase install chromedriver
# or for a specific Chrome version:
seleniumbase install chromedriver 145
```

---

### ModuleNotFoundError: No module named 'seleniumbase'

The virtual environment is not active. Run:
```bash
source venv/bin/activate    # macOS / Linux
venv\Scripts\activate       # Windows
```

---

### 0 units extracted for a URL

1. Open the URL in Chrome manually to confirm the page loads correctly.
2. Check `engrain_results_latest.json` — the `method` field shows which scraping strategy ran.
3. The scraper retries automatically up to `--retries` times (default 2) after `--retry-delay` seconds (default 300).
4. If all retries fail, enable debug output (`DEBUG = True` in `run.py`) and re-run for detailed browser logs.

---

### OSError: No space left on device

Chrome writes session data to `.chrome_profiles/`. Clean it:
```bash
rm -rf .chrome_profiles/
```

---

### Script crashes immediately on a Linux server

Always use `--headless` on servers. If Chrome still fails to start, install missing system libraries:
```bash
# Ubuntu / Debian:
apt-get install -y libglib2.0-0 libnss3 libatk1.0-0 libatk-bridge2.0-0 \
                   libcups2 libdrm2 libxcomposite1 libxdamage1 libxrandr2 \
                   libgbm1 libxkbcommon0 libasound2
```

---

## 13. Quick Reference

```bash
# First-time setup — macOS
python3 -m venv venv && source venv/bin/activate
pip install seleniumbase pandas requests
codesign --force --sign - venv/lib/python3.*/site-packages/seleniumbase/drivers/chromedriver

# First-time setup — Linux / cloud
python3 -m venv venv && source venv/bin/activate
pip install seleniumbase pandas requests

# Run locally — interactive worker prompt
python3 run.py

# Run locally — explicit 4 workers, skip prompt
python3 run.py --workers 4

# Run on a cloud server — headless, 3 workers
python3 run.py --headless --workers 3

# Scrape a custom URL list with retries
python3 run.py --file urls.txt --retries 3 --retry-delay 600 --headless

# Pass Capsolver key at runtime
CAPSOLVER_API_KEY=abc123 python3 run.py --headless --workers 3
```
