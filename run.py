#!/usr/bin/env python3
"""
ENGRAIN SCRAPER v27.0
======================
v27.0 adds persistent failure tracking and alerting:
- Tracks per-URL consecutive failure days in failure_history.json
- Sends email + webhook alert when a URL fails 3 days in a row
- scheduler.py runs this script nightly at a random time 11pm-3am
"""

import sys, argparse, time, random, re, json, os, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from multiprocessing import Process, Queue
from urllib.parse import urlparse
from datetime import datetime,timedelta
import pandas as pd
import requests

from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from properties import DEFAULT_URLS, PROPERTY_NAMES

DEBUG = True
CAPSOLVER_API_KEY = os.environ.get("CAPSOLVER_API_KEY", "")

# ── PROPERTY NAME LOOKUP ──────────────────────────────────────────────────────


def get_property_name(url):
    """Lookup property name from URL. Falls back to domain parsing."""
    for key, name in PROPERTY_NAMES.items():
        if key in url: return name
    return urlparse(url).netloc.replace('www.','').split('.')[0]

# ── REGEX ─────────────────────────────────────────────────────────────────────
# PATCH 1: apt group now allows hyphens (FIX 8/14: "2-203", "12-1210")
RE_APT    = re.compile(
    r'(?:APT|Apt|HOME|Home)\s+([\d][\d\w-]*)\s*([A-Z]\d[A-Z0-9P]*)?'  # g1=apt  g2=model
    r'|#\s*([A-Z]{1,4})\s*-\s*(\d+)'                                    # g3=model g4=apt (UDR letters)
    r'|#\s*(\d{1,4})\s*-\s*(\d{3,})'                                    # g5=bldg g6=unit (UDR digits)
    r'|\bUNIT\s+([\w-]{2,})'                                             # g7=apt
    r'|#\s*([\w-]{2,})',                                                  # g8=apt generic
    re.I)
RE_BEDS   = re.compile(r'(\d)\s*(?:Bed|bed|Bd|bd|BR|br)s?\b', re.I)
RE_BATH   = re.compile(r'(\d\.?\d*)\s*(?:Bath|bath|Ba|ba)s?\b', re.I)
RE_SQFT   = re.compile(r'([\d,]+)\s*sq\.?\s*ft', re.I)
RE_DATE = re.compile(
    r'Available\s+(?:Now|Immediately|in\s+\d+\s+days?|on\s+[\w]+\s+\d+\w*|[\w]+\s+\d+\w*)'
    r'|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d+\w*'
    r'|\bNow\b',
    re.I)
RE_HEADER = re.compile(r'(\d+)\s+MATCHES?[\s\n\r]*FLOOR[\s\n\r]*(\d+)', re.I)
RE_PRICE  = re.compile(r'\$([\d,]+(?:\.\d{2})?)', re.I)
RE_PROMO = re.compile(
    r'(?:'
    r'\d+\s*(?:month|week|day)s?\s*(?:free|off|rent|credit)'
    r'|(?:one|two|three|four|five|six|seven|eight|nine|ten)\s+(?:month|week)s?\s*free'
    r'|\bfree\b.{0,40}(?:month|week|rent|fee|move|look)'
    r'|waived?\b.{0,30}(?:fee|admin|application|app\b)'
    r'|look\s*(?:&|and)\s*lease'
    r'|\$[\d,]+\s*(?:off|credit|gift|bonus|saving|when\s+you)'
    r'|save\s+up\s+to\s+\$[\d,]+'                     
    r'|save\s+\$[\d,]+'                                
    r'|up\s+to\s+\$[\d,]+\s*(?:off|savings?|back)'     
    r'|\$[\d,]+\s*(?:move.?in|move\s+in\s+special)'   
    r'|\$[\d,]+\s*(?:off|savings?)\s*(?:on|when|if|your)?'  
    r'|special\s+offer|limited\s+time|move.in\s+special'
    r'|\bspecials?\b|\bdeals?\b'
    r'|no\s+(?:application|admin|deposit)\s+fee'
    r'|(?:month|week)\s+free\b'
    r'|free\s+(?:month|week|rent)'
    r'|reduced\s+(?:rent|rate|price)'
    r'|concession|incentive'
    r')',
    re.I)

RE_DATE_EXPLICIT = re.compile(
    r'\b(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b'
    r'|\b(\d{1,2}/\d{1,2})\b'                 
    r'|Available(?:\s+on)?\s+((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d+\w*)'
    r'|\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d+,?\s*\d{2,4})', re.I)
RE_FLOORPLAN_LABEL = re.compile(r'(?:Floor\s*Plan|Model|Floorplan)\s*[:\-]\s*([A-Za-z][\w\s-]{0,25})', re.I)
RE_ADDRESS = re.compile(r',\s*[A-Z]{2}\s+\d{5}', re.I)

def dbg(wid, msg):
    if DEBUG: print(f"[{datetime.now().strftime('%H:%M:%S')}][W{wid}]{msg}", flush=True)

def make_driver(headless=False):
    d = Driver(
        uc=True, headless=headless, incognito=True, disable_csp=True,
        no_sandbox=True,          # required inside Docker / cloud VMs
        disable_gpu=headless,     # GPU unnecessary in headless
        chromium_arg="--disable-dev-shm-usage",  # avoids /dev/shm OOM in containers
    )
    if not headless:
        d.maximize_window()
    d.set_window_size(1920, 1080)
    d.set_page_load_timeout(35)
    return d

def handle_turnstile_if_present(driver, url, wid):
    dbg(wid, "  Checking for Cloudflare Turnstile...")
    time.sleep(random.uniform(2.8, 5.1))
    try:
        if driver.find_elements(By.CSS_SELECTOR, 'iframe[src*="challenges.cloudflare.com"]') or \
           driver.find_elements(By.CSS_SELECTOR, '[data-sitekey]'):
            dbg(wid, "  → Turnstile detected")
            if CAPSOLVER_API_KEY:
                sitekey = driver.execute_script(
                    "return document.querySelector('[data-sitekey]')?.getAttribute('data-sitekey') or ''")
                if sitekey:
                    token = solve_turnstile(url, sitekey)
                    if token:
                        driver.execute_script(f'''
                            document.querySelectorAll('input[name="cf-turnstile-response"]').forEach(i => i.value = "{token}");
                            if (typeof turnstileCallback === "function") turnstileCallback();
                        ''')
                        dbg(wid, "  → Token injected")
                        time.sleep(random.uniform(3.5, 6.2))
                        return True
            else:
                dbg(wid, "  No Capsolver key → relying on UC stealth (wait longer)")
                time.sleep(random.uniform(7, 12))
    except Exception as e:
        dbg(wid, f"  Turnstile handling failed: {e}")
    return False

def solve_turnstile(site_url, sitekey):
    if not CAPSOLVER_API_KEY: return None
    try:
        payload = {"clientKey": CAPSOLVER_API_KEY, "task": {"type": "AntiTurnstileTaskProxyLess", "websiteURL": site_url, "websiteKey": sitekey}}
        resp = requests.post("https://api.capsolver.com/createTask", json=payload, timeout=15).json()
        task_id = resp.get("taskId")
        for _ in range(45):
            time.sleep(3.8)
            res = requests.post("https://api.capsolver.com/getTaskResult",
                                json={"clientKey": CAPSOLVER_API_KEY, "taskId": task_id}, timeout=10).json()
            if res.get("status") == "ready":
                return res["solution"]["token"]
    except: pass
    return None

def dismiss(driver):
    for kw in ['accept all cookies','i accept all cookies','accept cookies',
               'i accept','close','got it','dismiss','no thanks','accept']:
        try:
            for b in driver.find_elements(By.XPATH,
                    f"//*[translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ',"
                    f"'abcdefghijklmnopqrstuvwxyz')='{kw}']")[:3]:
                if b.is_displayed():
                    driver.execute_script("arguments[0].click();", b)
                    time.sleep(0.4)
        except: continue
    try: driver.find_element(By.TAG_NAME,'body').send_keys(Keys.ESCAPE)
    except: pass

def js_click(driver, el):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    time.sleep(0.15)
    driver.execute_script("arguments[0].click();", el)

# PATCH 3: get_rent skips deposits + call-for-details (FIX 19/20)
def get_rent(text):
    m = re.search(r'\$([\d,]+(?:\.\d{2})?)\s*Base Rent', text)
    if m:
        v = float(m.group(1).replace(',',''))
        if 300 <= v <= 15000: return f"${int(v):,}"
    m = re.search(r'\$([\d,]+)\s*[-\u2013]\s*\$([\d,]+)', text)
    if m:
        v = float(m.group(1).replace(',',''))
        if 300 <= v <= 15000: return f"${int(v):,}"
    for m in RE_PRICE.finditer(text):
        v = float(m.group(1).replace(',',''))
        snip = text[max(0,m.start()-40):m.end()+40]
        if 300 <= v <= 15000 and not re.search(r'deposit|admin fee|app(?:lication)? fee|call\s+for', snip, re.I):
            return f"${int(v):,}"
    return ''

# PATCH 4: get_date prefers explicit dates (FIX 3)
def get_date(text):
    text = str(text or '').strip()
    if not text:
        return ''
    today = datetime.now()
    current_year = today.year
    clean_text = re.sub(r'(\d)(st|nd|rd|th)\b', r'\1', text, flags=re.I)
    clean_compact = re.sub(r'[^a-z]+', '', clean_text.lower())

    # --- "available in X days" → scrape date + X days ---
    m_days = re.search(r'available\s+in\s+(\d+)\s+days?', clean_text, re.I)
    if m_days:
        days = int(m_days.group(1))
        target = today + timedelta(days=days)
        return target.strftime('%m/%d/%Y')

    # --- "Available Now" / "Immediately" etc ---
    # FIX: removed bare \bNow\b — it falsely matched "Apply Now" / "Schedule Now" button text
    # FIX: added guard — if an explicit date exists on the card, it always wins over "Now"
    if re.search(
        r'\bAvailable\s+Now\b|\bAvailable\s+Immediately\b'
        r'|\bImmediately\s+Available\b|\bMove\s*-?\s*in\s+(?:Today|Now)\b'
        r'|\bReady\s+Now\b|\bVacant\b',
        clean_text, re.I
    ):
        if not RE_DATE_EXPLICIT.search(clean_text):
            return today.strftime('%m/%d/%Y')

    if clean_compact in {'now', 'availablenow', 'immediate', 'immediately', 'availableimmediately', 'readynow', 'movinnow', 'moveintoday', 'vacant'}:
        return today.strftime('%m/%d/%Y')

    raw = ''

    m_iso = re.search(r'\b(\d{4}[-/]\d{1,2}[-/]\d{1,2})\b', clean_text)
    if m_iso:
        raw = m_iso.group(1).strip()

    m = RE_DATE_EXPLICIT.search(clean_text)
    if m and not raw:
        raw = (m.group(1) or m.group(2) or m.group(3) or m.group(4) or '').strip()
    if not raw:
        m = RE_DATE.search(clean_text)
        raw = m.group(0).strip() if m else ''
    if not raw:
        return ''

    parsed = None

    # --- Full date with year: 05/07/2026 or 5-7-2026 ---
    for fmt in ('%m/%d/%Y', '%m-%d-%Y', '%m/%d/%y', '%m-%d-%y'):
        try:
            parsed = datetime.strptime(raw, fmt)
            break
        except: pass

    # --- ISO date with year: 2026-03-31 or 2026/03/31 ---
    if not parsed:
        for fmt in ('%Y-%m-%d', '%Y/%m/%d'):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except: pass

    # --- Bare MM/DD like 05/07 → assume current year ---
    if not parsed:
        m2 = re.match(r'^(\d{1,2})[/-](\d{1,2})$', raw)
        if m2:
            month, day = int(m2.group(1)), int(m2.group(2))
            if 1 <= month <= 12 and 1 <= day <= 31:
                try:
                    parsed = datetime(current_year, month, day)
                    if parsed.date() < today.date():
                        parsed = datetime(current_year + 1, month, day)
                except: pass

    # --- Short month: "Apr 15", "Available Apr 15" ---
    if not parsed:
        month_map = {
            'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
            'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12
        }
        m3 = re.search(
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+(\d{1,2}),?\s*(\d{4})?',
            raw, re.I)
        if m3:
            month = month_map[m3.group(1)[:3].lower()]
            day   = int(m3.group(2))
            year  = int(m3.group(3)) if m3.group(3) else current_year
            try:
                parsed = datetime(year, month, day)
                if not m3.group(3) and parsed.date() < today.date():
                    parsed = datetime(current_year + 1, month, day)
            except: pass

    # --- Long month: "March 30", "December 1 2026" ---
    if not parsed:
        long_months = {
            'january':1,'february':2,'march':3,'april':4,'may':5,'june':6,
            'july':7,'august':8,'september':9,'october':10,'november':11,'december':12
        }
        m4 = re.search(
            r'(January|February|March|April|May|June|July|August|'
            r'September|October|November|December)\s+(\d{1,2}),?\s*(\d{4})?',
            clean_text, re.I)
        if m4:
            month = long_months[m4.group(1).lower()]
            day   = int(m4.group(2))
            year  = int(m4.group(3)) if m4.group(3) else current_year
            try:
                parsed = datetime(year, month, day)
                if not m4.group(3) and parsed.date() < today.date():
                    parsed = datetime(current_year + 1, month, day)
            except: pass

    if not parsed:
        return raw  # couldn't parse, return as-is

    return parsed.strftime('%m/%d/%Y')
# PATCH 5: parse_card with FIX 2/3/4/6
def parse_card(text):
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    apt = model = ''
    m = RE_APT.search(text)
    if m:
        if m.group(3) and m.group(4):
            model = m.group(3).upper(); apt = m.group(4)  # UDR letters: #CT - 730
        elif m.group(5) and m.group(6):
            apt = f"{m.group(5)}-{m.group(6)}"  # UDR digits: #01 - 3192 → "01-3192"
        else:
            apt = (m.group(1) or m.group(7) or m.group(8) or '').strip()
            model = (m.group(2) or '').strip()
    # Extend model with remaining text on same line after RE_APT match (e.g. "B4 GARAGE" → g2 got "B4", need "GARAGE")
    if apt and model and m:
        rest_of_line = text[m.end():].split('\n')[0].strip()
        # Grab words that are model qualifiers (GARAGE, RENOVATED, VIEW, TH, etc.)
        extra = []
        for word in rest_of_line.split():
            if re.match(r'^(?:GARAGE|RENOVATED|RENO|RENOVATION|VIEW|TH|TOWNHOME|LOFT|PENTHOUSE|DEN|CLASSIC|TRADITIONAL|DESIGNER|STUDIO|GRAND|COURTYARD|PREMIUM|DELUXE|UPGRADED)$', word, re.I):
                extra.append(word.upper())
            else:
                break
        if extra: model = model + ' ' + ' '.join(extra)
    # FIX 2: compound "12244 SP" from apt itself
    if apt and not model:
        parts = apt.split()
        if len(parts)==2 and re.match(r'^\d+$', parts[0]) and re.match(r'^[A-Za-z]', parts[1]):
            apt, model = parts[0], parts[1].upper()
    # FIX 2b: model from text immediately after # match (e.g. "#12244 SP")
    if apt and not model and m:
        remaining = text[m.end():].strip().split('\n')[0].strip().split()
        if remaining and re.match(r'^[A-Za-z]{1,5}$', remaining[0]):
            model = remaining[0].upper()
    # FIX 4: "Floor Plan: Agean"
    if not model:
        fm = RE_FLOORPLAN_LABEL.search(text)
        if fm: model = fm.group(1).strip()
    # FIX 6: standalone model line after unit header
    if not model and len(lines) >= 2:
        for i, line in enumerate(lines):
            if re.match(r'^(?:#|APT|Apt|Unit|Home|\d)', line, re.I) and i+1 < len(lines):
                nl = lines[i+1].strip()
                if (re.match(r'^[A-Za-z]', nl) and len(nl) <= 30 and '$' not in nl
                    and not RE_BEDS.search(nl) and not RE_BATH.search(nl)
                    and not RE_SQFT.search(nl) and not re.search(r'available|floor\s*plan|starting', nl, re.I)):
                    model = nl; break
    if not model and lines:
        first = lines[0]
        if not re.match(r'^(?:APT|Apt|HOME|Home|#|UNIT)\b', first): model = first
    if not apt and model and re.match(r'^[A-Za-z][A-Za-z0-9 ]{0,9}$', model.strip()):
        apt = model.strip()
    # FIX 2: filter address strings
    if model and RE_ADDRESS.search(model): model = ''
    b1 = RE_BEDS.search(text); b2 = RE_BATH.search(text)
    sq = RE_SQFT.search(text)
    return {
        'apt_number': apt, 'model_number': model,
        'beds': b1.group(1) if b1 else '', 'baths': b2.group(1) if b2 else '',
        'sqft': sq.group(1).replace(',','') if sq else '',
        'available_date': get_date(text), 'rent': get_rent(text), 'raw_text': text,
    }

# PATCH 6: split_apt_model keeps full alphanumeric unit ids intact
def split_apt_model(apt_raw, model_raw):
    apt_raw   = re.sub(r'^(?:APT|Apt|apt|#)\s*', '', str(apt_raw)).strip()
    model_raw = re.sub(r'^(?:APT|Apt|apt|#)\s*', '', str(model_raw)).strip()
    if model_raw and RE_ADDRESS.search(model_raw): model_raw = ''
    if re.match(r'^\d+$', apt_raw): return apt_raw, model_raw
    if '-' in apt_raw and re.match(r'^[\d][\w-]+$', apt_raw): return apt_raw, model_raw
    if model_raw and re.match(r'^\d+[A-Za-z]+$', apt_raw): return apt_raw, model_raw
    return apt_raw, model_raw

def _model_tokens(*parts):
    toks = []
    for part in parts:
        for tok in re.findall(r'[A-Za-z0-9]+', str(part or '')):
            toks.append(tok.upper())
    return toks

def _has_suffix_pair(parts, suffixes):
    toks = _model_tokens(*parts)
    for i, tok in enumerate(toks[:-1]):
        # Require a true code token (letters + digits), so plain numbers like "12 TH" don't trigger.
        if (
            re.match(r'^[A-Z0-9]+$', tok)
            and re.search(r'\d', tok)
            and re.search(r'[A-Z]', tok)
            and toks[i+1] in suffixes
        ):
            return True
    return False

def _token_has_marker_suffix(tok, suffix):
    tok = str(tok or '').upper()
    if tok in {'GARAGE', 'RENOVATED', 'RENOVATION', 'RENNOVATED', 'RENNOVATION', 'TOWNHOME', 'TOWNHOUSE'}:
        return False
    if suffix == 'TH':
        # Strict TH suffix codes only (avoid words like BATH, MONTH, etc.).
        if tok.endswith('BATH') or tok.endswith('MONTH') or tok.endswith('MONTHS'):
            return False
        return bool(re.search(r'\d', tok)) and tok.endswith('TH')
    return bool(re.search(r'\d', tok)) and tok.endswith(suffix)

def _has_garage_code(*parts):
    return (
        any(_token_has_marker_suffix(tok, 'G') for tok in _model_tokens(*parts))
        or _has_suffix_pair(parts, {'G'})
    )

def _has_renovated_code(*parts):
    return (
        any(_token_has_marker_suffix(tok, 'R') for tok in _model_tokens(*parts))
        or _has_suffix_pair(parts, {'R'})
    )

def _has_townhome_code(*parts):
    return (
        any(tok == 'TH' or _token_has_marker_suffix(tok, 'TH') for tok in _model_tokens(*parts))
        or _has_suffix_pair(parts, {'TH'})
    )

def _has_renovated_term(text):
    return bool(re.search(r'\b(?:renov(?:ated|ation)?|rennov(?:ated|ation)?|reno|updated|upgrade[sd]?)\b', text, re.I))

def _has_garage_term(text):
    return bool(re.search(r'\b(?:garage|gar|attached garage|detached garage)\b', text, re.I))

def _has_townhome_term(text):
    # Keep townhome detection strict to avoid accidental matches.
    return bool(re.search(r'\btownhome\b', text, re.I))

def _dedupe_model_descriptors(model):
    text = re.sub(r'\s+', ' ', str(model or '')).strip()
    for phrase in ['Designer Loft', 'Classic Loft', 'Renovated', 'Garage', 'TownHome', 'View', 'Designer', 'Classic', 'Loft']:
        pat = re.compile(r'(?i)(?:\b' + re.escape(phrase) + r'\b)(?:\s+\b' + re.escape(phrase) + r'\b)+')
        text = pat.sub(phrase, text)
    return re.sub(r'\s+', ' ', text).strip()

def _enrich_model(model, *parts):
    model = re.sub(r'\s+', ' ', str(model or '')).strip()
    source_text = ' '.join(str(p or '') for p in ((model,) + parts))
    source_text = re.sub(r'[_/,+()-]+', ' ', source_text.lower())
    source_text = re.sub(r'\s+', ' ', source_text).strip()
    existing = f" {model.lower()} "
    additions = []
    has_g_code = _has_garage_code(model)
    has_r_code = _has_renovated_code(model)
    has_th_code = _has_townhome_code(model)

    def add(label, patterns):
        nonlocal existing
        if any(re.search(pattern, source_text) for pattern in patterns) and f" {label.lower()} " not in existing:
            additions.append(label)
            existing += f"{label.lower()} "

    add('Designer Loft', [r'\bdesigner\s+loft\b', r'\bdesigned\s+loft\b'])
    add('Classic Loft', [r'\bclassic\s+loft\b'])
    add('Designer', [r'\bdesigner\b', r'\bdesigned\b'])
    add('Classic', [r'\bclassic\b'])
    add('Loft', [r'\bloft\b'])
    if (has_g_code or _has_garage_term(source_text)) and not _has_garage_term(model):
        add('Garage', [r'.'])
    if (has_r_code or _has_renovated_term(source_text)) and not _has_renovated_term(model):
        add('Renovated', [r'.'])
    if (has_th_code or _has_townhome_term(source_text)) and not _has_townhome_term(model):
        add('TownHome', [r'.'])
    add('View', [r'\bview\b'])

    return _dedupe_model_descriptors(f"{model} {' '.join(additions)}".strip())

def _marker_flags(model, *parts):
    text = ' '.join(str(p or '') for p in ((model,) + parts))
    normalized = re.sub(r'[_/,+()-]+', ' ', text.lower())
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return {
        'garage': '✓' if (
            _has_garage_term(normalized)
            or _has_garage_code(model, *parts)
        ) else '',
        'renovated': '✓' if (
            _has_renovated_term(normalized)
            or _has_renovated_code(model, *parts)
        ) else '',
        'townhome': '✓' if (
            _has_townhome_term(normalized)
            or _has_townhome_code(model, *parts)
        ) else '',
    }

def _table_col_index(headers, *patterns):
    for i, header in enumerate(headers):
        if any(re.search(pattern, header, re.I) for pattern in patterns):
            return i
    return None

def _collapse_repeated_columns(items):
    items = list(items)
    if len(items) >= 8 and len(items) % 2 == 0:
        half = len(items) // 2
        left = [str(x).strip().lower() for x in items[:half]]
        right = [str(x).strip().lower() for x in items[half:]]
        if left == right:
            return items[:half]
    return items

def _unit_identity(row):
    combined = str(row.get('unit_model_combined') or '').strip()
    if combined:
        return combined.lower()
    apt = str(row.get('apt_number') or '').strip()
    model = str(row.get('model_number') or '').strip()
    fallback = f"{apt} {model}".strip() or apt
    return fallback.lower()

# PATCH 7: make_row adds unit_model_combined
def make_row(d, prop, floor, url):
    d = dict(d)
    apt, model = split_apt_model(d.get('apt_number',''), d.get('model_number',''))
    model = _enrich_model(model, apt, d.get('raw_model', ''))
    d['apt_number'] = apt; d['model_number'] = model
    d['available_date'] = get_date(d.get('available_date', ''))
    d['unit_model_combined'] = f"{apt} {model}".strip() if (apt or model) else ''
    d.update(_marker_flags(model, apt, d.get('raw_model', ''), d.get('unit_model_combined', '')))
    return {**d, 'property_name': prop, 'floor': floor,
            'source_url': url, 'scraped_at': datetime.now().isoformat()}

# ── WAIT HELPERS (IDENTICAL to v24) ──────────────────────────────────────────
def wait_for_any(driver, selectors, timeout=15):
    start = time.time()
    while time.time() - start < timeout:
        for sel in selectors:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if any(e.is_displayed() for e in els): return True
            except: pass
        time.sleep(0.5)
    return False

def wait_for_text(driver, selector, timeout=8):
    start = time.time()
    while time.time() - start < timeout:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, selector)
            if els and any('$' in (e.get_attribute('textContent') or '') for e in els[:5]):
                return True
        except: pass
        time.sleep(0.5)
    return False

# ── FLOOR DISCOVERY + CLICKING (IDENTICAL to v24) ────────────────────────────
def discover_floor_nums(driver, wid):
    strategies = [lambda: _nums_from_container(driver, "[class*='floors-container']"),
        lambda: _nums_from_container(driver, "[class*='frame-row--floors']"),
        lambda: _nums_from_title_spans(driver), lambda: _nums_from_floor_label(driver),
        lambda: _nums_from_scan(driver)]
    for i, strategy in enumerate(strategies):
        try:
            nums = strategy()
            if nums: dbg(wid, f"  🏢 Floors (strategy {i+1}): {nums}"); return nums
        except: continue
    return []

def _sequential(candidates):
    if not candidates: return []
    s = set(candidates); seq, exp = [], (0 if 0 in s else 1)
    while exp in s: seq.append(exp); exp += 1
    if len(seq) >= 2: return seq
    sorted_c = sorted(set(candidates))
    if len(sorted_c) >= 2:
        if all(sorted_c[i]+1 == sorted_c[i+1] for i in range(len(sorted_c)-1)) and max(sorted_c) <= 10: return sorted_c
    return []

def _extract_nums(elements):
    seen, nums = set(), []
    for el in elements:
        try:
            raw = (el.get_attribute('textContent') or el.text or '').strip().split('\n')[0].strip()
            if re.match(r'^\d{1,2}$', raw):
                n = int(raw)
                if n not in seen and 0 <= n <= 20: seen.add(n); nums.append(n)
        except: continue
    return _sequential(nums)

def _nums_from_container(driver, sel):
    for c in driver.find_elements(By.CSS_SELECTOR, sel):
        if not c.is_displayed(): continue
        nums = _extract_nums(c.find_elements(By.XPATH, ".//*"))
        if nums: return nums
    return []

def _nums_from_title_spans(driver):
    return _extract_nums(driver.find_elements(By.CSS_SELECTOR, "[class*='floors-item-content-label']"))

def _nums_from_floor_label(driver):
    for xp in ["//*[normalize-space(text())='FLOOR' or normalize-space(text())='Floor']",
               "//*[contains(translate(normalize-space(text()),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'select a floor')]"]:
        try:
            node = driver.find_element(By.XPATH, xp)
            for _ in range(6):
                node = driver.execute_script("return arguments[0].parentElement;", node)
                if not node: break
                nums = _extract_nums(node.find_elements(By.XPATH, ".//*"))
                if nums: return nums
        except: continue
    return []

def _nums_from_scan(driver):
    for ctx in driver.find_elements(By.XPATH,
        "//*[contains(@class,'floor') or contains(@class,'Floor') or "
        "normalize-space(text())='FLOOR' or normalize-space(text())='Floor']"):
        try:
            node = ctx
            for _ in range(5):
                node = driver.execute_script("return arguments[0].parentElement;", node)
                if not node: break
                nums = _extract_nums(node.find_elements(By.XPATH, ".//*"))
                if nums: return nums
        except: continue
    return []

def click_floor(driver, n, wid):
    try:
        for label in driver.find_elements(By.CSS_SELECTOR, "[class*='floors-item-content-label']"):
            if (label.get_attribute('textContent') or '').strip() == str(n):
                parent = driver.execute_script("let el=arguments[0]; while(el && !el.className.includes('floors-item')) el=el.parentElement; return el;", label)
                if parent: driver.execute_script("arguments[0].click();", parent); dbg(wid, f"    ✅ floor {n} (label)"); return True
    except: pass
    for xp in [f"//*[contains(@class,'floors-container') or contains(@class,'frame-row--floors')]//*[starts-with(normalize-space(text()),'{n}')]",
        f"//button[normalize-space(text())='{n}']", f"//div[normalize-space(text())='{n}']",
        f"//span[normalize-space(text())='{n}']", f"//li[normalize-space(text())='{n}']",
        f"//*[normalize-space(text())='FLOOR' or normalize-space(text())='Floor']/following::*[starts-with(normalize-space(text()),'{n}')]"]:
        try:
            for el in driver.find_elements(By.XPATH, xp):
                if el.text.strip().split('\n')[0].strip() == str(n) and el.is_displayed():
                    js_click(driver, el); dbg(wid, f"    ✅ floor {n}"); return True
        except: continue
    dbg(wid, f"    ❌ floor {n} not found"); return False

# ── SIGHTMAP HELPERS (IDENTICAL to v24 except PATCH 8: model regex) ──────────
def _is_google_maps(el):
    try:
        href = el.get_attribute('href') or ''
        return 'google.com/maps' in href or 'maps.google' in href
    except: return False

def _find_sightmap_iframe(driver, wid, extra_wait=20):
    start = time.time()
    while time.time() - start < extra_wait:
        for f in driver.find_elements(By.TAG_NAME, 'iframe'):
            src = f.get_attribute('src') or ''
            if 'sightmap.com' in src: dbg(wid, f"    SightMap iframe: {src[:70]}"); return f
        time.sleep(0.8)
    return None

def _wait_for_sightmap_units(driver, wid, timeout=20):
    unit_sels = ["button[id*='list-item']","[data-testid='unit-list'] button",
        "[class*='unit-list'] button","[class*='result-list'] button",
        "[aria-label*='APT']","[aria-label*='Unit']","[aria-label*='Home']","[aria-label*='HOME']"]
    start = time.time()
    while time.time() - start < timeout:
        for sel in unit_sels:
            if driver.find_elements(By.CSS_SELECTOR, sel): dbg(wid, f"    unit list ready ({sel})"); return True
        try:
            body = driver.find_element(By.TAG_NAME, 'body').text
            if 'MATCH' in body and ('APT' in body or '$' in body): dbg(wid, "    unit list ready (body text)"); return True
        except: pass
        time.sleep(0.8)
    dbg(wid, f"    unit list not detected after {timeout}s"); return False

def _scrape_sightmap_unit_list(driver, prop, url, wid, floors):
    all_units, seen = [], set()
    def scrape_one_floor(floor_label):
        for sel in ["[data-testid='unit-list']","[class*='unit-list']","[class*='result']"]:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                if el.is_displayed() and el.size.get('height', 0) > 50:
                    for _ in range(30): driver.execute_script("arguments[0].scrollTop+=400;", el); time.sleep(0.07)
                    driver.execute_script("arguments[0].scrollTop=0;", el); time.sleep(0.3); break
        t0 = time.time()
        while time.time() - t0 < 8:
            btns = driver.find_elements(By.CSS_SELECTOR,
                "[data-testid='unit-list'] button, [class*='unit-list'] button, "
                "button[id*='list-item'], [aria-label*='APT'], [aria-label*='Unit'], "
                "[aria-label*='Home'], [aria-label*='HOME']")
            if btns: break
            time.sleep(0.5)
        units = []
        for btn in btns:
            aria = btn.get_attribute('aria-label') or ''
            if not aria: continue
            m = re.match(r'^(?:APT|Apt|PT|Unit|Home|HOME)\s+(\S+?)[\.,\s]', aria, re.I)
            if not m: continue
            apt = m.group(1).strip('.,')
            if not apt: continue
            # PATCH 8: improved model extraction (FIX 5/15/18 + Garage/Renovated/TH/V)
            model = ''
            try:
                stripped = re.sub(r'^(?:APT\s+|Unit\s+|Home\s+)?' + re.escape(apt) + r'\s*',
                                  '', btn.text.strip(), flags=re.I)
                slines = [l.strip() for l in stripped.split('\n') if l.strip()]
                # Collect all lines BEFORE the first bed/bath/sqft/price line as model parts
                model_parts = []
                for sl in slines:
                    if RE_BEDS.search(sl) or RE_BATH.search(sl) or RE_SQFT.search(sl) or RE_PRICE.search(sl):
                        break
                    if sl and len(sl) <= 40:
                        model_parts.append(sl)
                model = ' '.join(model_parts).strip()
            except: pass
            b  = RE_BEDS.search(aria); ba = RE_BATH.search(aria)
            sq = re.search(r'([\d,]+)\s*sq\.?\s*ft', aria, re.I)
            rp = re.search(r'\$([\d,]+(?:\.\d{2})?)\s*per\s*month', aria, re.I)
            av = re.search(r'Available\s+(?:in\s+\d+\s+days?|Now|Immediately|[\w]+(?:\s+\d+\w*)?)', aria, re.I)
            # Rent: try per-month pattern, then get_rent on aria, then on button text
            rent = ''
            if rp:
                rent = f"${int(float(rp.group(1).replace(',','')))}"
            if not rent:
                rent = get_rent(aria)
            if not rent:
                try: rent = get_rent(btn.text)
                except: pass
            row = make_row({
                'apt_number': apt, 'model_number': model,
                'beds': b.group(1) if b else '', 'baths': ba.group(1) if ba else '',
                'sqft': sq.group(1).replace(',','') if sq else '',
                'available_date': get_date(av.group(0)) if av else get_date(aria),
                'rent': rent, 'raw_text': f"{aria}\n{btn.text}", 'raw_model': model,
            }, prop, floor_label, url)
            uid = _unit_identity(row)
            if uid in seen: continue
            seen.add(uid)
            units.append(row)
        return units
    if not floors: return scrape_one_floor('All')
    prev_fp = None
    for n in floors:
        click_floor(driver, n, wid); time.sleep(1.2)
        t0 = time.time()
        while time.time() - t0 < 8:
            fp = _floor_fingerprint(driver, None)
            if fp != prev_fp: break
            time.sleep(0.4)
        prev_fp = _floor_fingerprint(driver, None)
        new_units = scrape_one_floor(str(n))
        dbg(wid, f"    floor {n}: {len(new_units)} units"); all_units.extend(new_units)
    return all_units

def _click_map_trigger(driver, wid):
    for sel in ["a[href='#sightMapModal']","a[href*='sightmap']","button[data-target*='sightmap']","button[data-bs-target*='sightmap']"]:
        for el in driver.find_elements(By.CSS_SELECTOR, sel):
            if el.is_displayed() and not _is_google_maps(el): js_click(driver, el); dbg(wid, f"    map trigger: {sel}"); time.sleep(3); return True
    try: driver.execute_script("window.scrollBy(0, 600);"); time.sleep(2)
    except: pass
    for sel in ["a[href='#sightMapModal']","a[href*='sightmap']","button[data-target*='sightmap']","button[data-bs-target*='sightmap']"]:
        for el in driver.find_elements(By.CSS_SELECTOR, sel):
            if el.is_displayed() and not _is_google_maps(el): js_click(driver, el); dbg(wid, f"    map trigger (post-scroll): {sel}"); time.sleep(3); return True
    for pat in ["view interactive site map","interactive site map","interactive map","view map","map view","site map"]:
        xp = f"//*[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'{pat}')]"
        for el in driver.find_elements(By.XPATH, xp):
            try:
                if not el.is_displayed(): continue
                if el.tag_name not in ('a','button','div','span','li'): continue
                if _is_google_maps(el): continue
                js_click(driver, el); dbg(wid, f"    map trigger text: {el.text.strip()!r}"); time.sleep(3); return True
            except: continue
    return False

# ── Brookfield (IDENTICAL to v24 except PATCH 9: REST model FIX 1) ──────────
def _try_brookfield(driver, prop, url, wid):
    driver.switch_to.default_content()
    if 'brookfieldproperties.com' not in driver.current_url: return []
    dbg(wid, "    Brookfield: finding Interactive Map toggle...")
    orig_url = driver.current_url; orig_handles = set(driver.window_handles)
    candidates = []
    try:
        for inp in driver.find_elements(By.CSS_SELECTOR, "input[type='radio'], input[type='checkbox']"):
            iid = (inp.get_attribute('id') or inp.get_attribute('value') or '').lower()
            if 'map' in iid or 'interactive' in iid: candidates.insert(0, inp)
    except: pass
    for sel in ["[class*='toggle']","[class*='switch']","[class*='slider']"]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                if el.is_displayed() and not _is_google_maps(el): candidates.append(el)
        except: continue
    for xp in ["//label[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'interactive map')]",
        "//span[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'interactive map')]",
        "//button[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'interactive map')]",
        "//*[normalize-space(text())='Interactive Map' or normalize-space(text())='INTERACTIVE MAP']",
        "//div[contains(translate(normalize-space(text()),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'interactive map') and not(.//a)]"]:
        for el in driver.find_elements(By.XPATH, xp):
            try:
                if el.is_displayed() and not _is_google_maps(el): candidates.append(el)
            except: continue
    candidates.sort(key=lambda e: len((e.text or e.get_attribute('textContent') or '')))
    clicked = False
    for el in candidates:
        try: js_click(driver, el); clicked = True; break
        except: continue
    if not clicked: return []
    t0 = time.time(); sightmap_appeared = False
    while time.time() - t0 < 8:
        new_handles = set(driver.window_handles) - orig_handles
        if new_handles: driver.switch_to.window(list(new_handles)[0]); break
        if driver.current_url != orig_url: break
        for f in driver.find_elements(By.TAG_NAME, 'iframe'):
            if 'sightmap.com' in (f.get_attribute('src') or ''): sightmap_appeared = True; break
        if sightmap_appeared: break
        time.sleep(0.5)
    else:
        nab0 = re.sub(r'nab=\d+', 'nab=0', orig_url)
        if nab0 == orig_url: nab0 = orig_url + ('&' if '?' in orig_url else '?') + 'nab=0'
        try: driver.get(nab0)
        except: pass
    time.sleep(3)
    if 'sightmap.com' in driver.current_url:
        _wait_for_sightmap_units(driver, wid, timeout=25)
        return _scrape_sightmap_unit_list(driver, prop, url, wid, discover_floor_nums(driver, wid))
    iframe = _find_sightmap_iframe(driver, wid, extra_wait=20)
    if iframe:
        try:
            driver.switch_to.frame(iframe); time.sleep(3)
            _wait_for_sightmap_units(driver, wid, timeout=25)
            units = _scrape_sightmap_unit_list(driver, prop, url, wid, discover_floor_nums(driver, wid))
            driver.switch_to.default_content(); return units
        except: driver.switch_to.default_content()
    # REST fallback — PATCH 9: extract model from more fields (FIX 1)
    m = re.search(r'propertyId\[?\]?=(\d+)', url)
    if not m: return []
    pid = m.group(1)
    api_url = f"https://rent.brookfieldproperties.com/wp-json/brookfield/v1/units?propertyId[]={pid}&per_page=500"
    data_str = None
    try:
        data_str = driver.execute_async_script(f"""var done=arguments[arguments.length-1];
            fetch("{api_url}",{{headers:{{'Accept':'application/json','X-Requested-With':'XMLHttpRequest'}}}})
              .then(r=>r.text()).then(t=>done(t)).catch(e=>done(''));""")
    except: pass
    if not data_str:
        try:
            cookies = {c['name']:c['value'] for c in driver.get_cookies()}
            r = requests.get(api_url, cookies=cookies, timeout=15, headers={'Accept':'application/json','User-Agent':'Mozilla/5.0'})
            data_str = r.text
        except: return []
    try: data = json.loads(data_str)
    except: return []
    if not isinstance(data, list): data = data.get('units') or data.get('data') or data.get('results') or []
    units, seen = [], set()
    for item in data:
        try:
            apt = str(item.get('unit_number') or item.get('unitNumber') or item.get('unit') or '').strip()
            if not apt or apt in seen: continue
            seen.add(apt)
            rent_raw = item.get('price') or item.get('rent') or item.get('base_rent') or ''
            try: rent = f"${int(float(str(rent_raw).replace(',','').replace('$','')))}"
            except: rent = ''
            model = str(item.get('floor_plan') or item.get('floorPlan') or item.get('floorplan') or item.get('plan_name') or item.get('model') or item.get('unit_type') or '').strip()
            units.append(make_row({'apt_number':apt,'model_number':model,
                'beds':str(item.get('bedrooms') or item.get('beds') or ''),'baths':str(item.get('bathrooms') or item.get('baths') or ''),
                'sqft':str(item.get('square_feet') or item.get('sqft') or ''),'available_date':str(item.get('available_date') or item.get('availableDate') or ''),'rent':rent,
                'raw_text': json.dumps(item, ensure_ascii=True), 'raw_model': model}, prop, str(item.get('floor','')), url))
        except: continue
    return units

# ── DISCOVERY CASCADE (IDENTICAL to v24) ─────────────────────────────────────
def discover_and_extract(driver, url, wid):
    prop = get_property_name(url)
    methods = [("brookfield", lambda: _try_brookfield(driver, prop, url, wid)),
        ("jd-fp-unit-card", lambda: _try_jdfp_unit_cards(driver, prop, url, wid)),
        ("sightmap-iframe", lambda: _try_sightmap_iframe(driver, prop, url, wid)),
        ("entrata-beans-map", lambda: _try_entrata_map(driver, prop, url, wid)),
        ("irt-unit-list", lambda: _try_irt_unit_list(driver, prop, url, wid)),
        ("yardi-sightmap", lambda: _try_yardi_sightmap(driver, prop, url, wid)),
        ("jd-fp-floorplan", lambda: _try_jdfp_floorplan_cards(driver, prop, url, wid)),
        ("body-text", lambda: _try_body_text(driver, prop, url, wid))]
    for name, method in methods:
        try:
            dbg(wid, f"  🔍 Trying: {name}")
            units = method()
            if units: dbg(wid, f"  ✅ {len(units)} units via '{name}'"); return units, name
            dbg(wid, f"  ↩️  '{name}' → 0")
        except Exception as e:
            dbg(wid, f"  ⚠️  '{name}' error: {e}")
            try: driver.switch_to.default_content()
            except: pass
    return [], "none"

# ── ALL REMAINING METHODS: IDENTICAL TO v24 ──────────────────────────────────
# (jd-fp-unit-card, sightmap-iframe, entrata, IRT, yardi, floorplan-card,
#  body-text, floor-loop, scrape-cards, iframe-text, parse-text-blocks)
# These are copied BYTE-FOR-BYTE from the original v24 code in document 5.

def _try_jdfp_unit_cards(driver, prop, url, wid):
    driver.switch_to.default_content()
    if not driver.find_elements(By.CSS_SELECTOR, "a[class*='jd-fp-unit-card']"): return []
    _click_map_tab(driver, wid)
    dbg(wid, "    waiting for unit numbers...")
    start = time.time()
    while time.time() - start < 15:
        spans = driver.find_elements(By.CSS_SELECTOR, "a[class*='jd-fp-unit-card'] [class*='card-info__title--large']")
        if any(s.text.strip() for s in spans): break
        time.sleep(0.5)
    return _extract_with_floors(driver, prop, url, wid, card_sel="a[class*='jd-fp-unit-card']")

def _click_map_tab(driver, wid):
    for xp in ["//*[contains(@class,'jd-fp__toolbar')]//a[normalize-space(text())='Map']",
               "//*[contains(@class,'jd-fp__toolbar')]//button[normalize-space(text())='Map']",
               "//*[contains(@class,'jd-fp__toolbar-col--tabs')]//*[normalize-space(text())='Map']"]:
        for el in driver.find_elements(By.XPATH, xp):
            if el.is_displayed():
                driver.execute_script("arguments[0].click();", el); dbg(wid, "    🗺️  clicked Engrain Map tab"); time.sleep(2); return

def _try_sightmap_iframe(driver, prop, url, wid):
    driver.switch_to.default_content()
    is_entrata = bool(driver.find_elements(By.CSS_SELECTOR, "[class*='beans-floorplans']") or driver.find_elements(By.ID, "beans-maps-iframe") or 'entrata' in driver.page_source.lower())
    sightmap_frames = []
    for iframe in driver.find_elements(By.TAG_NAME, 'iframe'):
        src = iframe.get_attribute('src') or ''; cls = iframe.get_attribute('class') or ''; iid = iframe.get_attribute('id') or ''
        if 'sightmap.com' in src: sightmap_frames.append(iframe)
        elif not src and ('sightmap' in iid.lower() or 'jd-fp-map-embed' in cls):
            dbg(wid, "  ⏳ blank sightmap iframe — waiting for src...")
            for _ in range(20):
                time.sleep(1); src = iframe.get_attribute('src') or ''
                if 'sightmap.com' in src: sightmap_frames.append(iframe); break
    # Also detect Yardi — skip trigger to avoid conflicting with yardi method
    is_yardi = '.aspx' in driver.current_url.lower()
    if not sightmap_frames and not is_entrata and not is_yardi:
        orig_handles = set(driver.window_handles)
        if _click_map_trigger(driver, wid):
            time.sleep(2)
            new_handles = set(driver.window_handles) - orig_handles
            if new_handles:
                driver.switch_to.window(list(new_handles)[0]); dbg(wid, f"    new tab: {driver.current_url}"); time.sleep(3)
                if 'sightmap.com' in driver.current_url:
                    _wait_for_sightmap_units(driver, wid); return _scrape_sightmap_unit_list(driver, prop, url, wid, discover_floor_nums(driver, wid))
                f = _find_sightmap_iframe(driver, wid, extra_wait=15)
                if f:
                    try:
                        driver.switch_to.frame(f); time.sleep(3); _wait_for_sightmap_units(driver, wid)
                        units = _scrape_sightmap_unit_list(driver, prop, url, wid, discover_floor_nums(driver, wid))
                        driver.switch_to.default_content(); return units
                    except: driver.switch_to.default_content()
                return []
            else:
                f = _find_sightmap_iframe(driver, wid, extra_wait=20)
                if f: sightmap_frames.append(f)
    for iframe in sightmap_frames:
        try:
            driver.switch_to.frame(iframe); time.sleep(2)
            body = driver.find_element(By.TAG_NAME, 'body').text
            if len(body) < 50: driver.switch_to.default_content(); continue
            dbg(wid, f"    sightmap iframe: {len(body)} chars")
            units = _extract_with_floors(driver, prop, url, wid, card_sel=None)
            driver.switch_to.default_content()
            if units: return units
        except: driver.switch_to.default_content()
    return []

def _try_entrata_map(driver, prop, url, wid):
    driver.switch_to.default_content()
    has_beans = (driver.find_elements(By.CSS_SELECTOR, "[class*='beans-floorplans']") or driver.find_elements(By.ID, "beans-maps-iframe"))
    if not has_beans: return []
    for xp in ["//*[contains(@class,'beans-floorplans-map-tab-name')]","//*[contains(translate(normalize-space(text()),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'interactive map')]"]:
        for el in driver.find_elements(By.XPATH, xp):
            if el.is_displayed(): driver.execute_script("arguments[0].click();", el); dbg(wid, "    clicked Interactive Map tab (Entrata)"); time.sleep(3); break
    iframe = None
    for f in driver.find_elements(By.TAG_NAME, 'iframe'):
        src = f.get_attribute('src') or ''; iid = f.get_attribute('id') or ''
        if 'view_beans_map' in src or 'beans-maps-iframe' in iid: iframe = f; break
    if iframe:
        dbg(wid, "    Entrata: beans iframe found")
        try: driver.switch_to.frame(iframe)
        except: iframe = None
    if iframe:
        start = time.time()
        while time.time() - start < 15:
            if re.search(r'\d+-\d+', driver.find_element(By.TAG_NAME,'body').text): break
            time.sleep(0.5)
        floors = []
        try:
            body_txt = driver.find_element(By.TAG_NAME,'body').text
            mi = re.search(r'FLOORS\s*\n?([\s\S]{1,40}?)(?:\n\n|\Z)', body_txt, re.I)
            if mi: floors = sorted(set(int(n) for n in re.findall(r'\b(\d+)\b', mi.group(1)) if 1 <= int(n) <= 20))
        except: pass
        all_units_iframe, seen = [], set()
        def scrape_in_iframe(floor_label):
            for sel in ["[class*='beans-map-preview']","[class*='unit-list']"]:
                for el in driver.find_elements(By.CSS_SELECTOR, sel):
                    if el.is_displayed() and el.size.get('height',0) > 100:
                        for _ in range(25): driver.execute_script("arguments[0].scrollTop+=400;", el); time.sleep(0.1)
                        driver.execute_script("arguments[0].scrollTop=0;", el); time.sleep(0.3); break
            units = []
            for title_el in driver.find_elements(By.CSS_SELECTOR, "[class*='beans-map-preview-content-title'],[class*='beans-map-preview-content-header-title']"):
                apt = title_el.text.strip()
                if not apt or apt in seen or not re.search(r'\d', apt) or len(apt) > 30 or '\n' in apt: continue
                try:
                    card = driver.execute_script("""let el=arguments[0];for(let i=0;i<8;i++){el=el.parentElement;if(!el)break;if(el.querySelectorAll("[class*='detail']").length>0)return el;}return arguments[0].parentElement;""", title_el)
                except: card = title_el
                floor_n=beds=baths=sqft=avail=rent=''
                try:
                    for el in card.find_elements(By.CSS_SELECTOR, "[class*='floor']"):
                        m2 = re.search(r'\d+', el.text)
                        if m2 and 'Floor' in el.text: floor_n = m2.group(); break
                    for el in card.find_elements(By.CSS_SELECTOR, "[class*='detail-text'],[class*='detail-item']"):
                        t = el.text.replace('\xa0',' ').strip(); b=RE_BEDS.search(t);ba=RE_BATH.search(t);sq=RE_SQFT.search(t)
                        if b and not beds: beds=b.group(1)
                        if ba and not baths: baths=ba.group(1)
                        if sq and not sqft: sqft=sq.group(1).replace(',','')
                    for el in card.find_elements(By.CSS_SELECTOR, "[class*='avail'],[class*='status'],[class*='date']"):
                        t = el.text.strip()
                        if re.search(r'available|now', t, re.I): avail = t; break
                    rent = get_rent(card.text)
                except: pass
                seen.add(apt)
                units.append(make_row({'apt_number':apt,'model_number':'','beds':beds,'baths':baths,'sqft':sqft,'available_date':avail,'rent':rent,
                    'raw_text': card.text}, prop, floor_n or floor_label, url))
            if not units:
                body = driver.find_element(By.TAG_NAME,'body').text
                for block in re.split(r'\n(?=(?:Blg\s+\d|\d+-\d+))', body):
                    lines = [l.strip() for l in block.split('\n') if l.strip()]
                    if not lines: continue
                    apt = lines[0]
                    if not apt or apt in seen or not re.search(r'\d', apt) or len(apt) > 30: continue
                    floor_n=beds=baths=sqft=avail=rent=''
                    for line in lines[1:]:
                        if re.match(r'^Floor\s+\d+$', line, re.I) and not floor_n: floor_n = re.search(r'\d+', line).group()
                        b=RE_BEDS.search(line);ba=RE_BATH.search(line);sq=RE_SQFT.search(line)
                        if b and not beds: beds=b.group(1)
                        if ba and not baths: baths=ba.group(1)
                        if sq and not sqft: sqft=sq.group(1).replace(',','')
                        if re.search(r'available', line, re.I) and not avail: avail = line
                        if '$' in line and not rent: rent = get_rent(line)
                    seen.add(apt)
                    units.append(make_row({'apt_number':apt,'model_number':'','beds':beds,'baths':baths,'sqft':sqft,'available_date':avail,'rent':rent,
                        'raw_text': block}, prop, floor_n or floor_label, url))
            return units
        if floors:
            for fn in floors:
                for xp in [f"//button[normalize-space(text())='{fn}']",f"//*[normalize-space(text())='{fn}']"]:
                    for el in driver.find_elements(By.XPATH, xp):
                        if el.is_displayed(): driver.execute_script("arguments[0].click();",el); dbg(wid,f"    floor {fn}"); time.sleep(1.5); break
                all_units_iframe.extend(scrape_in_iframe(str(fn)))
        else: all_units_iframe = scrape_in_iframe('All')
        driver.switch_to.default_content()
        if all_units_iframe: return all_units_iframe
    dbg(wid, "    Entrata: trying inline DOM scrape")
    t0 = time.time()
    while time.time() - t0 < 15:
        try:
            if driver.find_elements(By.CSS_SELECTOR, "[class*='beans-map-preview-content-title']"): break
            body_txt = driver.find_element(By.TAG_NAME,'body').text
            if re.search(r'\d+-\d+', body_txt) and re.search(r'Bed|Bath|Available', body_txt, re.I): break
        except: pass
        time.sleep(0.8)
    for panel_sel in ["[class*='beans-map-preview']","[class*='unit-list']","[class*='beans-floorplans-list']"]:
        for el in driver.find_elements(By.CSS_SELECTOR, panel_sel):
            if el.is_displayed() and el.size.get('height',0) > 80:
                for _ in range(40): driver.execute_script("arguments[0].scrollTop+=300;", el); time.sleep(0.06)
                driver.execute_script("arguments[0].scrollTop=0;", el); time.sleep(0.5); break
    seen2, all_units_inline = set(), []
    def scrape_inline(floor_label):
        units = []
        for title_el in driver.find_elements(By.CSS_SELECTOR, "[class*='beans-map-preview-content-title'],[class*='beans-map-preview-content-header-title'],[class*='beans-unit-list-item-title']"):
            apt = title_el.text.strip()
            if not apt or apt in seen2 or not re.search(r'\d', apt) or len(apt) > 30 or '\n' in apt: continue
            try:
                card = driver.execute_script("""let el=arguments[0];for(let i=0;i<10;i++){el=el.parentElement;if(!el)break;let t=el.innerText||'';if((t.includes('Bed')||t.includes('Bath')||t.includes('sq')||t.includes('$'))&&el.offsetHeight>30)return el;}return arguments[0].parentElement;""", title_el)
            except: card = title_el
            card_text = ''
            try: card_text = card.get_attribute('textContent') or ''
            except: card_text = apt
            floor_n = floor_label
            try:
                fm = re.search(r'Floor\s+(\d+)', card_text, re.I)
                if fm: floor_n = fm.group(1)
            except: pass
            b=RE_BEDS.search(card_text);ba=RE_BATH.search(card_text);sq=RE_SQFT.search(card_text);dt=RE_DATE.search(card_text)
            rent = get_rent(card_text)
            seen2.add(apt)
            units.append(make_row({'apt_number':apt,'model_number':'','beds':b.group(1) if b else '','baths':ba.group(1) if ba else '','sqft':sq.group(1).replace(',','') if sq else '','available_date':dt.group(0).strip() if dt else '','rent':rent,
                'raw_text': card_text}, prop, floor_n, url))
        if not units:
            try:
                body = driver.find_element(By.TAG_NAME,'body').text
                for block in re.split(r'\n(?=(?:Blg\s+\d|\d+-\d+))', body):
                    lines = [l.strip() for l in block.split('\n') if l.strip()]
                    if not lines: continue
                    apt = lines[0]
                    if not apt or apt in seen2 or not re.search(r'\d',apt) or len(apt)>30: continue
                    block_txt = ' '.join(lines); b2=RE_BEDS.search(block_txt);ba2=RE_BATH.search(block_txt);sq2=RE_SQFT.search(block_txt);dt2=RE_DATE.search(block_txt);rent2=get_rent(block_txt)
                    if not (b2 or sq2 or rent2): continue
                    seen2.add(apt)
                    units.append(make_row({'apt_number':apt,'model_number':'','beds':b2.group(1) if b2 else '','baths':ba2.group(1) if ba2 else '','sqft':sq2.group(1).replace(',','') if sq2 else '','available_date':dt2.group(0).strip() if dt2 else '','rent':rent2,
                        'raw_text': block_txt}, prop, floor_label, url))
            except: pass
        return units
    floors2 = []
    try:
        body_txt2 = driver.find_element(By.TAG_NAME,'body').text
        m3 = re.search(r'FLOORS\s*\n?([\s\S]{1,60}?)(?:\n\n|\Z)', body_txt2, re.I)
        if m3: floors2 = sorted(set(int(n) for n in re.findall(r'\b(\d+)\b', m3.group(1)) if 1 <= int(n) <= 20))
    except: pass
    if floors2:
        for fn in floors2:
            for xp in [f"//button[normalize-space(text())='{fn}']",f"//*[contains(@class,'floor') and normalize-space(text())='{fn}']"]:
                for el in driver.find_elements(By.XPATH, xp):
                    if el.is_displayed(): driver.execute_script("arguments[0].click();",el); dbg(wid,f"    Entrata inline floor {fn}"); time.sleep(1.5); break
            all_units_inline.extend(scrape_inline(str(fn)))
    else: all_units_inline = scrape_inline('All')
    return all_units_inline

def _try_irt_unit_list(driver, prop, url, wid):
    driver.switch_to.default_content()
    if 'irtliving.com' not in driver.current_url: return []
    for xp in ["//*[contains(translate(normalize-space(text()),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'interactive map')]","//a[contains(@href,'map')]"]:
        done = False
        for el in driver.find_elements(By.XPATH, xp):
            if el.is_displayed(): driver.execute_script("arguments[0].click();",el); done=True; break
        if done: break
    iframe = None; start = time.time()
    while time.time() - start < 12:
        for f in driver.find_elements(By.TAG_NAME, 'iframe'):
            if 'sightmap.com' in (f.get_attribute('src') or ''): iframe = f; break
        if iframe: break
        time.sleep(0.5)
    if not iframe: return []
    try: driver.switch_to.frame(iframe)
    except: return []
    time.sleep(3)
    floors = discover_floor_nums(driver, wid)
    # Uses shared _scrape_sightmap_unit_list (has PATCH 8 model fix)
    units = _scrape_sightmap_unit_list(driver, prop, url, wid, floors)
    driver.switch_to.default_content()
    return units

def _try_yardi_sightmap(driver, prop, url, wid):
    driver.switch_to.default_content()
    cur = driver.current_url; ps = driver.page_source
    is_yardi = ('.aspx' in cur.lower() or 'yardi' in ps.lower() or 'rentcafe' in ps.lower() or 'securecafe.com' in ps.lower())
    if not is_yardi: return []
    dbg(wid, "    Yardi/RentCafe detected — clicking Interactive Map button")
    iframe = _find_sightmap_iframe(driver, wid, extra_wait=3)
    if not iframe:
        clicked = False
        for xp in ["//*[normalize-space(text())='Interactive Map' or normalize-space(text())='INTERACTIVE MAP']",
            "//*[contains(translate(normalize-space(text()),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'interactive map')]",
            "//button[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'interactive map')]",
            "//a[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'interactive map')]",
            "//label[contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'interactive map')]"]:
            for el in driver.find_elements(By.XPATH, xp):
                try:
                    if not el.is_displayed(): continue
                    if _is_google_maps(el): continue
                    js_click(driver, el); dbg(wid, f"    Yardi: clicked '{el.text.strip()}'"); clicked = True; break
                except: continue
            if clicked: break
        if not clicked: return []
        time.sleep(3); iframe = _find_sightmap_iframe(driver, wid, extra_wait=15)
    if not iframe: return []
    try:
        driver.switch_to.frame(iframe); time.sleep(2)
        body = driver.find_element(By.TAG_NAME, 'body').text
        if len(body) < 50: driver.switch_to.default_content(); return []
        # Try aria-label path first (handles multi-floor properly)
        _wait_for_sightmap_units(driver, wid, timeout=15)
        floors = discover_floor_nums(driver, wid)
        if floors:
            dbg(wid, f"    Yardi: {len(floors)} floors detected, using aria-label path")
            units = _scrape_sightmap_unit_list(driver, prop, url, wid, floors)
            if units: driver.switch_to.default_content(); return units
        # Fallback: body-text path
        units = _extract_with_floors(driver, prop, url, wid, card_sel=None)
        if not units:
            units = _scrape_sightmap_unit_list(driver, prop, url, wid, floors)
        driver.switch_to.default_content(); return units
    except Exception as e:
        dbg(wid, f"    Yardi SightMap error: {e}"); driver.switch_to.default_content(); return []

def _try_jdfp_floorplan_cards(driver, prop, url, wid):
    driver.switch_to.default_content()
    cards = driver.find_elements(By.CSS_SELECTOR, "a[class*='jd-fp-floorplan-card']")
    if not cards: return []
    start = time.time()
    while time.time() - start < 12:
        if any(s.text.strip() and '$' in s.text for s in driver.find_elements(By.CSS_SELECTOR, "[class*='card-info-term-and-base--base']")): break
        time.sleep(0.5)
    cards = driver.find_elements(By.CSS_SELECTOR, "a[class*='jd-fp-floorplan-card']")
    units, seen = [], set()
    for c in cards:
        try:
            model = ''
            for sel in ["p[class*='card-info__title']","[class*='floorplan-card__title']","p","h3","h4"]:
                els = c.find_elements(By.CSS_SELECTOR, sel)
                if els: model = els[0].text.strip()
                if model: break
            if not model:
                lines = [l.strip() for l in (c.get_attribute('textContent') or '').split('\n') if l.strip()]
                model = lines[0] if lines else ''
            rent = ''
            for span in c.find_elements(By.CSS_SELECTOR, "[class*='card-info-term-and-base--base']"):
                t = span.text.strip()
                if t and '$' in t: rent = t; break
            full_text = c.get_attribute('textContent') or ''; b1=RE_BEDS.search(full_text);b2=RE_BATH.search(full_text);sq=RE_SQFT.search(full_text)
            apt = model if (model and re.match(r'^[A-Za-z][A-Za-z0-9 ]{0,15}$', model.strip())) else ''
            if not apt or not (rent or sq) or apt in seen: continue
            seen.add(apt)
            units.append(make_row({'apt_number':apt,'model_number':apt,'beds':b1.group(1) if b1 else '','baths':b2.group(1) if b2 else '','sqft':sq.group(1).replace(',','') if sq else '','available_date':'','rent':get_rent(rent) if rent else '',
                'raw_text': full_text}, prop, 'All', url))
        except: continue
    return units

def _try_body_text(driver, prop, url, wid):
    driver.switch_to.default_content()
    try:
        # Try HTML table parsing first (shoronclearlake, villasbythebay)
        tables = driver.find_elements(By.CSS_SELECTOR, "table")
        for table in tables:
            header_cells = table.find_elements(By.XPATH, ".//tr[1]/*[self::th or self::td]")
            headers = []
            for c in header_cells:
                try:
                    if not c.is_displayed():
                        continue
                except:
                    continue
                headers.append(c.text.strip().lower())
            headers = _collapse_repeated_columns(headers)
            if any('apartment' in h or 'unit' in h for h in headers) and any('rent' in h or 'bed' in h for h in headers):
                dbg(wid, f"    Found unit table with {len(headers)} columns: {headers[:6]}")
                apt_i   = _table_col_index(headers, r'\bapartment\b', r'\bunit\b', r'\bapt\b')
                floor_i = _table_col_index(headers, r'^floor$')
                model_i = _table_col_index(headers, r'floor\s*plan', r'\bplan\b', r'\bmodel\b')
                rent_i  = _table_col_index(headers, r'\brent\b', r'\bprice\b', r'monthly\s*rent')
                avail_i = _table_col_index(headers, r'available\s*date', r'\bavailable\b', r'move\s*-?\s*in', r'\bdate\b')
                beds_i  = _table_col_index(headers, r'\bbeds?\b', r'\bbedrooms?\b')
                baths_i = _table_col_index(headers, r'\bbaths?\b', r'\bbathrooms?\b')
                sqft_i  = _table_col_index(headers, r'sq\.?\s*ft', r'\bsqft\b', r'square\s*feet')
                if apt_i is None and len(headers) >= 4:
                    apt_i = 0
                if floor_i is None and len(headers) >= 4:
                    floor_i = 1
                if model_i is None and len(headers) >= 4:
                    model_i = 2
                if rent_i is None and len(headers) >= 4:
                    rent_i = 3
                if beds_i is None and len(headers) >= 2:
                    beds_i = len(headers) - 2
                if baths_i is None and len(headers) >= 1:
                    baths_i = len(headers) - 1
                if avail_i is None and len(headers) >= 7:
                    avail_i = 6 if len(headers) >= 9 else 5
                rows = table.find_elements(By.CSS_SELECTOR, "tr")
                units = []
                for row in rows[1:]:  # skip header
                    cells = []
                    for td in row.find_elements(By.XPATH, "./*[self::td or self::th]"):
                        try:
                            if not td.is_displayed():
                                continue
                        except:
                            continue
                        cells.append(td.text.strip())
                    cells = _collapse_repeated_columns(cells)
                    if len(cells) < 3:
                        continue

                    def cell(idx):
                        return cells[idx].strip() if idx is not None and idx < len(cells) else ''

                    apt = cell(apt_i).lstrip('#').strip()
                    if not apt or not re.search(r'\d', apt): continue
                    floor_val = cell(floor_i) or 'All'
                    model = cell(model_i)
                    rent_text = cell(rent_i)
                    beds = cell(beds_i)
                    baths = cell(baths_i)
                    sqft_text = cell(sqft_i) or model
                    avail = cell(avail_i)
                    if ('liveatshoronclearlake.com' in url or 'liveatvillasbythebay.com' in url) and len(cells) >= 8:
                        apt = cells[0].lstrip('#').strip()
                        floor_val = cells[1].strip() or 'All'
                        model = cells[2].strip()
                        rent_text = cells[3].strip()
                        avail = cells[6].strip() if len(cells) >= 9 else cells[5].strip()
                        beds = cells[-2].strip()
                        baths = cells[-1].strip()
                        sqft_text = model
                    if not avail and len(cells) >= 7:
                        avail = cells[6].strip() if len(cells) >= 9 else cells[5].strip()
                    if not beds and len(cells) >= 2:
                        beds = cells[-2].strip()
                    if not baths and len(cells) >= 1:
                        baths = cells[-1].strip()
                    b = re.search(r'(\d)', str(beds)); ba = re.search(r'(\d\.?\d*)', str(baths))
                    sq = RE_SQFT.search(str(sqft_text)) if sqft_text else re.search(r'(\d{3,4})\s*sq\.?\s*ft', str(model), re.I)
                    if not sq and model:
                        sq = re.search(r'\b(\d{3,4})\b', str(model))
                    units.append(make_row({
                        'apt_number': apt, 'model_number': str(model or ''),
                        'beds': b.group(1) if b else '', 'baths': ba.group(1) if ba else '',
                        'sqft': sq.group(1).replace(',','') if sq else re.sub(r'[^\d]','', str(sqft_text or '')),
                        'available_date': str(avail or ''), 'rent': get_rent(str(rent_text or '')),
                        'raw_text': ' | '.join(cells), 'raw_model': str(model or ''),
                    }, prop, floor_val, url))
                if units:
                    dbg(wid, f"    Table: {len(units)} units"); return units

        # Scroll overflow panels incrementally to force lazy-load all items
        driver.execute_script("""
            var panels=document.querySelectorAll('*');
            for(var i=0;i<panels.length;i++){
                var el=panels[i];var st=window.getComputedStyle(el);
                if((st.overflowY==='auto'||st.overflowY==='scroll')&&el.scrollHeight>el.clientHeight+50){
                    var h=0;while(h<el.scrollHeight){h+=300;el.scrollTop=h;}
                }}""")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);"); time.sleep(1)
        driver.execute_script("window.scrollTo(0, 0);"); time.sleep(0.5)
        return _parse_text_blocks(driver.find_element(By.TAG_NAME,'body').text, prop, url, 'All')
    except: return []

def _extract_with_floors(driver, prop, url, wid, card_sel):
    floors = discover_floor_nums(driver, wid); seen, all_units = set(), []
    def scrape_current(floor_label):
        if card_sel: return _scrape_cards(driver, card_sel, prop, url, floor_label, wid)
        else: return _scrape_iframe_text(driver, prop, url, floor_label, wid)
    if not floors:
        for u in scrape_current('All'):
            uid = _unit_identity(u)
            if uid not in seen: seen.add(uid); all_units.append(u)
        return all_units
    prev_fp = _floor_fingerprint(driver, card_sel)
    for n in floors:
        if not click_floor(driver, n, wid): continue
        time.sleep(1.2); start = time.time()
        while time.time() - start < 12:
            fp = _floor_fingerprint(driver, card_sel)
            if fp != prev_fp: break
            try:
                m = RE_HEADER.search(driver.find_element(By.TAG_NAME,'body').text)
                if m and int(m.group(2)) == n: break
            except: pass
            time.sleep(0.5)
        prev_fp = _floor_fingerprint(driver, card_sel)
        new = [u for u in scrape_current(str(n)) if _unit_identity(u) not in seen]
        for u in new: seen.add(_unit_identity(u))
        all_units.extend(new); time.sleep(random.uniform(0.7, 1.2))
    return all_units

def _floor_fingerprint(driver, card_sel):
    try:
        if card_sel:
            els = driver.find_elements(By.CSS_SELECTOR, card_sel)
            return hash(tuple(e.get_attribute('textContent')[:30] for e in els[:3]))
        body = driver.find_element(By.TAG_NAME,'body').text
        m = RE_HEADER.search(body)
        return m.group(0) if m else body[:100]
    except: return None

# PATCH 10: _scrape_cards skips deposit prices (FIX 20)
def _scrape_cards(driver, sel, prop, url, floor_label, wid):
    try:
        for list_sel in ["[class*='unit-list']","[class*='cards-container']"]:
            for el in driver.find_elements(By.CSS_SELECTOR, list_sel):
                if el.is_displayed() and el.size.get('height',0) > 50:
                    for _ in range(10): driver.execute_script("arguments[0].scrollTop+=400;", el); time.sleep(0.08)
                    driver.execute_script("arguments[0].scrollTop=0;", el); break
    except: pass
    start = time.time()
    while time.time() - start < 8:
        spans = driver.find_elements(By.CSS_SELECTOR, f"{sel} [class*='card-info__text'] span")
        if any(re.search(r'\d\s*(bed|bath|sq)', s.text, re.I) for s in spans[:10]): break
        time.sleep(0.4)
    units = []; cards = driver.find_elements(By.CSS_SELECTOR, sel)
    for c in cards:
        try:
            unit_num = ''
            for span in c.find_elements(By.CSS_SELECTOR, "[class*='card-info__title--large']"):
                t = span.text.strip()
                if t: unit_num = t.lstrip('#').strip(); break
            model = ''
            for p in c.find_elements(By.CSS_SELECTOR, "[class*='unit-card__floorplan-title']"):
                t = p.text.strip()
                if t: model = t; break
            avail = ''
            for span in c.find_elements(By.CSS_SELECTOR, "[class*='card-info__text--brand']"):
                t = span.text.strip()
                if t: avail = t; break
            rent = ''
            for price_sel in ["[class*='card-info-term-and-base--base']","[class*='strong-text']","[class*='card-info__text--stack']"]:
                for span in c.find_elements(By.CSS_SELECTOR, price_sel):
                    t = span.text.strip()
                    if t and '$' in t:
                        try: pt = (span.find_element(By.XPATH,'..').get_attribute('textContent') or '').lower()
                        except: pt = ''
                        if 'deposit' not in pt: rent = get_rent(t); break
                if rent: break
            beds=baths=sqft=''
            for span in c.find_elements(By.CSS_SELECTOR, "[class*='card-info__text'] span"):
                t = span.text.strip()
                if not t: continue
                if not beds:
                    m = re.match(r'^(\d)\s*(?:bed|BR|bd)s?$', t, re.I)
                    if m: beds = m.group(1); continue
                if not baths:
                    m = re.match(r'^(\d\.?\d*)\s*(?:bath|BA|ba)s?$', t, re.I)
                    if m: baths = m.group(1); continue
                if not sqft:
                    m = re.match(r'^([\d,]+)\s*sq\.?\s*ft', t, re.I)
                    if m: sqft = m.group(1).replace(',',''); continue
            if not beds or not sqft:
                full = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', re.sub(r'(\d)([a-zA-Z])', r'\1 \2', c.get_attribute('textContent') or ''))
                if not beds:
                    m = RE_BEDS.search(full)
                    if m: beds = m.group(1)
                if not baths:
                    m = RE_BATH.search(full)
                    if m: baths = m.group(1)
                if not sqft:
                    m = RE_SQFT.search(full)
                    if m: sqft = m.group(1).replace(',','')
            if not unit_num or not (rent or sqft): continue
            units.append(make_row({'apt_number':unit_num,'model_number':model,'beds':beds,'baths':baths,'sqft':sqft,'available_date':avail,'rent':rent,
                'raw_text': c.get_attribute('textContent') or '', 'raw_model': model}, prop, floor_label, url))
        except: continue
    return units

def _scrape_iframe_text(driver, prop, url, floor_label, wid):
    try:
        for sel in ["[class*='unit-list']","[class*='result']"]:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                if el.is_displayed() and el.size.get('height',0) > 50:
                    for _ in range(8): driver.execute_script("arguments[0].scrollTop+=300;",el); time.sleep(0.08)
                    driver.execute_script("arguments[0].scrollTop=0;",el); break
    except: pass
    text = ''
    try:
        for el in driver.find_elements(By.XPATH, "//*[contains(text(),'MATCHES')]"):
            node = el
            for _ in range(8):
                node = driver.execute_script("return arguments[0].parentElement;", node)
                if not node: break
                t = node.text.strip()
                if len(t) > 200 and RE_PRICE.search(t): text = t; break
            if text: break
    except: pass
    if not text:
        try: text = driver.find_element(By.TAG_NAME,'body').text
        except: pass
    return _parse_text_blocks(text, prop, url, floor_label)

def _parse_text_blocks(text, prop, url, floor_label):
    units = []
    apt_blocks = re.split(r'(?=(?:APT|Apt|HOME|Home)\s+\d|\bUNIT\s+[A-Z0-9]|#[\w-]{2,})', text)
    if len(apt_blocks) > 2:
        for block in apt_blocks:
            block = block.strip()
            if len(block) < 15: continue
            d = parse_card(block)
            if d['apt_number'] and (d['rent'] or d['sqft'] or d['beds']):
                units.append(make_row(d, prop, floor_label, url))
        if units: return units
    fp_blocks = re.split(r'\n(?=[A-Z][A-Z0-9 ]{0,8}\n\d\s*(?:bed|Bed|BR))', text)
    if len(fp_blocks) > 1:
        for block in fp_blocks:
            block = block.strip()
            if len(block) < 15 or ('$' not in block and not RE_BEDS.search(block)): continue
            d = parse_card(block)
            if d['apt_number'] and (d['rent'] or d['sqft'] or d['beds']):
                units.append(make_row(d, prop, floor_label, url))
    return units

# ── PROMOTIONS (PATCH 11: enhanced for popups/modals FIX 9-12) ───────────────
def scrape_promotions(driver):
    promos, seen, candidates = [], set(), []
    def add_promo(txt):
        txt = ' '.join(str(txt or '').split()).strip()
        if len(txt) <= 10:
            return
        if RE_PROMO.search(txt):
            key = txt.lower()[:80]
            if key not in seen:
                seen.add(key)
                promos.append(txt)
    for sel in ["[class*='promo']","[class*='banner']","[class*='offer']","[class*='special']",
                "[class*='announcement']","[class*='alert']","[class*='notice']",
                "[class*='ribbon']","[class*='incentive']","header","[role='banner']",
                "[class*='hero']","[class*='marquee']","[class*='callout']",
                "[class*='move-in']","[class*='movein']"]:
        try: candidates.extend(driver.find_elements(By.CSS_SELECTOR, sel))
        except: pass
    for sel in ["[class*='modal'][class*='show']","[class*='modal'].active",".modal.show",
                "[role='dialog']","[class*='modal-content']","[class*='popup']",
                "[class*='modal'][style*='display: block']","[class*='lightbox']"]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                if el.is_displayed(): candidates.append(el)
        except: pass
    try:
        for line in driver.find_element(By.TAG_NAME,'body').text.split('\n'):
            add_promo(line)
    except: pass
    for el in candidates:
        try:
            t = el.text.strip()
            if not t:
                t = (el.get_attribute('textContent') or '').strip()
            if not t:
                continue
            joined = ' '.join(t.split())
            add_promo(joined[:500])
            for line in t.split('\n'):
                add_promo(line)
            for attr in ['aria-label', 'title', 'alt']:
                add_promo((el.get_attribute(attr) or '').strip())
            for img in el.find_elements(By.CSS_SELECTOR, "img"):
                add_promo((img.get_attribute('alt') or '').strip())
                add_promo((img.get_attribute('title') or '').strip())
        except: continue
    return ' | '.join(promos[:5]) if promos else ''

# ── EXTRACT / WORKER / SAVE / MAIN (IDENTICAL to v24 except save cols) ───────
def extract(url, driver, wid):
    url = url.strip()
    dbg(wid, f"\n{'='*50}\n  {url}")
    try:
        driver.get(url); time.sleep(random.uniform(5.5, 9.0))
        handle_turnstile_if_present(driver, url, wid)
        # Scrape promotions BEFORE dismiss — popups are visible now
        promotion = scrape_promotions(driver)
        dismiss(driver)
        driver.execute_script("window.scrollTo(0, 300);"); time.sleep(random.uniform(2.5, 4.5))
        # Scrape again after scroll in case banner specials appeared
        promo2 = scrape_promotions(driver)
        if promo2 and promo2 != promotion:
            promotion = (promotion + ' | ' + promo2).strip(' |') if promotion else promo2
        if promotion: dbg(wid, f"  🎁 Promo: {promotion[:80]}")
        wait_for_any(driver, ["[class*='jd-fp-unit-card']","[class*='jd-fp-floorplan-card']","[class*='jd-fp-map-embed']","iframe[class*='jd-fp']","[class*='beans-floorplans']","[data-testid='unit-list']"], timeout=18)
        all_units, method = discover_and_extract(driver, url, wid)
        for u in all_units: u['special_promotion'] = promotion
        seen_keys, unique = set(), []
        for u in all_units:
            key = f"{u.get('property_name','')}_{u.get('unit_model_combined') or u.get('apt_number','')}"
            if key not in seen_keys: seen_keys.add(key); unique.append(u)
        try: driver.switch_to.default_content()
        except: pass
        dbg(wid, f"  🎯 {len(unique)} unique units via '{method}'")
        return {'url':url,'method':method,'units':unique,'success':bool(unique),'timestamp':datetime.now().isoformat()}
    except Exception as e:
        dbg(wid, f"  ❌ {e}"); import traceback; traceback.print_exc()
        try: driver.switch_to.default_content()
        except: pass
        return {'url':url,'error':str(e),'timestamp':datetime.now().isoformat()}

def worker(wid, urls, queue, headless=False):
    try:
        d = make_driver(headless=headless)
    except Exception as e:
        dbg(wid, f"  ❌ make_driver failed: {e}")
        for url in urls:
            queue.put({'url': url, 'error': f'driver_init: {e}', 'timestamp': datetime.now().isoformat()})
        return
    try:
        for i, url in enumerate(urls, 1):
            dbg(wid, f"\n[{i}/{len(urls)}]"); queue.put(extract(url, d, wid))
            if i < len(urls): time.sleep(random.uniform(5, 10))
    finally: d.quit()

# PATCH 12: save with unit_model_combined column
def save(all_units):
    if not all_units: print("\n⚠️  No units extracted."); return None
    cols = ['property_name','floor','apt_number','model_number','unit_model_combined',
            'garage','renovated','townhome',
            'rent','sqft','beds','baths','available_date','special_promotion','source_url','scraped_at']
    df = pd.DataFrame(all_units).reindex(columns=cols, fill_value='')
    df['_key'] = df['property_name'] + '_' + df['unit_model_combined'].fillna(df['apt_number'])
    dupes = df[df.duplicated('_key', keep=False)]
    print(f"\n{'✅ No duplicates' if dupes.empty else f'⚠️  {len(dupes)} duplicate rows'}")
    df = df.drop(columns=['_key'])
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    df.to_csv(f"engrain_units_{ts}.csv", index=False)
    df.to_json(f"engrain_units_{ts}.json", orient='records', indent=2)
    print(f"📁 engrain_units_{ts}.csv  ({len(df)} units)")
    print("\n🔍 Coverage:")
    for col in ['apt_number','model_number','unit_model_combined','rent','sqft','beds','baths','available_date']:
        n = df[col].astype(str).str.strip().ne('').sum(); pct = n/len(df)*100
        print(f"  {'✅' if pct>=80 else '⚠️' if pct>=50 else '❌'} {col:25s}: {n}/{len(df)} ({pct:.0f}%)")
    for f in ['extracted_units_v5.csv','extracted_units_combined.csv']:
        if os.path.exists(f):
            old = pd.read_csv(f)
            for c in cols:
                if c not in old.columns: old[c] = ''
            pd.concat([old.reindex(columns=cols,fill_value=''), df], ignore_index=True).to_csv('extracted_units_combined.csv', index=False)
            print("📁 extracted_units_combined.csv (merged)"); break
    return df

# ── FAILURE TRACKING + NOTIFICATIONS ─────────────────────────────────────────
FAILURE_HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'failure_history.json')

def _load_failure_history():
    if os.path.exists(FAILURE_HISTORY_FILE):
        try:
            with open(FAILURE_HISTORY_FILE) as f:
                return json.load(f)
        except: pass
    return {}

def _save_failure_history(history):
    with open(FAILURE_HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)

def _update_failure_history(all_results, history):
    """Update consecutive failure day counts. Returns (updated_history, critical_list).
    critical_list = [(url, days)] for URLs that have failed 3+ consecutive days."""
    today = datetime.now().strftime('%Y-%m-%d')
    critical = []
    for r in all_results:
        url = r['url']
        succeeded = bool(r.get('units'))
        entry = history.get(url, {
            'consecutive_fail_days': 0,
            'last_fail_date': None,
            'last_success_date': None,
            'notified_on': None,
        })
        if succeeded:
            entry['consecutive_fail_days'] = 0
            entry['last_success_date'] = today
            entry['notified_on'] = None       # reset so next streak sends a fresh alert
        else:
            # Only count once per calendar day (retries within same day don't stack)
            if entry.get('last_fail_date') != today:
                entry['consecutive_fail_days'] = entry.get('consecutive_fail_days', 0) + 1
            entry['last_fail_date'] = today
            days = entry['consecutive_fail_days']
            if days >= 3 and entry.get('notified_on') != today:
                critical.append((url, days))
                entry['notified_on'] = today
        history[url] = entry
    return history, critical

def _send_notification(critical_urls):
    """Send email and/or webhook alert for URLs that have failed 3+ consecutive days."""
    lines = [
        f"The following URL(s) have returned 0 units for {critical_urls[0][1]}+ consecutive days,",
        f"even after all retry attempts. Please investigate.",
        "",
    ]
    for url, days in critical_urls:
        prop = get_property_name(url)
        lines.append(f"  - [{prop}] {url}  ({days} consecutive days)")
    lines += ["", f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
              "Check failure_history.json for the full history."]
    body = '\n'.join(lines)
    subject = f"[Engrain Scraper] {len(critical_urls)} URL(s) failing 3+ days in a row"

    # ── Webhook (Slack / Discord / Teams / any HTTP endpoint) ─────────────────
    webhook_url = os.environ.get('NOTIFY_WEBHOOK_URL', '')
    if webhook_url:
        try:
            payload = {'text': f"*{subject}*\n```{body}```"}
            requests.post(webhook_url, json=payload, timeout=10)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Webhook alert sent.", flush=True)
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Webhook failed: {e}", flush=True)

    # ── Email via SMTP ─────────────────────────────────────────────────────────
    to_addr   = os.environ.get('NOTIFY_EMAIL_TO', '')
    smtp_user = os.environ.get('SMTP_USER', '')
    smtp_pass = os.environ.get('SMTP_PASS', '')
    smtp_host = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.environ.get('SMTP_PORT', '587'))

    if not (to_addr and smtp_user and smtp_pass):
        if not webhook_url:
            # Neither email nor webhook is configured — log to stdout so it is never silent
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ALERT: {len(critical_urls)} URL(s) failing "
                  f"3+ days — no email/webhook configured.", flush=True)
            for url, days in critical_urls:
                print(f"   ❌ {url}  ({days} days)", flush=True)
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = to_addr
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        with smtplib.SMTP(smtp_host, smtp_port) as s:
            s.ehlo(); s.starttls(); s.login(smtp_user, smtp_pass)
            s.sendmail(smtp_user, to_addr, msg.as_string())
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Alert email sent to {to_addr}.", flush=True)
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Email send failed: {e}", flush=True)
        print(f"   Critical URLs: {[u for u, _ in critical_urls]}", flush=True)

def _run_batch(urls, nw, headless, t0):
    """Spin up `nw` worker processes for the given url list and collect results."""
    buckets = [[] for _ in range(nw)]
    for i, u in enumerate(urls): buckets[i % nw].append(u)
    q = Queue()
    ps = [Process(target=worker, args=(i+1, buckets[i], q, headless)) for i in range(nw)]
    for p in ps: p.start(); time.sleep(0.4)
    results = []
    while len(results) < len(urls):
        try:
            r = q.get(timeout=1)
            results.append(r)
            n = len(r.get('units', []))
            site = r['url'].split('/')[2].replace('www.', '')
            print(f"  {'✅' if n>0 else '❌'} {len(results)}/{len(urls)} | {n:3d} units | "
                  f"{(time.time()-t0)/60:.1f}m | {site} [{r.get('method','?')}]", flush=True)
            with open('engrain_results_latest.json', 'w') as _f:
                json.dump(results, _f, indent=2)
        except:
            if all(not p.is_alive() for p in ps):
                break
            continue
    for p in ps: p.join()
    return results

def main():
    ap = argparse.ArgumentParser(description="Engrain Scraper v27.0")
    ap.add_argument('urls', nargs='*')
    ap.add_argument('--file', '-f')
    ap.add_argument('--workers', '-w', default='auto',
                    help='Number of parallel workers (integer or "auto" = CPU count, default: auto)')
    ap.add_argument('--headless', action='store_true',
                    help='Run Chrome in headless mode (required on cloud/servers with no display)')
    ap.add_argument('--retries', type=int, default=2,
                    help='How many times to retry failed URLs (default: 2)')
    ap.add_argument('--retry-delay', type=int, default=300,
                    help='Seconds to wait before each retry pass (default: 300 = 5 min)')
    args = ap.parse_args()

    # ── URL collection ────────────────────────────────────────────────────────
    urls = list(args.urls)
    if args.file:
        with open(args.file) as f:
            urls += [l.strip() for l in f if l.strip() and not l.startswith('#')]
    if not urls:
        print("No URLs — using defaults."); urls = DEFAULT_URLS
    seen_u = set()
    urls = [u.strip() for u in urls if u.strip() and not (u.strip() in seen_u or seen_u.add(u.strip()))]

    # ── Worker count ──────────────────────────────────────────────────────────
    cpu_count = os.cpu_count() or 4
    if str(args.workers).lower() == 'auto' and sys.stdin.isatty():
        # Interactive mode: prompt the user
        recommended = min(3, len(urls), cpu_count)
        print(f"\n💻 This machine has {cpu_count} CPU cores.")
        print(f"   Each worker opens one Chrome browser and processes URLs in parallel.")
        print(f"   More workers = faster, but uses more RAM (~500 MB per worker).")
        print(f"\n   Recommended: {recommended}  |  Max sensible: {min(cpu_count, len(urls))}")
        while True:
            try:
                raw = input(f"\n   Enter number of workers [press Enter for {recommended}]: ").strip()
                nw = int(raw) if raw else recommended
                if 1 <= nw <= 20:
                    break
                print("   Please enter a number between 1 and 20.")
            except ValueError:
                print("   Please enter a valid integer.")
        nw = min(nw, len(urls))
    elif str(args.workers).lower() == 'auto':
        nw = min(cpu_count, 3, len(urls))   # safe default in non-interactive / cloud
    else:
        nw = min(int(args.workers), len(urls))

    mode_tag = "headless" if args.headless else "headed"
    print(f"🗺️  ENGRAIN SCRAPER v27.0  |  {len(urls)} URLs  |  {nw} workers  |  {mode_tag}\n")
    for u in urls: print(f"  • {u}")
    print(flush=True)

    # ── Initial run ───────────────────────────────────────────────────────────
    t0 = time.time()
    results = _run_batch(urls, nw, args.headless, t0)
    all_results = list(results)

    # ── Retry loop ────────────────────────────────────────────────────────────
    for attempt in range(1, args.retries + 1):
        failed = [r['url'] for r in results if not r.get('units')]
        if not failed:
            break
        print(f"\n⏳ Retry {attempt}/{args.retries}: {len(failed)} failed URL(s). "
              f"Waiting {args.retry_delay}s ({args.retry_delay//60}m {args.retry_delay%60}s)...", flush=True)
        time.sleep(args.retry_delay)
        print(f"\n🔄 Retry attempt {attempt} — {len(failed)} URLs\n", flush=True)
        retry_nw = min(nw, len(failed))
        results = _run_batch(failed, retry_nw, args.headless, t0)
        # Merge: replace original failed entries with retry results
        retry_map = {r['url']: r for r in results}
        all_results = [retry_map.get(r['url'], r) for r in all_results]

    # ── Failure tracking + notifications ──────────────────────────────────────
    history = _load_failure_history()
    history, critical = _update_failure_history(all_results, history)
    _save_failure_history(history)
    if critical:
        _send_notification(critical)

    # ── Summary ───────────────────────────────────────────────────────────────
    all_units = [u for r in all_results for u in r.get('units', [])]
    print(f"\n{'='*55}")
    print(f"📊 {len(all_units)} total  |  "
          f"{sum(1 for r in all_results if r.get('units'))}/{len(urls)} OK  |  "
          f"{(time.time()-t0)/60:.1f} min\n", flush=True)
    for r in all_results:
        site = r['url'].split('/')[2].replace('www.', '')
        print(f"  {'✅' if r.get('units') else '❌'} {site:<42} {len(r.get('units',[])):>5}  {r.get('method','?')}")
    save(all_units)
    print("\n✅ Done!", flush=True)

if __name__ == '__main__': main()
