#!/usr/bin/env python3
"""
Fetch newly added Indian movies, web-series, and documentaries from major
streaming platforms using the Streaming Availability API on RapidAPI.
Optionally email the results as an Excel attachment via Gmail SMTP.

Platforms queried:
  US region : Netflix, Prime Video, Hulu, Zee5
  IN region : Netflix, Prime Video, Hotstar, Zee5

Note: Aha and SunNxt are not supported by the Streaming Availability API.
      Hotstar is not available in the US region via this API.
      These limitations are logged as warnings at runtime.

Usage:
    export RAPIDAPI_KEY="your-rapidapi-key"
    export OMDB_API_KEY="your-omdb-key"       # free at https://www.omdbapi.com/apikey.aspx

    # Optional – enable email delivery:
    export SENDER_EMAIL="you@gmail.com"
    export SENDER_PASSWORD="xxxx xxxx xxxx xxxx"   # Gmail App Password
    export RECIPIENT_EMAIL="recipient@example.com"

    python indian_streaming_content.py
"""

import csv
import logging
import os
import smtplib
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_KEY = os.environ.get("RAPIDAPI_KEY", "")
OMDB_API_KEY = os.environ.get("OMDB_API_KEY", "")
BASE_URL = "https://streaming-availability.p.rapidapi.com"
OMDB_URL = "http://www.omdbapi.com/"
OUTPUT_CSV = "indian_streaming_content.csv"
LOOKBACK_DAYS = 7
CSV_FIELDS = ["title", "year", "type", "languages", "platform", "country", "date_added",
              "imdb_rating"]

# Email settings (all optional — email is skipped when any is missing).
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD", "")
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL", "")
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

# Catalogs to query per country.  Only services that the API actually
# supports in each region are listed here.
COUNTRY_CATALOGS = {
    "us": ["netflix", "prime", "hulu", "zee5"],
    "in": ["netflix", "prime", "hotstar", "zee5"],
}

# Services the user requested but that the API does not support.
UNSUPPORTED_SERVICES = ["aha", "sunnxt"]

# Indian languages — ISO 639-2 three-letter codes (the format the API uses).
INDIAN_LANG_CODES = {
    "hin",  # Hindi
    "tam",  # Tamil
    "tel",  # Telugu
    "mal",  # Malayalam
    "kan",  # Kannada
    "ben",  # Bengali
    "mar",  # Marathi
    "guj",  # Gujarati
    "pan",  # Punjabi
    "ori",  # Odia
    "asm",  # Assamese
    "urd",  # Urdu
    "san",  # Sanskrit
    "nep",  # Nepali
    "snd",  # Sindhi
    "kok",  # Konkani
    "mni",  # Manipuri
    "doi",  # Dogri
    "sat",  # Santali
    "mai",  # Maithili
    "kas",  # Kashmiri
    "bho",  # Bhojpuri
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _headers():
    return {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "streaming-availability.p.rapidapi.com",
    }


def _api_get(path, params, max_retries=3):
    """GET with simple retry + back-off for transient errors."""
    url = f"{BASE_URL}{path}"
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        if resp.status_code == 429:
            wait = 2 ** attempt
            log.warning("Rate-limited (429). Retrying in %ds…", wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()  # will raise on last failure


def fetch_changes(country, catalogs, from_timestamp):
    """
    Page through the /changes endpoint and return (changes, shows).

    changes : list of change objects
    shows   : dict mapping show-id -> show object
    """
    all_changes = []
    all_shows = {}
    cursor = None

    while True:
        params = {
            "country": country,
            "catalogs": ",".join(catalogs),
            "change_type": "new",
            "item_type": "show",
            "from": int(from_timestamp),
        }
        if cursor:
            params["cursor"] = cursor

        data = _api_get("/changes", params)
        all_changes.extend(data.get("changes", []))
        all_shows.update(data.get("shows", {}))

        if data.get("hasMore"):
            cursor = data.get("nextCursor")
        else:
            break

    return all_changes, all_shows


# ---------------------------------------------------------------------------
# OMDb / IMDb ratings
# ---------------------------------------------------------------------------

def fetch_imdb_ratings(rows):
    """Look up IMDb ratings for every unique imdbId in *rows*.

    Returns a dict ``{imdb_id: float|None}``.  Results are cached in-memory
    so the same id is never requested twice in one run.
    """
    if not OMDB_API_KEY:
        log.warning("OMDB_API_KEY not set — skipping IMDb rating lookups.")
        return {}

    cache = {}
    unique_ids = {r["imdb_id"] for r in rows if r.get("imdb_id")}
    log.info("Fetching IMDb ratings for %d unique title(s)…", len(unique_ids))

    for imdb_id in unique_ids:
        if imdb_id in cache:
            continue
        try:
            resp = requests.get(
                OMDB_URL,
                params={"apikey": OMDB_API_KEY, "i": imdb_id},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            raw = data.get("imdbRating", "N/A")
            cache[imdb_id] = float(raw) if raw != "N/A" else None
        except (requests.RequestException, ValueError) as exc:
            log.warning("Could not fetch rating for %s: %s", imdb_id, exc)
            cache[imdb_id] = None

    return cache


def _fmt_rating(val):
    """Format a rating for HTML display: one decimal or '–'."""
    if val is None:
        return "\u2013"  # en-dash
    return f"{val:.1f}"


# ---------------------------------------------------------------------------
# Filtering & extraction
# ---------------------------------------------------------------------------

def _audio_languages(show):
    """Return a set of ISO 639-2 language codes from all streaming options."""
    langs = set()
    for options in show.get("streamingOptions", {}).values():
        for opt in options:
            for audio in opt.get("audios", []):
                code = audio.get("language", "")
                if code:
                    langs.add(code)
    return langs


def is_indian_content(show):
    """Heuristic: a show is considered Indian if any audio track uses an Indian language."""
    return bool(_audio_languages(show) & INDIAN_LANG_CODES)


def classify_type(show):
    """Map the API show_type + genres to the user-facing content type."""
    genre_ids = {g.get("id", "") for g in show.get("genres", [])}
    if "documentary" in genre_ids:
        return "documentary"
    if show.get("showType") == "series":
        return "web-series"
    return "movie"


def readable_languages(show):
    """Return a human-friendly, sorted, comma-separated list of audio languages."""
    # Map common ISO 639-2 codes to readable names.
    code_to_name = {
        "hin": "Hindi",    "tam": "Tamil",      "tel": "Telugu",
        "mal": "Malayalam", "kan": "Kannada",    "ben": "Bengali",
        "mar": "Marathi",   "guj": "Gujarati",  "pan": "Punjabi",
        "ori": "Odia",      "asm": "Assamese",  "urd": "Urdu",
        "san": "Sanskrit",  "nep": "Nepali",    "snd": "Sindhi",
        "kok": "Konkani",   "mni": "Manipuri",  "doi": "Dogri",
        "sat": "Santali",   "mai": "Maithili",  "kas": "Kashmiri",
        "bho": "Bhojpuri",  "eng": "English",   "jpn": "Japanese",
        "kor": "Korean",    "zho": "Chinese",   "spa": "Spanish",
        "fra": "French",    "deu": "German",    "por": "Portuguese",
        "ita": "Italian",   "ara": "Arabic",    "tha": "Thai",
        "rus": "Russian",
    }
    codes = _audio_languages(show)
    names = sorted(code_to_name.get(c, c) for c in codes)
    return ", ".join(names)


# ---------------------------------------------------------------------------
# Excel output
# ---------------------------------------------------------------------------

def write_excel(rows, path):
    """Write *rows* (list of dicts) to an .xlsx file with basic formatting."""
    wb = Workbook()
    ws = wb.active
    ws.title = "New Releases"

    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")

    # Header row
    for col_idx, field in enumerate(CSV_FIELDS, start=1):
        cell = ws.cell(row=1, column=col_idx, value=field)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")

    # Data rows
    for row_idx, row in enumerate(rows, start=2):
        for col_idx, field in enumerate(CSV_FIELDS, start=1):
            ws.cell(row=row_idx, column=col_idx, value=row.get(field, ""))

    # Auto-width (approximate)
    for col_idx, field in enumerate(CSV_FIELDS, start=1):
        max_len = len(field)
        for row in rows:
            val = str(row.get(field, ""))
            if len(val) > max_len:
                max_len = len(val)
        ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = min(max_len + 4, 50)

    wb.save(path)
    log.info("Wrote Excel file to %s", path)


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def _build_summary(rows):
    """Return a string summarising release counts by country and platform."""
    by_country = Counter()
    by_combo = Counter()
    for r in rows:
        by_country[r["country"]] += 1
        by_combo[(r["country"], r["platform"])] += 1

    lines = [f"Total new Indian releases found: {len(rows)}"]
    for country in ("US", "IN"):
        if by_country[country]:
            lines.append(f"\n  {country}: {by_country[country]} title(s)")
            for (c, p), n in sorted(by_combo.items()):
                if c == country:
                    lines.append(f"    - {p}: {n}")
    return "\n".join(lines)


def _is_tamil(row):
    """Return True if the row's languages field includes Tamil."""
    langs_lower = row.get("languages", "").lower()
    # Match the readable name "Tamil" as well as the short code "ta".
    # Split on ", " so that "ta" doesn't false-match inside longer words.
    tokens = {t.strip() for t in langs_lower.split(",")}
    return "tamil" in tokens or "ta" in tokens


def _build_html_body(rows):
    """Build an HTML email body with summary, top-5 US, and Tamil releases."""
    today = datetime.now(timezone.utc).strftime("%B %d, %Y")

    # --- summary counts ---
    by_country = Counter()
    by_combo = Counter()
    for r in rows:
        by_country[r["country"]] += 1
        by_combo[(r["country"], r["platform"])] += 1

    summary_rows = ""
    for country in ("US", "IN"):
        for (c, p), n in sorted(by_combo.items()):
            if c == country:
                summary_rows += (
                    f"<tr><td>{c}</td><td>{p}</td>"
                    f"<td style='text-align:center'>{n}</td></tr>\n"
                )

    # --- top 5 US releases ---
    us_rows = [r for r in rows if r["country"] == "US"][:5]
    top5_html = ""
    for r in us_rows:
        top5_html += (
            f"<tr>"
            f"<td>{r['title']}</td>"
            f"<td style='text-align:center'>{r['year']}</td>"
            f"<td>{r['type']}</td>"
            f"<td>{r['platform']}</td>"
            f"<td style='text-align:center'>{r['date_added']}</td>"
            f"<td style='text-align:center'>{_fmt_rating(r.get('imdb_rating'))}</td>"
            f"</tr>\n"
        )
    if not top5_html:
        top5_html = "<tr><td colspan='6' style='text-align:center'>No US releases found</td></tr>"

    # --- top Tamil releases: US first → IN → others, newest first ---
    tamil_country_order = {"US": 0, "IN": 1}
    tamil_rows = [r for r in rows if _is_tamil(r)]
    tamil_rows.sort(key=lambda r: r["date_added"], reverse=True)      # newest first
    tamil_rows.sort(key=lambda r: tamil_country_order.get(r["country"], 99))  # US → IN
    if tamil_rows:
        tamil_html = ""
        for r in tamil_rows:
            tamil_html += (
                f"<tr>"
                f"<td>{r['title']}</td>"
                f"<td style='text-align:center'>{r['year']}</td>"
                f"<td>{r['type']}</td>"
                f"<td>{r['platform']}</td>"
                f"<td>{r['country']}</td>"
                f"<td style='text-align:center'>{r['date_added']}</td>"
                f"<td>{r['languages']}</td>"
                f"<td style='text-align:center'>{_fmt_rating(r.get('imdb_rating'))}</td>"
                f"</tr>\n"
            )
        tamil_section = f"""\
<h3>Top Tamil Releases</h3>
<table border="1" cellpadding="6" cellspacing="0"
       style="border-collapse:collapse;min-width:680px">
  <tr style="background:#4472C4;color:#fff">
    <th>Title</th><th>Year</th><th>Type</th><th>Platform</th>
    <th>Country</th><th>Date Added</th><th>Languages</th><th>IMDb</th>
  </tr>
  {tamil_html}
</table>"""
    else:
        tamil_section = """\
<h3>Top Tamil Releases</h3>
<p><em>No new Tamil releases in this period.</em></p>"""

    return f"""\
<html>
<body style="font-family:Arial,sans-serif;color:#333">
<h2>New Indian Movies &amp; Shows &mdash; {today}</h2>

<h3>Summary by Platform &amp; Country</h3>
<table border="1" cellpadding="6" cellspacing="0"
       style="border-collapse:collapse;min-width:320px">
  <tr style="background:#4472C4;color:#fff">
    <th>Country</th><th>Platform</th><th>Count</th>
  </tr>
  {summary_rows}
  <tr style="font-weight:bold">
    <td colspan="2">Grand Total</td>
    <td style="text-align:center">{len(rows)}</td>
  </tr>
</table>

<h3>Top 5 US Releases</h3>
<table border="1" cellpadding="6" cellspacing="0"
       style="border-collapse:collapse;min-width:520px">
  <tr style="background:#4472C4;color:#fff">
    <th>Title</th><th>Year</th><th>Type</th><th>Platform</th><th>Date Added</th><th>IMDb</th>
  </tr>
  {top5_html}
</table>

{tamil_section}

<p style="margin-top:18px;font-size:0.9em;color:#888">
  Full data is attached as an Excel file.
</p>
</body>
</html>"""


def send_email(rows, excel_path):
    """Send the results email with the Excel attachment via Gmail SMTP/TLS."""
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    msg = MIMEMultipart("mixed")
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECIPIENT_EMAIL
    msg["Subject"] = f"New Indian Movies & Shows - {today_str}"

    # HTML body
    html_body = _build_html_body(rows)
    msg.attach(MIMEText(html_body, "html"))

    # Excel attachment
    attachment_name = f"new_releases_{today_str}.xlsx"
    with open(excel_path, "rb") as fh:
        part = MIMEBase("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        part.set_payload(fh.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f"attachment; filename={attachment_name}")
    msg.attach(part)

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        log.info("Email sent successfully to %s", RECIPIENT_EMAIL)
    except smtplib.SMTPAuthenticationError:
        log.error(
            "SMTP authentication failed. Verify SENDER_EMAIL and "
            "SENDER_PASSWORD (must be a Gmail App Password, not your "
            "regular password)."
        )
    except smtplib.SMTPException as exc:
        log.error("Failed to send email: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not API_KEY:
        log.error(
            "RAPIDAPI_KEY environment variable is not set. "
            "Export it before running:\n  export RAPIDAPI_KEY='your-key'"
        )
        sys.exit(1)

    # Warn about unsupported services
    for svc in UNSUPPORTED_SERVICES:
        log.warning(
            "Service '%s' is not supported by the Streaming Availability API — skipping.", svc
        )
    log.warning(
        "Hotstar is not available in the US region via this API — queried only for IN."
    )

    from_ts = int((datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).timestamp())
    log.info(
        "Fetching new additions since %s (last %d days) for regions: %s",
        datetime.fromtimestamp(from_ts, tz=timezone.utc).strftime("%Y-%m-%d"),
        LOOKBACK_DAYS,
        ", ".join(COUNTRY_CATALOGS.keys()),
    )

    rows = []
    seen = set()

    for country, catalogs in COUNTRY_CATALOGS.items():
        log.info("Querying %s — catalogs: %s", country.upper(), ", ".join(catalogs))
        try:
            changes, shows = fetch_changes(country, catalogs, from_ts)
        except requests.HTTPError as exc:
            log.error("API error for %s: %s", country.upper(), exc)
            continue

        log.info("  %d change(s) returned, %d unique show(s)", len(changes), len(shows))

        for change in changes:
            show_id = change.get("showId")
            if not show_id or show_id not in shows:
                continue

            show = shows[show_id]
            if not is_indian_content(show):
                continue

            service_info = change.get("service", {})
            platform = service_info.get("name") or service_info.get("id", "unknown")

            key = (show_id, service_info.get("id", ""), country)
            if key in seen:
                continue
            seen.add(key)

            timestamp = change.get("timestamp")
            date_added = (
                datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")
                if timestamp
                else ""
            )

            rows.append({
                "title": show.get("title", ""),
                "year": show.get("releaseYear") or show.get("firstAirYear", ""),
                "type": classify_type(show),
                "languages": readable_languages(show),
                "platform": platform,
                "country": country.upper(),
                "date_added": date_added,
                "imdb_id": show.get("imdbId", ""),
                "imdb_rating": None,
            })

    # Sort: US first then IN → platform (Netflix, Prime Video, Hulu, …)
    # → date_added newest-first.  Two stable sorts achieve this cleanly.
    country_order = {"US": 0, "IN": 1}
    platform_order = {"Netflix": 0, "Prime Video": 1, "Hulu": 2,
                      "Hotstar": 3, "Zee5": 4}

    rows.sort(key=lambda r: r["date_added"], reverse=True)   # newest first
    rows.sort(key=lambda r: (                                 # stable: keeps
        country_order.get(r["country"], 99),                  # date order
        platform_order.get(r["platform"], 99),                # within groups
    ))

    # Enrich with IMDb ratings
    ratings = fetch_imdb_ratings(rows)
    for r in rows:
        imdb_id = r.get("imdb_id", "")
        if imdb_id and imdb_id in ratings:
            r["imdb_rating"] = ratings[imdb_id]

    # Write CSV (replace None ratings with empty string for clean output)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            csv_row = {k: (v if v is not None else "") for k, v in r.items()}
            writer.writerow(csv_row)

    log.info("Wrote %d record(s) to %s", len(rows), OUTPUT_CSV)

    # Write Excel and (optionally) email it
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    excel_path = f"new_releases_{today_str}.xlsx"
    write_excel(rows, excel_path)

    if SENDER_EMAIL and SENDER_PASSWORD and RECIPIENT_EMAIL:
        send_email(rows, excel_path)
    else:
        log.info(
            "Email skipped — set SENDER_EMAIL, SENDER_PASSWORD, and "
            "RECIPIENT_EMAIL to enable."
        )


if __name__ == "__main__":
    main()
