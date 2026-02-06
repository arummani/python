#!/usr/bin/env python3
"""
Fetch newly added Indian movies, web-series, and documentaries from major
streaming platforms using the Streaming Availability API on RapidAPI.

Platforms queried:
  US region : Netflix, Prime Video, Hulu, Zee5
  IN region : Netflix, Prime Video, Hotstar, Zee5

Note: Aha and SunNxt are not supported by the Streaming Availability API.
      Hotstar is not available in the US region via this API.
      These limitations are logged as warnings at runtime.

Usage:
    export RAPIDAPI_KEY="your-rapidapi-key"
    python indian_streaming_content.py
"""

import csv
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_KEY = os.environ.get("RAPIDAPI_KEY", "")
BASE_URL = "https://streaming-availability.p.rapidapi.com"
OUTPUT_FILE = "indian_streaming_content.csv"
LOOKBACK_DAYS = 7
CSV_FIELDS = ["title", "year", "type", "languages", "platform", "country", "date_added"]

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

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    log.info("Wrote %d record(s) to %s", len(rows), OUTPUT_FILE)


if __name__ == "__main__":
    main()
