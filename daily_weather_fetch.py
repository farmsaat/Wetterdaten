#!/usr/bin/env python3
"""
daily_fetch_weather.py
Reads locations.csv and downloads Open-Meteo daily weather data
(temperature_2m_max, temperature_2m_min, precipitation_sum)
for the current year from April 1st through yesterday.
Designed for daily GitHub Actions runs — always overwrites the JSON file.

Output filename:  weather_<id>_<year>.json
"""

import csv
import json
import os
import sys
import time
import urllib.request
import urllib.parse
from datetime import date, timedelta

# ── Configuration ─────────────────────────────────────────────────────────────
LOCATIONS_FILE = "locations.csv"
OUTPUT_DIR     = "."
DAILY_VARS     = "temperature_2m_max,temperature_2m_min,precipitation_sum"
FORECAST_URL   = "https://historical-forecast-api.open-meteo.com/v1/forecast"
RETRY_WAIT     = 3   # seconds between retries
MAX_RETRIES    = 3

# ── Date range ────────────────────────────────────────────────────────────────
today      = date.today()
YEAR       = today.year
START_DATE = date(YEAR, 4, 1)           # fixed: April 1st of the current year
END_DATE   = today - timedelta(days=1)  # yesterday (most recent complete day)

# Guard: if the workflow runs before April 1st, nothing to fetch yet
if END_DATE < START_DATE:
    print(f"Nothing to fetch yet — current date {today} is before {START_DATE}.")
    sys.exit(0)

# ── Helpers ───────────────────────────────────────────────────────────────────
def fetch_url(url: str) -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                return json.loads(resp.read())
        except Exception as exc:
            print(f"  [attempt {attempt}/{MAX_RETRIES}] Error: {exc}", flush=True)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
    raise RuntimeError(f"Failed to fetch: {url}")


def build_url(lat: float, lon: float, timezone: str) -> str:
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": START_DATE.isoformat(),   # e.g. "2026-04-01"
        "end_date":   END_DATE.isoformat(),     # e.g. "2026-05-31"
        "daily":      DAILY_VARS,
        "timezone":   timezone,
    }
    return FORECAST_URL + "?" + urllib.parse.urlencode(params)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(LOCATIONS_FILE, newline="", encoding="utf-8") as f:
        locations = list(csv.DictReader(f))

    print(
        f"Loaded {len(locations)} location(s). "
        f"Fetching {YEAR} data from {START_DATE} to {END_DATE}.",
        flush=True,
    )
    errors = []

    for loc in locations:
        loc_id   = loc["id"].strip()
        name     = loc["name"].strip()
        lat      = float(loc["lat"])
        lon      = float(loc["lon"])
        timezone = loc.get("timezone", "UTC").strip()

        out_file = os.path.join(OUTPUT_DIR, f"weather_{loc_id}_{YEAR}.json")
        url      = build_url(lat, lon, timezone)

        print(f"  Fetching {name} ({loc_id}) / {YEAR} ...", flush=True)
        try:
            data = fetch_url(url)
            with open(out_file, "w", encoding="utf-8") as fout:
                json.dump(data, fout, separators=(",", ":"))
            print(f"    ✓ Saved {out_file}", flush=True)
        except RuntimeError as exc:
            print(f"    ✗ FAILED: {exc}", flush=True)
            errors.append(f"{loc_id}/{YEAR}: {exc}")

        time.sleep(0.2)   # be polite to the free API

    if errors:
        print("\n── Errors ──────────────────────────────────────────", flush=True)
        for e in errors:
            print(f"  {e}", flush=True)
        sys.exit(1)
    else:
        print("\nAll downloads complete.", flush=True)


if __name__ == "__main__":
    main()