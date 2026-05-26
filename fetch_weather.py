#!/usr/bin/env python3
"""
fetch_weather.py
Reads locations.csv and downloads Open-Meteo daily weather data
(temperature_2m_max, temperature_2m_min, precipitation_sum)
for every year specified in YEARS, saving one JSON file per location+year.

Output filenames:  weather_<id>_<year>.json
"""

import csv
import json
import os
import sys
import time
import urllib.request
import urllib.parse
from datetime import date

# ── Configuration ─────────────────────────────────────────────────────────────
LOCATIONS_FILE = "locations.csv"
OUTPUT_DIR     = "."          # same folder as the HTML; change if needed
YEARS          = [2018, 2024, 2025]   # add/remove years here
START_MONTH    = "04-01"      # fetch full-year data so the HTML can slice it
END_MONTH      = "10-31"
DAILY_VARS     = "temperature_2m_max,temperature_2m_min,precipitation_sum"
BASE_URL       = "https://archive-api.open-meteo.com/v1/archive"
CURRENT_YEAR   = date.today().year
FORECAST_URL   = "https://api.open-meteo.com/v1/forecast"  # for current/future year
RETRY_WAIT     = 3   # seconds between retries
MAX_RETRIES    = 3

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


def build_url(lat: float, lon: float, year: int, timezone: str) -> str:
    if year < CURRENT_YEAR:
        # Historical archive
        params = {
            "latitude":   lat,
            "longitude":  lon,
            "start_date": f"{year}-{START_MONTH}",
            "end_date":   f"{year}-{END_MONTH}",
            "daily":      DAILY_VARS,
            "timezone":   timezone,
        }
        return BASE_URL + "?" + urllib.parse.urlencode(params)
    else:
        # Current year: combine past archive with forecast
        params = {
            "latitude":     lat,
            "longitude":    lon,
            "start_date":   f"{year}-{START_MONTH}",
            "end_date":     f"{year}-{END_MONTH}",
            "daily":        DAILY_VARS,
            "timezone":     timezone,
        }
        # open-meteo forecast endpoint supports past_days + forecast_days
        # For current year, use the historical-forecast API which is seamless
        hf_params = {
            "latitude":   lat,
            "longitude":  lon,
            "start_date": f"{year}-{START_MONTH}",
            "end_date":   f"{year}-{END_MONTH}",
            "daily":      DAILY_VARS,
            "timezone":   timezone,
        }
        return "https://historical-forecast-api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(hf_params)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Read locations
    with open(LOCATIONS_FILE, newline="", encoding="utf-8") as f:
        locations = list(csv.DictReader(f))

    print(f"Loaded {len(locations)} locations, fetching years: {YEARS}", flush=True)
    errors = []

    for loc in locations:
        loc_id   = loc["id"].strip()
        name     = loc["name"].strip()
        lat      = float(loc["lat"])
        lon      = float(loc["lon"])
        timezone = loc.get("timezone", "UTC").strip()

        for year in YEARS:
            out_file = os.path.join(OUTPUT_DIR, f"weather_{loc_id}_{year}.json")

            # Skip if file already exists and is fresh (optional: remove to always refresh)
            # if os.path.exists(out_file):
            #     print(f"  SKIP  {out_file} (already exists)", flush=True)
            #     continue

            url = build_url(lat, lon, year, timezone)
            print(f"  Fetching {name} ({loc_id}) / {year} ...", flush=True)
            try:
                data = fetch_url(url)
                with open(out_file, "w", encoding="utf-8") as fout:
                    json.dump(data, fout, separators=(",", ":"))
                print(f"    ✓ Saved {out_file}", flush=True)
            except RuntimeError as exc:
                print(f"    ✗ FAILED: {exc}", flush=True)
                errors.append(f"{loc_id}/{year}: {exc}")

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
