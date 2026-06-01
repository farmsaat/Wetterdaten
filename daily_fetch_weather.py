#!/usr/bin/env python3
"""
daily_fetch_weather.py
Reads locations.csv and downloads Open-Meteo daily weather data
(temperature_2m_max, temperature_2m_min, precipitation_sum)
for the current year from April 1st through yesterday.
Designed for daily GitHub Actions runs — always overwrites the JSON files.

Uses the Open-Meteo batch API (up to 1000 locations per request) to avoid
per-request overhead and timeout issues on the free tier.

Output filename:  weather_<id>_<year>.json
"""

import csv
import json
import os
import sys
import time
import socket
import urllib.parse
from datetime import date, timedelta

import requests

# ── Global socket timeout (covers TLS handshake) ──────────────────────────────
socket.setdefaulttimeout(30)

# ── Configuration ─────────────────────────────────────────────────────────────
LOCATIONS_FILE = "locations.csv"
OUTPUT_DIR     = "."
DAILY_VARS     = "temperature_2m_max,temperature_2m_min,precipitation_sum"
FORECAST_URL   = "https://historical-forecast-api.open-meteo.com/v1/forecast"
BATCH_SIZE     = 100   # locations per API call (max 1000; keep lower for free tier)
RETRY_WAIT     = 5     # base seconds between retries (multiplied per attempt)
MAX_RETRIES    = 3
HEADERS        = {"User-Agent": "daily-weather-fetch/1.0"}

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
def fetch_batch(batch: list[dict]) -> list[dict]:
    """
    Fetch weather data for a batch of locations in a single API call.
    Returns a list of result dicts in the same order as the input batch.
    """
    params = {
        "latitude":   ",".join(str(loc["lat"])      for loc in batch),
        "longitude":  ",".join(str(loc["lon"])      for loc in batch),
        "timezone":   ",".join(loc["timezone"]      for loc in batch),
        "start_date": START_DATE.isoformat(),
        "end_date":   END_DATE.isoformat(),
        "daily":      DAILY_VARS,
    }
    url = FORECAST_URL + "?" + urllib.parse.urlencode(params)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                timeout=(120, 10),  # 10s connect, 120s read for large batches
                headers=HEADERS,
            )
            resp.raise_for_status()
            data = resp.json()
            # API returns a list when multiple locations are requested,
            # or a single dict when only one location is requested
            return data if isinstance(data, list) else [data]
        except Exception as exc:
            print(f"  [attempt {attempt}/{MAX_RETRIES}] Batch error: {exc}", flush=True)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT * attempt)  # backoff: 5s, 10s

    raise RuntimeError(f"Failed to fetch batch of {len(batch)} locations")


def chunked(lst: list, size: int):
    """Split a list into chunks of at most `size` items."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(LOCATIONS_FILE, newline="", encoding="utf-8") as f:
        locations = list(csv.DictReader(f))

    # Normalise fields
    for loc in locations:
        loc["lat"]      = float(loc["lat"])
        loc["lon"]      = float(loc["lon"])
        loc["timezone"] = loc.get("timezone", "UTC").strip()
        loc["id"]       = loc["id"].strip()
        loc["name"]     = loc["name"].strip()

    total   = len(locations)
    batches = list(chunked(locations, BATCH_SIZE))
    print(
        f"Loaded {total} location(s) → {len(batches)} batch(es) of up to {BATCH_SIZE}. "
        f"Fetching {YEAR} data from {START_DATE} to {END_DATE}.",
        flush=True,
    )

    errors = []

    for batch_idx, batch in enumerate(batches, 1):
        print(f"\nBatch {batch_idx}/{len(batches)} ({len(batch)} locations) …", flush=True)
        try:
            results = fetch_batch(batch)
        except RuntimeError as exc:
            print(f"  ✗ Entire batch FAILED: {exc}", flush=True)
            for loc in batch:
                errors.append(f"{loc['id']}/{YEAR}: batch failed")
            time.sleep(RETRY_WAIT)
            continue

        # Save one JSON file per location
        for loc, data in zip(batch, results):
            loc_id   = loc["id"]
            out_file = os.path.join(OUTPUT_DIR, f"weather_{loc_id}_{YEAR}.json")
            try:
                with open(out_file, "w", encoding="utf-8") as fout:
                    json.dump(data, fout, separators=(",", ":"))
                print(f"  ✓ {loc['name']} ({loc_id}) → {out_file}", flush=True)
            except OSError as exc:
                print(f"  ✗ {loc['name']} ({loc_id}) write error: {exc}", flush=True)
                errors.append(f"{loc_id}/{YEAR}: write error: {exc}")

        time.sleep(1)  # brief pause between batches — polite to the free API

    if errors:
        print("\n── Errors ──────────────────────────────────────────", flush=True)
        for e in errors:
            print(f"  {e}", flush=True)
        sys.exit(1)
    else:
        print("\nAll downloads complete.", flush=True)


if __name__ == "__main__":
    main()
