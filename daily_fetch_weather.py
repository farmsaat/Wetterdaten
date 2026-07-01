#!/usr/bin/env python3
"""
daily_fetch_weather.py
Reads locations.csv and downloads Open-Meteo daily weather data
(temperature_2m_max, temperature_2m_min, precipitation_sum)
for the current year from April 1st through yesterday.
Designed for daily GitHub Actions runs — always overwrites the JSON files.

Uses the Open-Meteo Professional batch API (up to 1000 locations per request)
on the dedicated customer endpoint with API key authentication.
Concurrent batch fetching is enabled since the Professional plan has no
per-minute or per-hour rate limits.

Output filename:  weather_<id>_<year>.json
"""

import csv
import json
import os
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import requests

# ── Configuration ─────────────────────────────────────────────────────────────
LOCATIONS_FILE = "locations.csv"
OUTPUT_DIR     = "."
DAILY_VARS     = "temperature_2m_max,temperature_2m_min,precipitation_sum"

# Professional plan: dedicated customer endpoint
API_BASE_URL   = "https://customer-historical-forecast-api.open-meteo.com/v1/forecast"
API_KEY        = os.environ.get("OPEN_METEO_API_KEY", "")

if not API_KEY:
    print("ERROR: OPEN_METEO_API_KEY environment variable is not set.", flush=True)
    sys.exit(1)

BATCH_SIZE     = 150   # max allowed by API — Professional plan has no rate limits
MAX_WORKERS    = 4      # concurrent batch requests
RETRY_WAIT     = 3      # base seconds between retries
MAX_RETRIES    = 4
HEADERS        = {"User-Agent": "daily-weather-fetch/2.0 (professional)"}

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
def fetch_batch(batch: list[dict], batch_idx: int = 0) -> list[dict]:
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
        "apikey":     API_KEY,
    }
    url = API_BASE_URL + "?" + urllib.parse.urlencode(params)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                timeout=(30, 180),  # generous timeouts for large 1000-loc batches
                headers=HEADERS,
            )
            resp.raise_for_status()
            data = resp.json()
            # API returns a list when multiple locations are requested,
            # or a single dict when only one location is requested
            return data if isinstance(data, list) else [data]
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            # Don't retry on client errors (except 429 which shouldn't happen on Pro)
            if status and 400 <= status < 500 and status != 429:
                print(f"  [batch {batch_idx}] Client error {status}: {exc}", flush=True)
                raise
            print(f"  [batch {batch_idx}, attempt {attempt}/{MAX_RETRIES}] HTTP {status}: {exc}", flush=True)
        except Exception as exc:
            print(f"  [batch {batch_idx}, attempt {attempt}/{MAX_RETRIES}] Error: {exc}", flush=True)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_WAIT * attempt)

    raise RuntimeError(f"Failed to fetch batch {batch_idx} ({len(batch)} locations)")


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
        f"Loaded {total} location(s) → {len(batches)} batch(es) of up to {BATCH_SIZE}.\n"
        f"Fetching {YEAR} data from {START_DATE} to {END_DATE}.\n"
        f"Endpoint: {API_BASE_URL} | Concurrency: {MAX_WORKERS}",
        flush=True,
    )

    errors = []

    # Fetch all batches concurrently — Professional plan has no rate limits
    def process_batch(batch_idx_and_batch):
        batch_idx, batch = batch_idx_and_batch
        results = fetch_batch(batch, batch_idx)
        return batch_idx, batch, results

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_batch, (idx, batch)): idx
            for idx, batch in enumerate(batches, 1)
        }

        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                _, batch, results = future.result()
                print(f"\n✓ Batch {batch_idx}/{len(batches)} ({len(batch)} locations) fetched.", flush=True)

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

            except RuntimeError as exc:
                print(f"\n✗ Batch {batch_idx}/{len(batches)} FAILED: {exc}", flush=True)
                # We need the batch from the enumeration — recover from the index
                batch = batches[batch_idx - 1]
                for loc in batch:
                    errors.append(f"{loc['id']}/{YEAR}: batch failed")

    if errors:
        print("\n── Errors ──────────────────────────────────────────", flush=True)
        for e in errors:
            print(f"  {e}", flush=True)
        sys.exit(1)
    else:
        print("\nAll downloads complete.", flush=True)


if __name__ == "__main__":
    main()
