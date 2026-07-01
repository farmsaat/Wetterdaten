#!/usr/bin/env python3
"""
fetch_weather.py
Reads locations.csv and downloads Open-Meteo daily weather data
(temperature_2m_max, temperature_2m_min, precipitation_sum)
for every year specified in YEARS, saving one JSON file per location+year.

Uses the Open-Meteo Professional batch API (up to 1000 locations per request)
on dedicated customer endpoints with API key authentication.
Concurrent batch fetching is enabled since the Professional plan has no
per-minute or per-hour rate limits.

Output filenames:  weather_<id>_<year>.json
"""

import csv
import json
import os
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import requests

# ── Configuration ─────────────────────────────────────────────────────────────
LOCATIONS_FILE = "locations.csv"
OUTPUT_DIR     = "."
YEARS          = [2018, 2024, 2025]   # add/remove years here
START_MONTH    = "04-01"
END_MONTH      = "10-31"
DAILY_VARS     = "temperature_2m_max,temperature_2m_min,precipitation_sum"

# Professional plan: dedicated customer endpoints
ARCHIVE_URL    = "https://customer-archive-api.open-meteo.com/v1/archive"
FORECAST_URL   = "https://customer-historical-forecast-api.open-meteo.com/v1/forecast"
API_KEY        = os.environ.get("OPEN_METEO_API_KEY", "")

if not API_KEY:
    print("ERROR: OPEN_METEO_API_KEY environment variable is not set.", flush=True)
    sys.exit(1)

CURRENT_YEAR   = date.today().year
BATCH_SIZE     = 1000   # max allowed by API — Professional plan has no rate limits
MAX_WORKERS    = 4      # concurrent batch requests
RETRY_WAIT     = 3      # base seconds between retries
MAX_RETRIES    = 4
HEADERS        = {"User-Agent": "fetch-weather/2.0 (professional)"}

# ── Helpers ───────────────────────────────────────────────────────────────────
def fetch_batch(batch: list[dict], year: int, batch_label: str = "") -> list[dict]:
    """
    Fetch weather data for a batch of locations in a single API call.
    Returns a list of result dicts in the same order as the input batch.
    """
    base_url = ARCHIVE_URL if year < CURRENT_YEAR else FORECAST_URL
    params = {
        "latitude":   ",".join(str(loc["lat"])  for loc in batch),
        "longitude":  ",".join(str(loc["lon"])  for loc in batch),
        "timezone":   ",".join(loc["timezone"]  for loc in batch),
        "start_date": f"{year}-{START_MONTH}",
        "end_date":   f"{year}-{END_MONTH}",
        "daily":      DAILY_VARS,
        "apikey":     API_KEY,
    }
    url = base_url + "?" + urllib.parse.urlencode(params)

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
                print(f"  [{batch_label}] Client error {status}: {exc}", flush=True)
                raise
            print(f"  [{batch_label}, attempt {attempt}/{MAX_RETRIES}] HTTP {status}: {exc}", flush=True)
        except Exception as exc:
            print(f"  [{batch_label}, attempt {attempt}/{MAX_RETRIES}] Error: {exc}", flush=True)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_WAIT * attempt)

    raise RuntimeError(f"Failed to fetch {batch_label} ({len(batch)} locations)")


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
        f"Fetching years: {YEARS}\n"
        f"Endpoints: {ARCHIVE_URL} / {FORECAST_URL} | Concurrency: {MAX_WORKERS}",
        flush=True,
    )

    errors = []

    # Build a list of all (year, batch_idx, pending_locations) work items
    work_items = []
    for year in YEARS:
        for batch_idx, batch in enumerate(batches, 1):
            # Skip locations where the output file already exists
            pending = [loc for loc in batch
                       if not os.path.exists(
                           os.path.join(OUTPUT_DIR, f"weather_{loc['id']}_{year}.json")
                       )]
            if pending:
                work_items.append((year, batch_idx, pending))
            else:
                print(f"  [{year}] Batch {batch_idx}/{len(batches)} — all files exist, skipping.", flush=True)

    print(f"\n{len(work_items)} batch request(s) to make.\n", flush=True)

    if not work_items:
        print("Nothing to fetch — all files already exist.", flush=True)
        return

    # Fetch all batches concurrently — Professional plan has no rate limits
    def process_work_item(item):
        year, batch_idx, pending = item
        label = f"{year}/batch-{batch_idx}"
        results = fetch_batch(pending, year, label)
        return year, batch_idx, pending, results

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_work_item, item): item
            for item in work_items
        }

        for future in as_completed(futures):
            item = futures[future]
            year, batch_idx, pending = item

            try:
                _, _, _, results = future.result()
                print(
                    f"✓ [{year}] Batch {batch_idx}/{len(batches)} "
                    f"({len(pending)} locations) fetched.",
                    flush=True,
                )

                # Save one JSON file per location
                for loc, data in zip(pending, results):
                    loc_id   = loc["id"]
                    out_file = os.path.join(OUTPUT_DIR, f"weather_{loc_id}_{year}.json")
                    try:
                        with open(out_file, "w", encoding="utf-8") as fout:
                            json.dump(data, fout, separators=(",", ":"))
                        print(f"    ✓ {loc['name']} ({loc_id}) → {out_file}", flush=True)
                    except OSError as exc:
                        print(f"    ✗ {loc['name']} ({loc_id}) write error: {exc}", flush=True)
                        errors.append(f"{loc_id}/{year}: write error: {exc}")

            except RuntimeError as exc:
                print(f"✗ [{year}] Batch {batch_idx}/{len(batches)} FAILED: {exc}", flush=True)
                for loc in pending:
                    errors.append(f"{loc['id']}/{year}: batch failed")

    if errors:
        print("\n── Errors ──────────────────────────────────────────", flush=True)
        for e in errors:
            print(f"  {e}", flush=True)
        sys.exit(3)
    else:
        print("\nAll downloads complete.", flush=True)


if __name__ == "__main__":
    main()
