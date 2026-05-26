## Adding or updating locations

All location coordinates live in **`locations.csv`** — one row per station.

```
id,name,lat,lon,country,timezone
101,Musterhausen,51.5000,9.8000,DE,Europe/Berlin
201,Amsterdam,52.3702,4.8952,NL,Europe/Amsterdam
```

| Column     | Description                                                      |
|------------|------------------------------------------------------------------|
| `id`       | Unique station ID — matches the filename pattern `weather_<id>_<year>.json` and the `SDO_ID` used in `sdo.csv` |
| `name`     | Human-readable station name                                      |
| `lat`      | Latitude (decimal degrees, WGS84)                                |
| `lon`      | Longitude (decimal degrees, WGS84)                               |
| `country`  | Two-letter country code (informational only)                     |
| `timezone` | TZ identifier, e.g. `Europe/Berlin`, `Europe/Warsaw`             |

### Steps to add 80 locations

1. Open `locations.csv` in Excel / LibreOffice / any text editor.
2. Append one row per location.  
   - Use the `id` value from your existing `sdo.csv` so the HTML picks up the right file.
   - Latitude/longitude in decimal degrees (no commas — use `.` as decimal separator).
3. Commit and push.
4. Trigger the workflow manually:  
   **Actions → Fetch Weather Data → Run workflow**  
   The script will download `weather_<id>_<year>.json` for every new row.

### Finding coordinates

- Google Maps: right-click any point → the decimal coordinates appear in the context menu.
- geojson.io: draw a point, copy `lat`/`lng` from the JSON panel.
- Your existing `sdo.csv` already has coordinates — you can copy them directly.
