#!/usr/bin/env python3
"""
Georgia Current — statewide power outage + weather poller
------------------------------------------------------------
Runs every 15 minutes via GitHub Actions.

DATA SOURCE (corrected): the U.S. Department of Energy / Oak Ridge National
Laboratory's ODIN (Outage Data Initiative Nationwide) real-time outage feed,
filtered to Georgia. This is a genuinely Georgia-specific, federally-run,
free, no-key-required source — see https://odin.ornl.gov/

IMPORTANT SHAPE OF THIS DATA (different from a typical incident feed):
Each row represents a UTILITY + COUNTY pairing and its currently-affected
meter count — not a single persistent "incident" with a stable ID. The same
physical storm can show up as a rising and falling meter count for a given
utility/county over many polls, rather than one long-lived incident record.
So instead of tracking "is this incident ID new," we track each utility+
county pairing as its own gauge: it becomes "active" when affected meters
goes from zero/absent to positive, and "resolved" when it drops back down.

What it does each run:
  1. Pulls every current Georgia row from the ODIN feed (paginated).
  2. For any utility+county pairing that just went from inactive to active,
     looks up weather conditions and NWS alerts at that county's location,
     anchored to the reported start time when available (falls back to
     detection time otherwise). Only done once per newly-active pairing —
     not every run — and all such lookups for a given run happen in
     parallel, clustering nearby/simultaneous ones to avoid redundant calls.
  3. Keeps a small "latest.json" (active + resolved in the last 48 hours)
     that powers the live map — kept small on purpose so the page loads fast.
  4. Archives every pairing's current snapshot permanently into
     data/YYYY-MM-DD.json, split by day. This is the full benchmark
     dataset — nothing is ever deleted from here, only from "latest.json".

Data sources (all free, no account/API key required):
  - Outages: DOE/ORNL ODIN real-time county outage feed
  - Weather: Open-Meteo (open-meteo.com)
  - Alerts:  National Weather Service (api.weather.gov)
"""

import json
import os
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

ODIN_URL = (
    "https://openenergyhub.ornl.gov/api/explore/v2.1/catalog/datasets/"
    "odin-real-time-outages-county/records"
)
PAGE_SIZE = 100          # ODS API's typical page size ceiling
MAX_PAGES = 30           # safety cap — 3,000 GA rows would be far beyond normal

REPO_ROOT = os.path.dirname(__file__)
LATEST_PATH = os.path.join(REPO_ROOT, "docs", "latest.json")
DATA_DIR = os.path.join(REPO_ROOT, "data")

NWS_HEADERS = {"User-Agent": "GeorgiaCurrentOutageTracker (personal project)"}

PRUNE_AFTER_HOURS = 48       # how long a resolved pairing stays in latest.json
TRAILING_WINDOW_HOURS = 3    # peak wind/precip window ending at onset
SATURATION_WINDOW_HOURS = 24 * 7  # 7-day trailing precip, soil-saturation proxy
WEATHER_WORKERS = 20         # unique location/hour weather lookups at once


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def to_datetime(value):
    """Parse a date that may arrive as an ISO string or epoch-millisecond number."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def parse_ert(raw):
    """estimatedrestorationtime arrives as a JSON-string-in-a-string, e.g.
    '{"ert": "2026-08-04T21:30:00Z"}' — pull the actual timestamp out."""
    if not raw:
        return None
    try:
        return json.loads(raw).get("ert")
    except (json.JSONDecodeError, AttributeError):
        return None


def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return default


def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


def fetch_georgia_rows():
    """Pull every current Georgia row from ODIN, paging through results."""
    all_rows = []
    offset = 0
    pages_fetched = 0

    while pages_fetched < MAX_PAGES:
        params = {
            "where": 'state="Georgia"',
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        resp = requests.get(ODIN_URL, params=params, timeout=30, headers=NWS_HEADERS)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("results", [])
        pages_fetched += 1
        all_rows.extend(rows)

        total_count = data.get("total_count", len(all_rows))
        if len(all_rows) >= total_count or not rows:
            break
        offset += PAGE_SIZE

    if pages_fetched >= MAX_PAGES:
        print(f"  (hit the {MAX_PAGES}-page safety cap — stopping; "
              f"this should not normally happen)")

    return all_rows


def weather_cache_key(lat, lon, onset_dt):
    """Round location to ~0.1 degree (~7 miles) and time to the nearest hour,
    so simultaneous nearby pairings share one weather lookup instead of each
    making a separate call."""
    return (round(lat, 1), round(lon, 1), onset_dt.replace(minute=0, second=0, microsecond=0).isoformat())


def fetch_weather_and_alerts(lat, lon, onset_dt):
    """
    Weather + active NWS alerts near a point, captured once per newly-active
    utility/county pairing. We pull a trailing window of hourly data ending
    at the reported (or detected) start time and report PEAK gust and TOTAL
    precipitation in that window, rather than a single "current" snapshot —
    a storm can pass through in under an hour, so "current" conditions at
    detection time can look deceptively calm. We also compute a 7-day
    trailing precipitation total as a rough soil-saturation proxy: the same
    wind gust is a very different outage risk on saturated vs. dry ground.
    """
    weather = {
        "tempF": None,
        "windGustMph": None, "windGustPeakWindowHrs": TRAILING_WINDOW_HOURS,
        "precipIn": None,
        "precip7dIn": None, "precip7dWindowHrs": SATURATION_WINDOW_HOURS,
        "alerts": [],
    }

    try:
        om = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat, "longitude": lon,
                "hourly": "temperature_2m,precipitation,wind_gusts_10m",
                "past_days": 7,
                "forecast_days": 1,
                "temperature_unit": "fahrenheit",
                "wind_speed_unit": "mph",
                "precipitation_unit": "inch",
                "timezone": "UTC",
            },
            timeout=15,
        ).json()
        hourly = om.get("hourly", {})
        times = hourly.get("time", [])
        gusts = hourly.get("wind_gusts_10m", [])
        precip = hourly.get("precipitation", [])
        temps = hourly.get("temperature_2m", [])

        if times and onset_dt is not None:
            onset_idx = min(
                range(len(times)),
                key=lambda i: abs(datetime.fromisoformat(times[i] + "+00:00") - onset_dt),
            )

            short_start = max(0, onset_idx - TRAILING_WINDOW_HOURS)
            window_gusts = [g for g in gusts[short_start:onset_idx + 1] if g is not None]
            window_precip = [p for p in precip[short_start:onset_idx + 1] if p is not None]
            weather["windGustMph"] = max(window_gusts) if window_gusts else None
            weather["precipIn"] = round(sum(window_precip), 3) if window_precip else None
            weather["tempF"] = temps[onset_idx] if onset_idx < len(temps) else None

            sat_start = max(0, onset_idx - SATURATION_WINDOW_HOURS)
            window_precip_7d = [p for p in precip[sat_start:onset_idx + 1] if p is not None]
            weather["precip7dIn"] = round(sum(window_precip_7d), 2) if window_precip_7d else None
    except Exception as e:
        print(f"  (weather lookup failed: {e})")

    try:
        nws = requests.get(
            "https://api.weather.gov/alerts/active",
            params={"point": f"{lat},{lon}"},
            headers=NWS_HEADERS,
            timeout=15,
        ).json()
        weather["alerts"] = [feat["properties"]["event"] for feat in nws.get("features", [])]
    except Exception as e:
        print(f"  (alerts lookup failed: {e})")
        # Reflects alerts active at DETECTION time, not necessarily true
        # onset — NWS has no simple free "historical alerts at a point in
        # time" endpoint, so treat this as a reasonable-but-imperfect proxy.

    return weather


def archive(rec):
    """Permanently store this pairing's current snapshot under its start date."""
    date = (rec.get("firstSeen") or now_iso())[:10]
    path = os.path.join(DATA_DIR, f"{date}.json")
    day = load_json(path, {})
    day[rec["id"]] = rec
    save_json(path, day)


def main():
    latest = load_json(LATEST_PATH, {})
    latest.pop("_lastChecked", None)  # strip marker before treating entries as records
    now = now_iso()

    try:
        rows = fetch_georgia_rows()
    except requests.RequestException as e:
        print(f"Fetch failed: {e}")
        return

    print(f"Fetched {len(rows)} Georgia row(s) from ODIN")

    seen_ids = set()
    needs_weather = {}  # key -> (lat, lon, onset_dt)

    for row in rows:
        utility_id = row.get("utility_id") or "unknown"
        county_fips = row.get("communitydescriptor") or "unknown"
        pairing_id = f"{utility_id}:{county_fips}"
        meters = row.get("metersaffected") or 0

        if meters <= 0:
            continue  # not currently active for this utility/county

        seen_ids.add(pairing_id)
        existing = latest.get(pairing_id)
        was_inactive = existing is None or existing.get("status") != "Active"

        centroid = row.get("centroid") or row.get("geo_point_2d") or {}
        lat, lon = centroid.get("lat"), centroid.get("lon")

        onset_raw = row.get("reportedstarttime")
        onset_dt = to_datetime(onset_raw)

        if was_inactive and lat is not None and lon is not None:
            needs_weather[pairing_id] = (lat, lon, onset_dt or datetime.now(timezone.utc))

        latest[pairing_id] = {
            "id": pairing_id,
            "utility": (row.get("name") or "").split(",")[0].strip() or "Unknown utility",
            "county": row.get("county") or "",
            "countyFips": county_fips,
            "cause": row.get("cause") or (existing or {}).get("cause") or "Unknown",
            "outageType": row.get("statuskind") or "",
            "customers": meters,
            "startDate": onset_raw or (existing or {}).get("startDate") or now,
            "estRestore": parse_ert(row.get("estimatedrestorationtime")),
            "lat": lat if lat is not None else (existing or {}).get("lat"),
            "lon": lon if lon is not None else (existing or {}).get("lon"),
            "firstSeen": (existing or {}).get("firstSeen") if not was_inactive else now,
            "lastSeen": now,
            "status": "Active",
            "resolvedAt": None,
            "weather": (existing or {}).get("weather") if not was_inactive else None,
        }
        if not latest[pairing_id]["firstSeen"]:
            latest[pairing_id]["firstSeen"] = now

    if needs_weather:
        key_to_ids = {}
        key_to_args = {}
        for pairing_id, (lat, lon, onset_dt) in needs_weather.items():
            key = weather_cache_key(lat, lon, onset_dt)
            key_to_ids.setdefault(key, []).append(pairing_id)
            key_to_args.setdefault(key, (lat, lon, onset_dt))

        print(f"{len(needs_weather)} newly-active pairing(s) → {len(key_to_args)} unique "
              f"location/hour lookups needed, {WEATHER_WORKERS} at a time...")

        with ThreadPoolExecutor(max_workers=WEATHER_WORKERS) as pool:
            future_to_key = {
                pool.submit(fetch_weather_and_alerts, lat, lon, onset_dt): key
                for key, (lat, lon, onset_dt) in key_to_args.items()
            }
            done_count = 0
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    weather = future.result()
                except Exception as e:
                    print(f"  (weather lookup failed for cluster {key}: {e})")
                    weather = None
                for pairing_id in key_to_ids[key]:
                    latest[pairing_id]["weather"] = weather
                done_count += 1
                if done_count % 25 == 0:
                    print(f"  ...{done_count}/{len(key_to_args)} lookups done")

    for pairing_id, rec in latest.items():
        if rec["status"] == "Active" and pairing_id not in seen_ids:
            rec["status"] = "Restored"
            rec["resolvedAt"] = now
            print(f"[RESOLVED] {pairing_id}")

    for rec in latest.values():
        archive(rec)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=PRUNE_AFTER_HOURS)
    to_remove = [
        rid for rid, rec in latest.items()
        if rec["status"] == "Restored" and rec["resolvedAt"]
        and datetime.fromisoformat(rec["resolvedAt"]) < cutoff
    ]
    for rid in to_remove:
        del latest[rid]

    latest["_lastChecked"] = now
    save_json(LATEST_PATH, latest)

    active_count = sum(1 for r in latest.values() if isinstance(r, dict) and r.get("status") == "Active")
    print(f"{now} — {active_count} active pairing(s) in Georgia, {len(needs_weather)} newly active this run")


if __name__ == "__main__":
    main()
