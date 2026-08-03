#!/usr/bin/env python3
"""
Georgia Current — statewide power outage + weather poller
------------------------------------------------------------
Runs every 15 minutes via GitHub Actions.

What it does each run:
  1. Pulls ALL current outage incidents statewide from Georgia's public
     outage feed (paginated — a big storm can produce thousands of records).
  2. For any outage it hasn't seen before, looks up the weather conditions
     and any active NWS alerts AT THAT LOCATION, at the moment it appeared.
     (We only do this once per outage, not every run, to keep things fast
     and stay well within free API limits.)
  3. Keeps a small "latest.json" (active outages + anything resolved in the
     last 48 hours) that powers the live map — kept small on purpose so the
     page loads fast.
  4. Archives every outage permanently into data/YYYY-MM-DD.json, split by
     day. This is the full benchmark dataset — nothing is ever deleted from
     here, only from the small "latest" file.

Data sources (all free, no account/API key required):
  - Outages: Georgia GEMA / PowerOutage.US ArcGIS feed
  - Weather: Open-Meteo (open-meteo.com)
  - Alerts:  National Weather Service (api.weather.gov)
"""

import json
import os
from datetime import datetime, timezone, timedelta
import requests

FEED_URL = (
    "https://services.arcgis.com/BLN4oKB0N1YSgvY8/ArcGIS/rest/services/"
    "Power_Outages_(View)/FeatureServer/0/query"
)
PAGE_SIZE = 1000

REPO_ROOT = os.path.dirname(__file__)
LATEST_PATH = os.path.join(REPO_ROOT, "docs", "latest.json")
DATA_DIR = os.path.join(REPO_ROOT, "data")

# Identify ourselves to the National Weather Service, as their API asks.
# (Not required to be a real email, just something identifiable.)
NWS_HEADERS = {"User-Agent": "GeorgiaCurrentOutageTracker (personal project)"}

PRUNE_AFTER_HOURS = 48  # how long a resolved outage stays in latest.json


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


def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return default


def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


MAX_PAGES = 30  # safety cap — 30,000 records is far beyond any realistic statewide count


def fetch_all_outages():
    """Pull every outage incident statewide, paging through results."""
    all_features = []
    seen_object_ids = set()
    offset = 0
    pages_fetched = 0

    while pages_fetched < MAX_PAGES:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "f": "geojson",
        }
        resp = requests.get(FEED_URL, params=params, timeout=30)
        resp.raise_for_status()
        feats = resp.json().get("features", [])
        pages_fetched += 1

        if not feats:
            break

        # Safety check: if the server ignores our offset and just returns the
        # same records again (some ArcGIS services don't support pagination
        # on every layer), stop instead of looping forever.
        page_ids = {f.get("properties", {}).get("OBJECTID") for f in feats}
        if page_ids and page_ids.issubset(seen_object_ids):
            print("  (pagination returned no new records — feed likely doesn't "
                  "support paging past this point; stopping here)")
            break
        seen_object_ids.update(page_ids)

        all_features.extend(feats)
        if len(feats) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    if pages_fetched >= MAX_PAGES:
        print(f"  (hit the {MAX_PAGES}-page safety cap — stopping; "
              f"this should not normally happen)")

    return all_features


TRAILING_WINDOW_HOURS = 3   # look back this far for peak gust/precip before onset
SATURATION_WINDOW_HOURS = 24 * 7  # look back this far for a soil-saturation proxy


def fetch_weather_and_alerts(lat, lon, onset_dt):
    """
    Weather + active NWS alerts near a single point, captured once per outage.

    Important: we do NOT just grab "current" weather at the moment we happen
    to detect the outage. Detection lags the real event (feed refresh delay,
    reporting delay), and a storm cell can pass through in well under an
    hour — so "current" conditions can look calm right next to an outage it
    caused. Instead we pull a trailing window of hourly data ending at
    detection time and report the PEAK gust and TOTAL precipitation in that
    window, which is a much more honest predictor of what actually happened.

    We also compute a 7-day trailing precipitation total as a rough soil
    saturation proxy: a wind gust hitting dry ground and the same gust
    hitting saturated ground (which lets trees uproot far more easily) are
    very different outage risks, even though the gust reading is identical.
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
                "past_days": 7,       # need a full week back for the saturation proxy
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
            # find the hourly index closest to onset time
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
        weather["alerts"] = [
            feat["properties"]["event"] for feat in nws.get("features", [])
        ]
    except Exception as e:
        print(f"  (alerts lookup failed: {e})")
        # Note: this only reflects alerts active AT DETECTION TIME, not
        # necessarily at true onset. NWS doesn't offer a simple free
        # "historical alerts at a point in time" endpoint, so this field
        # is a reasonable-but-imperfect proxy — worth keeping in mind
        # when using it for analysis.

    return weather


def archive(rec):
    """Permanently store this outage's current snapshot under its start date."""
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
        features = fetch_all_outages()
    except requests.RequestException as e:
        print(f"Fetch failed: {e}")
        return

    seen_ids = set()
    new_count = 0

    for feat in features:
        p = feat.get("properties", {})
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates", [None, None])
        lon, lat = (coords[0], coords[1]) if len(coords) >= 2 else (None, None)
        incident_id = str(p.get("IncidentId") or p.get("OBJECTID"))
        seen_ids.add(incident_id)

        existing = latest.get(incident_id)
        is_new = existing is None

        weather = existing.get("weather") if existing else None
        if is_new and lat is not None and lon is not None:
            onset_dt = to_datetime(p.get("StartDate")) or datetime.now(timezone.utc)
            weather = fetch_weather_and_alerts(lat, lon, onset_dt)
            new_count += 1
            print(f"[NEW OUTAGE] {incident_id} — {p.get('County')} County, "
                  f"{p.get('Cause')}, peak gust {weather.get('windGustMph')} mph "
                  f"in trailing {TRAILING_WINDOW_HOURS}h")

        latest[incident_id] = {
            "id": incident_id,
            "utility": p.get("UtilityCompany") or (existing or {}).get("utility") or "Georgia Power",
            "county": p.get("County") or (existing or {}).get("county") or "",
            "cause": p.get("Cause") or (existing or {}).get("cause") or "Unknown",
            "outageType": p.get("OutageType") or (existing or {}).get("outageType") or "",
            "customers": p.get("ImpactedCustomers", (existing or {}).get("customers")),
            "startDate": p.get("StartDate") or (existing or {}).get("startDate") or now,
            "estRestore": p.get("EstimatedRestoreDate", (existing or {}).get("estRestore")),
            "lat": lat if lat is not None else (existing or {}).get("lat"),
            "lon": lon if lon is not None else (existing or {}).get("lon"),
            "firstSeen": (existing or {}).get("firstSeen") or now,
            "lastSeen": now,
            "status": "Active",
            "resolvedAt": None,
            "weather": weather,
        }

    # Anything active last run but missing this run has been restored
    for incident_id, rec in latest.items():
        if rec["status"] == "Active" and incident_id not in seen_ids:
            rec["status"] = "Restored"
            rec["resolvedAt"] = now
            print(f"[RESTORED] {incident_id}")

    # Archive every record's current snapshot (permanent benchmark dataset)
    for rec in latest.values():
        archive(rec)

    # Prune old resolved records out of latest.json (already safely archived above)
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
    print(f"{now} — {active_count} active outage(s) statewide, {new_count} new this run")


if __name__ == "__main__":
    main()
