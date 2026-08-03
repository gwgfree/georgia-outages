#!/usr/bin/env python3
"""
Georgia Current — Georgia Power outage + weather poller (KUBRA version)
--------------------------------------------------------------------------
Runs every 15 minutes via GitHub Actions.

DATA SOURCE (corrected, second time): Georgia Power's own outage map,
built on KUBRA's "Storm Center" product — the same backend and technique
already proven working for Huntcliff Current. Two earlier sources were
tried and dropped because they turned out to be wrong or empty for
Georgia — see project history. This one is real, live, individual-outage
data confirmed against Georgia Power's actual public map.

HONEST SCOPE: this covers Georgia Power's service territory specifically
(the large majority of the state's population, but not electric
membership cooperatives or municipal utilities, which aren't part of this
map). "Georgia Current" therefore means "Georgia Power outages," not
literally every outage in Georgia — partial-but-real, which is the
tradeoff we chose deliberately.

What it does each run:
  1. Fetches Georgia Power's actual service-area boundary from KUBRA (not
     a guessed bounding box), then walks the map's tiles across that whole
     area, drilling into any clustered groupings until it finds individual
     outages — the same technique already proven for Huntcliff, just
     covering more ground.
  2. For any outage seen for the first time, looks up (once, not every
     run): weather conditions and NWS alerts at that exact location
     anchored to its actual start time, and its county via the FCC's free
     public geocoding API (KUBRA gives coordinates, not county names).
     Weather lookups for simultaneous nearby outages are clustered
     together to avoid redundant calls during a big storm.
  3. Keeps a small "latest.json" (active + resolved in the last 48 hours)
     that powers the live map — kept small on purpose so the page loads
     fast.
  4. Archives every outage's current snapshot permanently into
     data/YYYY-MM-DD.json, split by day. This is the full benchmark
     dataset — nothing is ever deleted from here, only from "latest.json".

Data sources (all free, no account/API key required):
  - Outages: Georgia Power's KUBRA Storm Center backend
  - Weather: Open-Meteo (open-meteo.com)
  - Alerts:  National Weather Service (api.weather.gov)
  - County:  FCC Census Area API (geo.fcc.gov)
"""

import json
import os
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import mercantile
import polyline
import requests

INSTANCE_ID = "7b38c047-7950-444b-a25c-9b3e5ab986eb"
VIEW_ID = "67b44af5-3847-4ca3-9f4e-9190aac343d6"
BASE_URL = "https://kubra.io/"

MIN_ZOOM = 7      # starting zoom for the whole service territory
MAX_ZOOM = 14     # KUBRA's own ceiling — it groups anything closer than this
MAX_TILE_REQUESTS = 6000  # safety cap for a full-state scan during a big event

REPO_ROOT = os.path.dirname(__file__)
LATEST_PATH = os.path.join(REPO_ROOT, "docs", "latest.json")
DATA_DIR = os.path.join(REPO_ROOT, "data")

HEADERS = {"User-Agent": "GeorgiaCurrentOutageTracker (personal project)"}

PRUNE_AFTER_HOURS = 48
TRAILING_WINDOW_HOURS = 3
SATURATION_WINDOW_HOURS = 24 * 7
WEATHER_WORKERS = 20


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return default


def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


def _get(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp


def get_deployment_info():
    state_url = (
        f"{BASE_URL}stormcenter/api/v1/stormcenters/{INSTANCE_ID}/"
        f"views/{VIEW_ID}/currentState?preview=false"
    )
    state = _get(state_url).json()
    regions_key = list(state["datastatic"])[0]
    return {
        "data_path": state["data"]["interval_generation_data"],
        "cluster_data_path": state["data"]["cluster_interval_generation_data"],
        "deployment_id": state["stormcenterDeploymentId"],
        "regions": state["datastatic"][regions_key],
        "regions_key": regions_key,
    }


def get_cluster_layer_name(deployment_id):
    config_url = (
        f"{BASE_URL}stormcenter/api/v1/stormcenters/{INSTANCE_ID}/"
        f"views/{VIEW_ID}/configuration/{deployment_id}?preview=false"
    )
    config = _get(config_url).json()
    interval_data = config["config"]["layers"]["data"]["interval_generation_data"]
    cluster_layers = [l for l in interval_data if l["type"].startswith("CLUSTER_LAYER")]
    if not cluster_layers:
        raise RuntimeError("No cluster layer found in KUBRA configuration")
    return cluster_layers[0]["id"]


def get_service_area_bbox(regions, regions_key):
    """Georgia Power's actual service territory boundary, not a guess."""
    url = f"{BASE_URL}{regions}/{regions_key}/serviceareas.json"
    data = _get(url).json()
    areas = data["file_data"][0]["geom"]["a"]
    points = []
    for geom in areas:
        points += polyline.decode(geom)
    lats, lons = zip(*points)
    return (min(lons), min(lats), max(lons), max(lats))  # west, south, east, north


def quadkey_tile_url(cluster_data_path, layer_name, quadkey):
    data_path = cluster_data_path.format(qkh=quadkey[-3:][::-1])
    return f"{BASE_URL}{data_path}/public/{layer_name}/{quadkey}.json"


def fetch_all_outages(cluster_data_path, layer_name, bbox):
    outages = {}
    already_seen = set()
    start_tiles = list(mercantile.tiles(*bbox, zooms=[MIN_ZOOM]))
    quadkeys = [mercantile.quadkey(t) for t in start_tiles]

    print(f"Starting from {len(quadkeys)} tile(s) at zoom {MIN_ZOOM}")
    _walk_tiles(quadkeys, already_seen, cluster_data_path, layer_name, outages, zoom=MIN_ZOOM)

    if len(already_seen) >= MAX_TILE_REQUESTS:
        print(f"  (hit the {MAX_TILE_REQUESTS}-tile safety cap — stopping)")

    print(f"Made {len(already_seen)} tile request(s), found {len(outages)} outage(s)")
    return outages


def _walk_tiles(quadkeys, already_seen, cluster_data_path, layer_name, outages, zoom):
    for qk in quadkeys:
        if len(already_seen) >= MAX_TILE_REQUESTS:
            return

        url = quadkey_tile_url(cluster_data_path, layer_name, qk)
        if url in already_seen:
            continue
        already_seen.add(url)

        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
        except requests.RequestException:
            continue
        if not resp.ok:
            continue

        for item in resp.json().get("file_data", []):
            desc = item["desc"]
            point = polyline.decode(item["geom"]["p"][0])[0]
            lat, lon = point[0], point[1]

            if desc.get("cluster"):
                next_zoom = zoom + 1
                if next_zoom > MAX_ZOOM:
                    continue
                child_tile = mercantile.tile(lng=lon, lat=lat, zoom=next_zoom)
                child_qk = mercantile.quadkey(child_tile)
                _walk_tiles([child_qk], already_seen, cluster_data_path, layer_name,
                            outages, next_zoom)
            else:
                outage_id = desc.get("inc_id") or f"{item['geom']['p'][0]}-{desc.get('start_time')}"
                outages[outage_id] = {
                    "id": outage_id,
                    "cause": (desc.get("cause") or {}).get("EN-US") if desc.get("cause") else None,
                    "customers": desc.get("cust_a", {}).get("val") if desc.get("cust_a") else desc.get("n_out"),
                    "startTime": desc.get("start_time"),
                    "etr": desc.get("etr"),
                    "crewStatus": desc.get("crew_status"),
                    "lat": lat,
                    "lon": lon,
                }
                neighbor_qks = _neighboring_quadkeys(qk)
                _walk_tiles(neighbor_qks, already_seen, cluster_data_path, layer_name,
                            outages, zoom)


def _neighboring_quadkeys(quadkey):
    tile = mercantile.quadkey_to_tile(quadkey)
    offsets = [(0, -1), (1, 0), (0, 1), (-1, 0), (1, -1), (1, 1), (-1, -1), (-1, 1)]
    return [
        mercantile.quadkey(mercantile.Tile(x=tile.x + dx, y=tile.y + dy, z=tile.z))
        for dx, dy in offsets
    ]


def enrichment_cache_key(lat, lon, onset_dt):
    return (round(lat, 1), round(lon, 1), onset_dt.replace(minute=0, second=0, microsecond=0).isoformat())


def fetch_county(lat, lon):
    """Free, no-key, federal reverse geocode — KUBRA gives coordinates, not county names."""
    try:
        resp = requests.get(
            "https://geo.fcc.gov/api/census/area",
            params={"lat": lat, "lon": lon, "format": "json"},
            headers=HEADERS, timeout=15,
        )
        results = resp.json().get("results", [])
        if results:
            return results[0].get("county_name") or results[0].get("county_fips")
    except Exception as e:
        print(f"  (county lookup failed: {e})")
    return None


def fetch_enrichment(lat, lon, onset_dt):
    """Weather, NWS alerts, and county — captured once per newly-seen outage."""
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
                "past_days": 7, "forecast_days": 1,
                "temperature_unit": "fahrenheit", "wind_speed_unit": "mph",
                "precipitation_unit": "inch", "timezone": "UTC",
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
            params={"point": f"{lat},{lon}"}, headers=HEADERS, timeout=15,
        ).json()
        weather["alerts"] = [feat["properties"]["event"] for feat in nws.get("features", [])]
    except Exception as e:
        print(f"  (alerts lookup failed: {e})")

    county = fetch_county(lat, lon)
    return {"weather": weather, "county": county}


def archive(rec):
    date = (rec.get("firstSeen") or now_iso())[:10]
    path = os.path.join(DATA_DIR, f"{date}.json")
    day = load_json(path, {})
    day[rec["id"]] = rec
    save_json(path, day)


def main():
    latest = load_json(LATEST_PATH, {})
    latest.pop("_lastChecked", None)
    now = now_iso()

    try:
        info = get_deployment_info()
        layer_name = get_cluster_layer_name(info["deployment_id"])
        bbox = get_service_area_bbox(info["regions"], info["regions_key"])
        outages = fetch_all_outages(info["cluster_data_path"], layer_name, bbox)
    except requests.RequestException as e:
        print(f"Fetch failed: {e}")
        return
    except (KeyError, RuntimeError) as e:
        print(f"Unexpected response shape from KUBRA — Georgia Power may have "
              f"changed something on their end: {e}")
        return

    seen_ids = set()
    needs_enrichment = {}

    for outage_id, o in outages.items():
        seen_ids.add(outage_id)
        existing = latest.get(outage_id)
        is_new = existing is None

        if is_new:
            from datetime import datetime as dt
            onset_dt = None
            if o["startTime"]:
                try:
                    onset_dt = dt.fromisoformat(o["startTime"].replace("Z", "+00:00"))
                except ValueError:
                    onset_dt = None
            needs_enrichment[outage_id] = (o["lat"], o["lon"], onset_dt or dt.now(timezone.utc))

        latest[outage_id] = {
            "id": outage_id,
            "utility": "Georgia Power",
            "cause": o["cause"] or (existing or {}).get("cause") or "Unknown",
            "outageType": o["crewStatus"] or "",
            "customers": o["customers"],
            "startDate": o["startTime"] or (existing or {}).get("startDate") or now,
            "estRestore": o["etr"],
            "lat": o["lat"],
            "lon": o["lon"],
            "county": (existing or {}).get("county"),
            "firstSeen": (existing or {}).get("firstSeen") or now,
            "lastSeen": now,
            "status": "Active",
            "resolvedAt": None,
            "weather": (existing or {}).get("weather"),
        }

    if needs_enrichment:
        key_to_ids = {}
        key_to_args = {}
        for outage_id, (lat, lon, onset_dt) in needs_enrichment.items():
            key = enrichment_cache_key(lat, lon, onset_dt)
            key_to_ids.setdefault(key, []).append(outage_id)
            key_to_args.setdefault(key, (lat, lon, onset_dt))

        print(f"{len(needs_enrichment)} new outage(s) → {len(key_to_args)} unique "
              f"location/hour lookups, {WEATHER_WORKERS} at a time...")

        with ThreadPoolExecutor(max_workers=WEATHER_WORKERS) as pool:
            future_to_key = {
                pool.submit(fetch_enrichment, lat, lon, onset_dt): key
                for key, (lat, lon, onset_dt) in key_to_args.items()
            }
            done = 0
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    result = future.result()
                except Exception as e:
                    print(f"  (enrichment failed for {key}: {e})")
                    result = {"weather": None, "county": None}
                for outage_id in key_to_ids[key]:
                    latest[outage_id]["weather"] = result["weather"]
                    latest[outage_id]["county"] = result["county"]
                done += 1
                if done % 25 == 0:
                    print(f"  ...{done}/{len(key_to_args)} done")

    for outage_id, rec in latest.items():
        if rec["status"] == "Active" and outage_id not in seen_ids:
            rec["status"] = "Restored"
            rec["resolvedAt"] = now
            print(f"[RESOLVED] {outage_id}")

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
    print(f"{now} — {active_count} active outage(s), {len(needs_enrichment)} newly seen this run")


if __name__ == "__main__":
    main()
