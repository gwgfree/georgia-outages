#!/usr/bin/env python3
"""
Huntcliff Neighborhood Power Outage Poller (KUBRA version)
------------------------------------------------------------
Meant to be run by the included GitHub Actions workflow every 15 minutes.
Reads/writes docs/history.json so it can be committed back to the repo and
served by GitHub Pages as a live status page.

DATA SOURCE: Georgia Power's own outage map, which is built on KUBRA's
"Storm Center" product (the same backend many US utilities use). This gives
genuine individual-outage locations — not county-level aggregates — which
is what makes neighborhood-level precision possible at all.

HOW THIS WORKS (KUBRA's map is tile-based, not a simple query API):
  1. Hit a "currentState" endpoint to learn where this deployment's data
     currently lives (these paths rotate over time as Georgia Power
     publishes updates, so we look them up fresh every run rather than
     hardcoding them).
  2. Hit a "configuration" endpoint to find which map layer holds outage
     clusters.
  3. Starting from a single map tile that covers all of Huntcliff, fetch
     that tile's outage data. If the map has grouped multiple outages
     together into one "cluster" (because they're too close together to
     show individually at that zoom level), we zoom in on that cluster and
     ask again — repeating until we reach individual outages or hit the
     map's own maximum useful zoom level (14, chosen by Georgia Power /
     KUBRA, not by us).
  4. Every outage found gets checked against Huntcliff's actual boundary —
     the starting tile is intentionally a bit larger than the neighborhood,
     so this final check keeps out anything just outside it.

Credit: this technique was originally documented by Code for Kentuckiana
(https://github.com/openkentuckiana/kubra-scraper) for the same underlying
KUBRA product used by many utilities nationwide, including Georgia Power.
"""

import json
import os
from datetime import datetime, timezone

import mercantile
import polyline
import requests

# Georgia Power's KUBRA Storm Center identifiers (confirmed via browser
# devtools against https://outagemap.georgiapower.com/ — see project notes).
INSTANCE_ID = "7b38c047-7950-444b-a25c-9b3e5ab986eb"
VIEW_ID = "67b44af5-3847-4ca3-9f4e-9190aac343d6"
BASE_URL = "https://kubra.io/"

# Huntcliff's bounding box (same one used for the map-review conversation).
HUNTCLIFF_BBOX = (-84.375, 33.9746, -84.351, 34.0001)  # west, south, east, north

START_ZOOM = 12   # one tile fully covers Huntcliff with margin at this zoom
MAX_ZOOM = 14      # KUBRA's own ceiling — it groups anything closer than this

HISTORY_PATH = os.path.join(os.path.dirname(__file__), "docs", "history.json")
HEADERS = {"User-Agent": "HuntcliffCurrentOutageTracker (personal project)"}


def load_history():
    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH, "r") as f:
            return json.load(f)
    return {}


def save_history(history):
    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2, sort_keys=True)


def _get(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp


def get_deployment_info():
    """Look up where this deployment's live data currently lives."""
    state_url = (
        f"{BASE_URL}stormcenter/api/v1/stormcenters/{INSTANCE_ID}/"
        f"views/{VIEW_ID}/currentState?preview=false"
    )
    state = _get(state_url).json()
    return {
        "data_path": state["data"]["interval_generation_data"],
        "cluster_data_path": state["data"]["cluster_interval_generation_data"],
        "deployment_id": state["stormcenterDeploymentId"],
    }


def get_cluster_layer_name(deployment_id):
    """Find which map layer holds outage clusters."""
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


def quadkey_tile_url(cluster_data_path, layer_name, quadkey):
    data_path = cluster_data_path.format(qkh=quadkey[-3:][::-1])
    return f"{BASE_URL}{data_path}/public/{layer_name}/{quadkey}.json"


def point_in_bbox(lat, lon, bbox):
    west, south, east, north = bbox
    return south <= lat <= north and west <= lon <= east


MAX_TILE_REQUESTS = 500  # safety cap — Huntcliff is tiny, this is far more than needed


def fetch_huntcliff_outages(cluster_data_path, layer_name):
    """
    Walk the KUBRA tile tree starting from Huntcliff's covering tile,
    zooming into clusters as needed, and return every individual outage
    found — filtered to those actually within Huntcliff's boundary.
    """
    outages = {}
    already_seen = set()

    start_tiles = list(mercantile.tiles(*HUNTCLIFF_BBOX, zooms=[START_ZOOM]))
    quadkeys = [mercantile.quadkey(t) for t in start_tiles]

    _walk_tiles(quadkeys, already_seen, cluster_data_path, layer_name, outages, zoom=START_ZOOM)

    if len(already_seen) >= MAX_TILE_REQUESTS:
        print(f"  (hit the {MAX_TILE_REQUESTS}-tile safety cap — stopping; "
              f"this should not normally happen for an area this small)")

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
            continue  # no file = no outages in this tile, which is normal

        for item in resp.json().get("file_data", []):
            desc = item["desc"]
            point = polyline.decode(item["geom"]["p"][0])[0]
            lat, lon = point[0], point[1]

            if desc.get("cluster"):
                next_zoom = zoom + 1
                if next_zoom > MAX_ZOOM:
                    continue  # can't resolve any further, KUBRA's own limit
                child_tile = mercantile.tile(lng=lon, lat=lat, zoom=next_zoom)
                child_qk = mercantile.quadkey(child_tile)
                _walk_tiles([child_qk], already_seen, cluster_data_path, layer_name,
                            outages, next_zoom)
            else:
                if not point_in_bbox(lat, lon, HUNTCLIFF_BBOX):
                    continue  # just outside the neighborhood, skip

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

                # A resolved individual outage might still have unresolved
                # neighbors nearby that weren't in this same tile — check
                # around it once, same as the reference implementation.
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


def main():
    history = load_history()
    history.pop("_lastChecked", None)
    now = datetime.now(timezone.utc).isoformat()
    seen_ids = set()

    try:
        info = get_deployment_info()
        layer_name = get_cluster_layer_name(info["deployment_id"])
        outages = fetch_huntcliff_outages(info["cluster_data_path"], layer_name)
    except requests.RequestException as e:
        print(f"Fetch failed: {e}")
        return
    except (KeyError, RuntimeError) as e:
        print(f"Unexpected response shape from KUBRA — Georgia Power may have "
              f"changed something on their end: {e}")
        return

    for outage_id, o in outages.items():
        seen_ids.add(outage_id)
        existing = history.get(outage_id, {})
        history[outage_id] = {
            "id": outage_id,
            "utility": "Georgia Power",
            "startDate": o["startTime"] or existing.get("startDate") or now,
            "estRestore": o["etr"],
            "cause": o["cause"] or existing.get("cause") or "Unknown",
            "customers": o["customers"],
            "county": "Fulton",  # Huntcliff sits in Fulton County
            "outageType": o["crewStatus"] or "",
            "lat": o["lat"],
            "lon": o["lon"],
            "firstSeen": existing.get("firstSeen") or now,
            "lastSeen": now,
            "status": "Active",
            "resolvedAt": None,
        }
        if outage_id not in existing:
            print(f"[NEW OUTAGE] {outage_id} — {o['cause']}, {o['customers']} customers")

    for outage_id, rec in history.items():
        if rec.get("status") == "Active" and outage_id not in seen_ids:
            rec["status"] = "Restored"
            rec["resolvedAt"] = now
            print(f"[RESTORED] {outage_id}")

    history["_lastChecked"] = now
    save_history(history)
    print(f"{now} — {len(outages)} outage(s) currently active in Huntcliff")


if __name__ == "__main__":
    main()
