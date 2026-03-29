"""
Generate a standalone animated HTML map for the Oct 30 2025 storm (storm_id=504).

Usage (local):
    python generate_map.py

Usage (Docker):
    docker compose run --rm genmap

Output: output/storm_oct30.html
"""

import ast
import json
from collections import defaultdict
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from shapely import wkt as shapely_wkt
from shapely.geometry import LinearRing, mapping, shape

_INTERP_PTS = 64   # vertices per resampled polygon ring for interpolation


def _dominant(geojson):
    """
    Normalise a GeoJSON Polygon or MultiPolygon to a single Polygon by returning
    the largest sub-polygon by area.  Returns None if geojson is None.
    """
    if geojson is None:
        return None
    if geojson["type"] == "Polygon":
        return geojson
    # MultiPolygon — pick the largest component
    geom = shape(geojson)
    largest = max(geom.geoms, key=lambda g: g.area)
    return mapping(largest)


def _resample_ring(ring_coords):
    """Resample a GeoJSON ring (list of [lon,lat]) to _INTERP_PTS evenly-spaced points."""
    ring = LinearRing(ring_coords)
    return np.array([
        [ring.interpolate(i / _INTERP_PTS, normalized=True).x,
         ring.interpolate(i / _INTERP_PTS, normalized=True).y]
        for i in range(_INTERP_PTS)
    ])


def _align_rotation(a, b):
    """Return rotation index r that minimises sum-of-squared distances between a and roll(b, -r)."""
    costs = np.array([np.sum((a - np.roll(b, -r, axis=0)) ** 2) for r in range(_INTERP_PTS)])
    return int(np.argmin(costs))


def _interpolate_frames(contour_rows, interval_ms=60_000):
    """
    Expand MRMS contour rows to interval_ms resolution by linearly interpolating
    polygon vertices between adjacent frames.

    Each frame is normalised to its dominant (largest-area) polygon before
    interpolation, eliminating jumps caused by MultiPolygon → Polygon transitions.
    None frames (below threshold) pass through unchanged; interpolation is skipped
    across any None boundary.
    """
    base = []
    for t, geom_wkt in contour_rows:
        t_ms = int(pd.Timestamp(t).timestamp() * 1000)
        raw = mapping(shapely_wkt.loads(geom_wkt)) if geom_wkt else None
        base.append({"t_ms": t_ms, "geojson": _dominant(raw)})

    result = []
    for i, f0 in enumerate(base):
        result.append(f0)
        if i + 1 >= len(base):
            break
        f1 = base[i + 1]

        g0, g1 = f0["geojson"], f1["geojson"]
        if not (g0 and g1):
            continue

        a = _resample_ring(g0["coordinates"][0])
        b = _resample_ring(g1["coordinates"][0])
        b_aligned = np.roll(b, -_align_rotation(a, b), axis=0)

        t0, t1 = f0["t_ms"], f1["t_ms"]
        n_steps = max(1, round((t1 - t0) / interval_ms))

        for step in range(1, n_steps):
            alpha = step / n_steps
            coords = (a + alpha * (b_aligned - a)).tolist()
            coords.append(coords[0])  # close the ring
            result.append({
                "t_ms": int(t0 + alpha * (t1 - t0)),
                "geojson": {"type": "Polygon", "coordinates": [coords]},
            })

    result.sort(key=lambda f: f["t_ms"])
    return result

DB_PATH       = Path("output/floodnet.duckdb")
TEMPLATE_PATH = Path("map_template.html")
OUT_PATH      = Path("output/storm_oct30.html")
STORM_ID      = 504


def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    # Storm metadata
    row = con.execute(
        "SELECT storm_start, storm_end FROM storm_events WHERE storm_id = ?",
        [STORM_ID],
    ).fetchone()
    storm_start = pd.Timestamp(row[0])
    storm_end = pd.Timestamp(row[1])

    # All sensors deployed at storm time (one row per sensor)
    deployed = con.execute("""
        SELECT
            sensor_id,
            ANY_VALUE(sensor_name)           AS sensor_name,
            ANY_VALUE(latitude)              AS latitude,
            ANY_VALUE(longitude)             AS longitude,
            ANY_VALUE(borough)               AS borough,
            ANY_VALUE(tidally_influenced)    AS tidally_influenced
        FROM flood_events
        WHERE date_installed <= ?
          AND latitude IS NOT NULL AND longitude IS NOT NULL
        GROUP BY sensor_id
    """, [storm_start]).df()

    # Flood events for this storm (with profiles)
    floods = con.execute("""
        SELECT sensor_id, flood_start_time,
               flood_profile_depth_inches,
               flood_profile_time_secs
        FROM flood_events
        WHERE storm_id = ? AND latitude IS NOT NULL AND longitude IS NOT NULL
    """, [STORM_ID]).df()

    # 311 complaints
    complaints = con.execute("""
        SELECT latitude, longitude, descriptor, borough, created_date
        FROM complaints_311
        WHERE storm_id = ? AND latitude IS NOT NULL AND longitude IS NOT NULL
    """, [STORM_ID]).df()

    # ------------------------------------------------------------------
    # MRMS contours — {t_ms, geojson} per frame; geojson is None when
    # VIL was below threshold (rendered as no layer in the map).
    # ------------------------------------------------------------------
    contour_rows = con.execute("""
        SELECT timestamp_utc, geom_wkt
        FROM mrms_contours
        WHERE storm_id = ?
        ORDER BY timestamp_utc
    """, [STORM_ID]).fetchall()

    con.close()

    mrms_frames = _interpolate_frames(contour_rows)

    # ------------------------------------------------------------------
    # Build per-sensor flood time series
    # Keep raw profile points (≈60s cadence) for smooth JS interpolation.
    # Format: { sensor_id: [[t_ms, depth], ...] }  (sorted by t_ms)
    # ------------------------------------------------------------------
    flood_series: dict[str, list] = defaultdict(list)
    for _, ev in floods.iterrows():
        try:
            depths = ast.literal_eval(ev["flood_profile_depth_inches"])
            times_secs = ast.literal_eval(ev["flood_profile_time_secs"])
        except (ValueError, SyntaxError, TypeError):
            continue
        if not depths or not times_secs:
            continue
        start_ts = pd.Timestamp(ev["flood_start_time"])
        start_ms = int(start_ts.timestamp() * 1000)
        sid = ev["sensor_id"]
        for t_sec, depth in zip(times_secs, depths):
            flood_series[sid].append([start_ms + int(float(t_sec) * 1000), float(depth)])

    # Sort each sensor's series by time and deduplicate on timestamp (keep max depth)
    clean_series: dict[str, list] = {}
    for sid, pts in flood_series.items():
        by_t: dict[int, float] = {}
        for t_ms, d in pts:
            if t_ms not in by_t or d > by_t[t_ms]:
                by_t[t_ms] = d
        clean_series[sid] = sorted([t_ms, d] for t_ms, d in by_t.items())

    # ------------------------------------------------------------------
    # Sensor location list
    # ------------------------------------------------------------------
    sensor_locs = []
    for _, row in deployed.iterrows():
        sensor_locs.append({
            "id":     row["sensor_id"],
            "name":   str(row["sensor_name"] or row["sensor_id"]),
            "lat":    float(row["latitude"]),
            "lon":    float(row["longitude"]),
            "borough": str(row["borough"] or ""),
            "tidal":  (row["tidally_influenced"] or "") == "Yes",
        })

    # ------------------------------------------------------------------
    # 311 complaints — convert ET → UTC for alignment with sensor times
    # Convert the whole column at once so ambiguous='infer' works on a Series.
    # ------------------------------------------------------------------
    complaints["created_utc"] = (
        pd.to_datetime(complaints["created_date"], errors="coerce")
        .dt.tz_localize("America/New_York", ambiguous="infer", nonexistent="shift_forward")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )
    complaints_list = []
    for _, row in complaints.dropna(subset=["created_utc"]).iterrows():
        complaints_list.append({
            "lat":        float(row["latitude"]),
            "lon":        float(row["longitude"]),
            "descriptor": str(row["descriptor"] or ""),
            "borough":    str(row["borough"] or ""),
            "created_ms": int(row["created_utc"].timestamp() * 1000),
        })

    storm_start_ms = int(storm_start.timestamp() * 1000)
    storm_end_ms   = int((storm_end - pd.Timedelta(hours=2)).timestamp() * 1000)
    max_depth = max(
        (max(d for _, d in series) for series in clean_series.values() if series),
        default=1.0,
    )

    # ------------------------------------------------------------------
    # Inject into template
    # ------------------------------------------------------------------
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    html = (
        template
        .replace("__SENSOR_LOCS__",    json.dumps(sensor_locs))
        .replace("__FLOOD_SERIES__",   json.dumps(clean_series))
        .replace("__COMPLAINTS__",     json.dumps(complaints_list))
        .replace("__STORM_START_MS__", str(storm_start_ms))
        .replace("__STORM_END_MS__",   str(storm_end_ms))
        .replace("__MAX_DEPTH__",      f"{max_depth:.4f}")
        .replace("__MRMS_FRAMES__",    json.dumps(mrms_frames))
    )

    OUT_PATH.parent.mkdir(exist_ok=True)
    OUT_PATH.write_text(html, encoding="utf-8")
    print(f"✓  Written: {OUT_PATH.resolve()}")
    print(f"   Sensors:    {len(sensor_locs)}")
    print(f"   W/ profiles:{len(clean_series)}")
    print(f"   Complaints: {len(complaints_list)}")
    print(f"   Max depth:  {max_depth:.1f} in")
    print(f"   Duration:   {(storm_end_ms - storm_start_ms) / 3600000:.1f} h")


if __name__ == "__main__":
    main()
