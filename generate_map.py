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

import base64
import io

import duckdb
import matplotlib.image as mimg
import numpy as np
import pandas as pd

# ── VIL → RGBA colormap ────────────────────────────────────────────────────────
# Brewer Blues ramp; matches the CSS gradient in the map legend.
VIL_SHOW_MIN = 4.0    # kg/m²; below this: fully transparent
VIL_SHOW_MAX = 15.0   # kg/m²; above this: max opacity

_RAMP_POS = np.array([0.0, 0.25, 0.5, 0.75, 1.0])
_RAMP_RGB  = np.array([
    [255, 255, 255],   # white
    [198, 219, 239],   # light blue
    [107, 174, 214],   # medium blue
    [ 33, 113, 181],   # blue
    [  8,  48, 107],   # deep navy
], dtype=np.float32)


def _vil_to_png_b64(arr: np.ndarray) -> str:
    """Convert a 2D VIL float32 array to a base64-encoded RGBA PNG.

    Pixels below VIL_SHOW_MIN are fully transparent; pixels at VIL_SHOW_MAX
    reach 85 % opacity.  RGB follows the Brewer YlOrRd ramp.
    """
    norm = np.clip((arr - VIL_SHOW_MIN) / (VIL_SHOW_MAX - VIL_SHOW_MIN), 0.0, 1.0)
    flat = norm.ravel()

    r = np.interp(flat, _RAMP_POS, _RAMP_RGB[:, 0]).reshape(arr.shape)
    g = np.interp(flat, _RAMP_POS, _RAMP_RGB[:, 1]).reshape(arr.shape)
    b = np.interp(flat, _RAMP_POS, _RAMP_RGB[:, 2]).reshape(arr.shape)
    a = np.where(arr < VIL_SHOW_MIN, 0.0, norm * 0.85) * 255

    rgba = np.stack([r, g, b, a], axis=-1).astype(np.uint8)

    buf = io.BytesIO()
    mimg.imsave(buf, rgba, format="png")
    return base64.b64encode(buf.getvalue()).decode("ascii")

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
    # MRMS VIL raster frames — {t_ms, png_b64} per frame.
    # png_b64 is None for frames where max VIL < VIL_SHOW_MIN (no visible content).
    # ------------------------------------------------------------------
    vil_rows = con.execute("""
        SELECT timestamp_utc, n_lat, n_lon, vil_flat
        FROM mrms_vil
        WHERE storm_id = ?
        ORDER BY timestamp_utc
    """, [STORM_ID]).fetchall()

    con.close()

    mrms_frames = []
    for t, n_lat, n_lon, vil_flat in vil_rows:
        t_ms = int(pd.Timestamp(t).timestamp() * 1000)
        arr  = np.array(vil_flat, dtype=np.float32).reshape(n_lat, n_lon)
        png_b64 = _vil_to_png_b64(arr) if float(arr.max()) >= VIL_SHOW_MIN else None
        mrms_frames.append({"t_ms": t_ms, "png_b64": png_b64})

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
