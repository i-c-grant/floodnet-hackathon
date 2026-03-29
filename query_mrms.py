"""
Fetch MRMS VIL (Vertically Integrated Liquid) frames for a storm, store the
clipped raw arrays in DuckDB, then derive >=4 kg/m² contour polygons.

The full 2D grid is preserved alongside its coordinate metadata so that any
downstream step (contour extraction, PNG rendering, etc.) can reconstruct the
spatial data without re-downloading.

Usage:
    python query_mrms.py --storm-id 504
    python query_mrms.py --storm-id 504 --interval-min 5
    python query_mrms.py --storm-id 504 --force-download   # re-fetch even if data exists

Skips the S3 download if mrms_vil already has rows for the requested storm
(unless --force-download is passed), then always regenerates mrms_contours.

Output tables: mrms_vil, mrms_contours in output/floodnet.duckdb
Docker:
    docker compose run --rm mrms
    docker compose run --rm mrms --storm-id 504 --interval-min 5
    docker compose run --rm mrms --storm-id 504 --force-download
"""

import argparse
import gzip
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import duckdb
import eccodes
import numpy as np
import pandas as pd
import requests

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

DB_PATH   = Path("output/floodnet.duckdb")
MRMS_BASE = "https://noaa-mrms-pds.s3.amazonaws.com"
PRODUCT   = "CONUS/LVL3_HighResVIL_00.50"

# Clipping bbox — matches MRMS_BOUNDS in map_template.html
NYC_BBOX = dict(west=-74.35, south=40.45, east=-73.60, north=40.95)

_S3_NS    = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
_FNAME_RE = re.compile(r"_(\d{8})-(\d{6})\.grib2\.gz$")


# ── Storm metadata ─────────────────────────────────────────────────────────────

def get_storm_window(con: duckdb.DuckDBPyConnection, storm_id: int
                     ) -> tuple[pd.Timestamp, pd.Timestamp]:
    row = con.execute(
        "SELECT storm_start, storm_end FROM storm_events WHERE storm_id = ?",
        [storm_id],
    ).fetchone()
    if row is None:
        raise SystemExit(f"Storm {storm_id} not found in DB")
    return pd.Timestamp(row[0]), pd.Timestamp(row[1])


# ── DuckDB schema ──────────────────────────────────────────────────────────────

def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS mrms_vil (
            timestamp_utc   TIMESTAMP   NOT NULL,
            storm_id        INTEGER     NOT NULL,
            -- grid provenance (enough to reconstruct lat/lon for every cell)
            lat_min         DOUBLE      NOT NULL,   -- southernmost latitude in clipped grid
            lat_max         DOUBLE      NOT NULL,   -- northernmost latitude
            lon_min         DOUBLE      NOT NULL,   -- westernmost longitude
            lon_max         DOUBLE      NOT NULL,   -- easternmost longitude
            lat_step        DOUBLE      NOT NULL,   -- grid spacing (degrees)
            lon_step        DOUBLE      NOT NULL,
            n_lat           INTEGER     NOT NULL,   -- number of rows
            n_lon           INTEGER     NOT NULL,   -- number of columns
            -- VIL values: flattened row-major, lat order N→S (matching GRIB2 storage order)
            vil_flat        FLOAT[]     NOT NULL,
            PRIMARY KEY (timestamp_utc)
        )
    """)


def ensure_contours_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS mrms_contours (
            timestamp_utc   TIMESTAMP   NOT NULL,
            storm_id        INTEGER     NOT NULL,
            -- WKT of the unioned >=4 kg/m² region; NULL when no cell reaches threshold
            geom_wkt        VARCHAR,
            PRIMARY KEY (timestamp_utc, storm_id)
        )
    """)


def upsert_frame(con: duckdb.DuckDBPyConnection, storm_id: int, t: pd.Timestamp,
                 lats: np.ndarray, lons: np.ndarray, arr: np.ndarray) -> None:
    con.execute("""
        INSERT OR REPLACE INTO mrms_vil VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        t.to_pydatetime(),
        storm_id,
        float(lats.min()), float(lats.max()),
        float(lons.min()), float(lons.max()),
        float(abs(lats[1] - lats[0])) if len(lats) > 1 else 0.01,
        float(abs(lons[1] - lons[0])) if len(lons) > 1 else 0.01,
        int(arr.shape[0]),
        int(arr.shape[1]),
        arr.flatten().tolist(),
    ])


# ── NOAA S3 helpers ────────────────────────────────────────────────────────────

def list_keys_for_date(date_str: str) -> list[tuple[pd.Timestamp, str]]:
    """List all available VIL keys for one UTC date."""
    prefix = f"{PRODUCT}/{date_str}/"
    results = []
    continuation_token = None

    while True:
        params = f"list-type=2&prefix={prefix}&max-keys=1000"
        if continuation_token:
            import urllib.parse
            params += f"&continuation-token={urllib.parse.quote(continuation_token)}"
        r = requests.get(f"{MRMS_BASE}/?{params}", timeout=30)
        r.raise_for_status()
        root = ET.fromstring(r.text)

        for key_el in root.findall(".//s3:Key", _S3_NS):
            key = key_el.text
            m = _FNAME_RE.search(key)
            if m:
                d, t = m.group(1), m.group(2)
                ts = pd.Timestamp(
                    f"{d[:4]}-{d[4:6]}-{d[6:8]}T{t[:2]}:{t[2:4]}:{t[4:6]}"
                )
                results.append((ts, key))

        next_el = root.find(".//s3:NextContinuationToken", _S3_NS)
        if next_el is None:
            break
        continuation_token = next_el.text

    return sorted(results)


def pick_frames(all_keys: list[tuple[pd.Timestamp, str]],
                start: pd.Timestamp, end: pd.Timestamp,
                interval_min: int) -> list[tuple[pd.Timestamp, str]]:
    """For each target time spaced interval_min apart, pick the closest available key."""
    if not all_keys:
        return []
    times = np.array([t.timestamp() for t, _ in all_keys])

    selected, seen_keys = [], set()
    t = start.ceil(f"{interval_min}min")
    while t <= end:
        idx = int(np.argmin(np.abs(times - t.timestamp())))
        key = all_keys[idx][1]
        if key not in seen_keys:
            seen_keys.add(key)
            selected.append((all_keys[idx][0], key))
        t += pd.Timedelta(minutes=interval_min)
    return selected


def download_grib(key: str) -> bytes | None:
    url = f"{MRMS_BASE}/{key}"
    try:
        r = requests.get(url, timeout=45)
        return r.content if r.status_code == 200 else None
    except Exception as exc:
        log.debug(f"Download error: {exc}")
        return None


# ── GRIB2 parsing + clipping ──────────────────────────────────────────────────

def parse_grib(gz_bytes: bytes) -> tuple[np.ndarray, np.ndarray, np.ndarray] | None:
    """
    Decompress + parse a MRMS GRIB2.gz blob using eccodes Python bindings directly.

    Bypasses cfgrib/xarray to avoid the 'dataTime: non-zero seconds' error that
    cfgrib raises on MRMS files whose timestamps have irregular sub-minute offsets.

    Returns (lats, lons, vil_array) clipped to NYC_BBOX, or None on failure.
    lats is 1-D in N→S order; lons is 1-D in W→E order; vil_array is (n_lat, n_lon).
    """
    tmp = Path("/tmp/mrms_vil.grib2")
    try:
        tmp.write_bytes(gzip.decompress(gz_bytes))

        with open(tmp, "rb") as f:
            msg_id = eccodes.codes_grib_new_from_file(f)
            if msg_id is None:
                log.debug("No GRIB messages in file")
                return None
            try:
                ni          = eccodes.codes_get(msg_id, "Ni")
                nj          = eccodes.codes_get(msg_id, "Nj")
                lat_first   = eccodes.codes_get(msg_id, "latitudeOfFirstGridPointInDegrees")
                lon_first   = eccodes.codes_get(msg_id, "longitudeOfFirstGridPointInDegrees")
                lat_last    = eccodes.codes_get(msg_id, "latitudeOfLastGridPointInDegrees")
                lon_last    = eccodes.codes_get(msg_id, "longitudeOfLastGridPointInDegrees")
                missing_val = eccodes.codes_get(msg_id, "missingValue")
                values      = eccodes.codes_get_values(msg_id)
            finally:
                eccodes.codes_release(msg_id)

        arr_full = values.reshape(nj, ni).astype(np.float32)
        # Mask GRIB fill values
        arr_full = np.where((arr_full >= missing_val * 0.99) | (arr_full > 1e10), np.nan, arr_full)

        lats_all = np.linspace(lat_first, lat_last, nj)
        lons_all = np.linspace(lon_first, lon_last, ni)
        # MRMS stores longitudes in 0–360; convert to -180/180 for bbox comparison
        lons_all = np.where(lons_all > 180, lons_all - 360, lons_all)

        lat_idx = np.where(
            (lats_all >= NYC_BBOX["south"]) & (lats_all <= NYC_BBOX["north"])
        )[0]
        lon_idx = np.where(
            (lons_all >= NYC_BBOX["west"]) & (lons_all <= NYC_BBOX["east"])
        )[0]

        if lat_idx.size == 0 or lon_idx.size == 0:
            log.debug("NYC bbox outside data extent")
            return None

        lats = lats_all[lat_idx]
        lons = lons_all[lon_idx]
        arr  = arr_full[lat_idx[0] : lat_idx[-1] + 1,
                        lon_idx[0] : lon_idx[-1] + 1]

        arr = np.where(np.isfinite(arr) & (arr > 0), arr, 0).astype(np.float32)
        return lats, lons, arr

    except Exception as exc:
        log.debug(f"Parse error: {exc}")
        return None
    finally:
        tmp.unlink(missing_ok=True)


# ── Contour generation ────────────────────────────────────────────────────────

VIL_THRESHOLD  = 6.0   # kg/m²
CLUSTER_DIST   = 0.04  # degrees (~4 km); sub-polygons within 2× this distance merge into one cluster


def generate_contours(con: duckdb.DuckDBPyConnection, storm_id: int) -> None:
    """
    For each frame in mrms_vil, find all grid cells >= VIL_THRESHOLD, build a
    bounding box for each cell, union them into a single (Multi)Polygon, and
    upsert the result as WKT into mrms_contours.
    """
    from shapely.geometry import box, MultiPolygon
    from shapely.ops import unary_union

    rows = con.execute("""
        SELECT timestamp_utc, lat_min, lat_max, lon_min, lon_max,
               n_lat, n_lon, vil_flat
        FROM mrms_vil
        WHERE storm_id = ?
        ORDER BY timestamp_utc
    """, [storm_id]).fetchall()

    log.info(f"Generating contours for {len(rows)} frames (threshold={VIL_THRESHOLD} kg/m²)…")
    n_ok = n_empty = 0

    for row in rows:
        t, lat_min, lat_max, lon_min, lon_max, n_lat, n_lon, vil_flat = row
        arr  = np.array(vil_flat, dtype=np.float32).reshape(n_lat, n_lon)
        lats = np.linspace(lat_max, lat_min, n_lat)   # N→S, matches storage order
        lons = np.linspace(lon_min, lon_max, n_lon)

        # Half-cell size for building per-cell bounding boxes
        dlat = float(abs(lats[1] - lats[0])) / 2 if n_lat > 1 else 0.005
        dlon = float(abs(lons[1] - lons[0])) / 2 if n_lon > 1 else 0.005

        geom_wkt = None
        mask = arr >= VIL_THRESHOLD
        if mask.any():
            cells = [
                box(lons[j] - dlon, lats[i] - dlat, lons[j] + dlon, lats[i] + dlat)
                for i, j in zip(*np.where(mask))
            ]
            raw = unary_union(cells)

            # ── Cluster by proximity, then convex-hull each cluster ──────────
            # Expand each sub-polygon by CLUSTER_DIST; overlapping expanded
            # polygons get merged by unary_union into one connected component.
            # Polygons farther than 2×CLUSTER_DIST apart stay separate.
            polys    = list(raw.geoms) if raw.geom_type == 'MultiPolygon' else [raw]
            expanded = unary_union([p.buffer(CLUSTER_DIST) for p in polys])
            clusters = list(expanded.geoms) if expanded.geom_type == 'MultiPolygon' else [expanded]

            result_polys = []
            for cluster in clusters:
                members = [p for p in polys if cluster.intersects(p)]
                if members:
                    result_polys.append(unary_union(members).convex_hull)

            geom     = MultiPolygon(result_polys) if len(result_polys) > 1 else result_polys[0]
            geom_wkt = geom.wkt
            n_ok += 1
        else:
            n_empty += 1

        con.execute(
            "INSERT OR REPLACE INTO mrms_contours VALUES (?, ?, ?)",
            [t, storm_id, geom_wkt],
        )

    log.info(f"Contours done: {n_ok} with geometry, {n_empty} below threshold / empty")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest MRMS VIL frames into DuckDB and generate contours")
    parser.add_argument("--storm-id",      type=int, default=504)
    parser.add_argument("--interval-min",  type=int, default=5,
                        help="Sampling interval in minutes (default 5)")
    parser.add_argument("--force-download", action="store_true",
                        help="Re-download from S3 even if data already exists in DB")
    args = parser.parse_args()

    DB_PATH.parent.mkdir(exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    ensure_table(con)
    ensure_contours_table(con)

    storm_start, storm_end = get_storm_window(con, args.storm_id)
    log.info(f"Storm {args.storm_id}: {storm_start} → {storm_end} UTC")

    # ── Download phase ─────────────────────────────────────────────────────────
    existing = con.execute(
        "SELECT COUNT(*) FROM mrms_vil WHERE storm_id = ?", [args.storm_id]
    ).fetchone()[0]

    if existing > 0 and not args.force_download:
        log.info(f"{existing} frames already in DB — skipping download (use --force-download to re-fetch)")
    else:
        if existing > 0:
            log.info(f"--force-download set; re-fetching over {existing} existing frames")

        dates = pd.date_range(storm_start.date(), storm_end.date(), freq="D")
        all_keys: list[tuple[pd.Timestamp, str]] = []
        for d in dates:
            date_str = d.strftime("%Y%m%d")
            log.info(f"Listing S3 keys for {date_str}...")
            keys = list_keys_for_date(date_str)
            log.info(f"  {len(keys)} files available")
            all_keys.extend(keys)

        frames = pick_frames(all_keys, storm_start, storm_end, args.interval_min)
        log.info(f"Selected {len(frames)} frames at {args.interval_min}-min intervals")

        n_ok = n_err = 0
        for i, (t, key) in enumerate(frames):
            log.info(f"  [{i+1:3d}/{len(frames)}] {key.split('/')[-1]}")

            gz = download_grib(key)
            if gz is None:
                log.info("    → download failed")
                n_err += 1
                continue

            result = parse_grib(gz)
            if result is None:
                log.info("    → parse failed")
                n_err += 1
                continue

            lats, lons, arr = result
            upsert_frame(con, args.storm_id, t, lats, lons, arr)
            log.info(f"    → ok  shape={arr.shape}  max={arr.max():.1f} kg/m²")
            n_ok += 1

        total = con.execute(
            "SELECT COUNT(*) FROM mrms_vil WHERE storm_id = ?", [args.storm_id]
        ).fetchone()[0]
        log.info(f"Download done.  ok={n_ok}  errors={n_err}  total in DB: {total}")

    # ── Contour phase (always runs) ────────────────────────────────────────────
    generate_contours(con, args.storm_id)
    con.close()
    log.info("✓  Done.")


if __name__ == "__main__":
    main()
