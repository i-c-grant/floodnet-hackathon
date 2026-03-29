"""
Query FloodNet flood events for a date range and join sensor metadata.

Usage:
    python query_floods.py --start 2024-01-01 --end 2024-12-31
    python query_floods.py --start 2024-06-01 --end 2024-06-30 --no-map

Outputs to ./output/:
    floods_<start>_<end>.csv      — joined tabular data
    floods_<start>_<end>.geojson  — spatial data (one point per event)
    map_<start>_<end>.html        — interactive folium map
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

import folium
import geopandas as gpd
import pandas as pd
import requests
from dotenv import load_dotenv
from shapely.geometry import Point

load_dotenv()

EVENTS_URL = os.environ["FLOODNET_SOCRATA_EVENTS_ENDPOINT"]
METADATA_URL = os.environ["FLOODNET_SOCRATA_DEPLOYMENT_METADATA_ENDPOINT"]
APP_TOKEN = os.environ["FLOODNET_SOCRATA_APP_TOKEN"]

PAGE_SIZE = 5000


def socrata_get(url: str, params: dict) -> list[dict]:
    """Paginate through a Socrata resource endpoint and return all rows.

    Sends repeated GET requests with $limit and $offset until the API returns
    fewer rows than PAGE_SIZE, indicating the last page.

    Args:
        url: Socrata resource endpoint URL (e.g. .../resource/<id>.json).
        params: SoQL query parameters such as $where, $select, $order.

    Returns:
        List of row dicts concatenated across all pages.
    """
    headers = {"X-App-Token": APP_TOKEN}
    rows = []
    offset = 0
    while True:
        resp = requests.get(
            url,
            headers=headers,
            params={**params, "$limit": PAGE_SIZE, "$offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def fetch_events(start: str, end: str) -> pd.DataFrame:
    """Fetch FloodNet sensor events whose start time falls within [start, end].

    Numeric and timestamp columns are coerced; Socrata internal columns
    (prefixed with ':') are dropped.

    Args:
        start: Start date string in YYYY-MM-DD format.
        end: End date string in YYYY-MM-DD format.

    Returns:
        DataFrame of flood events, or an empty DataFrame if none found.
    """
    where = (
        f"flood_start_time >= '{start}T00:00:00' "
        f"AND flood_start_time <= '{end}T23:59:59'"
    )
    log.info(f"Fetching events: {where}")
    rows = socrata_get(EVENTS_URL, {"$where": where})
    log.info(f"  {len(rows)} events returned")
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Drop Socrata internal columns
    df = df[[c for c in df.columns if not c.startswith(":")]]

    numeric_cols = [
        "max_depth_inches",
        "onset_time_mins",
        "drain_time_mins",
        "duration_mins",
        "duration_above_4_inches_mins",
        "duration_above_12_inches_mins",
        "duration_above_24_inches_mins",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["flood_start_time", "flood_end_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def fetch_metadata() -> pd.DataFrame:
    """Fetch all FloodNet sensor deployment metadata (no date filter).

    Drops the nested GeoJSON location field in favour of the scalar
    latitude/longitude columns. Numeric and timestamp columns are coerced.

    Returns:
        DataFrame of sensor metadata, or an empty DataFrame if none found.
    """
    log.info("Fetching sensor metadata...")
    rows = socrata_get(METADATA_URL, {})
    log.info(f"  {len(rows)} sensors returned")
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = df[[c for c in df.columns if not c.startswith(":")]]

    # Drop nested location dict — we'll use lat/lon directly
    df = df.drop(columns=["location"], errors="ignore")

    for col in ["latitude", "longitude", "lowest_point_height_delta_inches"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "date_installed" in df.columns:
        df["date_installed"] = pd.to_datetime(df["date_installed"], errors="coerce")

    return df


def build_geodataframe(merged: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert a merged flood events DataFrame to a GeoDataFrame.

    Rows with null latitude or longitude are dropped.

    Args:
        merged: DataFrame with latitude and longitude columns.

    Returns:
        GeoDataFrame with Point geometry in EPSG:4326.
    """
    valid = merged.dropna(subset=["latitude", "longitude"])
    geometry = [Point(lon, lat) for lon, lat in zip(valid["longitude"], valid["latitude"])]
    return gpd.GeoDataFrame(valid, geometry=geometry, crs="EPSG:4326")


def make_map(gdf: gpd.GeoDataFrame, out_path: Path) -> None:
    """Render an interactive folium map of flood events and save to HTML.

    Circle markers are sized and colored on a blue-to-red gradient scaled to
    the maximum max_depth_inches in the dataset. Popups show sensor name,
    borough, street, flood start time, max depth, duration, and tidal influence.

    Args:
        gdf: GeoDataFrame of flood events with latitude, longitude, and
            max_depth_inches columns.
        out_path: Path to write the output HTML file.
    """
    center = [gdf["latitude"].mean(), gdf["longitude"].mean()]
    m = folium.Map(location=center, zoom_start=11, tiles="CartoDB positron")

    max_depth = gdf["max_depth_inches"].max() or 1

    for _, row in gdf.iterrows():
        if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
            continue

        depth = row.get("max_depth_inches", 0) or 0
        # Color: blue → yellow → red with depth
        ratio = min(depth / max_depth, 1.0)
        r = int(255 * ratio)
        b = int(255 * (1 - ratio))
        color = f"#{r:02x}80{b:02x}"
        radius = 4 + 12 * ratio

        start = row.get("flood_start_time")
        start_str = start.strftime("%Y-%m-%d %H:%M") if pd.notna(start) else "unknown"

        popup_html = f"""
        <b>{row.get('sensor_name', '')}</b><br>
        Borough: {row.get('borough', '—')}<br>
        Street: {row.get('street_name', '—')}<br>
        Start: {start_str}<br>
        Max depth: {depth:.1f} in<br>
        Duration: {row.get('duration_mins', '—')} min<br>
        Tidally influenced: {row.get('tidally_influenced', '—')}
        """

        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=250),
            tooltip=f"{row.get('sensor_name', '')} — {depth:.1f} in",
        ).add_to(m)

    m.save(str(out_path))
    print(f"  Map saved: {out_path}")


def main():
    """Entry point: parse args, fetch and join flood data, write CSV/GeoJSON/map."""
    parser = argparse.ArgumentParser(description="Query FloodNet events for a date range")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--no-map", action="store_true", help="Skip generating the HTML map")
    args = parser.parse_args()

    # Validate dates
    for val, name in [(args.start, "start"), (args.end, "end")]:
        try:
            datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            print(f"Error: --{name} must be YYYY-MM-DD, got '{val}'")
            sys.exit(1)

    out_dir = Path("output")
    out_dir.mkdir(exist_ok=True)
    slug = f"{args.start}_{args.end}"

    events = fetch_events(args.start, args.end)
    if events.empty:
        print("No events found for this date range.")
        sys.exit(0)

    metadata = fetch_metadata()

    # Join metadata onto events on sensor_id; suffix _meta for overlapping cols
    merged = events.merge(
        metadata,
        on="sensor_id",
        how="left",
        suffixes=("", "_meta"),
    )
    # Drop redundant sensor_name_meta if present
    merged = merged.drop(columns=["sensor_name_meta"], errors="ignore")

    print(f"\nJoined dataset: {len(merged)} rows, {len(merged.columns)} columns")
    print(f"Sensors with metadata: {merged['latitude'].notna().sum()} / {len(merged)}")

    # Save CSV
    csv_path = out_dir / f"floods_{slug}.csv"
    merged.to_csv(csv_path, index=False)
    print(f"  CSV saved:  {csv_path}")

    # Save GeoJSON
    gdf = build_geodataframe(merged)
    geojson_path = out_dir / f"floods_{slug}.geojson"
    gdf.to_file(str(geojson_path), driver="GeoJSON")
    print(f"  GeoJSON saved: {geojson_path}")

    # Map
    if not args.no_map:
        map_path = out_dir / f"map_{slug}.html"
        make_map(gdf, map_path)

    # Summary
    print(f"\n--- Summary ---")
    print(f"Date range:   {args.start} → {args.end}")
    print(f"Total events: {len(merged)}")
    print(f"Unique sensors with events: {merged['sensor_id'].nunique()}")
    if merged["max_depth_inches"].notna().any():
        print(f"Max depth:    {merged['max_depth_inches'].max():.2f} in")
        print(f"Median depth: {merged['max_depth_inches'].median():.2f} in")
    if "borough" in merged.columns:
        print("\nEvents by borough:")
        print(merged["borough"].value_counts().to_string())


if __name__ == "__main__":
    main()
