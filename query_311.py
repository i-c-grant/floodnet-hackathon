"""
Query NYC 311 flood-related complaints for a date range.

Usage:
    python query_311.py --start 2024-09-01 --end 2024-09-30
    python query_311.py --start 2024-09-01 --end 2024-09-30 --no-map

Outputs to ./output/:
    311_<start>_<end>.csv
    311_<start>_<end>.geojson
    map_311_<start>_<end>.html
"""

import argparse
import logging
import os
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

import folium
import geopandas as gpd
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

ENDPOINT = os.environ["NYC_311_ENDPOINT"]
APP_TOKEN = os.environ["FLOODNET_SOCRATA_APP_TOKEN"]

PAGE_SIZE = 1000


def fetch_311(start: str, end: str) -> pd.DataFrame:
    """Fetch flood-related NYC 311 complaints filed between start and end.

    Filters on descriptors containing 'flood', 'catch basin', 'backup',
    'overflow', or 'culvert', plus the 'Standing Water' complaint type.
    Excludes lamp-related descriptors. Only returns rows with valid coordinates.
    Retries each page up to 5 times with exponential backoff on request errors.

    Args:
        start: ISO datetime string, e.g. '2024-09-01T00:00:00'.
        end: ISO datetime string, e.g. '2024-09-01T06:30:00'.

    Returns:
        DataFrame of matching complaints, or an empty DataFrame if none found
        or all retries are exhausted.
    """
    query_template = """
SELECT
  unique_key,
  created_date,
  closed_date,
  agency,
  agency_name,
  complaint_type,
  descriptor,
  incident_zip,
  street_name,
  city,
  status,
  due_date,
  resolution_description,
  resolution_action_updated_date,
  borough,
  latitude,
  longitude
WHERE
  (
    caseless_contains(descriptor, "flood")
    OR caseless_contains(descriptor, "catch basin")
    OR caseless_contains(descriptor, "backup")
    OR caseless_contains(descriptor, "overflow")
    OR caseless_contains(descriptor, "culvert")
    OR complaint_type = "Standing Water"
  )
  AND NOT caseless_contains(descriptor, "lamp")
  AND NOT caseless_eq(descriptor, "Wastewater Into Catch Basin (IEB)")
  AND NOT caseless_eq(descriptor, "Swimming Pool - Unmaintained")
  AND NOT caseless_eq(descriptor, "Other - Explain Below")
  AND (created_date BETWEEN "{start}" AND "{end}")
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
ORDER BY created_date DESC
""".strip()

    base_query = query_template.format(start=start, end=end)

    headers = {"X-App-Token": APP_TOKEN}
    rows = []
    offset = 0

    log.debug(f"Fetching 311: {start} → {end}")
    while True:
        query = f"{base_query}\nLIMIT {PAGE_SIZE}\nOFFSET {offset}"
        url = f"{ENDPOINT}?$query={urllib.parse.quote_plus(query)}"

        for attempt in range(5):
            try:
                resp = requests.get(url, headers=headers, timeout=60)
                resp.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                wait = 2 ** attempt
                log.warning(f"Request failed (attempt {attempt + 1}/5): {e} — retrying in {wait}s")
                import time; time.sleep(wait)
        else:
            log.error(f"All retries exhausted for {start} → {end}, skipping window")
            return pd.DataFrame()

        batch = resp.json()

        if not batch:
            break

        rows.extend(batch)

        if len(batch) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    log.debug(f"  {len(rows)} complaints returned")
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    for col in ["created_date", "closed_date", "due_date", "resolution_action_updated_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def build_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert a 311 complaints DataFrame to a GeoDataFrame.

    Rows with null latitude or longitude are dropped.

    Args:
        df: DataFrame with latitude and longitude columns.

    Returns:
        GeoDataFrame with Point geometry in EPSG:4326.
    """
    valid = df.dropna(subset=["latitude", "longitude"])
    return gpd.GeoDataFrame(
        valid,
        geometry=gpd.points_from_xy(valid["longitude"], valid["latitude"]),
        crs="EPSG:4326",
    )


def make_map(gdf: gpd.GeoDataFrame, out_path: Path) -> None:
    """Render an interactive folium map of 311 complaints and save to HTML.

    Circle markers are colored by complaint status (green=Closed, red=Open,
    orange=Pending, blue=In Progress, grey=Other). A legend is rendered in the
    bottom-left corner. Popups show complaint type, descriptor, borough, street,
    created date, status, and a truncated resolution description.

    Args:
        gdf: GeoDataFrame of 311 complaints with latitude, longitude, and
            status columns.
        out_path: Path to write the output HTML file.
    """
    center = [gdf["latitude"].mean(), gdf["longitude"].mean()]
    m = folium.Map(location=center, zoom_start=11, tiles="CartoDB positron")

    status_colors = {
        "Closed": "#2ecc71",
        "Open": "#e74c3c",
        "Pending": "#f39c12",
        "In Progress": "#3498db",
    }

    for _, row in gdf.iterrows():
        color = status_colors.get(row.get("status", ""), "#95a5a6")
        created = row.get("created_date")
        created_str = created.strftime("%Y-%m-%d %H:%M") if pd.notna(created) else "unknown"

        popup_html = f"""
        <b>{row.get('complaint_type', '')}</b><br>
        Descriptor: {row.get('descriptor', '—')}<br>
        Borough: {row.get('borough', '—')}<br>
        Street: {row.get('street_name', '—')}<br>
        Created: {created_str}<br>
        Status: {row.get('status', '—')}<br>
        Resolution: {str(row.get('resolution_description', '—'))[:120]}
        """

        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=5,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=f"{row.get('descriptor', '')} — {row.get('status', '')}",
        ).add_to(m)

    # Simple legend
    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;background:white;padding:10px;
                border-radius:5px;border:1px solid #ccc;font-size:12px;z-index:1000;">
      <b>Status</b><br>
      <span style="color:#2ecc71">●</span> Closed<br>
      <span style="color:#e74c3c">●</span> Open<br>
      <span style="color:#f39c12">●</span> Pending<br>
      <span style="color:#3498db">●</span> In Progress<br>
      <span style="color:#95a5a6">●</span> Other
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    m.save(str(out_path))
    print(f"  Map saved: {out_path}")


def main():
    """Entry point: parse args, fetch 311 complaints, write CSV/GeoJSON/map."""
    parser = argparse.ArgumentParser(description="Query NYC 311 flood complaints for a date range")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--no-map", action="store_true", help="Skip generating the HTML map")
    args = parser.parse_args()

    for val, name in [(args.start, "start"), (args.end, "end")]:
        try:
            datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            print(f"Error: --{name} must be YYYY-MM-DD, got '{val}'")
            sys.exit(1)

    out_dir = Path("output")
    out_dir.mkdir(exist_ok=True)
    slug = f"{args.start}_{args.end}"

    df = fetch_311(f"{args.start}T00:00:00", f"{args.end}T23:59:59")
    if df.empty:
        print("No complaints found for this date range.")
        sys.exit(0)

    gdf = build_geodataframe(df)

    csv_path = out_dir / f"311_{slug}.csv"
    df.to_csv(csv_path, index=False)
    print(f"  CSV saved:  {csv_path}")

    geojson_path = out_dir / f"311_{slug}.geojson"
    gdf.to_file(str(geojson_path), driver="GeoJSON")
    print(f"  GeoJSON saved: {geojson_path}")

    if not args.no_map:
        map_path = out_dir / f"map_311_{slug}.html"
        make_map(gdf, map_path)

    print(f"\n--- Summary ---")
    print(f"Date range:      {args.start} → {args.end}")
    print(f"Total complaints: {len(df)}")
    if "status" in df.columns:
        print("\nBy status:")
        print(df["status"].value_counts().to_string())
    if "borough" in df.columns:
        print("\nBy borough:")
        print(df["borough"].value_counts().to_string())
    if "descriptor" in df.columns:
        print("\nTop descriptors:")
        print(df["descriptor"].value_counts().head(10).to_string())


if __name__ == "__main__":
    main()
