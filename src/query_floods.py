"""
FloodNet flood event and sensor metadata fetching functions.

Provides fetch_events() and fetch_metadata() for querying the FloodNet
Socrata API. Orchestrated by pipeline.py.
"""

import logging
import os

import pandas as pd
import requests
from dotenv import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()

EVENTS_URL   = os.environ["FLOODNET_SOCRATA_EVENTS_ENDPOINT"]
METADATA_URL = os.environ["FLOODNET_SOCRATA_DEPLOYMENT_METADATA_ENDPOINT"]
APP_TOKEN    = os.environ["FLOODNET_SOCRATA_APP_TOKEN"]

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
