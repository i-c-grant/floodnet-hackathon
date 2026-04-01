"""
NYC 311 flood-related complaint fetching functions.

Provides fetch_311() for querying the NYC Open Data 311 API.
Orchestrated by pipeline.py.
"""

import logging
import os
import time
import urllib.parse

import pandas as pd
import requests
from dotenv import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()

ENDPOINT  = os.environ["NYC_311_ENDPOINT"]
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
                time.sleep(wait)
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
