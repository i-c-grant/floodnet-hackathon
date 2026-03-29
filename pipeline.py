"""
Orchestration pipeline: fetch FloodNet events + NYC 311 complaints for a date
range and load both into a persistent DuckDB database.

Usage:
    python pipeline.py --start 2024-09-01 --end 2024-09-30
    python pipeline.py --start 2023-01-01 --end 2026-03-28 --storm-gap-hours 6

Database: output/floodnet.duckdb
Tables:
    storm_events    — clustered storm periods (greedy temporal merge of sensor events)
    flood_events    — FloodNet sensor events joined with deployment metadata + storm_id
    complaints_311  — NYC 311 flood-related complaints + storm_id
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from query_floods import fetch_events, fetch_metadata
from query_311 import fetch_311

load_dotenv()

DB_PATH = Path("output/floodnet.duckdb")
LOG_PATH = Path("output/pipeline.log")


def setup_logging() -> logging.Logger:
    """Configure root logger with file and terminal handlers.

    Creates output/ if it doesn't exist. File handler captures DEBUG and above
    with full timestamp/level formatting. Terminal handler captures INFO and above
    with plain message formatting so tqdm progress bars render cleanly.

    Returns:
        The configured root logger.
    """
    Path("output").mkdir(exist_ok=True)
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s — %(message)s")

    # File handler: DEBUG and above (full detail)
    fh = logging.FileHandler(LOG_PATH, mode="a")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Terminal handler: INFO and above (milestones only; tqdm covers progress)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter("%(message)s"))

    log.addHandler(fh)
    log.addHandler(sh)

    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return log


def cluster_storms(df: pd.DataFrame, gap_hours: float = 3.0) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Group sensor-level flood events into city-level storms via greedy interval merge.

    Events are sorted by flood_start_time. A new storm begins whenever the next
    event's start is more than gap_hours after the current storm's latest
    flood_end_time. Events with null start times are always assigned to a new storm.

    Args:
        df: DataFrame of flood events with flood_start_time, flood_end_time,
            and sensor_id columns.
        gap_hours: Maximum hours of inactivity between events before a new storm
            is declared. Defaults to 3.0.

    Returns:
        A tuple of (events_df, storms_df). events_df is df with a storm_id column
        added. storms_df has one row per storm with columns storm_id, storm_start,
        storm_end, event_count, and sensor_count.
    """
    gap = pd.Timedelta(hours=gap_hours)
    df = df.sort_values("flood_start_time").reset_index(drop=True)

    storm_id_col = []
    storm_records: dict[int, dict] = {}
    current_id = 0
    current_end: pd.Timestamp | None = None

    for _, row in df.iterrows():
        start = row["flood_start_time"]
        end = row["flood_end_time"]

        if current_end is None or pd.isna(start) or start > current_end + gap:
            current_id += 1
            storm_records[current_id] = {
                "storm_id": current_id,
                "storm_start": start,
                "storm_end": end,
                "sensors": set(),
                "event_count": 0,
            }
            current_end = end
        else:
            if pd.notna(end) and end > current_end:
                current_end = end
                storm_records[current_id]["storm_end"] = current_end

        storm_records[current_id]["sensors"].add(row.get("sensor_id"))
        storm_records[current_id]["event_count"] += 1
        storm_id_col.append(current_id)

    df = df.copy()
    df["storm_id"] = storm_id_col

    storms_df = pd.DataFrame([
        {
            "storm_id": s["storm_id"],
            "storm_start": s["storm_start"],
            "storm_end": s["storm_end"],
            "event_count": s["event_count"],
            "sensor_count": len(s["sensors"]),
        }
        for s in storm_records.values()
    ])

    return df, storms_df


def upsert_storms(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Create storm_events table if absent and upsert storm records.

    Args:
        con: Active DuckDB connection.
        df: DataFrame with columns storm_id, storm_start, storm_end,
            event_count, sensor_count.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS storm_events (
            storm_id      INTEGER PRIMARY KEY,
            storm_start   TIMESTAMP,
            storm_end     TIMESTAMP,
            event_count   INTEGER,
            sensor_count  INTEGER
        )
    """)
    con.execute("""
        INSERT OR REPLACE INTO storm_events
        SELECT storm_id, storm_start, storm_end, event_count, sensor_count
        FROM df
    """)


def upsert_floods(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Create flood_events table if absent and upsert sensor event records.

    Stringifies flood_profile_depth_inches and flood_profile_time_secs list
    columns before insert. Keyed on (sensor_id, flood_start_time).

    Args:
        con: Active DuckDB connection.
        df: Merged DataFrame of flood events and sensor metadata, including
            a storm_id column from cluster_storms.
    """
    # Stringify list-valued profile columns
    for col in ["flood_profile_depth_inches", "flood_profile_time_secs"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    con.execute("""
        CREATE TABLE IF NOT EXISTS flood_events (
            sensor_id               VARCHAR,
            sensor_name             VARCHAR,
            flood_start_time        TIMESTAMP,
            flood_end_time          TIMESTAMP,
            max_depth_inches        DOUBLE,
            onset_time_mins         DOUBLE,
            drain_time_mins         DOUBLE,
            duration_mins           DOUBLE,
            duration_above_4_inches_mins  DOUBLE,
            duration_above_12_inches_mins DOUBLE,
            duration_above_24_inches_mins DOUBLE,
            flood_profile_depth_inches    VARCHAR,
            flood_profile_time_secs       VARCHAR,
            -- metadata columns
            date_installed          TIMESTAMP,
            tidally_influenced      VARCHAR,
            street_name             VARCHAR,
            borough                 VARCHAR,
            zipcode                 VARCHAR,
            community_board         VARCHAR,
            council_district        VARCHAR,
            census_tract            VARCHAR,
            nta                     VARCHAR,
            latitude                DOUBLE,
            longitude               DOUBLE,
            lowest_point_height_delta_inches DOUBLE,
            storm_id                INTEGER,
            PRIMARY KEY (sensor_id, flood_start_time)
        )
    """)

    con.execute("""
        INSERT OR REPLACE INTO flood_events
        SELECT
            sensor_id, sensor_name,
            flood_start_time, flood_end_time,
            max_depth_inches, onset_time_mins, drain_time_mins, duration_mins,
            duration_above_4_inches_mins,
            duration_above_12_inches_mins,
            duration_above_24_inches_mins,
            flood_profile_depth_inches,
            flood_profile_time_secs,
            date_installed, tidally_influenced,
            street_name, borough, zipcode,
            community_board, council_district, census_tract, nta,
            latitude, longitude,
            lowest_point_height_delta_inches,
            storm_id
        FROM df
    """)


def upsert_311(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Create complaints_311 table if absent and upsert 311 complaint records.

    Fills missing columns with None before insert, as the 311 API does not
    always return every field. Keyed on unique_key.

    Args:
        con: Active DuckDB connection.
        df: DataFrame of 311 complaints including a storm_id column.
    """
    # Ensure all expected columns exist (API doesn't always return every field)
    timestamp_cols = ["created_date", "closed_date", "due_date", "resolution_action_updated_date"]
    varchar_cols = ["unique_key", "agency", "agency_name", "complaint_type", "descriptor",
                    "incident_zip", "street_name", "city", "status", "resolution_description", "borough"]
    for col in timestamp_cols + varchar_cols:
        if col not in df.columns:
            df[col] = None

    con.execute("""
        CREATE TABLE IF NOT EXISTS complaints_311 (
            unique_key              VARCHAR PRIMARY KEY,
            created_date            TIMESTAMP,
            closed_date             TIMESTAMP,
            agency                  VARCHAR,
            agency_name             VARCHAR,
            complaint_type          VARCHAR,
            descriptor              VARCHAR,
            incident_zip            VARCHAR,
            street_name             VARCHAR,
            city                    VARCHAR,
            status                  VARCHAR,
            due_date                TIMESTAMP,
            resolution_description  VARCHAR,
            resolution_action_updated_date TIMESTAMP,
            borough                 VARCHAR,
            latitude                DOUBLE,
            longitude               DOUBLE,
            storm_id                INTEGER
        )
    """)

    con.execute("""
        INSERT OR REPLACE INTO complaints_311
        SELECT
            unique_key, created_date, closed_date,
            agency, agency_name, complaint_type, descriptor,
            incident_zip, street_name, city, status, due_date,
            resolution_description, resolution_action_updated_date,
            borough, latitude, longitude,
            storm_id
        FROM df
    """)


def main():
    """Entry point: parse args, run the full ingestion pipeline, write to DuckDB."""
    log = setup_logging()

    parser = argparse.ArgumentParser(description="FloodNet + 311 ingestion pipeline")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--storm-gap-hours", type=float, default=3.0,
                        help="Hours of inactivity between sensor events before a new storm is declared (default: 3)")
    args = parser.parse_args()

    for val, name in [(args.start, "start"), (args.end, "end")]:
        try:
            datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            log.error(f"--{name} must be YYYY-MM-DD, got '{val}'")
            sys.exit(1)

    log.info(f"=== Pipeline start: {args.start} → {args.end} (storm gap: {args.storm_gap_hours}h) ===")
    con = duckdb.connect(str(DB_PATH))

    # --- FloodNet ---
    events = fetch_events(args.start, args.end)
    if events.empty:
        log.info("No flood events found, exiting.")
        con.close()
        return

    metadata = fetch_metadata()
    merged = events.merge(metadata, on="sensor_id", how="left", suffixes=("", "_meta"))
    merged = merged.drop(columns=["sensor_name_meta"], errors="ignore")

    # --- Storm clustering ---
    merged, storms = cluster_storms(merged, gap_hours=args.storm_gap_hours)
    log.info(f"{len(storms)} storms identified from {len(merged)} sensor events")

    upsert_storms(con, storms)
    upsert_floods(con, merged)
    flood_total = con.execute("SELECT COUNT(*) FROM flood_events").fetchone()[0]
    log.info(f"flood_events: {flood_total} total rows in DB")

    # --- 311: one query per storm ---
    valid_storms = storms.dropna(subset=["storm_start", "storm_end"])
    log.info(f"Fetching 311 for {len(valid_storms)} storms...")
    all_complaints = []

    with logging_redirect_tqdm():
        for _, storm in tqdm(valid_storms.iterrows(), total=len(valid_storms), unit="storm",
                             desc="311 queries", dynamic_ncols=True):
            # FloodNet timestamps are UTC; 311 created_date is Eastern Time.
            # Convert storm window from UTC to ET before querying 311.
            def _utc_to_et(ts: pd.Timestamp) -> str:
                return (ts.tz_localize("UTC")
                          .tz_convert(ZoneInfo("America/New_York"))
                          .tz_localize(None)
                          .isoformat())

            start_dt = _utc_to_et(storm["storm_start"] - pd.Timedelta(minutes=30))
            end_dt   = _utc_to_et(storm["storm_end"])
            log.debug(f"Storm {storm['storm_id']}: {start_dt} → {end_dt} ({storm['event_count']} sensor events)")
            batch = fetch_311(start_dt, end_dt)
            if not batch.empty:
                batch["storm_id"] = int(storm["storm_id"])
                all_complaints.append(batch)

    if all_complaints:
        complaints = pd.concat(all_complaints).drop_duplicates(subset=["unique_key"], keep="last")
        log.info(f"{len(complaints)} unique 311 complaints across all storms")
        upsert_311(con, complaints)
        complaints_total = con.execute("SELECT COUNT(*) FROM complaints_311").fetchone()[0]
        log.info(f"complaints_311: {complaints_total} total rows in DB")
    else:
        log.info("No 311 complaints found for any storm window")

    con.close()
    log.info(f"=== Pipeline complete. Database: {DB_PATH.resolve()} ===")


if __name__ == "__main__":
    main()
