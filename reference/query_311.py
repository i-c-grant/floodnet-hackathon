import urllib.parse
from datetime import datetime
import geopandas as gpd
import pandas as pd
import requests

base_query = f"""
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
  caseless_contains(descriptor, "flood") AND NOT caseless_contains(descriptor, "lamp")
  AND (created_date BETWEEN "{start_time}" AND "{end_time}")
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
ORDER BY created_date DESC
""".strip()
base_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
page_size = 1000
offset = 0
rows = []

# Loop through query results one page_size at a time to get around the API limits
while True:
    query = f"{base_query}\nLIMIT {page_size}\nOFFSET {offset}"
    url = f"{base_url}?$query={urllib.parse.quote_plus(query)}"
    batch = requests.get(url, timeout=30).json()

    if not batch:
        break

    rows.extend(batch)

    if len(batch) < page_size:
        break

    offset += page_size

# Create geojson object from json rows returned by API
nyc311_gdf = gpd.GeoDataFrame(
    rows,
    geometry=gpd.points_from_xy(
        [float(r["longitude"]) for r in rows],
        [float(r["latitude"]) for r in rows]
        ),
    crs="EPSG:4326",
)

#
nyc311_gdf['created_date'] = pd.to_datetime(nyc311_gdf.created_date).dt.tz_localize('America/New_York')
nyc311_gdf['closed_date'] = pd.to_datetime(nyc311_gdf.closed_date).dt.tz_localize('America/New_York', ambiguous=True)

print(nyc311_gdf.shape)
# nyc311_gdf.head()
