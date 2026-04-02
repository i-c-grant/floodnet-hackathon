# FloodNet NYC — Oct 30 2025 Storm Map

An animated, interactive map of the October 30, 2025 NYC rainstorm, combining three real-time data sources into a single deck.gl visualization:

- **FloodNet sensors** — ultrasonic depth sensors measuring street flooding across NYC
- **NYC 311 complaints** — flood- and sewer-related service requests filed during the storm
- **NOAA MRMS radar** — Vertically Integrated Liquid (VIL) raster showing rain band intensity

The animated map is published at: **https://i-c-grant.github.io/floodnet-hackathon/**

---

## Quick start

### Prerequisites

- Docker
- GNU Make
- A `.env` file in the repo root (see [Environment variables](#environment-variables))

### Build and generate the map

```bash
# Ingest Oct 30 data only and generate the map (~2 min, MRMS cached after first run)
make run-oct30

# Open the map
open output/storm_oct30.html
```

### Publish to GitHub Pages

```bash
make publish   # copies output/storm_oct30.html → docs/index.html
git push
```

### Other useful commands

| Command | Description |
|---|---|
| `make run` | Full historical ingest (2019 → today) + map |
| `make run-oct30` | Oct 30 storm only + map (faster) |
| `make map` | Re-generate map from existing DB data |
| `make pipeline-oct30` | Re-ingest Oct 30 data without rebuilding image |
| `make mrms-force` | Force re-download of MRMS radar data from S3 |
| `make build` | Rebuild the Docker image |

---

## Environment variables

Create a `.env` file in the repo root:

```
FLOODNET_SOCRATA_EVENTS_ENDPOINT=https://data.cityofnewyork.us/resource/<id>.json
FLOODNET_SOCRATA_DEPLOYMENT_METADATA_ENDPOINT=https://data.cityofnewyork.us/resource/<id>.json
NYC_311_ENDPOINT=https://data.cityofnewyork.us/resource/erm2-nwe9.json
FLOODNET_SOCRATA_APP_TOKEN=<your_socrata_app_token>
```

App tokens are free from [NYC Open Data](https://data.cityofnewyork.us/profile/app_tokens).

---

## How it works

### Pipeline (`src/pipeline.py`)

The pipeline is the single entry point for all data ingestion. It runs inside Docker and writes everything to a DuckDB database at `output/floodnet.duckdb`.

1. **Fetch FloodNet events** — queries the FloodNet Socrata API for flood sensor events in the given date range, joins sensor deployment metadata (location, borough, tidal influence)
2. **Cluster storms** — groups sensor events into city-level storms using a greedy temporal merge: a new storm begins when there's a gap of more than 3 hours between any sensor activity
3. **Fetch 311 complaints** — for each storm window, queries NYC Open Data for flood- and sewer-related 311 complaints, filtering to 9 relevant descriptor categories
4. **Ingest MRMS radar** — lists NOAA MRMS VIL files on AWS S3, downloads one frame every 5 minutes for the storm window, parses the GRIB2 format, and clips to the NYC bounding box. Skips download if data is already in the DB (use `--force-download` to re-fetch)

All data is stored as named tables in `output/floodnet.duckdb`.

### Map generation (`src/generate_map.py`)

Reads from the DuckDB database, looks up the storm that started on `STORM_DATE` (October 30, 2025), and injects all data as JSON into `src/map_template.html`. The output is a single self-contained HTML file with no external dependencies beyond the deck.gl CDN script.

MRMS frames are pre-rendered to base64-encoded PNGs using a Brewer Blues colormap (transparent below 4 kg/m², ramping to 85% opacity at 15 kg/m²).

### Map (`src/map_template.html`)

A [deck.gl](https://deck.gl) 8.9.x visualization with:

- **CartoDB Positron** basemap tiles
- **ScatterplotLayer** for flood sensors — dot size and color encode flood depth in real time
- **BitmapLayer** for MRMS radar — nearest-neighbour frame lookup per animation tick
- **IconLayer (×2)** for 311 complaints — yellow squares with black borders; each complaint is shown for 15 minutes after it was filed, then fades over 3 minutes

The animation covers 1:30 PM – 11:00 PM ET on October 30, 2025.

---

## Project structure

```
├── src/
│   ├── pipeline.py          # Orchestrates all ingestion; entry point for make pipeline*
│   ├── query_floods.py      # FloodNet Socrata API functions
│   ├── query_311.py         # NYC 311 Socrata API functions
│   ├── query_mrms.py        # NOAA MRMS S3 download + GRIB2 parsing functions
│   ├── generate_map.py      # Reads DB, renders map template → output/storm_oct30.html
│   └── map_template.html    # deck.gl map template with __PLACEHOLDER__ injection points
├── docker/
│   └── Dockerfile
├── docs/
│   └── index.html           # GitHub Pages (copy of output/storm_oct30.html)
├── output/                  # Generated files (gitignored except docs/)
│   ├── floodnet.duckdb
│   └── storm_oct30.html
├── Makefile
└── .env                     # Not committed; see Environment variables above
```

---

## Data sources

| Source | License |
|---|---|
| [FloodNet NYC flood sensors](https://data.cityofnewyork.us/Environment/FloodNet-Flood-Sensor-Readings/aq7i-eu5q) | NYC Open Data |
| [NYC 311 service requests](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9) | NYC Open Data |
| [NOAA MRMS (AWS Open Data)](https://registry.opendata.aws/noaa-mrms-pds/) | NOAA public domain |
