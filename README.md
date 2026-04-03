# FloodNet NYC — Oct 30 2025 Storm Map

An animated, interactive map of the October 30, 2025 NYC rainstorm, combining three data sources into a single deck.gl visualization:

- **FloodNet sensors** — ultrasonic depth sensors measuring street flooding across NYC
- **NYC 311 complaints** — flood- and sewer-related service requests filed during the storm
- **NOAA MRMS radar** — Vertically Integrated Liquid (VIL) raster showing rain band intensity

The map is published at: **https://i-c-grant.github.io/floodnet-hackathon/**

---

## Quick start

### Prerequisites

- Docker
- GNU Make
- A `.env` file in the repo root (see [Environment variables](#environment-variables))

### Generate the map

```bash
# Ingest Oct 30 data and generate the map (~2 min, MRMS cached after first run)
make oct30

# Open the map
open output/storm_oct30.html
```

### Other commands

| Command | Description |
|---|---|
| `make oct30` | Ingest Oct 30 storm data + generate map |
| `make map` | Regenerate map from existing DB (no ingest) |
| `make mp4` | Capture a 15-second MP4 of the animation |
| `make publish` | Copy map to `docs/` for GitHub Pages |
| `make ingest` | Full historical ingest, 2019 → today — **very slow** |
| `make build-image` | Rebuild the Docker image |

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

`make oct30` runs `pipeline.py` for the Oct 30 storm window to ingest 311, FloodNet, and MRMS data for that event. Then, `generate_map.py` injects the data into `map_template.html` and produces a self-contained [deck.gl](https://deck.gl) 8.9.x HTML file. The animation covers 1:30 PM – 11:00 PM ET on October 30, 2025.

Though this project covers Oct. 30, 2025, the pipeline also works for other events. `make ingest` pulls all FloodNet data since 2019, clusters sensor-level events into city-level storms, and fetches 311 and MRMS data for each one (this is very slow for MRMS data). `make map` renders whichever storm matches `STORM_DATE` in `generate_map.py`, so retargeting is a one-line change. This should allow for quick visualizations of future events.

---

## Project structure

```
├── src/
│   ├── pipeline.py          # Orchestrates all ingestion (make oct30 / make ingest)
│   ├── query_floods.py      # FloodNet Socrata API functions
│   ├── query_311.py         # NYC 311 Socrata API functions
│   ├── query_mrms.py        # NOAA MRMS S3 download + GRIB2 parsing functions
│   ├── generate_map.py      # Reads DB, renders map template → output/storm_oct30.html
│   └── map_template.html    # deck.gl map template with __PLACEHOLDER__ injection points
├── scripts/
│   └── capture_mp4.py       # Playwright + ffmpeg MP4 capture (make mp4)
├── docker/
│   ├── Dockerfile           # Main image (pipeline + map generation)
│   └── Dockerfile.generate_mp4  # MP4 capture image (Playwright + ffmpeg)
├── docs/
│   └── index.html           # GitHub Pages (copy of output/storm_oct30.html)
├── output/                  # Generated files (gitignored except docs/)
│   ├── floodnet.duckdb
│   ├── storm_oct30.html
│   └── storm_oct30.mp4
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
