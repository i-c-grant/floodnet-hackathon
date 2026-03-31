.PHONY: build run map mrms mrms-force floods nyc311 pipeline dashboard

# Rebuild the image (required after editing map_template.html or any Python file)
build:
	docker compose build

# Full pipeline: ingest all data sources then generate the map
run: build mrms nyc311 floods map

# Generate the map (requires build first if template changed)
map: build
	docker compose run --rm genmap

# MRMS ingest (skip download if data already present)
mrms:
	docker compose run --rm mrms

# MRMS ingest, force fresh S3 download
mrms-force: build
	docker compose run --rm mrms --force-download

# FloodNet sensor ingest
floods:
	docker compose run --rm floods

# NYC 311 ingest
nyc311:
	docker compose run --rm nyc311

# Full pipeline script
pipeline:
	docker compose run --rm pipeline

# Streamlit dashboard
dashboard:
	docker compose up dashboard
