.PHONY: build run run-oct30 map mrms-force pipeline pipeline-oct30 publish

IMAGE      := floodnet-hackathon
RUN        := docker run --rm --env-file .env -v $(PWD)/output:/app/output $(IMAGE)

# Full historical date range (FloodNet launch → today)
FULL_START := 2019-01-01
FULL_END   := $(shell date +%Y-%m-%d)

# Oct 30 2025 storm only
OCT30_START := 2025-10-30
OCT30_END   := 2025-10-31

# Rebuild the image (required after editing src/ or docker/Dockerfile)
build:
	docker build -t $(IMAGE) -f docker/Dockerfile .

# Full pipeline: ingest all historical data then generate the map
run: build pipeline map

# Oct 30 storm only (faster; skips full historical ingest)
run-oct30: build pipeline-oct30 map

# Generate the map (requires build first if template changed)
map: build
	$(RUN) src/generate_map.py

# Force re-download of MRMS data for the Oct 30 storm
mrms-force: build
	$(RUN) src/pipeline.py --start $(OCT30_START) --end $(OCT30_END) --force-download

# Full historical ingest into DuckDB (floods + 311 + MRMS + storm clustering)
pipeline:
	$(RUN) src/pipeline.py --start $(FULL_START) --end $(FULL_END)

# Oct 30 storm only ingest into DuckDB
pipeline-oct30:
	$(RUN) src/pipeline.py --start $(OCT30_START) --end $(OCT30_END)

# Copy generated map to docs/ for GitHub Pages
publish: map
	cp output/storm_oct30.html docs/index.html
