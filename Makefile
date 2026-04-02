.PHONY: build run run-oct30 map mrms-force pipeline pipeline-oct30 publish gif gif-build

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

# Build the GIF capture image
gif-build:
	docker build -t floodnet-gif -f docker/Dockerfile.gif .

# Capture animated GIF from the generated map (14:00–17:00 ET, 15 s)
gif: gif-build
	docker run --rm \
		-v $(PWD)/output:/app/output \
		-v $(PWD)/scripts:/app/scripts \
		floodnet-gif python /app/scripts/capture_gif.py
