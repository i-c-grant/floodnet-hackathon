.PHONY: build run map mrms mrms-force floods nyc311 pipeline publish

IMAGE := floodnet-hackathon
RUN   := docker run --rm --env-file .env -v $(PWD)/output:/app/output $(IMAGE)

# Rebuild the image (required after editing map_template.html or any Python file)
build:
	docker build -t $(IMAGE) .

# Full pipeline: ingest all data sources then generate the map
run: build mrms nyc311 floods map

# Generate the map (requires build first if template changed)
map: build
	$(RUN) generate_map.py

# MRMS ingest (skip download if data already present)
mrms:
	$(RUN) query_mrms.py

# MRMS ingest, force fresh S3 download
mrms-force: build
	$(RUN) query_mrms.py --force-download

# FloodNet sensor ingest
floods:
	$(RUN) query_floods.py

# NYC 311 ingest
nyc311:
	$(RUN) query_311.py

# Copy generated map to docs/ for GitHub Pages
publish: map
	cp output/storm_oct30.html docs/index.html

# Full pipeline script
pipeline:
	$(RUN) pipeline.py
