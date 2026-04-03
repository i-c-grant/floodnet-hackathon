.PHONY: oct30 map mp4 publish ingest build-image _build-mp4

IMAGE      := floodnet-hackathon
RUN        := docker run --rm --env-file .env -v $(PWD)/output:/app/output $(IMAGE)

OCT30_START := 2025-10-30
OCT30_END   := 2025-10-31
FULL_START  := 2019-01-01
FULL_END    := $(shell date +%Y-%m-%d)

# Ingest Oct 30 storm data + regenerate map (normal workflow)
oct30: build-image
	$(RUN) src/pipeline.py --start $(OCT30_START) --end $(OCT30_END)
	$(RUN) src/generate_map.py

# Regenerate map HTML from existing DB (no ingest)
map: build-image
	$(RUN) src/generate_map.py

# Capture MP4 from the generated map
mp4: map _build-mp4
	docker run --rm \
		-v $(PWD)/output:/app/output \
		-v $(PWD)/scripts:/app/scripts \
		floodnet-mp4 python /app/scripts/capture_mp4.py

# Copy generated map to docs/ for GitHub Pages
publish: map
	cp output/storm_oct30.html docs/index.html

# Full historical ingest (2019 → today) — very slow, downloads years of MRMS radar data
ingest: build-image
	$(RUN) src/pipeline.py --start $(FULL_START) --end $(FULL_END)

# Rebuild the Docker image (required after editing src/ or docker/Dockerfile)
build-image:
	docker build -t $(IMAGE) -f docker/Dockerfile .

# ── Internal targets ──────────────────────────────────────────────────

_build-mp4:
	docker build -t floodnet-mp4 -f docker/Dockerfile.generate_mp4 .
