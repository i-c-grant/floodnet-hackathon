FROM python:3.11-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_SYSTEM_PYTHON=1
ENV PYTHONPATH=/app/src

# eccodes C library — required for GRIB2 decoding (MRMS VIL)
RUN apt-get update \
 && apt-get install -y --no-install-recommends libeccodes-dev \
 && rm -rf /var/lib/apt/lists/*

# pyogrio ships its own GDAL binaries, shapely 2.x ships its own GEOS
# so no system-level geo dependencies needed
RUN uv pip install --no-cache \
    geopandas \
    pyogrio \
    shapely \
    matplotlib \
    pandas \
    requests \
    python-dotenv \
    duckdb \
    tqdm \
    eccodes

COPY . .

ENTRYPOINT ["python"]
