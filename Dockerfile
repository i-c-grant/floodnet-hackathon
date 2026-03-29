FROM python:3.11-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_SYSTEM_PYTHON=1

# pyogrio ships its own GDAL binaries, shapely 2.x ships its own GEOS
# so no system-level geo dependencies needed
RUN uv pip install --no-cache \
    geopandas \
    pyogrio \
    shapely \
    folium \
    matplotlib \
    pandas \
    requests \
    python-dotenv \
    branca \
    duckdb \
    tqdm \
    streamlit \
    plotly

COPY . .

ENTRYPOINT ["python"]
