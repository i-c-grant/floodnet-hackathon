"""
FloodNet Analytics Dashboard — Streamlit + Plotly + Folium backed by DuckDB.

Run:
    streamlit run dashboard.py
    # or via Docker:
    docker compose up dashboard
"""

import ast
from pathlib import Path

import duckdb
import folium
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

DB_PATH = Path("output/floodnet.duckdb")
MIN_PLUVIAL_SENSORS = 5  # minimum unique pluvial (non-tidal) sensors to qualify as a storm

st.set_page_config(
    page_title="FloodNet NYC Dashboard",
    page_icon="🌊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)


def q(sql: str, params: list | None = None) -> pd.DataFrame:
    con = get_connection()
    if params:
        return con.execute(sql, params).df()
    return con.execute(sql).df()


@st.cache_data(ttl=300)
def load_date_range() -> tuple[pd.Timestamp, pd.Timestamp]:
    row = q(f"""
        WITH pluvial_counts AS (
            SELECT storm_id FROM flood_events
            WHERE tidally_influenced = 'No'
            GROUP BY storm_id HAVING COUNT(DISTINCT sensor_id) >= {MIN_PLUVIAL_SENSORS}
        )
        SELECT MIN(se.storm_start::DATE), MAX(se.storm_start::DATE)
        FROM storm_events se JOIN pluvial_counts pc ON pc.storm_id = se.storm_id
    """).iloc[0]
    return pd.Timestamp(row[0]), pd.Timestamp(row[1])


INTENSITY_CTE = f"""
WITH pluvial_counts AS (
    SELECT storm_id, COUNT(DISTINCT sensor_id) AS pluvial_sensor_count
    FROM flood_events
    WHERE tidally_influenced = 'No'
    GROUP BY storm_id
    HAVING COUNT(DISTINCT sensor_id) >= {MIN_PLUVIAL_SENSORS}
),
sensor_registry AS (
    SELECT sensor_id, MIN(date_installed) AS date_installed
    FROM flood_events
    WHERE date_installed IS NOT NULL
    GROUP BY sensor_id
),
storm_intensity AS (
    SELECT
        se.storm_id,
        se.sensor_count                                                         AS flooded_sensors,
        COUNT(DISTINCT sr.sensor_id)                                            AS deployed_sensors,
        100.0 * se.sensor_count / NULLIF(COUNT(DISTINCT sr.sensor_id), 0)      AS flood_intensity
    FROM storm_events se
    JOIN pluvial_counts pc ON pc.storm_id = se.storm_id
    LEFT JOIN sensor_registry sr ON sr.date_installed <= se.storm_start
    GROUP BY se.storm_id, se.storm_start, se.sensor_count
)
"""

# Reusable CTE fragment for queries that don't need the full INTENSITY_CTE
PLUVIAL_CTE = f"""
WITH pluvial_counts AS (
    SELECT storm_id FROM flood_events
    WHERE tidally_influenced = 'No'
    GROUP BY storm_id HAVING COUNT(DISTINCT sensor_id) >= {MIN_PLUVIAL_SENSORS}
)
"""


@st.cache_data(ttl=300)
def load_kpis(start: str, end: str) -> dict:
    storms = q(
        PLUVIAL_CTE + """
        SELECT COUNT(*) FROM storm_events se
        JOIN pluvial_counts pc ON pc.storm_id = se.storm_id
        WHERE se.storm_start >= ? AND se.storm_start::DATE <= ?""",
        [start, end],
    ).iloc[0, 0]
    avg_intensity = q(
        INTENSITY_CTE + """
        SELECT ROUND(AVG(si.flood_intensity), 1)
        FROM storm_intensity si
        JOIN storm_events se ON si.storm_id = se.storm_id
        WHERE se.storm_start >= ? AND se.storm_start::DATE <= ?
        """,
        [start, end],
    ).iloc[0, 0]
    complaints = q(
        PLUVIAL_CTE + """
        SELECT COUNT(*) FROM complaints_311 c
        JOIN storm_events se ON c.storm_id = se.storm_id
        JOIN pluvial_counts pc ON pc.storm_id = se.storm_id
        WHERE se.storm_start >= ? AND se.storm_start::DATE <= ?""",
        [start, end],
    ).iloc[0, 0]
    avg_c = round(complaints / storms, 1) if storms else 0
    return {"storms": storms, "avg_intensity": avg_intensity, "complaints": complaints, "avg_c": avg_c}


@st.cache_data(ttl=300)
def load_storm_timeline(start: str, end: str) -> pd.DataFrame:
    return q(
        INTENSITY_CTE + """
        SELECT
            strftime(se.storm_start, '%Y-%m')   AS month,
            COUNT(*)                            AS storm_count,
            ROUND(AVG(si.flood_intensity), 1)   AS avg_intensity
        FROM storm_events se
        JOIN storm_intensity si ON si.storm_id = se.storm_id
        WHERE se.storm_start >= ? AND se.storm_start::DATE <= ?
        GROUP BY 1
        ORDER BY 1
        """,
        [start, end],
    )


@st.cache_data(ttl=300)
def load_borough_floods(start: str, end: str) -> pd.DataFrame:
    # Per-borough flood intensity: avg across storms of (sensors flooded in borough /
    # sensors deployed in borough at storm time).
    return q(
        INTENSITY_CTE + f"""
        , borough_deployed AS (
            SELECT
                se.storm_id,
                fe_meta.borough,
                COUNT(DISTINCT fe_meta.sensor_id) AS borough_deployed_sensors
            FROM storm_events se
            JOIN (
                SELECT DISTINCT sensor_id, borough,
                       MIN(date_installed) OVER (PARTITION BY sensor_id) AS date_installed
                FROM flood_events WHERE borough IS NOT NULL AND borough != ''
            ) fe_meta ON fe_meta.date_installed <= se.storm_start
            GROUP BY se.storm_id, fe_meta.borough
        ),
        borough_flooded AS (
            SELECT
                fe.borough,
                se.storm_id,
                COUNT(DISTINCT fe.sensor_id)    AS flooded_sensors,
                AVG(fe.max_depth_inches)        AS avg_depth,
                MAX(fe.max_depth_inches)        AS max_depth
            FROM flood_events fe
            JOIN storm_events se ON fe.storm_id = se.storm_id
            JOIN storm_intensity si2 ON si2.storm_id = se.storm_id
            WHERE se.storm_start >= ? AND se.storm_start::DATE <= ?
              AND fe.borough IS NOT NULL AND fe.borough != ''
            GROUP BY fe.borough, se.storm_id
        )
        SELECT
            bf.borough,
            ROUND(AVG(100.0 * bf.flooded_sensors / NULLIF(bd.borough_deployed_sensors, 0)), 1) AS avg_intensity,
            AVG(bf.avg_depth)   AS avg_depth,
            MAX(bf.max_depth)   AS max_depth
        FROM borough_flooded bf
        JOIN borough_deployed bd ON bf.storm_id = bd.storm_id AND bf.borough = bd.borough
        GROUP BY bf.borough
        ORDER BY avg_intensity DESC
        """,
        [start, end],
    )


@st.cache_data(ttl=300)
def load_borough_311(start: str, end: str) -> pd.DataFrame:
    return q(
        PLUVIAL_CTE + """
        SELECT
            c.borough,
            COUNT(*) AS complaint_count
        FROM complaints_311 c
        JOIN storm_events se ON c.storm_id = se.storm_id
        JOIN pluvial_counts pc ON pc.storm_id = se.storm_id
        WHERE se.storm_start >= ? AND se.storm_start::DATE <= ?
          AND c.borough IS NOT NULL AND c.borough != ''
        GROUP BY c.borough
        ORDER BY complaint_count DESC
        """,
        [start, end],
    )


@st.cache_data(ttl=300)
def load_top_storms(start: str, end: str) -> pd.DataFrame:
    return q(
        INTENSITY_CTE + """
        SELECT
            se.storm_id,
            se.storm_start::DATE                                                AS date,
            ROUND(epoch(se.storm_end - se.storm_start) / 3600.0, 1)            AS duration_hrs,
            ROUND(si.flood_intensity, 1)                                        AS intensity_pct,
            se.sensor_count                                                     AS sensors_flooded,
            si.deployed_sensors,
            COUNT(c.unique_key)                                                 AS complaints_311
        FROM storm_events se
        JOIN storm_intensity si ON si.storm_id = se.storm_id
        LEFT JOIN complaints_311 c ON c.storm_id = se.storm_id
        WHERE se.storm_start >= ? AND se.storm_start::DATE <= ?
        GROUP BY se.storm_id, se.storm_start, se.storm_end, si.flood_intensity, se.sensor_count, si.deployed_sensors
        ORDER BY complaints_311 DESC
        LIMIT 20
        """,
        [start, end],
    )



@st.cache_data(ttl=300)
def load_descriptors(start: str, end: str) -> pd.DataFrame:
    return q(
        PLUVIAL_CTE + """
        SELECT
            c.descriptor,
            COUNT(*) AS count
        FROM complaints_311 c
        JOIN storm_events se ON c.storm_id = se.storm_id
        JOIN pluvial_counts pc ON pc.storm_id = se.storm_id
        WHERE se.storm_start >= ? AND se.storm_start::DATE <= ?
        GROUP BY c.descriptor
        ORDER BY count DESC
        LIMIT 15
        """,
        [start, end],
    )



@st.cache_data(ttl=300)
def load_depth_dist(start: str, end: str) -> pd.DataFrame:
    return q(
        PLUVIAL_CTE + """
        SELECT
            fe.max_depth_inches,
            fe.borough
        FROM flood_events fe
        JOIN storm_events se ON fe.storm_id = se.storm_id
        JOIN pluvial_counts pc ON pc.storm_id = se.storm_id
        WHERE se.storm_start >= ? AND se.storm_start::DATE <= ?
          AND fe.max_depth_inches IS NOT NULL
        """,
        [start, end],
    )


@st.cache_data(ttl=300)
def load_storm_list(start: str, end: str) -> pd.DataFrame:
    return q(
        INTENSITY_CTE + f"""
        SELECT
            se.storm_id,
            se.storm_start,
            se.storm_start::DATE                        AS date,
            se.sensor_count                             AS sensors_flooded,
            si.deployed_sensors,
            ROUND(si.flood_intensity, 1)                AS intensity_pct,
            COUNT(c.unique_key)                         AS complaints_311
        FROM storm_events se
        JOIN storm_intensity si ON si.storm_id = se.storm_id
        LEFT JOIN complaints_311 c ON c.storm_id = se.storm_id
        WHERE se.storm_start >= ? AND se.storm_start::DATE <= ?
        GROUP BY se.storm_id, se.storm_start, se.sensor_count, si.deployed_sensors, si.flood_intensity
        ORDER BY se.storm_start DESC
        """,
        [start, end],
    )


@st.cache_data(ttl=300)
def load_storm_flood_events(storm_id: int) -> pd.DataFrame:
    return q(
        "SELECT * FROM flood_events WHERE storm_id = ? AND latitude IS NOT NULL AND longitude IS NOT NULL",
        [storm_id],
    )


@st.cache_data(ttl=300)
def load_storm_complaints(storm_id: int) -> pd.DataFrame:
    return q(
        "SELECT * FROM complaints_311 WHERE storm_id = ? AND latitude IS NOT NULL AND longitude IS NOT NULL",
        [storm_id],
    )


@st.cache_data(ttl=300)
def load_quiet_sensors(storm_id: int) -> pd.DataFrame:
    """Sensors deployed at storm time that had no flood event during this storm."""
    return q("""
        WITH storm AS (
            SELECT storm_start FROM storm_events WHERE storm_id = ?
        ),
        deployed AS (
            SELECT DISTINCT ON (sensor_id)
                sensor_id, sensor_name, latitude, longitude, borough
            FROM flood_events
            WHERE date_installed <= (SELECT storm_start FROM storm)
              AND latitude IS NOT NULL AND longitude IS NOT NULL
        ),
        flooded AS (
            SELECT DISTINCT sensor_id FROM flood_events WHERE storm_id = ?
        )
        SELECT d.*
        FROM deployed d
        LEFT JOIN flooded f ON f.sensor_id = d.sensor_id
        WHERE f.sensor_id IS NULL
    """, [storm_id, storm_id])


# ---------------------------------------------------------------------------
# Map builder
# ---------------------------------------------------------------------------

def build_sensor_map(df: pd.DataFrame) -> str:
    center = [df["latitude"].mean(), df["longitude"].mean()]
    m = folium.Map(location=center, zoom_start=11, tiles="CartoDB positron")

    max_depth = df["max_depth"].max() or 1
    max_count = df["flood_count"].max() or 1

    for _, row in df.iterrows():
        ratio = min((row["avg_depth"] or 0) / max_depth, 1.0)
        r = int(255 * ratio)
        b = int(255 * (1 - ratio))
        color = f"#{r:02x}80{b:02x}"
        radius = 5 + 15 * (row["flood_count"] / max_count)

        popup_html = (
            f"<b>{row.get('sensor_name', row['sensor_id'])}</b><br>"
            f"Borough: {row.get('borough', '—')}<br>"
            f"Flood events: {row['flood_count']}<br>"
            f"Avg depth: {row['avg_depth']:.1f} in<br>"
            f"Max depth: {row['max_depth']:.1f} in"
        )
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            popup=folium.Popup(popup_html, max_width=240),
            tooltip=f"{row.get('sensor_name', row['sensor_id'])} — {row['flood_count']} events",
        ).add_to(m)

    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;background:white;padding:10px;
                border-radius:5px;border:1px solid #ccc;font-size:12px;z-index:1000;">
      <b>Avg depth</b><br>
      <span style="color:#0080ff">●</span> Low<br>
      <span style="color:#808080">●</span> Medium<br>
      <span style="color:#ff8000">●</span> High<br>
      <i>Circle size = flood count</i>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    return m._repr_html_()


def build_storm_map(floods: pd.DataFrame, complaints: pd.DataFrame) -> str:
    all_lats = list(floods["latitude"]) + list(complaints["latitude"].dropna())
    all_lons = list(floods["longitude"]) + list(complaints["longitude"].dropna())
    center = [sum(all_lats) / len(all_lats), sum(all_lons) / len(all_lons)]
    m = folium.Map(location=center, zoom_start=12, tiles="CartoDB positron")

    flood_layer = folium.FeatureGroup(name="Flood sensor events", show=True)
    complaints_layer = folium.FeatureGroup(name="311 complaints", show=True)

    # Flood events — blue→red by depth
    max_depth = floods["max_depth_inches"].max() or 1
    for _, row in floods.iterrows():
        depth = row.get("max_depth_inches") or 0
        ratio = min(depth / max_depth, 1.0)
        r = int(255 * ratio)
        b = int(255 * (1 - ratio))
        color = f"#{r:02x}80{b:02x}"
        start_str = pd.Timestamp(row["flood_start_time"]).strftime("%Y-%m-%d %H:%M") if pd.notna(row.get("flood_start_time")) else "—"
        popup_html = (
            f"<b>{row.get('sensor_name', row.get('sensor_id', ''))}</b><br>"
            f"Borough: {row.get('borough', '—')}<br>"
            f"Street: {row.get('street_name', '—')}<br>"
            f"Start: {start_str}<br>"
            f"Max depth: {depth:.1f} in<br>"
            f"Duration: {row.get('duration_mins', '—')} min"
        )
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=6 + 10 * ratio,
            color=color, fill=True, fill_color=color, fill_opacity=0.8,
            popup=folium.Popup(popup_html, max_width=250),
            tooltip=f"{row.get('sensor_name', '')} — {depth:.1f} in",
        ).add_to(flood_layer)

    # 311 complaints — pin markers
    for _, row in complaints.iterrows():
        created = row.get("created_date")
        created_str = pd.Timestamp(created).strftime("%Y-%m-%d %H:%M") if pd.notna(created) else "—"
        popup_html = (
            f"<b>{row.get('complaint_type', '')}</b><br>"
            f"Descriptor: {row.get('descriptor', '—')}<br>"
            f"Borough: {row.get('borough', '—')}<br>"
            f"Street: {row.get('street_name', '—')}<br>"
            f"Created: {created_str}<br>"
            f"Resolution: {str(row.get('resolution_description', '—'))[:120]}"
        )
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            icon=folium.DivIcon(
                html='<div style="width:9px;height:9px;background:#e74c3c;'
                     'border:1.5px solid white;border-radius:50%;'
                     'box-shadow:0 0 2px rgba(0,0,0,0.4);"></div>',
                icon_size=(9, 9),
                icon_anchor=(4, 4),
            ),
            popup=folium.Popup(popup_html, max_width=260),
            tooltip=row.get("descriptor", "311 complaint"),
        ).add_to(complaints_layer)

    flood_layer.add_to(m)
    complaints_layer.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)

    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;background:white;padding:10px;
                border-radius:5px;border:1px solid #ccc;font-size:12px;z-index:1000;">
      <b>Flood sensors</b><br>
      <span style="color:#0080ff">●</span> Shallow &nbsp;
      <span style="color:#ff8000">●</span> Deep<br>
      <i>Size = depth</i><br><br>
      <b>311 complaints</b><br>
      <span style="display:inline-block;width:9px;height:9px;background:#e74c3c;
        border:1.5px solid white;border-radius:50%;box-shadow:0 0 2px rgba(0,0,0,0.4);
        vertical-align:middle;"></span> Individual complaint
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    return m._repr_html_()


def build_animation_df(floods: pd.DataFrame) -> pd.DataFrame:
    """Expand flood profile arrays into a per-(sensor, 5-min bin) dataframe."""
    rows = []
    for _, event in floods.iterrows():
        try:
            depths = ast.literal_eval(event["flood_profile_depth_inches"])
            times_secs = ast.literal_eval(event["flood_profile_time_secs"])
        except (ValueError, SyntaxError, TypeError):
            continue
        if not depths or not times_secs:
            continue
        start = pd.Timestamp(event["flood_start_time"])
        for t_sec, depth in zip(times_secs, depths):
            rows.append({
                "sensor_id": event["sensor_id"],
                "sensor_name": event.get("sensor_name") or event["sensor_id"],
                "lat": event["latitude"],
                "lon": event["longitude"],
                "borough": event.get("borough", ""),
                "timestamp": start + pd.Timedelta(seconds=float(t_sec)),
                "depth": float(depth),
            })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["time_bin"] = df["timestamp"].dt.floor("5min")
    return (
        df.groupby(["sensor_id", "sensor_name", "lat", "lon", "borough", "time_bin"], as_index=False)
        ["depth"].max()
    )


def build_storm_animation(anim_df: pd.DataFrame, complaints: pd.DataFrame,
                          quiet_sensors: pd.DataFrame) -> go.Figure:
    """Build an animated Plotly Scattermapbox figure for a storm."""
    all_lats = (list(anim_df["lat"]) + list(complaints["latitude"].dropna())
                + list(quiet_sensors["latitude"].dropna()))
    all_lons = (list(anim_df["lon"]) + list(complaints["longitude"].dropna())
                + list(quiet_sensors["longitude"].dropna()))
    center_lat = sum(all_lats) / len(all_lats)
    center_lon = sum(all_lons) / len(all_lons)

    max_depth = anim_df["depth"].max() or 1
    time_steps = sorted(anim_df["time_bin"].unique())
    FADE_HRS = 1.5

    # Pre-parse complaint timestamps once.
    # 311 created_date is Eastern Time; convert to UTC to align with FloodNet sensor times.
    has_complaints = not complaints.empty
    if has_complaints:
        c_times = (pd.to_datetime(complaints["created_date"], errors="coerce")
                   .dt.tz_localize("America/New_York",
                                   ambiguous="infer",
                                   nonexistent="shift_forward")
                   .dt.tz_convert("UTC")
                   .dt.tz_localize(None))
        c_lat = complaints["latitude"].tolist()
        c_lon = complaints["longitude"].tolist()
        c_text = complaints.apply(
            lambda r: f"{r.get('descriptor', '')} — {r.get('borough', '')}", axis=1
        ).tolist()

    def complaints_trace_at(t: pd.Timestamp, show_legend: bool) -> go.Scattermapbox:
        if not has_complaints:
            return go.Scattermapbox(lat=[], lon=[], mode="markers",
                                    marker=dict(size=5), name="311 complaints",
                                    showlegend=show_legend)
        # Normalise both to tz-naive before arithmetic to avoid unit/tz mismatches
        t_norm = t.tz_localize(None) if t.tzinfo is not None else t
        c_norm = c_times.dt.tz_localize(None) if c_times.dt.tz is not None else c_times
        age_hrs = (t_norm - c_norm).dt.total_seconds() / 3600
        age_hrs = age_hrs.fillna(-1).values
        visible = (age_hrs >= 0) & (age_hrs < FADE_HRS)
        vis_idx = [i for i, v in enumerate(visible) if v]
        opacity = [(1.0 - age_hrs[i] / FADE_HRS) for i in vis_idx]
        colors = [f"rgba(231,76,60,{max(0.0, min(1.0, op)):.2f})" for op in opacity]
        return go.Scattermapbox(
            lat=[c_lat[i] for i in vis_idx],
            lon=[c_lon[i] for i in vis_idx],
            mode="markers",
            marker=dict(size=5, color=colors),
            text=[c_text[i] for i in vis_idx],
            hovertemplate="%{text}<extra>311 complaint</extra>",
            name="311 complaints",
            showlegend=show_legend,
        )

    # All deployed sensors (flooding + quiet) for the background gray layer
    flooding_locs = (anim_df[["sensor_id", "sensor_name", "lat", "lon", "borough"]]
                     .drop_duplicates("sensor_id"))
    quiet_locs = (quiet_sensors.rename(columns={"latitude": "lat", "longitude": "lon"})
                  [["sensor_id", "sensor_name", "lat", "lon", "borough"]]
                  if not quiet_sensors.empty else pd.DataFrame(columns=["sensor_id", "sensor_name", "lat", "lon", "borough"]))
    all_deployed = pd.concat([flooding_locs, quiet_locs], ignore_index=True).drop_duplicates("sensor_id")

    def background_trace(t: pd.Timestamp, show_legend: bool) -> go.Scattermapbox:
        """All deployed sensors not currently active — shown as gray."""
        active_ids = set(anim_df[anim_df["time_bin"] == t]["sensor_id"])
        bg = all_deployed[~all_deployed["sensor_id"].isin(active_ids)]
        return go.Scattermapbox(
            lat=bg["lat"].tolist(),
            lon=bg["lon"].tolist(),
            mode="markers",
            marker=dict(size=7, color="#aaaaaa", opacity=0.75),
            text=bg.apply(
                lambda r: f"{r.get('sensor_name', r['sensor_id'])} — {r.get('borough', '')}", axis=1
            ).tolist(),
            hovertemplate="%{text}<extra>No active flood</extra>",
            name="No active flood",
            showlegend=show_legend,
        )

    def sensor_trace(t: pd.Timestamp, show_colorbar: bool) -> go.Scattermapbox:
        subset = anim_df[anim_df["time_bin"] == t]
        depths = subset["depth"].tolist()
        sizes = [8 + 18 * min(d / max_depth, 1.0) for d in depths]
        hover = subset.apply(
            lambda r: f"{r['sensor_name']}<br>{r['depth']:.1f} in — {r['borough']}", axis=1
        ).tolist()
        return go.Scattermapbox(
            lat=subset["lat"].tolist(),
            lon=subset["lon"].tolist(),
            mode="markers",
            marker=dict(
                size=sizes,
                color=depths,
                colorscale=[[0, "#cccccc"], [1, "#08306b"]],
                cmin=0,
                cmax=max_depth,
                colorbar=dict(title="Depth (in)", thickness=12) if show_colorbar else None,
                showscale=show_colorbar,
            ),
            text=hover,
            hovertemplate="%{text}<extra>Flood sensor</extra>",
            name="Flood sensors",
            showlegend=True,
        )

    frames = [
        go.Frame(
            data=[background_trace(t, show_legend=False), sensor_trace(t, show_colorbar=False), complaints_trace_at(t, show_legend=False)],
            name=t.strftime("%Y-%m-%d %H:%M"),
        )
        for t in time_steps
    ]

    slider_steps = [
        {
            "args": [[f.name], {"frame": {"duration": 0, "redraw": True}, "mode": "immediate"}],
            "label": f.name,
            "method": "animate",
        }
        for f in frames
    ]

    fig = go.Figure(
        data=[background_trace(time_steps[0], show_legend=True),
              sensor_trace(time_steps[0], show_colorbar=True),
              complaints_trace_at(time_steps[0], show_legend=True)],
        frames=frames,
        layout=go.Layout(
            mapbox=dict(
                style="white-bg",
                layers=[{
                    "below": "traces",
                    "sourcetype": "raster",
                    "source": ["https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png"],
                    "sourceattribution": "© CartoDB",
                }],
                center={"lat": 40.7128, "lon": -73.9760},
                zoom=9.5,
            ),
            height=580,
            margin=dict(l=0, r=0, t=0, b=60),
            legend=dict(x=0.01, y=0.99, bgcolor="rgba(255,255,255,0.8)"),
            updatemenus=[{
                "type": "buttons",
                "showactive": False,
                "x": 0.01, "y": -0.05, "xanchor": "left",
                "buttons": [
                    {"label": "▶ Play", "method": "animate",
                     "args": [None, {"frame": {"duration": 75, "redraw": True}, "fromcurrent": True}]},
                    {"label": "⏸ Pause", "method": "animate",
                     "args": [[None], {"frame": {"duration": 0, "redraw": False}, "mode": "immediate"}]},
                ],
            }],
            sliders=[{
                "steps": slider_steps,
                "currentvalue": {"prefix": "Time: ", "font": {"size": 12}},
                "pad": {"t": 50, "b": 10},
                "x": 0.1, "len": 0.88,
            }],
        ),
    )
    return fig


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

st.title("🌊 FloodNet NYC — Flood Analytics Dashboard")

db_min, db_max = load_date_range()

with st.sidebar:
    st.header("Filters")
    start_date = st.date_input("From", value=db_min.date(), min_value=db_min.date(), max_value=db_max.date())
    end_date = st.date_input("To", value=db_max.date(), min_value=db_min.date(), max_value=db_max.date())
    if start_date > end_date:
        st.error("Start must be before end.")
        st.stop()
    st.caption(f"DB covers {db_min.date()} → {db_max.date()}")

start_str = str(start_date)
end_str = str(end_date)

# --- KPIs ---
kpis = load_kpis(start_str, end_str)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Storms", f"{kpis['storms']:,}")
c2.metric("Avg Flood Intensity", f"{kpis['avg_intensity']}%")
c3.metric("311 Complaints", f"{kpis['complaints']:,}")
c4.metric("Avg 311 / Storm", f"{kpis['avg_c']}")

# --- Storm Explorer ---
st.subheader("Storm Explorer")
storm_list = load_storm_list(start_str, end_str)
if storm_list.empty:
    st.info("No qualifying storms in this period.")
else:
    storm_list["storm_start_et"] = (
        pd.to_datetime(storm_list["storm_start"])
        .dt.tz_localize("UTC")
        .dt.tz_convert("America/New_York")
        .dt.strftime("%Y-%m-%d %H:%M ET")
    )
    storm_list["label"] = storm_list.apply(
        lambda r: (
            f"{r['storm_start_et']}  —  {r['sensors_flooded']}/{r['deployed_sensors']} sensors"
            f"  ({r['intensity_pct']}% intensity)  ·  {r['complaints_311']} complaints"
        ),
        axis=1,
    )
    labels = storm_list["label"].tolist()
    default_idx = next(
        (i for i, lbl in enumerate(labels) if "-10-30" in lbl),
        0,
    )
    selected_label = st.selectbox("Select a storm", labels, index=default_idx)
    selected_row = storm_list[storm_list["label"] == selected_label].iloc[0]
    storm_id = int(selected_row["storm_id"])

    s_floods = load_storm_flood_events(storm_id)
    s_complaints = load_storm_complaints(storm_id)

    col_s1, col_s2, col_s3 = st.columns(3)
    col_s1.metric(
        "Flood sensors triggered",
        f"{s_floods['sensor_id'].nunique()} out of {int(selected_row['deployed_sensors'])}",
    )
    col_s2.metric("311 complaints", len(s_complaints))
    col_s3.metric(
        "Max depth (in)",
        f"{s_floods['max_depth_inches'].max():.1f}" if not s_floods.empty else "—",
    )

    if not s_floods.empty or not s_complaints.empty:
        s_quiet = load_quiet_sensors(storm_id)
        anim_df = build_animation_df(s_floods)
        if not anim_df.empty:
            fig_anim = build_storm_animation(anim_df, s_complaints, s_quiet)
            st.plotly_chart(fig_anim, use_container_width=True)
        else:
            st.caption("No depth profile data available for this storm — showing static map.")
            storm_map_html = build_storm_map(s_floods, s_complaints)
            components.html(storm_map_html, height=580, scrolling=False)

st.divider()

# --- Storm timeline ---
st.subheader("Storm Activity Over Time")
timeline = load_storm_timeline(start_str, end_str)
if not timeline.empty:
    fig = px.bar(
        timeline,
        x="month",
        y="storm_count",
        color="avg_intensity",
        color_continuous_scale="Blues",
        labels={"month": "Month", "storm_count": "Storms", "avg_intensity": "Avg intensity (%)"},
        title="Monthly storm count (colour = avg flood intensity)",
    )
    fig.update_layout(coloraxis_colorbar_title="Intensity (%)", xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# --- Borough breakdown ---
st.subheader("Borough Breakdown")
col_l, col_r = st.columns(2)

with col_l:
    bf = load_borough_floods(start_str, end_str)
    if not bf.empty:
        fig2 = px.bar(
            bf.sort_values("avg_intensity"),
            x="avg_intensity",
            y="borough",
            color="avg_depth",
            color_continuous_scale="YlOrRd",
            orientation="h",
            labels={"avg_intensity": "Avg flood intensity (%)", "borough": "", "avg_depth": "Avg depth (in)"},
            title="Avg flood intensity by borough (% of deployed sensors flooded)",
        )
        st.plotly_chart(fig2, use_container_width=True)

with col_r:
    bc = load_borough_311(start_str, end_str)
    if not bc.empty:
        fig3 = px.bar(
            bc.sort_values("complaint_count"),
            x="complaint_count",
            y="borough",
            orientation="h",
            labels={"complaint_count": "Complaints", "borough": ""},
            title="311 complaints by borough",
            color="complaint_count",
            color_continuous_scale="Teal",
        )
        fig3.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig3, use_container_width=True)

st.divider()

# --- Top storms table ---
st.subheader("Top 20 Storms by 311 Complaint Volume")
top = load_top_storms(start_str, end_str)
if not top.empty:
    st.dataframe(
        top.rename(columns={
            "storm_id": "Storm ID",
            "date": "Date",
            "duration_hrs": "Duration (hrs)",
            "intensity_pct": "Intensity (%)",
            "sensors_flooded": "Sensors Flooded",
            "deployed_sensors": "Deployed Sensors",
            "complaints_311": "311 Complaints",
        }),
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# --- 311 breakdown ---
st.subheader("311 Complaint Breakdown")
col_a, col_b = st.columns(2)

desc = load_descriptors(start_str, end_str)
with col_a:
    if not desc.empty:
        fig4 = px.bar(
            desc.sort_values("count"),
            x="count",
            y="descriptor",
            orientation="h",
            labels={"count": "Complaints", "descriptor": ""},
            title="Top 15 descriptors",
            color="count",
            color_continuous_scale="Teal",
        )
        fig4.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig4, use_container_width=True)

with col_b:
    if not desc.empty:
        fig5 = px.pie(
            desc.head(8),
            names="descriptor",
            values="count",
            title="Top descriptor share",
        )
        st.plotly_chart(fig5, use_container_width=True)

st.divider()

# --- Depth distribution ---
st.subheader("Max Flood Depth Distribution by Borough")
depths = load_depth_dist(start_str, end_str)
if not depths.empty:
    fig6 = px.histogram(
        depths,
        x="max_depth_inches",
        color="borough",
        nbins=40,
        opacity=0.7,
        barmode="overlay",
        labels={"max_depth_inches": "Max depth (inches)", "borough": "Borough"},
        title="Distribution of max flood depth per sensor event",
    )
    fig6.update_layout(legend_title="Borough")
    st.plotly_chart(fig6, use_container_width=True)
