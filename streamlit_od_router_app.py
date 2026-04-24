import io
import os
import time
import zipfile
import tempfile
from datetime import datetime

import pandas as pd
import geopandas as gpd
import polyline
import pytz
import requests
import streamlit as st
import pydeck as pdk
from shapely.geometry import LineString, MultiLineString, GeometryCollection
from shapely.ops import unary_union


st.set_page_config(page_title="STV OD Route Builder", layout="wide")

EASTERN_TZ = pytz.timezone("US/Eastern")
REQUIRED_COLUMNS = ["GEOID", "orig_LAT", "orig_LON", "dest_LAT", "dest_LON", "Trips"]
ARRIVAL_OPTIONS = [f"{hour:02d}:00:00" for hour in range(6, 19)]
WEEKDAY_OPTIONS = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]
MODE_OPTIONS = ["Auto", "Subway", "Bus"]


def apply_stv_theme():
    st.markdown(
        """
        <style>
        :root {
            --stv-navy: #0b1f3a;
            --stv-blue: #163f73;
            --stv-orange: #f36f21;
            --stv-light: #f5f7fa;
            --stv-gray: #5f6b7a;
        }

        .stApp {
            background: linear-gradient(180deg, #ffffff 0%, #f6f8fb 100%);
        }

        section[data-testid="stSidebar"] {
            background-color: #0b1f3a;
        }

        .stv-hero {
            background: linear-gradient(120deg, #0b1f3a 0%, #163f73 68%, #f36f21 100%);
            padding: 34px 38px;
            border-radius: 22px;
            color: white;
            margin-bottom: 24px;
            box-shadow: 0 10px 30px rgba(11, 31, 58, 0.18);
        }

        .stv-logo {
            font-size: 42px;
            line-height: 1;
            font-weight: 900;
            letter-spacing: -2px;
            margin-bottom: 10px;
        }

        .stv-kicker {
            color: #f36f21;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 1.8px;
            font-weight: 800;
            margin-bottom: 8px;
        }

        .stv-title {
            font-size: 34px;
            font-weight: 800;
            margin-bottom: 8px;
        }

        .stv-subtitle {
            font-size: 17px;
            color: #e8eef7;
            max-width: 950px;
        }

        .stv-card {
            background: white;
            border: 1px solid #e4e8ef;
            border-radius: 18px;
            padding: 20px 22px;
            box-shadow: 0 6px 22px rgba(11, 31, 58, 0.07);
            margin-bottom: 18px;
        }

        .stv-section-title {
            color: #0b1f3a;
            font-size: 22px;
            font-weight: 800;
            margin-top: 14px;
            margin-bottom: 6px;
            border-left: 6px solid #f36f21;
            padding-left: 12px;
        }

        div.stButton > button,
        div.stDownloadButton > button,
        button[kind="primary"] {
            border-radius: 999px !important;
            border: 1px solid #f36f21 !important;
            background-color: #f36f21 !important;
            color: white !important;
            font-weight: 700 !important;
        }

        div.stDownloadButton > button:hover,
        div.stButton > button:hover {
            background-color: #d95f17 !important;
            border-color: #d95f17 !important;
            color: white !important;
        }

        .stProgress > div > div > div > div {
            background-color: #f36f21;
        }

        [data-testid="stMetricValue"] {
            color: #0b1f3a;
            font-weight: 800;
        }

        hr {
            border: none;
            border-top: 1px solid #e4e8ef;
            margin: 24px 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_stv_header():
    st.markdown(
        """
        <div class="stv-hero">
            <div class="stv-logo">STV</div>
            <div class="stv-kicker">Infrastructure analysis tool</div>
            <div class="stv-title">OD Route Builder and Corridor Load Mapper</div>
            <div class="stv-subtitle">
                A planning workflow to translate origin destination demand into route shapefiles,
                loaded roadway segments, and corridor level trip patterns.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def stv_section(title: str):
    st.markdown(f'<div class="stv-section-title">{title}</div>', unsafe_allow_html=True)



@st.cache_data(show_spinner=False)
def sample_template_bytes() -> bytes:
    sample_df = pd.DataFrame(
        {
            "GEOID": ["360610001001", "360610001002"],
            "orig_LAT": [40.7580, 40.7527],
            "orig_LON": [-73.9855, -73.9772],
            "dest_LAT": [40.7128, 40.7306],
            "dest_LON": [-74.0060, -73.9352],
            "Trips": [125, 80],
        }
    )
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        sample_df.to_excel(writer, index=False, sheet_name="OD_Input")
    buffer.seek(0)
    return buffer.getvalue()


def convert_to_utc_timestamp(
    local_date_str: str,
    local_time_str: str,
    timezone_name: str = "US/Eastern",
) -> int:
    local_tz = pytz.timezone(timezone_name)
    naive_time = datetime.strptime(
        f"{local_date_str} {local_time_str}",
        "%Y-%m-%d %H:%M:%S",
    )
    local_time = local_tz.localize(naive_time, is_dst=None)
    utc_time = local_time.astimezone(pytz.utc)
    return int(utc_time.timestamp())


def get_route(
    origin_lat,
    origin_lon,
    dest_lat,
    dest_lon,
    mode,
    api_key,
    arrival_time=None,
    transit_mode=None,
):
    origin = f"{origin_lat},{origin_lon}"
    destination = f"{dest_lat},{dest_lon}"

    params = {
        "origin": origin,
        "destination": destination,
        "mode": mode,
        "key": api_key,
    }

    if arrival_time is not None:
        params["arrival_time"] = arrival_time

    if mode == "transit" and transit_mode:
        params["transit_mode"] = transit_mode

    response = requests.get(
        "https://maps.googleapis.com/maps/api/directions/json",
        params=params,
        timeout=60,
    )

    try:
        data = response.json()
    except Exception:
        return {
            "status": "REQUEST_FAILED",
            "error_message": "Invalid response from Google Maps API.",
        }

    if response.status_code != 200:
        data.setdefault("status", "REQUEST_FAILED")
        data.setdefault("error_message", f"HTTP {response.status_code}")

    return data


def parse_uploaded_table(uploaded_file) -> pd.DataFrame:
    file_name = uploaded_file.name.lower()
    if file_name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file, engine="openpyxl")


def validate_input_table(df: pd.DataFrame):
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError("Missing required columns: " + ", ".join(missing_columns))


def estimate_job_size(df: pd.DataFrame):
    """
    Estimate request count and give a simple caution level.
    Assumes roughly 1 Directions API request per usable OD pair.
    """
    usable_df = df.dropna(subset=["GEOID"]).copy()
    od_pairs = len(usable_df)
    estimated_requests = od_pairs

    if estimated_requests >= 5000:
        level = "high"
        message = "Large run. This may take a while and may create noticeable API charges."
    elif estimated_requests >= 1000:
        level = "medium"
        message = "Moderate to large run. Double check your API billing setup before running."
    elif estimated_requests >= 100:
        level = "low"
        message = "Medium run. Charges are usually manageable, but still worth checking."
    else:
        level = "minimal"
        message = "Small run. Still billed per request, but the overall cost is usually modest."

    return {
        "od_pairs": od_pairs,
        "estimated_requests": estimated_requests,
        "level": level,
        "message": message,
    }


def create_zipped_shapefile(gdf: gpd.GeoDataFrame, base_name: str) -> bytes:
    with tempfile.TemporaryDirectory() as temp_dir:
        shp_path = os.path.join(temp_dir, f"{base_name}.shp")
        gdf.to_file(shp_path, driver="ESRI Shapefile")

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for filename in os.listdir(temp_dir):
                full_path = os.path.join(temp_dir, filename)
                zip_file.write(full_path, arcname=filename)

        zip_buffer.seek(0)
        return zip_buffer.getvalue()


def build_loaded_segments(
    routes_gdf: gpd.GeoDataFrame,
    trips_field: str = "Trips",
    buffer_meters: float = 1.0,
) -> gpd.GeoDataFrame:
    """
    Creates a new line layer where shared route segments are split and Trips are summed.

    Notes:
    - This uses the route geometry returned by Google Directions.
    - It does not snap to an official roadway centerline file.
    - The midpoint buffer helps catch tiny geometry precision differences.
    """

    if routes_gdf.empty:
        raise ValueError("Route GeoDataFrame is empty.")

    routes = routes_gdf.copy()

    if trips_field not in routes.columns:
        raise ValueError(f"Trips field '{trips_field}' was not found.")

    routes[trips_field] = pd.to_numeric(routes[trips_field], errors="coerce").fillna(0)

    # Project to a metric CRS so the buffer is in meters
    projected_crs = routes.estimate_utm_crs()
    if projected_crs is None:
        projected_crs = "EPSG:3857"

    routes_proj = routes.to_crs(projected_crs)

    # Split overlapping lines into shared segment pieces
    merged = unary_union(routes_proj.geometry)

    if isinstance(merged, LineString):
        segment_geoms = [merged]
    elif isinstance(merged, MultiLineString):
        segment_geoms = list(merged.geoms)
    elif isinstance(merged, GeometryCollection):
        segment_geoms = [
            geom
            for geom in merged.geoms
            if isinstance(geom, LineString) and not geom.is_empty
        ]
    else:
        segment_geoms = []

    segments = gpd.GeoDataFrame(
        {"SegID": range(1, len(segment_geoms) + 1)},
        geometry=segment_geoms,
        crs=projected_crs,
    )

    if segments.empty:
        raise ValueError("No valid line segments were created.")

    # Create small buffers around each segment midpoint.
    # This is safer than intersecting full lines because it helps avoid double-counting
    # when a route only touches a segment at an endpoint.
    midpoint_gdf = segments.copy()
    midpoint_gdf["geometry"] = midpoint_gdf.geometry.interpolate(
        0.5,
        normalized=True,
    ).buffer(buffer_meters)

    joined = gpd.sjoin(
        midpoint_gdf[["SegID", "geometry"]],
        routes_proj[["GEOID", trips_field, "geometry"]],
        how="left",
        predicate="intersects",
    )

    summary = (
        joined.groupby("SegID")
        .agg(
            TotalTrips=(trips_field, "sum"),
            RouteCount=("GEOID", "count"),
        )
        .reset_index()
    )

    loaded_segments = segments.merge(summary, on="SegID", how="left")
    loaded_segments["TotalTrips"] = loaded_segments["TotalTrips"].fillna(0)
    loaded_segments["RouteCount"] = loaded_segments["RouteCount"].fillna(0).astype(int)
    loaded_segments["LenMile"] = loaded_segments.geometry.length / 1609.34

    # Shapefile field names are limited, so keep them short
    loaded_segments = loaded_segments.rename(
        columns={
            "TotalTrips": "TotTrips",
            "RouteCount": "RtCount",
        }
    )

    return loaded_segments.to_crs("EPSG:4326")


def make_loaded_segments_map(loaded_segments_gdf: gpd.GeoDataFrame):
    """
    Creates an interactive map for the loaded roadway segment layer.
    This uses Streamlit/PyDeck PathLayer, which is reliable for polylines.
    """

    if loaded_segments_gdf.empty:
        st.info("No loaded segments are available to map.")
        return

    map_gdf = loaded_segments_gdf.copy()

    # PyDeck needs WGS84 longitude/latitude coordinates
    if map_gdf.crs is None:
        map_gdf = map_gdf.set_crs("EPSG:4326")
    else:
        map_gdf = map_gdf.to_crs("EPSG:4326")

    # Explode multipart geometry, just in case
    map_gdf = map_gdf.explode(index_parts=False).reset_index(drop=True)

    # Keep only valid LineString geometry
    map_gdf = map_gdf[
        map_gdf.geometry.notnull()
        & (~map_gdf.geometry.is_empty)
        & (map_gdf.geometry.geom_type == "LineString")
    ].copy()

    if map_gdf.empty:
        st.info("No valid LineString geometry is available to map.")
        return

    # Create path coordinate lists for PyDeck PathLayer
    map_gdf["path"] = map_gdf.geometry.apply(
        lambda geom: [[float(x), float(y)] for x, y in geom.coords]
    )

    max_trips = float(map_gdf["TotTrips"].max()) if "TotTrips" in map_gdf.columns else 0

    def get_color(trips):
        if max_trips <= 0:
            return [40, 40, 40]

        ratio = float(trips) / max_trips

        if ratio >= 0.75:
            return [200, 0, 0]
        elif ratio >= 0.50:
            return [230, 100, 0]
        elif ratio >= 0.25:
            return [230, 180, 0]
        else:
            return [0, 90, 200]

    def get_width(trips):
        if max_trips <= 0:
            return 4

        ratio = float(trips) / max_trips
        return max(3, min(16, 3 + ratio * 13))

    map_gdf["color"] = map_gdf["TotTrips"].apply(get_color)
    map_gdf["width"] = map_gdf["TotTrips"].apply(get_width)

    # Round for cleaner tooltip display
    map_gdf["LenMile"] = map_gdf["LenMile"].round(3)
    map_gdf["TotTrips"] = map_gdf["TotTrips"].round(2)

    center = map_gdf.geometry.unary_union.centroid

    map_df = pd.DataFrame(
        {
            "path": map_gdf["path"],
            "color": map_gdf["color"],
            "width": map_gdf["width"],
            "SegID": map_gdf["SegID"],
            "TotTrips": map_gdf["TotTrips"],
            "RtCount": map_gdf["RtCount"],
            "LenMile": map_gdf["LenMile"],
        }
    )

    layer = pdk.Layer(
        "PathLayer",
        data=map_df,
        get_path="path",
        get_color="color",
        get_width="width",
        width_units="pixels",
        pickable=True,
        auto_highlight=True,
    )

    view_state = pdk.ViewState(
        latitude=float(center.y),
        longitude=float(center.x),
        zoom=12,
        pitch=0,
    )

    tooltip = {
        "html": """
        <b>Segment ID:</b> {SegID}<br/>
        <b>Total Trips:</b> {TotTrips}<br/>
        <b>Route Count:</b> {RtCount}<br/>
        <b>Length:</b> {LenMile} miles
        """,
        "style": {
            "backgroundColor": "white",
            "color": "black",
        },
    }

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    )

    st.pydeck_chart(deck, use_container_width=True)


def build_routes(
    df: pd.DataFrame,
    api_key: str,
    arrival_time_val: str,
    weekday: str,
    mode_input: str,
):
    df = df.copy()
    df = df.dropna(subset=["GEOID"])

    if df.empty:
        raise ValueError(
            "The uploaded file has no usable rows after removing blank GEOID values."
        )

    mode = "driving" if mode_input == "Auto" else "transit"
    transit_mode = (
        "subway" if mode_input == "Subway" else ("bus" if mode_input == "Bus" else None)
    )
    mode_label = "driving" if mode_input == "Auto" else f"transit-{transit_mode}"

    today_local = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d")
    arrival_time = convert_to_utc_timestamp(today_local, arrival_time_val)

    route_data_list = []
    errors = []

    total_rows = len(df)
    progress = st.progress(0, text="Starting route processing...")
    status_box = st.empty()

    for i, row in enumerate(df.itertuples(index=False), start=1):
        geoid = str(getattr(row, "GEOID"))
        status_box.info(f"Processing {i:,} of {total_rows:,} — GEOID {geoid}")

        route_data = get_route(
            getattr(row, "orig_LAT"),
            getattr(row, "orig_LON"),
            getattr(row, "dest_LAT"),
            getattr(row, "dest_LON"),
            mode,
            api_key,
            arrival_time,
            transit_mode,
        )

        if route_data and route_data.get("routes"):
            leg = route_data["routes"][0]["legs"][0]
            encoded_polyline = route_data["routes"][0]["overview_polyline"]["points"]
            route_coords = polyline.decode(encoded_polyline)
            route_geometry = LineString([(lon, lat) for lat, lon in route_coords])

            travel_time = leg["duration"]["value"] / 60.0
            distance = leg["distance"]["value"] / 1609.34

            if "departure_time" in leg:
                dep_time_utc = datetime.utcfromtimestamp(
                    leg["departure_time"]["value"]
                ).replace(tzinfo=pytz.utc)
            else:
                dep_time_utc = datetime.utcfromtimestamp(
                    arrival_time - int(travel_time * 60)
                ).replace(tzinfo=pytz.utc)

            arr_time_local = (
                datetime.utcfromtimestamp(arrival_time)
                .replace(tzinfo=pytz.utc)
                .astimezone(EASTERN_TZ)
            )
            dep_time_local = dep_time_utc.astimezone(EASTERN_TZ)

            sbwy_lines = []
            bus_lines = []

            if mode == "transit":
                for step in leg.get("steps", []):
                    if step.get("travel_mode") == "TRANSIT":
                        transit_details = step.get("transit_details", {})
                        line = transit_details.get("line", {})
                        vehicle = line.get("vehicle", {}).get("type", "").lower()
                        name = line.get("short_name") or line.get("name", "")

                        if vehicle == "subway":
                            sbwy_lines.append(name)
                        elif vehicle == "bus":
                            bus_lines.append(name)

            route_data_list.append(
                {
                    "GEOID": geoid,
                    "Trips": getattr(row, "Trips"),
                    "ModeOfTran": mode_label,
                    "TravelTime": round(travel_time, 2),
                    "DistMile": round(distance, 2),
                    "SbwyLine": ", ".join(sbwy_lines) if sbwy_lines else "n/a",
                    "BusLine": ", ".join(bus_lines) if bus_lines else "n/a",
                    "DayOfWk": weekday,
                    "Arr_Time": arr_time_local.strftime("%H:%M:%S"),
                    "Dep_Time": dep_time_local.strftime("%H:%M:%S"),
                    "geometry": route_geometry,
                }
            )
        else:
            error_message = (
                route_data.get("error_message", route_data.get("status", "Unknown error"))
                if route_data
                else "No data"
            )
            errors.append({"GEOID": geoid, "Error": error_message})

        progress.progress(i / total_rows, text=f"Processed {i:,} of {total_rows:,} rows")
        time.sleep(0.05)

    status_box.empty()

    if not route_data_list:
        raise RuntimeError(
            "No routes were returned. Check the API key, inputs, and Google Maps billing setup."
        )

    gdf = gpd.GeoDataFrame(route_data_list, crs="EPSG:4326", geometry="geometry")
    error_df = pd.DataFrame(errors)
    return gdf, error_df


apply_stv_theme()
render_stv_header()

st.markdown(
    """
    <div class="stv-card">
    Upload an OD Excel or CSV file, enter your Google Maps API key, choose the arrival time and travel mode,
    then download both the original route shapefile and the loaded roadway segment shapefile.
    </div>
    """,
    unsafe_allow_html=True,
)

stv_section("Input setup")
with st.expander("Required input columns"):
    st.write(REQUIRED_COLUMNS)
    st.download_button(
        label="Download sample Excel template",
        data=sample_template_bytes(),
        file_name="od_route_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

with st.expander("How to get a Google Maps API key"):
    st.markdown(
        """
1. Go to Google Cloud Console
2. Create a new project, or select an existing one
3. Go to APIs & Services → Library
4. Enable Directions API
5. Go to APIs & Services → Credentials
6. Click Create Credentials → API Key
7. Copy the key and paste it here

Important:
- Billing usually must be enabled
- Restrict the key to Directions API if possible
- Each user should use their own key
        """
    )

with st.expander("Estimated API usage and cost warning"):
    st.markdown(
        """
- This app usually sends about **1 Google Directions request per usable OD pair**
- Actual billing can vary by request type, SKU, monthly volume tier, and your Google Maps plan
- Entering a value below is optional and is only for a rough planning estimate
        """
    )

with st.expander("Loaded segment output"):
    st.markdown(
        """
In addition to the original route shapefile, the app also creates a second shapefile:

**Loaded roadway segments shapefile**

This layer splits overlapping route lines into smaller shared line segments and sums the `Trips` field for each segment.

Main fields:
- `SegID`: unique segment ID
- `TotTrips`: total trips using that segment
- `RtCount`: number of routes using that segment
- `LenMile`: segment length in miles

Important: this is based on Google route geometry, not an official roadway centerline layer.
        """
    )

stv_section("Run settings")
with st.form("od_route_form"):
    uploaded_file = st.file_uploader(
        "Upload OD file",
        type=["xlsx", "xls", "csv"],
        help="Required columns: GEOID, orig_LAT, orig_LON, dest_LAT, dest_LON, Trips",
    )

    api_key = st.text_input(
        "Google Maps API key",
        type="password",
        help="Each user should use their own key. The app does not store it.",
    )

    cost_per_1000 = st.number_input(
        "Optional: your estimated cost per 1,000 requests ($)",
        min_value=0.0,
        value=0.0,
        step=0.5,
        help="Leave as 0 if you only want a usage warning and not a dollar estimate.",
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        arrival_time_val = st.selectbox("Arrival time", ARRIVAL_OPTIONS, index=3)

    with col2:
        weekday = st.selectbox("Weekday", WEEKDAY_OPTIONS, index=2)

    with col3:
        mode_input = st.selectbox("Transport mode", MODE_OPTIONS, index=0)

    submitted = st.form_submit_button("Build routes")


# Preview cost and usage warning before running
if uploaded_file is not None:
    try:
        preview_df = parse_uploaded_table(uploaded_file)
        validate_input_table(preview_df)

        estimate = estimate_job_size(preview_df)
        od_pairs = estimate["od_pairs"]
        estimated_requests = estimate["estimated_requests"]

        stv_section("Run size warning")
        st.write(f"Usable OD pairs: **{od_pairs:,}**")
        st.write(f"Estimated API requests: **{estimated_requests:,}**")

        if cost_per_1000 > 0:
            estimated_cost = (estimated_requests / 1000.0) * cost_per_1000
            st.write(f"Estimated cost using your rate: **${estimated_cost:,.2f}**")

        if estimate["level"] == "high":
            st.error(estimate["message"])
        elif estimate["level"] in ["medium", "low"]:
            st.warning(estimate["message"])
        else:
            st.info(estimate["message"])

    except Exception as exc:
        st.warning(f"Could not estimate run size yet: {exc}")


if submitted:
    if uploaded_file is None:
        st.error("Please upload an Excel or CSV file.")
    elif not api_key.strip():
        st.error("Please enter a Google Maps API key.")
    else:
        try:
            with st.spinner("Reading file..."):
                df = parse_uploaded_table(uploaded_file)
                validate_input_table(df)

            with st.spinner(
                "Calling Google Maps Directions API and building route geometry..."
            ):
                gdf, error_df = build_routes(
                    df,
                    api_key.strip(),
                    arrival_time_val,
                    weekday,
                    mode_input,
                )

            with st.spinner("Building loaded segment layer..."):
                loaded_segments_gdf = build_loaded_segments(gdf)

            base_name = os.path.splitext(uploaded_file.name)[0]

            route_zip_bytes = create_zipped_shapefile(
                gdf,
                f"{base_name}_{mode_input}",
            )

            loaded_zip_bytes = create_zipped_shapefile(
                loaded_segments_gdf,
                f"{base_name}_{mode_input}_loaded_segments",
            )

            st.success(
                f"Done. {len(gdf):,} routes and {len(loaded_segments_gdf):,} loaded segments were created."
            )

            preview_cols = [
                "GEOID",
                "Trips",
                "ModeOfTran",
                "TravelTime",
                "DistMile",
                "SbwyLine",
                "BusLine",
                "DayOfWk",
                "Arr_Time",
                "Dep_Time",
            ]

            stv_section("Route preview")
            st.dataframe(gdf[preview_cols].head(25), use_container_width=True)

            st.download_button(
                label="Download original route shapefile ZIP",
                data=route_zip_bytes,
                file_name=f"{base_name}_{mode_input}.zip",
                mime="application/zip",
            )

            loaded_preview_cols = [
                "SegID",
                "TotTrips",
                "RtCount",
                "LenMile",
            ]

            stv_section("Loaded segment preview")
            st.dataframe(
                loaded_segments_gdf[loaded_preview_cols]
                .sort_values("TotTrips", ascending=False)
                .head(25),
                use_container_width=True,
            )

            st.download_button(
                label="Download loaded roadway segments shapefile ZIP",
                data=loaded_zip_bytes,
                file_name=f"{base_name}_{mode_input}_loaded_segments.zip",
                mime="application/zip",
            )

            stv_section("Loaded roadway segments map")
            st.caption(
                "Thicker and warmer colored lines represent higher loaded roadway segments based on total trips."
            )
            make_loaded_segments_map(loaded_segments_gdf)

            if not error_df.empty:
                stv_section("Rows with errors")
                st.dataframe(error_df, use_container_width=True)

                csv_bytes = error_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download error log CSV",
                    data=csv_bytes,
                    file_name=f"{base_name}_{mode_input}_errors.csv",
                    mime="text/csv",
                )

        except Exception as exc:
            st.error(f"The app could not finish the run: {exc}")

with st.expander("Important notes"):
    st.markdown(
        """
- This app asks each user to enter their own Google Maps API key
- The key is used only for the current run and is not written to the output
- A shapefile is downloaded as a ZIP because a shapefile is made of multiple files
- The loaded segment output is based on Google route geometry
- If you need exact NYCDOT/LION roadway segment totals, the next step would be matching or snapping these routes to an official roadway centerline layer
        """
    )
