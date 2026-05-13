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
from shapely.geometry import LineString

st.set_page_config(page_title="OD Route Builder", layout="wide")

EASTERN_TZ = pytz.timezone("US/Eastern")
REQUIRED_COLUMNS = ["GEOID", "orig_LAT", "orig_LON", "dest_LAT", "dest_LON", "Trips"]
ARRIVAL_OPTIONS = [f"{hour:02d}:00:00" for hour in range(6, 19)]
WEEKDAY_OPTIONS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MODE_OPTIONS = ["Auto", "Subway", "Bus"]

for key, default in {
    "result_zip_bytes": None,
    "result_zip_name": None,
    "route_preview_df": None,
    "loaded_preview_df": None,
    "loaded_map_gdf": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


@st.cache_data(show_spinner=False)
def sample_template_bytes() -> bytes:
    sample_df = pd.DataFrame({
        "GEOID": ["360610001001", "360610001002"],
        "orig_LAT": [40.7580, 40.7527],
        "orig_LON": [-73.9855, -73.9772],
        "dest_LAT": [40.7128, 40.7306],
        "dest_LON": [-74.0060, -73.9352],
        "Trips": [125, 80],
    })
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        sample_df.to_excel(writer, index=False, sheet_name="OD_Input")
    buffer.seek(0)
    return buffer.getvalue()


def convert_to_utc_timestamp(local_date_str: str, local_time_str: str, timezone_name: str = "US/Eastern") -> int:
    local_tz = pytz.timezone(timezone_name)
    naive_time = datetime.strptime(f"{local_date_str} {local_time_str}", "%Y-%m-%d %H:%M:%S")
    local_time = local_tz.localize(naive_time, is_dst=None)
    utc_time = local_time.astimezone(pytz.utc)
    return int(utc_time.timestamp())


def get_route(origin_lat, origin_lon, dest_lat, dest_lon, mode, api_key, arrival_time=None, transit_mode=None):
    params = {
        "origin": f"{origin_lat},{origin_lon}",
        "destination": f"{dest_lat},{dest_lon}",
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
        return {"status": "REQUEST_FAILED", "error_message": "Invalid response from Google Maps API."}

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
    od_pairs = len(df.dropna(subset=["GEOID"]))
    if od_pairs >= 5000:
        return od_pairs, "Large run. This may take a while and may create noticeable API charges.", "high"
    if od_pairs >= 1000:
        return od_pairs, "Moderate to large run. Double check your API billing setup before running.", "medium"
    if od_pairs >= 100:
        return od_pairs, "Medium run. Charges are usually manageable, but still worth checking.", "low"
    return od_pairs, "Small run. Overall cost is usually modest.", "minimal"


def normalize_segment_key(pt1, pt2, precision=6):
    p1 = (round(pt1[0], precision), round(pt1[1], precision))
    p2 = (round(pt2[0], precision), round(pt2[1], precision))
    return tuple(sorted([p1, p2]))


def create_loaded_segments(routes_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    segment_dict = {}
    total_trips = routes_gdf["Trips"].fillna(0).astype(float).sum()

    for _, row in routes_gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty or geom.geom_type != "LineString":
            continue

        coords = list(geom.coords)
        if len(coords) < 2:
            continue

        try:
            trips = float(row.get("Trips", 0))
        except Exception:
            trips = 0.0

        geoid = str(row.get("GEOID", ""))

        for i in range(len(coords) - 1):
            start = coords[i]
            end = coords[i + 1]
            if start == end:
                continue

            key = normalize_segment_key(start, end)
            if key not in segment_dict:
                segment_dict[key] = {
                    "TotTrips": 0.0,
                    "RouteCnt": 0,
                    "GEOIDs": set(),
                    "geometry": LineString([start, end]),
                }
            segment_dict[key]["TotTrips"] += trips
            segment_dict[key]["RouteCnt"] += 1
            if geoid:
                segment_dict[key]["GEOIDs"].add(geoid)

    records = []
    for item in segment_dict.values():
        pct_trips = (item["TotTrips"] / total_trips * 100.0) if total_trips else 0.0
        records.append({
            "TotTrips": round(item["TotTrips"], 2),
            "PctTrips": round(pct_trips, 2),
            "RouteCnt": int(item["RouteCnt"]),
            "GEOIDCnt": len(item["GEOIDs"]),
            "geometry": item["geometry"],
        })

    if not records:
        return gpd.GeoDataFrame(columns=["TotTrips", "PctTrips", "RouteCnt", "GEOIDCnt", "geometry"], crs="EPSG:4326", geometry="geometry")

    return gpd.GeoDataFrame(records, crs="EPSG:4326", geometry="geometry").sort_values("TotTrips", ascending=False).reset_index(drop=True)


def write_shapefile(gdf: gpd.GeoDataFrame, folder: str, base_name: str):
    os.makedirs(folder, exist_ok=True)
    shp_path = os.path.join(folder, f"{base_name}.shp")
    gdf.to_file(shp_path, driver="ESRI Shapefile")


def create_output_package(routes_gdf: gpd.GeoDataFrame, loaded_gdf: gpd.GeoDataFrame, base_name: str, error_df: pd.DataFrame = None) -> bytes:
    with tempfile.TemporaryDirectory() as temp_dir:
        route_folder = os.path.join(temp_dir, "route_shapefile")
        loaded_folder = os.path.join(temp_dir, "loaded_trips_shapefile")

        write_shapefile(routes_gdf, route_folder, f"{base_name}_routes")
        write_shapefile(loaded_gdf, loaded_folder, f"{base_name}_loaded_trips")

        if error_df is not None and not error_df.empty:
            error_csv_path = os.path.join(temp_dir, "error_log.csv")
            error_df.to_csv(error_csv_path, index=False)

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for folder_name in ["route_shapefile", "loaded_trips_shapefile"]:
                folder_path = os.path.join(temp_dir, folder_name)
                for filename in os.listdir(folder_path):
                    full_path = os.path.join(folder_path, filename)
                    zip_file.write(full_path, arcname=os.path.join(folder_name, filename))

            if error_df is not None and not error_df.empty:
                zip_file.write(error_csv_path, arcname="error_log.csv")

        zip_buffer.seek(0)
        return zip_buffer.getvalue()


def show_loaded_map(loaded_gdf: gpd.GeoDataFrame):
    if loaded_gdf is None or loaded_gdf.empty:
        st.info("No loaded trips map is available.")
        return

    map_gdf = loaded_gdf.copy().to_crs(epsg=4326)
    max_trips = map_gdf["TotTrips"].max() or 1
    map_gdf["LineWidth"] = 2 + (map_gdf["TotTrips"] / max_trips) * 8
    centroid = map_gdf.geometry.unary_union.centroid

    layer = pdk.Layer(
        "GeoJsonLayer",
        map_gdf.__geo_interface__,
        pickable=True,
        stroked=True,
        filled=False,
        get_line_color=[220, 20, 60],
        get_line_width="properties.LineWidth",
        line_width_min_pixels=1,
        line_width_max_pixels=10,
    )

    tooltip = {
        "html": "<b>Total Trips:</b> {TotTrips}<br/><b>% Trips:</b> {PctTrips}<br/><b>Route Count:</b> {RouteCnt}",
        "style": {"backgroundColor": "white", "color": "black"},
    }

    st.pydeck_chart(
        pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(latitude=centroid.y, longitude=centroid.x, zoom=11, pitch=0),
            layers=[layer],
            tooltip=tooltip,
        )
    )


def build_routes(df: pd.DataFrame, api_key: str, arrival_time_val: str, weekday: str, mode_input: str):
    df = df.copy().dropna(subset=["GEOID"])
    if df.empty:
        raise ValueError("The uploaded file has no usable rows after removing blank GEOID values.")

    mode = "driving" if mode_input == "Auto" else "transit"
    transit_mode = "subway" if mode_input == "Subway" else ("bus" if mode_input == "Bus" else None)
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
            getattr(row, "orig_LAT"), getattr(row, "orig_LON"), getattr(row, "dest_LAT"), getattr(row, "dest_LON"),
            mode, api_key, arrival_time, transit_mode,
        )

        if route_data and route_data.get("routes"):
            route = route_data["routes"][0]
            leg = route["legs"][0]
            route_coords = polyline.decode(route["overview_polyline"]["points"])
            route_geometry = LineString([(lon, lat) for lat, lon in route_coords])

            travel_time = leg["duration"]["value"] / 60.0
            distance = leg["distance"]["value"] / 1609.34

            if "departure_time" in leg:
                dep_time_utc = datetime.utcfromtimestamp(leg["departure_time"]["value"]).replace(tzinfo=pytz.utc)
            else:
                dep_time_utc = datetime.utcfromtimestamp(arrival_time - int(travel_time * 60)).replace(tzinfo=pytz.utc)

            arr_time_local = datetime.utcfromtimestamp(arrival_time).replace(tzinfo=pytz.utc).astimezone(EASTERN_TZ)
            dep_time_local = dep_time_utc.astimezone(EASTERN_TZ)

            sbwy_lines = []
            bus_lines = []
            if mode == "transit":
                for step in leg.get("steps", []):
                    if step.get("travel_mode") == "TRANSIT":
                        line = step.get("transit_details", {}).get("line", {})
                        vehicle = line.get("vehicle", {}).get("type", "").lower()
                        name = line.get("short_name") or line.get("name", "")
                        if vehicle == "subway":
                            sbwy_lines.append(name)
                        elif vehicle == "bus":
                            bus_lines.append(name)

            route_data_list.append({
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
            })
        else:
            error_message = route_data.get("error_message", route_data.get("status", "Unknown error")) if route_data else "No data"
            errors.append({"GEOID": geoid, "Error": error_message})

        progress.progress(i / total_rows, text=f"Processed {i:,} of {total_rows:,} rows")
        time.sleep(0.05)

    status_box.empty()

    if not route_data_list:
        raise RuntimeError("No routes were returned. Check the API key and billing setup.")

    routes_gdf = gpd.GeoDataFrame(route_data_list, crs="EPSG:4326", geometry="geometry")
    loaded_gdf = create_loaded_segments(routes_gdf)
    error_df = pd.DataFrame(errors)
    return routes_gdf, loaded_gdf, error_df


st.title("OD Route Builder")
st.write("Upload an OD Excel or CSV file, enter your own Google Maps API key, choose the time and mode, and download the output shapefile package.")

with st.expander("Required input columns"):
    st.write(REQUIRED_COLUMNS)
    st.download_button(
        label="Download sample Excel template",
        data=sample_template_bytes(),
        file_name="od_route_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

with st.expander("How to get a Google Maps API key"):
    st.markdown("""
1. Go to Google Cloud Console
2. Create a new project
3. Go to APIs & Services → Library
4. Enable Directions API
5. Go to APIs & Services → Credentials
6. Create an API key
7. Paste the key into this app

Important:
- Billing must usually be enabled
- Each user should use their own key
    """)

with st.expander("Estimated API usage and cost warning"):
    st.markdown("""
- This app generally sends about 1 Google Directions request per usable OD pair
- Google Maps Platform billing is monthly
- Large jobs may create noticeable API charges
    """)

with st.form("od_route_form"):
    uploaded_file = st.file_uploader("Upload OD file", type=["xlsx", "xls", "csv"], help="Required columns: GEOID, orig_LAT, orig_LON, dest_LAT, dest_LON, Trips")
    api_key = st.text_input("Google Maps API key", type="password", help="The app does not store the key.")
    cost_per_1000 = st.number_input("Optional estimated cost per 1,000 requests ($)", min_value=0.0, value=0.0, step=0.5)

    col1, col2, col3 = st.columns(3)
    with col1:
        arrival_time_val = st.selectbox("Arrival time", ARRIVAL_OPTIONS, index=3)
    with col2:
        weekday = st.selectbox("Weekday", WEEKDAY_OPTIONS, index=2)
    with col3:
        mode_input = st.selectbox("Transport mode", MODE_OPTIONS, index=0)

    submitted = st.form_submit_button("Build routes")

if uploaded_file is not None:
    try:
        preview_df = parse_uploaded_table(uploaded_file)
        validate_input_table(preview_df)
        od_pairs, message, level = estimate_job_size(preview_df)

        st.subheader("Run size warning")
        st.write(f"Usable OD pairs: **{od_pairs:,}**")
        st.write(f"Estimated API requests: **{od_pairs:,}**")

        if cost_per_1000 > 0:
            estimated_cost = (od_pairs / 1000.0) * cost_per_1000
            st.write(f"Estimated cost: **${estimated_cost:,.2f}**")

        if level == "high":
            st.error(message)
        elif level in ["medium", "low"]:
            st.warning(message)
        else:
            st.info(message)
    except Exception as exc:
        st.warning(f"Could not estimate run size: {exc}")

if submitted:
    if uploaded_file is None:
        st.error("Please upload an Excel or CSV file.")
    elif not api_key.strip():
        st.error("Please enter a Google Maps API key.")
    else:
        try:
            with st.spinner("Reading input file..."):
                df = parse_uploaded_table(uploaded_file)
                validate_input_table(df)

            with st.spinner("Building routes and loaded trip segments..."):
                routes_gdf, loaded_gdf, error_df = build_routes(df, api_key.strip(), arrival_time_val, weekday, mode_input)

            base_name = os.path.splitext(uploaded_file.name)[0]
            zip_bytes = create_output_package(routes_gdf, loaded_gdf, f"{base_name}_{mode_input}", error_df)

            st.session_state.result_zip_bytes = zip_bytes
            st.session_state.result_zip_name = f"{base_name}_{mode_input}_output_package.zip"

            route_preview_cols = ["GEOID", "Trips", "ModeOfTran", "TravelTime", "DistMile", "SbwyLine", "BusLine", "DayOfWk", "Arr_Time", "Dep_Time"]
            loaded_preview_cols = ["TotTrips", "PctTrips", "RouteCnt", "GEOIDCnt"]

            st.session_state.route_preview_df = routes_gdf[route_preview_cols].head(25)
            st.session_state.loaded_preview_df = loaded_gdf[loaded_preview_cols].head(25)
            st.session_state.loaded_map_gdf = loaded_gdf

            st.success(f"Done. {len(routes_gdf):,} routes were created, and {len(loaded_gdf):,} loaded trip segments were generated.")
        except Exception as exc:
            st.error(f"The app could not finish the run: {exc}")

if st.session_state.route_preview_df is not None:
    st.subheader("Route preview")
    st.dataframe(st.session_state.route_preview_df, use_container_width=True)

if st.session_state.loaded_preview_df is not None:
    st.subheader("Loaded trips preview")
    st.dataframe(st.session_state.loaded_preview_df, use_container_width=True)

if st.session_state.result_zip_bytes is not None:
    st.download_button(
        label="Download output ZIP package",
        data=st.session_state.result_zip_bytes,
        file_name=st.session_state.result_zip_name,
        mime="application/zip",
    )

if st.session_state.loaded_map_gdf is not None:
    st.subheader("Loaded trips map")
    show_loaded_map(st.session_state.loaded_map_gdf)

with st.expander("Important notes"):
    st.markdown("""
- Each user should use their own API key
- The app does not store the API key
- The ZIP package includes:
    - route shapefile
    - loaded trips shapefile
    - error log, if applicable
- Large jobs may create API charges
    """)
