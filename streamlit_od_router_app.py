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
from shapely.geometry import LineString


st.set_page_config(page_title="OD Route Builder", layout="wide")

EASTERN_TZ = pytz.timezone("US/Eastern")

REQUIRED_COLUMNS = [
    "GEOID",
    "orig_LAT",
    "orig_LON",
    "dest_LAT",
    "dest_LON",
    "Trips",
]

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


# -------------------------------------------------------
# Session state
# -------------------------------------------------------
if "result_zip_bytes" not in st.session_state:
    st.session_state.result_zip_bytes = None

if "result_zip_name" not in st.session_state:
    st.session_state.result_zip_name = None

if "preview_df" not in st.session_state:
    st.session_state.preview_df = None


# -------------------------------------------------------
# Sample template
# -------------------------------------------------------
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
        sample_df.to_excel(
            writer,
            index=False,
            sheet_name="OD_Input",
        )

    buffer.seek(0)

    return buffer.getvalue()


# -------------------------------------------------------
# Time conversion
# -------------------------------------------------------
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

    local_time = local_tz.localize(
        naive_time,
        is_dst=None,
    )

    utc_time = local_time.astimezone(pytz.utc)

    return int(utc_time.timestamp())


# -------------------------------------------------------
# Google route request
# -------------------------------------------------------
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

        data.setdefault(
            "error_message",
            f"HTTP {response.status_code}",
        )

    return data


# -------------------------------------------------------
# Parse uploaded table
# -------------------------------------------------------
def parse_uploaded_table(uploaded_file) -> pd.DataFrame:

    file_name = uploaded_file.name.lower()

    if file_name.endswith(".csv"):
        return pd.read_csv(uploaded_file)

    return pd.read_excel(
        uploaded_file,
        engine="openpyxl",
    )


# -------------------------------------------------------
# Validate input
# -------------------------------------------------------
def validate_input_table(df: pd.DataFrame):

    missing_columns = [
        col for col in REQUIRED_COLUMNS
        if col not in df.columns
    ]

    if missing_columns:

        raise ValueError(
            "Missing required columns: "
            + ", ".join(missing_columns)
        )


# -------------------------------------------------------
# Estimate job size
# -------------------------------------------------------
def estimate_job_size(df: pd.DataFrame):

    usable_df = df.dropna(subset=["GEOID"]).copy()

    od_pairs = len(usable_df)

    estimated_requests = od_pairs

    if estimated_requests >= 5000:

        level = "high"

        message = (
            "Large run. This may take a while and may "
            "create noticeable API charges."
        )

    elif estimated_requests >= 1000:

        level = "medium"

        message = (
            "Moderate to large run. Double check your "
            "API billing setup before running."
        )

    elif estimated_requests >= 100:

        level = "low"

        message = (
            "Medium run. Charges are usually manageable, "
            "but still worth checking."
        )

    else:

        level = "minimal"

        message = (
            "Small run. Overall cost is usually modest."
        )

    return {
        "od_pairs": od_pairs,
        "estimated_requests": estimated_requests,
        "level": level,
        "message": message,
    }


# -------------------------------------------------------
# Create combined output ZIP package
# -------------------------------------------------------
def create_output_package(
    gdf: gpd.GeoDataFrame,
    base_name: str,
    error_df: pd.DataFrame = None,
) -> bytes:

    with tempfile.TemporaryDirectory() as temp_dir:

        shp_folder = os.path.join(
            temp_dir,
            "shapefile",
        )

        os.makedirs(
            shp_folder,
            exist_ok=True,
        )

        shp_path = os.path.join(
            shp_folder,
            f"{base_name}.shp",
        )

        gdf.to_file(
            shp_path,
            driver="ESRI Shapefile",
        )

        if error_df is not None and not error_df.empty:

            error_csv_path = os.path.join(
                temp_dir,
                "error_log.csv",
            )

            error_df.to_csv(
                error_csv_path,
                index=False,
            )

        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(
            zip_buffer,
            "w",
            zipfile.ZIP_DEFLATED,
        ) as zip_file:

            for filename in os.listdir(shp_folder):

                full_path = os.path.join(
                    shp_folder,
                    filename,
                )

                zip_file.write(
                    full_path,
                    arcname=os.path.join(
                        "shapefile",
                        filename,
                    ),
                )

            if error_df is not None and not error_df.empty:

                zip_file.write(
                    error_csv_path,
                    arcname="error_log.csv",
                )

        zip_buffer.seek(0)

        return zip_buffer.getvalue()


# -------------------------------------------------------
# Build routes
# -------------------------------------------------------
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
            "The uploaded file has no usable rows "
            "after removing blank GEOID values."
        )

    mode = (
        "driving"
        if mode_input == "Auto"
        else "transit"
    )

    transit_mode = (
        "subway"
        if mode_input == "Subway"
        else (
            "bus"
            if mode_input == "Bus"
            else None
        )
    )

    mode_label = (
        "driving"
        if mode_input == "Auto"
        else f"transit-{transit_mode}"
    )

    today_local = datetime.now(
        EASTERN_TZ
    ).strftime("%Y-%m-%d")

    arrival_time = convert_to_utc_timestamp(
        today_local,
        arrival_time_val,
    )

    route_data_list = []

    errors = []

    total_rows = len(df)

    progress = st.progress(
        0,
        text="Starting route processing...",
    )

    status_box = st.empty()

    for i, row in enumerate(
        df.itertuples(index=False),
        start=1,
    ):

        geoid = str(getattr(row, "GEOID"))

        status_box.info(
            f"Processing {i:,} of "
            f"{total_rows:,} — GEOID {geoid}"
        )

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

            encoded_polyline = (
                route_data["routes"][0]
                ["overview_polyline"]["points"]
            )

            route_coords = polyline.decode(
                encoded_polyline
            )

            route_geometry = LineString(
                [
                    (lon, lat)
                    for lat, lon in route_coords
                ]
            )

            travel_time = (
                leg["duration"]["value"] / 60.0
            )

            distance = (
                leg["distance"]["value"] / 1609.34
            )

            if "departure_time" in leg:

                dep_time_utc = (
                    datetime.utcfromtimestamp(
                        leg["departure_time"]["value"]
                    )
                    .replace(tzinfo=pytz.utc)
                )

            else:

                dep_time_utc = (
                    datetime.utcfromtimestamp(
                        arrival_time
                        - int(travel_time * 60)
                    )
                    .replace(tzinfo=pytz.utc)
                )

            arr_time_local = (
                datetime.utcfromtimestamp(
                    arrival_time
                )
                .replace(tzinfo=pytz.utc)
                .astimezone(EASTERN_TZ)
            )

            dep_time_local = (
                dep_time_utc.astimezone(
                    EASTERN_TZ
                )
            )

            sbwy_lines = []

            bus_lines = []

            if mode == "transit":

                for step in leg.get("steps", []):

                    if (
                        step.get("travel_mode")
                        == "TRANSIT"
                    ):

                        transit_details = (
                            step.get(
                                "transit_details",
                                {},
                            )
                        )

                        line = transit_details.get(
                            "line",
                            {},
                        )

                        vehicle = (
                            line.get(
                                "vehicle",
                                {},
                            )
                            .get("type", "")
                            .lower()
                        )

                        name = (
                            line.get("short_name")
                            or line.get("name", "")
                        )

                        if vehicle == "subway":
                            sbwy_lines.append(name)

                        elif vehicle == "bus":
                            bus_lines.append(name)

            route_data_list.append(
                {
                    "GEOID": geoid,
                    "Trips": getattr(
                        row,
                        "Trips",
                    ),
                    "ModeOfTran": mode_label,
                    "TravelTime": round(
                        travel_time,
                        2,
                    ),
                    "DistMile": round(
                        distance,
                        2,
                    ),
                    "SbwyLine": (
                        ", ".join(sbwy_lines)
                        if sbwy_lines
                        else "n/a"
                    ),
                    "BusLine": (
                        ", ".join(bus_lines)
                        if bus_lines
                        else "n/a"
                    ),
                    "DayOfWk": weekday,
                    "Arr_Time": (
                        arr_time_local.strftime(
                            "%H:%M:%S"
                        )
                    ),
                    "Dep_Time": (
                        dep_time_local.strftime(
                            "%H:%M:%S"
                        )
                    ),
                    "geometry": route_geometry,
                }
            )

        else:

            error_message = (
                route_data.get(
                    "error_message",
                    route_data.get(
                        "status",
                        "Unknown error",
                    ),
                )
                if route_data
                else "No data"
            )

            errors.append(
                {
                    "GEOID": geoid,
                    "Error": error_message,
                }
            )

        progress.progress(
            i / total_rows,
            text=(
                f"Processed {i:,} "
                f"of {total_rows:,} rows"
            ),
        )

        time.sleep(0.05)

    status_box.empty()

    if not route_data_list:

        raise RuntimeError(
            "No routes were returned. Check "
            "the API key and billing setup."
        )

    gdf = gpd.GeoDataFrame(
        route_data_list,
        crs="EPSG:4326",
        geometry="geometry",
    )

    error_df = pd.DataFrame(errors)

    return gdf, error_df


# -------------------------------------------------------
# App UI
# -------------------------------------------------------
st.title("OD Route Builder")

st.write(
    "Upload an OD Excel or CSV file, enter "
    "your own Google Maps API key, choose "
    "the time and mode, and download the "
    "output shapefile package."
)


# -------------------------------------------------------
# Required columns
# -------------------------------------------------------
with st.expander("Required input columns"):

    st.write(REQUIRED_COLUMNS)

    st.download_button(
        label="Download sample Excel template",
        data=sample_template_bytes(),
        file_name="od_route_template.xlsx",
        mime=(
            "application/vnd.openxmlformats-"
            "officedocument.spreadsheetml.sheet"
        ),
    )


# -------------------------------------------------------
# API instructions
# -------------------------------------------------------
with st.expander(
    "How to get a Google Maps API key"
):

    st.markdown(
        """
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
        """
    )


# -------------------------------------------------------
# Cost warning
# -------------------------------------------------------
with st.expander(
    "Estimated API usage and cost warning"
):

    st.markdown(
        """
- This app generally sends about 1 Google
  Directions request per usable OD pair

- Google Maps Platform billing is monthly

- Large jobs may create noticeable API charges
        """
    )


# -------------------------------------------------------
# Input form
# -------------------------------------------------------
with st.form("od_route_form"):

    uploaded_file = st.file_uploader(
        "Upload OD file",
        type=["xlsx", "xls", "csv"],
        help=(
            "Required columns: GEOID, "
            "orig_LAT, orig_LON, "
            "dest_LAT, dest_LON, Trips"
        ),
    )

    api_key = st.text_input(
        "Google Maps API key",
        type="password",
        help=(
            "The app does not store the key."
        ),
    )

    cost_per_1000 = st.number_input(
        "Optional estimated cost per 1,000 requests ($)",
        min_value=0.0,
        value=0.0,
        step=0.5,
    )

    col1, col2, col3 = st.columns(3)

    with col1:

        arrival_time_val = st.selectbox(
            "Arrival time",
            ARRIVAL_OPTIONS,
            index=3,
        )

    with col2:

        weekday = st.selectbox(
            "Weekday",
            WEEKDAY_OPTIONS,
            index=2,
        )

    with col3:

        mode_input = st.selectbox(
            "Transport mode",
            MODE_OPTIONS,
            index=0,
        )

    submitted = st.form_submit_button(
        "Build routes"
    )


# -------------------------------------------------------
# Preview run size
# -------------------------------------------------------
if uploaded_file is not None:

    try:

        preview_df = parse_uploaded_table(
            uploaded_file
        )

        validate_input_table(preview_df)

        estimate = estimate_job_size(
            preview_df
        )

        od_pairs = estimate["od_pairs"]

        estimated_requests = (
            estimate["estimated_requests"]
        )

        st.subheader("Run size warning")

        st.write(
            f"Usable OD pairs: "
            f"**{od_pairs:,}**"
        )

        st.write(
            f"Estimated API requests: "
            f"**{estimated_requests:,}**"
        )

        if cost_per_1000 > 0:

            estimated_cost = (
                estimated_requests / 1000.0
            ) * cost_per_1000

            st.write(
                f"Estimated cost: "
                f"**${estimated_cost:,.2f}**"
            )

        if estimate["level"] == "high":

            st.error(estimate["message"])

        elif estimate["level"] in [
            "medium",
            "low",
        ]:

            st.warning(estimate["message"])

        else:

            st.info(estimate["message"])

    except Exception as exc:

        st.warning(
            f"Could not estimate run size: {exc}"
        )


# -------------------------------------------------------
# Run app
# -------------------------------------------------------
if submitted:

    if uploaded_file is None:

        st.error(
            "Please upload an Excel or CSV file."
        )

    elif not api_key.strip():

        st.error(
            "Please enter a Google Maps API key."
        )

    else:

        try:

            with st.spinner(
                "Reading input file..."
            ):

                df = parse_uploaded_table(
                    uploaded_file
                )

                validate_input_table(df)

            with st.spinner(
                "Building routes..."
            ):

                gdf, error_df = build_routes(
                    df,
                    api_key.strip(),
                    arrival_time_val,
                    weekday,
                    mode_input,
                )

            base_name = os.path.splitext(
                uploaded_file.name
            )[0]

            zip_bytes = create_output_package(
                gdf,
                f"{base_name}_{mode_input}",
                error_df,
            )

            st.session_state.result_zip_bytes = (
                zip_bytes
            )

            st.session_state.result_zip_name = (
                f"{base_name}_{mode_input}.zip"
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

            st.session_state.preview_df = (
                gdf[preview_cols].head(25)
            )

            st.success(
                f"Done. {len(gdf):,} routes "
                f"were created."
            )

        except Exception as exc:

            st.error(
                f"The app could not finish "
                f"the run: {exc}"
            )


# -------------------------------------------------------
# Persistent download section
# -------------------------------------------------------
if st.session_state.preview_df is not None:

    st.subheader("Preview")

    st.dataframe(
        st.session_state.preview_df,
        use_container_width=True,
    )

if st.session_state.result_zip_bytes is not None:

    st.download_button(
        label="Download output ZIP package",
        data=st.session_state.result_zip_bytes,
        file_name=st.session_state.result_zip_name,
        mime="application/zip",
    )


# -------------------------------------------------------
# Notes
# -------------------------------------------------------
with st.expander("Important notes"):

    st.markdown(
        """
- Each user should use their own API key

- The app does not store the API key

- The ZIP package includes:
    - shapefile
    - error log (if applicable)

- Large jobs may create API charges
        """
    )
