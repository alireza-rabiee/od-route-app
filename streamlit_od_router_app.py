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

   with st.expander("Required input columns"):

       with st.expander("How to get a Google Maps API key"):
    st.markdown(
        """
1. Go to `https://console.cloud.google.com/`
2. Create a new project, or select an existing one
3. Open **APIs & Services → Library**
4. Search for and enable **Directions API**
5. Open **APIs & Services → Credentials**
6. Click **Create Credentials → API Key**
7. Copy the key and paste it into this app

**Important**
- Billing usually must be enabled
- It is better to restrict the key to the **Directions API**
- Each user should use their own key
        """
    )
