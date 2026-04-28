import streamlit as st
import requests
import json
import urllib.parse
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2.credentials import Credentials
import io

# =========================
# CONFIG
# =========================

CLIENT_ID = st.secrets["google_oauth"]["client_id"]
CLIENT_SECRET = st.secrets["google_oauth"]["client_secret"]
REDIRECT_URI = st.secrets["google_oauth"]["redirect_uri"]

SCOPES = "https://www.googleapis.com/auth/drive.file"

# =========================
# STEP 1: LOGIN BUTTON
# =========================

def get_auth_url():
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPES,
        "access_type": "offline",
        "prompt": "consent"
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)

# =========================
# STEP 2: EXCHANGE CODE
# =========================

def get_token(code):
    token_url = "https://oauth2.googleapis.com/token"

    data = {
        "code": code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code"
    }

    response = requests.post(token_url, data=data)
    return response.json()

# =========================
# STEP 3: BUILD DRIVE CLIENT
# =========================

def get_drive_service(token_data):
    creds = Credentials(
        token=token_data["access_token"],
        refresh_token=token_data.get("refresh_token"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    return build("drive", "v3", credentials=creds)

# =========================
# STEP 4: UPLOAD FILE
# =========================

def upload_to_drive(file_bytes, file_name, service):
    media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype="application/zip")

    file_metadata = {
        "name": file_name
    }

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, webViewLink"
    ).execute()

    return file

# =========================
# STREAMLIT APP
# =========================

st.title("OD Route Builder + Google Drive Upload")

# Check if logged in
query_params = st.query_params

if "code" not in query_params:
    st.markdown("### Step 1: Login to Google")
    auth_url = get_auth_url()
    st.link_button("Login with Google", auth_url)
    st.stop()

# Get token
code = query_params["code"]
token_data = get_token(code)

if "access_token" not in token_data:
    st.error("Login failed")
    st.stop()

st.success("Logged in successfully")

# Build Drive service
service = get_drive_service(token_data)

# =========================
# DEMO FILE (replace with your ZIP)
# =========================

uploaded_file = st.file_uploader("Upload your ZIP shapefile")

if uploaded_file:
    file_bytes = uploaded_file.read()

    if st.button("Upload to Google Drive"):
        try:
            file = upload_to_drive(file_bytes, uploaded_file.name, service)

            st.success("Uploaded to Google Drive")
            st.markdown(f"[Open file]({file['webViewLink']})")

        except Exception as e:
            st.error(f"Upload failed: {e}")
