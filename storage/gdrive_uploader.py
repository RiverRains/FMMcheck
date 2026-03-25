import os
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def _get_credentials():
    """Build Google OAuth2 credentials from stored refresh token."""
    try:
        from google.oauth2.credentials import Credentials
    except ImportError:
        logger.debug("google-auth not installed; Google Drive upload unavailable.")
        return None

    client_id = os.getenv("GDRIVE_CLIENT_ID")
    client_secret = os.getenv("GDRIVE_CLIENT_SECRET")
    refresh_token = os.getenv("GDRIVE_REFRESH_TOKEN")

    if client_id and client_secret and refresh_token:
        return Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=SCOPES,
        )

    # Try local token file (saved by gdrive_auth.py)
    token_path = Path(__file__).resolve().parent.parent / "gdrive_token.json"
    if token_path.exists():
        try:
            return Credentials.from_authorized_user_file(str(token_path), SCOPES)
        except Exception as e:
            logger.debug("Failed to load token from %s: %s", token_path, e)

    return None


def _find_existing_file(service, folder_id, filename):
    """Find an existing file by name in the target folder."""
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None


def download_from_gdrive(filename: str, local_path: str) -> bool:
    """Download a file from Google Drive by name. Returns True on success."""
    folder_id = os.getenv("GDRIVE_FOLDER_ID")
    if not folder_id:
        logger.debug("GDRIVE_FOLDER_ID not set. Skipping Google Drive download.")
        return False

    creds = _get_credentials()
    if not creds:
        logger.debug("No Google Drive credentials found. Skipping download.")
        return False

    try:
        from googleapiclient.discovery import build

        service = build("drive", "v3", credentials=creds)
        file_id = _find_existing_file(service, folder_id, filename)
        if not file_id:
            logger.info("File '%s' not found on Google Drive.", filename)
            return False

        content = service.files().get_media(fileId=file_id).execute()
        Path(local_path).write_bytes(content)
        logger.info("Downloaded '%s' from Google Drive to %s", filename, local_path)
        return True
    except Exception as e:
        logger.error("Google Drive download failed: %s", e)
        return False


def upload_to_gdrive(file_path: str) -> bool:
    """Upload file to Google Drive. Returns True on success, False on skip/failure."""
    folder_id = os.getenv("GDRIVE_FOLDER_ID")
    if not folder_id:
        logger.warning("GDRIVE_FOLDER_ID not set. Skipping Google Drive upload.")
        return False

    creds = _get_credentials()
    if not creds:
        logger.warning("No Google Drive credentials found. Skipping upload.")
        return False

    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        service = build("drive", "v3", credentials=creds)
        filename = Path(file_path).name
        media = MediaFileUpload(
            file_path,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        existing_id = _find_existing_file(service, folder_id, filename)
        if existing_id:
            service.files().update(fileId=existing_id, media_body=media).execute()
            logger.info("Updated existing file '%s' on Google Drive (ID: %s)", filename, existing_id)
        else:
            file_metadata = {"name": filename, "parents": [folder_id]}
            created = service.files().create(
                body=file_metadata, media_body=media, fields="id"
            ).execute()
            logger.info("Uploaded '%s' to Google Drive (ID: %s)", filename, created["id"])
        return True
    except Exception as e:
        logger.error("Google Drive upload failed: %s", e)
        return False
