import os
import json
import base64
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def _get_credentials():
    """Build Google Service Account credentials from env vars."""
    try:
        from google.oauth2 import service_account
    except ImportError:
        logger.debug("google-auth not installed; Google Drive upload unavailable.")
        return None

    # Try base64-encoded JSON string first (CI / GitHub Actions)
    sa_json_b64 = os.getenv("GDRIVE_SA_JSON")
    if sa_json_b64:
        sa_info = json.loads(base64.b64decode(sa_json_b64))
        return service_account.Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/drive.file"]
        )

    # Try file path (local dev)
    sa_path = os.getenv("GDRIVE_SA_JSON_PATH")
    if sa_path and Path(sa_path).exists():
        return service_account.Credentials.from_service_account_file(
            sa_path, scopes=["https://www.googleapis.com/auth/drive.file"]
        )

    return None


def _find_existing_file(service, folder_id, filename):
    """Find an existing file by name in the target folder."""
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None


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
