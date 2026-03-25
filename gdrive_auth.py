"""
One-time Google Drive OAuth2 setup.

Run this script locally to authorize your Google account:
    python gdrive_auth.py

It will open a browser for Google login, then save credentials to gdrive_token.json.
The refresh token from this file should be added as a GitHub secret (GDRIVE_REFRESH_TOKEN).
"""

import json
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
TOKEN_PATH = Path(__file__).resolve().parent / "gdrive_token.json"
CREDENTIALS_PATH = Path(__file__).resolve().parent / "gdrive_credentials.json"


def main():
    if not CREDENTIALS_PATH.exists():
        print(f"ERROR: {CREDENTIALS_PATH} not found.")
        print()
        print("To create it:")
        print("1. Go to https://console.cloud.google.com/apis/credentials")
        print("2. Click '+ Create Credentials' -> 'OAuth client ID'")
        print("3. Application type: 'Desktop app'")
        print("4. Download the JSON and save it as 'gdrive_credentials.json' in the FMM folder")
        return

    flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
    creds = flow.run_local_server(port=0)

    # Save token locally
    token_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes),
    }
    TOKEN_PATH.write_text(json.dumps(token_data, indent=2), encoding="utf-8")
    print(f"\nToken saved to {TOKEN_PATH}")

    print("\n=== Add these as GitHub Secrets ===")
    print(f"GDRIVE_CLIENT_ID={creds.client_id}")
    print(f"GDRIVE_CLIENT_SECRET={creds.client_secret}")
    print(f"GDRIVE_REFRESH_TOKEN={creds.refresh_token}")
    print(f"\nAlso add GDRIVE_FOLDER_ID with your Google Drive folder ID.")


if __name__ == "__main__":
    main()
