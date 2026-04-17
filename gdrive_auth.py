"""
One-time script to obtain a Google Drive OAuth2 refresh token.

Usage:
    python gdrive_auth.py

Requirements:
    - GDRIVE_CLIENT_ID and GDRIVE_CLIENT_SECRET must be set in your .env file
      (or as environment variables).
    - A browser window will open for you to log in and grant access.
    - The refresh token is printed at the end — copy it into the
      GDRIVE_REFRESH_TOKEN GitHub secret.
"""

import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def main():
    client_id = os.getenv("GDRIVE_CLIENT_ID", "").strip()
    client_secret = os.getenv("GDRIVE_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        print("ERROR: GDRIVE_CLIENT_ID and GDRIVE_CLIENT_SECRET must be set.")
        print("Add them to your .env file and re-run this script.")
        sys.exit(1)

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("ERROR: google-auth-oauthlib is not installed.")
        print("Run:  pip install google-auth-oauthlib")
        sys.exit(1)

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
        }
    }

    scopes = ["https://www.googleapis.com/auth/drive.file"]

    flow = InstalledAppFlow.from_client_config(client_config, scopes=scopes)

    print("\nOpening browser for Google authorization...")
    print("Log in with the Google account that owns the Drive folder.\n")

    creds = flow.run_local_server(port=0, prompt="consent", access_type="offline")

    refresh_token = creds.refresh_token
    if not refresh_token:
        print("\nERROR: No refresh token returned.")
        print("Make sure you authorized as a user (not a service account).")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("SUCCESS! Copy the value below into your GitHub secret")
    print("Secret name: GDRIVE_REFRESH_TOKEN")
    print("=" * 60)
    print(refresh_token)
    print("=" * 60 + "\n")

    # Optionally save to local token file for local dev use
    token_path = Path(__file__).parent / "gdrive_token.json"
    try:
        import json
        token_data = {
            "token": creds.token,
            "refresh_token": creds.refresh_token,
            "token_uri": creds.token_uri,
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
            "scopes": list(creds.scopes) if creds.scopes else scopes,
        }
        token_path.write_text(json.dumps(token_data, indent=2))
        print(f"Token also saved to {token_path} for local use.")
        print("(This file is in .gitignore and will not be committed.)\n")
    except Exception as e:
        print(f"Note: could not save token file ({e}) — that's OK, use the printed token above.\n")


if __name__ == "__main__":
    main()
