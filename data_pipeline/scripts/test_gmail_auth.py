from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import os

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

CREDENTIALS_PATH = "data_pipeline/configs/gmail_credentials.json"
TOKEN_PATH = "data_pipeline/configs/token.json"

def get_token():
    creds = None

    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_PATH, SCOPES
            )
            creds = flow.run_local_server(
                port=8080,
                open_browser=True
            )

        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())

    return creds

if __name__ == "__main__":
    creds = get_token()
    print("✅ Gmail OAuth successful")
