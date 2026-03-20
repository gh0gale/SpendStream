from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi import UploadFile, File
from fastapi.responses import RedirectResponse
from supabase import create_client
from datetime import datetime
import os
import io
import re
import requests
import pandas as pd
from dotenv import load_dotenv
import base64 

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Supabase clients ──────────────────────────────────────────────────────────
# Use the ANON key for user-scoped auth verification only.
# Use the SERVICE ROLE key for all database writes (bypasses RLS safely server-side).
supabase_anon = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_ANON_KEY")
)

supabase_admin = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")   # add this to your .env
)

GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
FRONTEND_URL         = os.getenv("FRONTEND_URL", "http://localhost:5173")
BACKEND_URL          = os.getenv("BACKEND_URL",  "http://127.0.0.1:8000")


# ─────────────────────────────────────────────────────────────────────────────
# Auth helper
# ─────────────────────────────────────────────────────────────────────────────

def get_user_from_token(request: Request):
    """
    Validates the Bearer token from the Authorization header and returns the user.
    Raises 401 if missing or invalid.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or malformed Authorization header")

    token = auth_header.split(" ")[1]

    try:
        user = supabase_anon.auth.get_user(token)
        return user.user
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


# ─────────────────────────────────────────────────────────────────────────────
# Protected test route
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/protected")
def protected_route(request: Request):
    user = get_user_from_token(request)
    return {
        "message": "You are authenticated",
        "user_id": user.id,
        "email": user.email
    }


# ─────────────────────────────────────────────────────────────────────────────
# File upload
# ─────────────────────────────────────────────────────────────────────────────

def clean_amount(val) -> float | None:
    """Strip currency symbols/commas and parse to float. Returns None on failure."""
    try:
        cleaned = re.sub(r"[^\d.]", "", str(val))
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None


def parse_date_safe(date_val):
    """Parse a date value to a Python datetime. Returns None on failure."""
    try:
        return pd.to_datetime(date_val).to_pydatetime()
    except Exception:
        return None


@app.post("/upload-file")
async def upload_file(request: Request, file: UploadFile = File(...)):
    user = get_user_from_token(request)

    contents = await file.read()

    # Parse file
    try:
        if file.filename.lower().endswith(".csv"):
            df = pd.read_csv(io.BytesIO(contents))
        else:
            df = pd.read_excel(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not parse file: {e}")

    # Normalise column names
    df.columns = [col.strip().lower() for col in df.columns]

    # Locate columns by keyword
    date_col       = next((c for c in df.columns if "date" in c), None)
    narration_col  = next((c for c in df.columns if "narration" in c or "description" in c), None)
    withdrawal_col = next((c for c in df.columns if "withdrawal" in c or "debit" in c), None)

    if not date_col or not withdrawal_col:
        raise HTTPException(
            status_code=422,
            detail=f"Required columns not found. Got: {list(df.columns)}"
        )

    df = df.dropna(subset=[withdrawal_col])

    transactions = []
    for _, row in df.iterrows():
        try:
            amount = clean_amount(row.get(withdrawal_col))
            if not amount or amount <= 0:
                continue

            timestamp = parse_date_safe(row.get(date_col))
            if not timestamp:
                continue

            narration = str(row.get(narration_col, "")).strip() if narration_col else ""

            transactions.append({
                "user_id":          user.id,
                "amount":           amount,
                "receiver":         narration[:100] if narration else "UNKNOWN",
                "transaction_type": "debit",
                "timestamp":        timestamp.isoformat(),
                "source":           "file",
                "raw_text":         narration
            })
        except Exception as e:
            print(f"Row parse error: {e}")

    if transactions:
        # FIX: use supabase_admin (service role) for DB writes — no need to
        # call set_session() with wrong arguments as was done before.
        supabase_admin.table("transactions").insert(transactions).execute()

    return {
        "message":             "File processed successfully",
        "transactions_found":  len(transactions)
    }


# ─────────────────────────────────────────────────────────────────────────────
# Gmail OAuth — initiate
# ─────────────────────────────────────────────────────────────────────────────
#
# SECURITY NOTE: passing the Supabase JWT as a query param is a pragmatic
# workaround for the stateless OAuth redirect flow. For production, replace
# this with a short-lived server-side session (e.g. store token in Redis with
# a random state key, pass only the state key through OAuth, look up the token
# in the callback). The Google OAuth `state` param is designed for exactly this.
#
@app.get("/auth/google")
def auth_google(token: str):
    """
    Kicks off Google OAuth. `token` is the user's Supabase JWT — we need it
    in the callback to know which user is completing the flow.
    Validates the token first so we fail early with a clear error.
    """
    # Validate token before redirecting so we don't start an OAuth flow for nobody
    try:
        supabase_anon.auth.get_user(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token — please log in again")

    # Pass the token through OAuth's state param (safer than a second query param)
    redirect_uri = f"{BACKEND_URL}/auth/callback"
    scope        = "https://www.googleapis.com/auth/gmail.readonly"

    google_auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={GOOGLE_CLIENT_ID}"
        f"&redirect_uri={redirect_uri}"
        f"&response_type=code"
        f"&scope={scope}"
        f"&access_type=offline"
        f"&prompt=consent"
        f"&state={token}"          # round-trip token via OAuth state param
    )

    return RedirectResponse(google_auth_url)


# ─────────────────────────────────────────────────────────────────────────────
# Gmail OAuth — callback
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/auth/callback")
def auth_callback(code: str, state: str):
    """
    Google redirects here after the user grants permission.
    `state` carries the Supabase JWT we sent in /auth/google.
    `code` is the one-time authorisation code we exchange for tokens.
    """
    # FIX: token now comes from `state` (set in /auth/google), not a custom param.
    # This keeps the redirect_uri clean and consistent with what Google expects.
    token = state

    try:
        user = supabase_anon.auth.get_user(token).user
    except Exception:
        raise HTTPException(status_code=401, detail="Session expired — please reconnect Gmail")

    # Exchange auth code for Google tokens
    redirect_uri = f"{BACKEND_URL}/auth/callback"
    token_res = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "code":          code,
            "client_id":     GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri":  redirect_uri,
            "grant_type":    "authorization_code",
        }
    )

    token_data = token_res.json()

    if "error" in token_data:
        raise HTTPException(status_code=400, detail=f"Google OAuth error: {token_data['error']}")

    access_token  = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")

    if not access_token:
        raise HTTPException(status_code=400, detail="No access token returned from Google")

    # FIX: use supabase_admin for DB writes (service role key)
    supabase_admin.table("gmail_sync").upsert({
        "user_id":       user.id,
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "updated_at":    datetime.utcnow().isoformat()
    }).execute()

    return RedirectResponse(FRONTEND_URL)


# ─────────────────────────────────────────────────────────────────────────────
# Gmail token refresh helper
# ─────────────────────────────────────────────────────────────────────────────

def refresh_google_token(user_id: str, refresh_token: str) -> str:
    """
    Exchanges a refresh token for a new access token and persists it.
    Returns the new access token, or raises HTTPException on failure.

    FIX: this was completely missing before — Gmail access tokens expire after
    ~1 hour and any fetch after that would silently return no messages.
    """
    res = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":     GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": refresh_token,
            "grant_type":    "refresh_token",
        }
    )
    data = res.json()

    if "error" in data:
        raise HTTPException(
            status_code=401,
            detail="Gmail token expired and refresh failed — please reconnect Gmail"
        )

    new_access_token = data["access_token"]

    supabase_admin.table("gmail_sync").upsert({
        "user_id":      user_id,
        "access_token": new_access_token,
        "updated_at":   datetime.utcnow().isoformat()
    }).execute()

    return new_access_token


# ─────────────────────────────────────────────────────────────────────────────
# Fetch Gmail transactions
# ─────────────────────────────────────────────────────────────────────────────

def extract_body(payload):
    """Recursively extract the raw base64 body from a Gmail message payload."""
    if "parts" in payload:
        for part in payload["parts"]:
            mime = part.get("mimeType", "")
            if mime in ["text/plain", "text/html"]:
                data = part["body"].get("data")
                if data:
                    return data
            if "parts" in part:
                result = extract_body(part)
                if result:
                    return result
    else:
        return payload.get("body", {}).get("data")
    return None


@app.get("/fetch-gmail")
def fetch_gmail(request: Request):
    user = get_user_from_token(request)

    # Get stored Gmail tokens using service role client
    token_res = supabase_admin.table("gmail_sync") \
        .select("*") \
        .eq("user_id", user.id) \
        .execute()

    if not token_res.data:
        raise HTTPException(status_code=400, detail="Gmail not connected — please click 'Connect Gmail' first")

    row           = token_res.data[0]
    access_token  = row["access_token"]
    refresh_token = row.get("refresh_token")
    last_fetched  = row.get("last_fetched")

    headers = {"Authorization": f"Bearer {access_token}"}

    # Build query: incremental if we have last_fetched, else current month
    if last_fetched:
        query = (
            f"from:(hdfc OR icici OR sbi OR axis OR kotak OR yesbank) "
            f"(debited OR spent OR txn OR transaction) after:{last_fetched[:10]}"
        )
        print("INCREMENTAL FETCH FROM:", last_fetched)
    else:
        first_day = datetime.utcnow().replace(day=1).strftime("%Y/%m/%d")
        query = (
            f"from:(hdfc OR icici OR sbi OR axis OR kotak OR yesbank) "
            f"(debited OR spent OR txn OR transaction) after:{first_day}"
        )
        print("FIRST TIME FETCH FROM:", first_day)

    # Paginate through results (up to 5 pages x 10 = 50 messages)
    all_messages    = []
    next_page_token = None

    for _ in range(5):
        url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages?q={query}&maxResults=10"
        if next_page_token:
            url += f"&pageToken={next_page_token}"

        res = requests.get(url, headers=headers)

        # Token expired — attempt refresh then retry once
        if res.status_code == 401:
            if not refresh_token:
                raise HTTPException(status_code=401, detail="Gmail token expired — please reconnect Gmail")
            access_token = refresh_google_token(user.id, refresh_token)
            headers      = {"Authorization": f"Bearer {access_token}"}
            res          = requests.get(url, headers=headers)

        data = res.json()

        if "messages" in data:
            all_messages.extend(data["messages"])

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    print("TOTAL MESSAGES:", len(all_messages))

    # Process each email
    transactions = []

    for msg in all_messages:
        try:
            msg_id   = msg["id"]
            msg_url  = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}"
            msg_res  = requests.get(msg_url, headers=headers)
            msg_data = msg_res.json()

            payload  = msg_data.get("payload", {})
            raw_body = extract_body(payload)

            if not raw_body:
                continue

            body = base64.urlsafe_b64decode(raw_body).decode("utf-8", errors="ignore")

            # Only process debit emails
            if not re.search(r"(?i)debited|spent|paid", body):
                continue

            # Extract amount — try "Rs X debited" then "debited Rs X"
            match = re.search(
                r"(?i)(?:rs\.?|inr|₹)\s?([\d,]+\.?\d{0,2}).*?(?:debited|spent|paid)",
                body
            )
            if not match:
                match = re.search(
                    r"(?i)(?:debited|spent|paid).*?(?:rs\.?|inr|₹)\s?([\d,]+\.?\d{0,2})",
                    body
                )
            if not match:
                continue

            amount = float(match.group(1).replace(",", ""))
            if amount <= 0:
                continue

            # Extract receiver
            receiver_match = re.search(r"(?i)to\s+(.*?)\s+on", body)
            receiver = receiver_match.group(1).strip() if receiver_match else "UNKNOWN"

            # Extract date from email headers
            email_date = None
            for header in payload.get("headers", []):
                if header.get("name") == "Date":
                    email_date = parse_date_safe(header["value"])
                    break

            transactions.append({
                "user_id":          user.id,
                "amount":           amount,
                "receiver":         receiver[:100],
                "transaction_type": "debit",
                "timestamp":        (email_date or datetime.utcnow()).isoformat(),
                "source":           "gmail",
                "raw_text":         body[:200]
            })

        except Exception as e:
            print(f"Email parse error (msg {msg.get('id', '?')}): {e}")

    # Insert transactions
    if transactions:
        supabase_admin.table("transactions").insert(transactions).execute()

    # Update last_fetched timestamp
    supabase_admin.table("gmail_sync").update({
        "last_fetched": datetime.utcnow().isoformat()
    }).eq("user_id", user.id).execute()

    return {"transactions_found": len(transactions)}