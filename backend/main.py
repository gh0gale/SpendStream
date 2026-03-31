from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi import UploadFile, File
from fastapi.responses import RedirectResponse
from supabase import create_client
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import io
import re
import base64
import logging
import requests
import pandas as pd

from dotenv import load_dotenv
from etl import run_pipeline
from ml.correction_routes import router as correction_router

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# App & middleware
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────────────
# Supabase clients
# ─────────────────────────────────────────────────────────────────────────────

supabase_anon = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_ANON_KEY"),
)

supabase_admin = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
FRONTEND_URL         = os.getenv("FRONTEND_URL", "http://localhost:5173")
BACKEND_URL          = os.getenv("BACKEND_URL",  "http://127.0.0.1:8000")
CRON_SECRET          = os.getenv("CRON_SECRET", "")          # set in .env
CRON_MAX_WORKERS     = int(os.getenv("CRON_MAX_WORKERS", "5"))

REAL_LABELS_PATH = os.path.join(
    os.path.dirname(__file__), "ml", "data", "real_labels.csv"
)


# ─────────────────────────────────────────────────────────────────────────────
# Auth helper
# ─────────────────────────────────────────────────────────────────────────────

def get_user_from_token(request: Request):
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
# File upload helpers
# ─────────────────────────────────────────────────────────────────────────────

def clean_amount(val) -> float | None:
    try:
        cleaned = re.sub(r"[^\d.]", "", str(val))
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None


def parse_date_safe(date_val):
    try:
        return pd.to_datetime(date_val).to_pydatetime()
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Gmail helpers (shared by manual endpoint + cron)
# ─────────────────────────────────────────────────────────────────────────────

def extract_body(payload: dict) -> str | None:
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


def _refresh_google_token(user_id: str, refresh_token: str) -> str:
    """
    Exchange a refresh token for a new access token and persist it.
    Raises RuntimeError on failure (caller decides how to handle).
    """
    res = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":     GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": refresh_token,
            "grant_type":    "refresh_token",
        },
        timeout=15,
    )
    data = res.json()

    if "error" in data:
        raise RuntimeError(
            f"Google token refresh failed for user {user_id}: {data['error']}"
        )

    new_access_token = data["access_token"]

    supabase_admin.table("gmail_sync").upsert({
        "user_id":      user_id,
        "access_token": new_access_token,
        "updated_at":   datetime.utcnow().isoformat(),
    }).execute()

    log.info("[Gmail] Token refreshed for user %s", user_id)
    return new_access_token


def _validate_and_refresh_token(user_id: str, access_token: str, refresh_token: str | None) -> str:
    """
    Proactively validate the access token with a cheap profile call.
    Auto-refreshes if expired. Raises RuntimeError if refresh is impossible.
    """
    test = requests.get(
        "https://gmail.googleapis.com/gmail/v1/users/me/profile",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=10,
    )
    if test.status_code != 401:
        return access_token                         # token is fine

    if not refresh_token:
        raise RuntimeError(
            f"Access token expired for user {user_id} and no refresh token available"
        )

    log.info("[Gmail] Access token expired for user %s — refreshing", user_id)
    return _refresh_google_token(user_id, refresh_token)


def _fetch_gmail_messages(user_id: str, access_token: str, refresh_token: str | None, last_fetched: str | None) -> list[dict]:
    """
    Paginate through Gmail and return raw transaction email dicts.
    Auto-refreshes token on 401. Returns a list of parsed transaction dicts.
    """
    if last_fetched:
        query = (
            f"from:(hdfc OR icici OR sbi OR axis OR kotak OR yesbank) "
            f"(debited OR spent OR txn OR transaction) after:{last_fetched[:10]}"
        )
        log.info("[Gmail] Incremental fetch from %s for user %s", last_fetched[:10], user_id)
    else:
        first_day = datetime.utcnow().replace(day=1).strftime("%Y/%m/%d")
        query = (
            f"from:(hdfc OR icici OR sbi OR axis OR kotak OR yesbank) "
            f"(debited OR spent OR txn OR transaction) after:{first_day}"
        )
        log.info("[Gmail] First-time fetch from %s for user %s", first_day, user_id)

    headers          = {"Authorization": f"Bearer {access_token}"}
    all_messages     = []
    next_page_token  = None

    for page in range(5):
        url = (
            f"https://gmail.googleapis.com/gmail/v1/users/me/messages"
            f"?q={query}&maxResults=10"
        )
        if next_page_token:
            url += f"&pageToken={next_page_token}"

        res = requests.get(url, headers=headers, timeout=15)

        if res.status_code == 401:
            if not refresh_token:
                raise RuntimeError(f"Gmail 401 for user {user_id} — no refresh token")
            access_token = _refresh_google_token(user_id, refresh_token)
            headers      = {"Authorization": f"Bearer {access_token}"}
            res          = requests.get(url, headers=headers, timeout=15)

        data = res.json()
        all_messages.extend(data.get("messages", []))
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    log.info("[Gmail] Found %d messages for user %s", len(all_messages), user_id)
    return all_messages


def _parse_gmail_messages(
    user_id: str,
    messages: list[dict],
    access_token: str,
) -> list[dict]:
    """
    Fetch full email bodies and extract transaction data.
    Returns a list of transaction dicts ready for DB insertion.
    """
    headers      = {"Authorization": f"Bearer {access_token}"}
    transactions = []

    for msg in messages:
        try:
            msg_id   = msg["id"]
            msg_res  = requests.get(
                f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}",
                headers=headers,
                timeout=15,
            )
            msg_data = msg_res.json()
            payload  = msg_data.get("payload", {})
            raw_body = extract_body(payload)

            if not raw_body:
                continue

            body = base64.urlsafe_b64decode(raw_body).decode("utf-8", errors="ignore")

            if not re.search(r"(?i)debited|spent|paid", body):
                continue

            # Amount extraction — try "Rs X debited" then "debited Rs X"
            match = re.search(
                r"(?i)(?:rs\.?|inr|₹)\s?([\d,]+\.?\d{0,2}).*?(?:debited|spent|paid)",
                body,
            )
            if not match:
                match = re.search(
                    r"(?i)(?:debited|spent|paid).*?(?:rs\.?|inr|₹)\s?([\d,]+\.?\d{0,2})",
                    body,
                )
            if not match:
                continue

            amount = float(match.group(1).replace(",", ""))
            if amount <= 0:
                continue

            receiver_match = re.search(r"(?i)to\s+(.*?)\s+on", body)
            receiver       = receiver_match.group(1).strip() if receiver_match else "UNKNOWN"

            email_date = None
            for header in payload.get("headers", []):
                if header.get("name") == "Date":
                    email_date = parse_date_safe(header["value"])
                    break

            transactions.append({
                "user_id":          user_id,
                "amount":           amount,
                "receiver":         receiver[:100],
                "transaction_type": "debit",
                "timestamp":        (email_date or datetime.utcnow()).isoformat(),
                "source":           "gmail",
                "raw_text":         body[:200],
            })

        except Exception as e:
            log.warning("[Gmail] Email parse error (msg %s, user %s): %s", msg.get("id", "?"), user_id, e)

    return transactions


# ─────────────────────────────────────────────────────────────────────────────
# Core reusable function — works without FastAPI context
# ─────────────────────────────────────────────────────────────────────────────

def fetch_gmail_for_user(user_id: str) -> dict:
    """
    Self-contained Gmail fetch + ETL pipeline for a single user.
    Safe to call from: background tasks, cron jobs, ThreadPoolExecutor.

    Returns a summary dict with keys: user_id, transactions_found, status, error.
    Never raises — errors are caught and returned in the summary.
    """
    log.info("[Cron] ── Starting Gmail fetch for user %s ──", user_id)

    result = {
        "user_id":             user_id,
        "transactions_found":  0,
        "status":              "ok",
        "error":               None,
    }

    try:
        # 1. Load Gmail tokens from DB
        token_res = supabase_admin.table("gmail_sync") \
            .select("*") \
            .eq("user_id", user_id) \
            .execute()

        if not token_res.data:
            raise RuntimeError("No Gmail tokens found — user has not connected Gmail")

        row           = token_res.data[0]
        access_token  = row["access_token"]
        refresh_token = row.get("refresh_token")
        last_fetched  = row.get("last_fetched")

        # 2. Validate token (refresh if expired)
        access_token = _validate_and_refresh_token(user_id, access_token, refresh_token)
        headers_live = {"Authorization": f"Bearer {access_token}"}  # keep for parse step

        # 3. Fetch message list (paginated)
        messages = _fetch_gmail_messages(user_id, access_token, refresh_token, last_fetched)

        if not messages:
            log.info("[Cron] No messages found for user %s — nothing to do", user_id)
            # Still update last_fetched so next run is incremental
            supabase_admin.table("gmail_sync").update({
                "last_fetched": datetime.utcnow().isoformat(),
            }).eq("user_id", user_id).execute()
            return result

        # 4. Parse emails → transaction dicts
        transactions = _parse_gmail_messages(user_id, messages, access_token)

        # 5. Insert into transactions table
        if transactions:
            supabase_admin.table("transactions").insert(transactions).execute()
            log.info("[Cron] Inserted %d transactions for user %s", len(transactions), user_id)

        # 6. Update last_fetched
        supabase_admin.table("gmail_sync").update({
            "last_fetched": datetime.utcnow().isoformat(),
        }).eq("user_id", user_id).execute()

        # 7. Run ETL pipeline (synchronous here — cron doesn't have BackgroundTasks)
        run_pipeline(user_id)

        result["transactions_found"] = len(transactions)
        log.info("[Cron] ── Done for user %s: %d transactions ──", user_id, len(transactions))

    except RuntimeError as e:
        # Expected operational errors (token expired, no Gmail connected, etc.)
        result["status"] = "skipped"
        result["error"]  = str(e)
        log.warning("[Cron] Skipped user %s: %s", user_id, e)

    except Exception as e:
        # Unexpected errors — log full traceback, don't crash the loop
        result["status"] = "error"
        result["error"]  = str(e)
        log.exception("[Cron] Unexpected error for user %s: %s", user_id, e)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Batch cron function with parallel processing
# ─────────────────────────────────────────────────────────────────────────────

def fetch_gmail_for_all_users() -> dict:
    """
    Fetches Gmail transactions for ALL users who have connected Gmail.
    Runs users concurrently via ThreadPoolExecutor (max CRON_MAX_WORKERS threads).

    Returns a summary dict with per-user results and aggregate counts.
    """
    log.info("[Cron] ══ Starting batch Gmail fetch ══")
    start = datetime.utcnow()

    # Load all users with Gmail tokens
    users_res = supabase_admin.table("gmail_sync").select("user_id").execute()
    user_ids  = [row["user_id"] for row in users_res.data]

    if not user_ids:
        log.info("[Cron] No users with Gmail connected — nothing to do")
        return {"users_processed": 0, "results": []}

    log.info("[Cron] Processing %d users with max_workers=%d", len(user_ids), CRON_MAX_WORKERS)

    results    = []
    ok_count   = 0
    skip_count = 0
    err_count  = 0

    with ThreadPoolExecutor(max_workers=CRON_MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_gmail_for_user, uid): uid
            for uid in user_ids
        }

        for future in as_completed(futures):
            uid = futures[future]
            try:
                result = future.result()
            except Exception as e:
                # fetch_gmail_for_user never raises, but guard anyway
                result = {
                    "user_id": uid,
                    "transactions_found": 0,
                    "status": "error",
                    "error": str(e),
                }
                log.exception("[Cron] Future raised unexpectedly for user %s", uid)

            results.append(result)

            if result["status"] == "ok":
                ok_count += 1
            elif result["status"] == "skipped":
                skip_count += 1
            else:
                err_count += 1

    elapsed = (datetime.utcnow() - start).total_seconds()

    log.info(
        "[Cron] ══ Batch complete in %.1fs — ok=%d skipped=%d errors=%d ══",
        elapsed, ok_count, skip_count, err_count,
    )

    return {
        "users_processed":   len(user_ids),
        "ok":                ok_count,
        "skipped":           skip_count,
        "errors":            err_count,
        "elapsed_seconds":   round(elapsed, 2),
        "results":           results,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Protected test route
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/protected")
def protected_route(request: Request):
    user = get_user_from_token(request)
    return {
        "message": "You are authenticated",
        "user_id": user.id,
        "email":   user.email,
    }


# ─────────────────────────────────────────────────────────────────────────────
# File upload
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/upload-file")
async def upload_file(request: Request, background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    user     = get_user_from_token(request)
    contents = await file.read()

    try:
        if file.filename.lower().endswith(".csv"):
            df = pd.read_csv(io.BytesIO(contents))
        else:
            df = pd.read_excel(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not parse file: {e}")

    df.columns = [col.strip().lower() for col in df.columns]

    date_col       = next((c for c in df.columns if "date" in c), None)
    narration_col  = next((c for c in df.columns if "narration" in c or "description" in c), None)
    withdrawal_col = next((c for c in df.columns if "withdrawal" in c or "debit" in c), None)

    if not date_col or not withdrawal_col:
        raise HTTPException(
            status_code=422,
            detail=f"Required columns not found. Got: {list(df.columns)}",
        )

    df           = df.dropna(subset=[withdrawal_col])
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
                "raw_text":         narration,
            })
        except Exception as e:
            log.warning("Row parse error: %s", e)

    if transactions:
        supabase_admin.table("transactions").insert(transactions).execute()

    background_tasks.add_task(run_pipeline, user.id)

    return {
        "message":            "File processed successfully",
        "transactions_found": len(transactions),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Gmail OAuth — initiate
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/auth/google")
def auth_google(token: str):
    try:
        supabase_anon.auth.get_user(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token — please log in again")

    redirect_uri    = f"{BACKEND_URL}/auth/callback"
    scope           = "https://www.googleapis.com/auth/gmail.readonly"
    google_auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={GOOGLE_CLIENT_ID}"
        f"&redirect_uri={redirect_uri}"
        f"&response_type=code"
        f"&scope={scope}"
        f"&access_type=offline"
        f"&prompt=consent"
        f"&state={token}"
    )
    return RedirectResponse(google_auth_url)


# ─────────────────────────────────────────────────────────────────────────────
# Gmail OAuth — callback
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/auth/callback")
def auth_callback(code: str, state: str):
    token = state
    try:
        user = supabase_anon.auth.get_user(token).user
    except Exception:
        raise HTTPException(status_code=401, detail="Session expired — please reconnect Gmail")

    redirect_uri = f"{BACKEND_URL}/auth/callback"
    token_res    = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "code":          code,
            "client_id":     GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri":  redirect_uri,
            "grant_type":    "authorization_code",
        },
        timeout=15,
    )
    token_data = token_res.json()

    if "error" in token_data:
        raise HTTPException(status_code=400, detail=f"Google OAuth error: {token_data['error']}")

    access_token  = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")

    if not access_token:
        raise HTTPException(status_code=400, detail="No access token returned from Google")

    supabase_admin.table("gmail_sync").upsert({
        "user_id":       user.id,
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "updated_at":    datetime.utcnow().isoformat(),
    }).execute()

    return RedirectResponse(FRONTEND_URL)


# ─────────────────────────────────────────────────────────────────────────────
# Manual Gmail fetch endpoint (unchanged behaviour, refactored internals)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/fetch-gmail")
def fetch_gmail(request: Request, background_tasks: BackgroundTasks):
    """
    Triggers Gmail fetch for the currently logged-in user.
    Returns immediately; fetch + ETL run in a background task.
    """
    user = get_user_from_token(request)
    background_tasks.add_task(fetch_gmail_for_user, user.id)
    return {"status": "started", "user_id": user.id}


# ─────────────────────────────────────────────────────────────────────────────
# Cron endpoint — fetch Gmail for ALL users
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/cron/fetch-all")
def cron_fetch_all(
    background_tasks: BackgroundTasks,
    x_cron_secret: str | None = Header(default=None),
):
    """
    Secure cron endpoint. Called by a scheduler 3x/day.
    Protected by X-Cron-Secret header — set CRON_SECRET in your .env.

    Runs the batch in a background task so the HTTP response is instant.
    The caller (cron job) gets a 202 Accepted immediately.
    """
    if CRON_SECRET and x_cron_secret != CRON_SECRET:
        raise HTTPException(status_code=403, detail="Invalid or missing cron secret")

    background_tasks.add_task(fetch_gmail_for_all_users)
    log.info("[Cron] /cron/fetch-all triggered — running in background")
    return {"status": "accepted", "message": "Batch fetch started in background"}


app.include_router(correction_router)

