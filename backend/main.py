"""
main.py — FastAPI application (web layer only).

All heavy ETL / ML work is delegated to Celery workers via task queues.
This process stays lean: it handles auth, parsing, DB inserts, and
immediately returns 202 Accepted to the caller.

What changed from the original
───────────────────────────────
• Removed: BackgroundTasks, ThreadPoolExecutor, as_completed
• Removed: fetch_gmail_for_user(), fetch_gmail_for_all_users(),
           _fetch_gmail_messages(), _parse_gmail_messages(), etc.
           (all moved to tasks.py)
• Added  : .delay() calls to tasks imported from tasks.py
• Added  : Celery task ID in API responses for optional status polling
"""

from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi import UploadFile, File
from fastapi.responses import RedirectResponse
from supabase import create_client
from datetime import datetime
import os
import io
import re
import logging
import requests
import pandas as pd

from dotenv import load_dotenv
from ml.correction_routes import router as correction_router

# Import Celery tasks — FastAPI never runs ETL/ML directly
from tasks import (
    fetch_gmail_for_user_task,
    fetch_gmail_for_all_users_task,
    run_pipeline_task,
)

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
    allow_origins=["*"],
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
FRONTEND_URL         = os.getenv("FRONTEND_URL", "https://spend-stream-phi.vercel.app")
BACKEND_URL          = os.getenv("BACKEND_URL",  "https://spendstream-api.onrender.com")
CRON_SECRET          = os.getenv("CRON_SECRET", "")


# ─────────────────────────────────────────────────────────────────────────────
# Auth helper
# ─────────────────────────────────────────────────────────────────────────────

def get_user_from_token(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or malformed Authorization header",
        )
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

@app.post("/upload-file", status_code=202)
async def upload_file(request: Request, file: UploadFile = File(...)):
    """
    Parse the uploaded CSV / Excel, insert raw transactions, then
    enqueue the ETL pipeline as a Celery task.

    Returns 202 Accepted immediately — processing happens asynchronously.
    """
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
                # raw_text retained briefly; scrubbed post-categorisation
                "raw_text":         narration,
            })
        except Exception as e:
            log.warning("Row parse error: %s", e)

    if transactions:
        supabase_admin.table("transactions").insert(transactions).execute()

    # ── Enqueue ETL pipeline — worker handles ML categorisation + scrub ──────
    task = run_pipeline_task.delay(user.id)
    log.info("[API] Enqueued run_pipeline_task %s for user %s", task.id, user.id)

    return {
        "status":             "accepted",
        "message":            "File processed — categorisation running in background",
        "transactions_found": len(transactions),
        "task_id":            task.id,   # client can poll /task/{id} if needed
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
        raise HTTPException(
            status_code=400,
            detail=f"Google OAuth error: {token_data['error']}",
        )

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
# Manual Gmail fetch — enqueue for current user
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/fetch-gmail", status_code=202)
def fetch_gmail(request: Request):
    """
    Enqueue a Gmail fetch + ETL pipeline for the logged-in user.
    Returns 202 Accepted immediately.
    """
    user = get_user_from_token(request)
    task = fetch_gmail_for_user_task.delay(user.id)
    log.info("[API] Enqueued fetch_gmail_for_user_task %s for user %s", task.id, user.id)

    return {
        "status":  "accepted",
        "message": "Gmail fetch started — check back shortly for updated transactions",
        "task_id": task.id,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Cron endpoint — fan-out fetch for ALL users
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/cron/fetch-all", status_code=202)
def cron_fetch_all(x_cron_secret: str | None = Header(default=None)):
    """
    Secure cron endpoint. Enqueues fetch_gmail_for_all_users_task which
    in turn fans out one task per connected user. Returns 202 instantly.

    Protected by X-Cron-Secret header — set CRON_SECRET in .env.
    """
    if CRON_SECRET and x_cron_secret != CRON_SECRET:
        raise HTTPException(status_code=403, detail="Invalid or missing cron secret")

    task = fetch_gmail_for_all_users_task.delay()
    log.info("[API] Enqueued fetch_gmail_for_all_users_task %s", task.id)

    return {
        "status":  "accepted",
        "message": "Batch fetch enqueued — workers will process all connected users",
        "task_id": task.id,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Optional: task status polling endpoint
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/task/{task_id}")
def get_task_status(task_id: str, request: Request):
    """
    Poll the status of any Celery task by ID.
    Useful for the frontend to show a progress indicator after /fetch-gmail.
    """
    get_user_from_token(request)   # ensure caller is authenticated

    from celery_app import celery
    result = celery.AsyncResult(task_id)

    return {
        "task_id": task_id,
        "status":  result.status,           # PENDING / STARTED / SUCCESS / FAILURE / RETRY
        "result":  result.result if result.ready() else None,
    }


app.include_router(correction_router)