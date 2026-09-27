"""
main.py — FastAPI application (web layer only).

All heavy ETL / ML work is delegated to FastAPI BackgroundTasks.
This process stays lean: it handles auth and Gmail OAuth, schedules
Gmail syncs, and immediately returns 202 Accepted to the caller.
Gmail is the only ingestion path; there is no file upload.
Category corrections do not pass through here: the frontend calls the
correct_category() Postgres function (supabase/migrations) directly.
No Celery. No Redis. No external broker needed.
"""

import hmac
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import requests
from fastapi import FastAPI, Request, HTTPException, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse

import config
import model_store
import token_crypto
from ml.categoriser import model_info

# Import plain task functions
from tasks import (
    INTERRUPTED_MESSAGE,
    RECONNECT_MESSAGE,
    create_sync_job,
    fetch_gmail_for_user_task,
    fetch_gmail_for_all_users_task,
)

# Refuse to start half-configured: missing keys, empty CRON_SECRET, bad Fernet key.
config.validate_api_config()

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Supabase clients
# ─────────────────────────────────────────────────────────────────────────────

supabase_anon  = config.anon_client()
supabase_admin = config.admin_client()

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

OAUTH_STATE_TTL      = timedelta(minutes=10)
OAUTH_START_LIMIT    = 5                       # Gmail connect attempts per user per OAUTH_STATE_TTL
MANUAL_SYNC_INTERVAL = timedelta(minutes=10)
GMAIL_SCOPE          = "https://www.googleapis.com/auth/gmail.readonly"

# Tables with per-user rows, children before parents (foreign keys).
# gold_monthly_summary is a view over silver_transactions, so it is not listed.
USER_TABLES = (
    "category_feedback",
    "user_merchant_rules",
    "silver_transactions",
    "bronze_transactions",
    "transactions",
    "sync_jobs",
    "gmail_credentials",
    "oauth_states",
    "gmail_sync",
)


# ─────────────────────────────────────────────────────────────────────────────
# Startup: syncs run inside this process, so any sync_jobs row still queued or
# running from a previous process died with it. Mark it failed so the
# dashboard stops waiting. Jobs owned by cron_runner.py are left alone.
# ─────────────────────────────────────────────────────────────────────────────

def _fail_interrupted_jobs() -> None:
    supabase_admin.table("sync_jobs").update({
        "status":      "failed",
        "error":       INTERRUPTED_MESSAGE,
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }).eq("runner", "api").in_("status", ["queued", "running"]).execute()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # A fresh deploy has no model file (it is not in git): fetch the live one.
    # No model means every synced row would be left uncategorised for good,
    # so a failure here stops startup.
    model_store.ensure_local()
    try:
        _fail_interrupted_jobs()
    except Exception as e:
        log.warning("[API] Could not clean up interrupted sync jobs: %s", e)
    yield


# ─────────────────────────────────────────────────────────────────────────────
# App & middleware
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(lifespan=lifespan)

@app.get("/ping")
def ping():
    """The process is up. Checks nothing else."""
    return {"status": "alive"}


@app.get("/health")
def health():
    """
    The process can reach the database: one tiny read. For uptime monitors
    (plan Phase 7); returns no data, so it needs no login. 503 when the read
    fails, with the reason kept in the server log, not the response.
    """
    try:
        supabase_admin.table("gmail_sync").select("user_id").limit(1).execute()
    except Exception as e:
        log.error("[health] database check failed: %s", type(e).__name__)
        return JSONResponse(status_code=503, content={"status": "unavailable", "database": "unreachable"})
    return {"status": "ok", "database": "ok"}

# Only the frontend may call the API from a browser. Auth travels in the
# Authorization header, never in cookies, so credentials are not allowed.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[config.FRONTEND_URL],
    allow_credentials=False,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)


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
# Gmail OAuth — a one-time state value links Google's redirect to the user,
# so the user's JWT never appears in a URL (SEC-04)
# ─────────────────────────────────────────────────────────────────────────────

def _redirect_uri() -> str:
    return f"{config.BACKEND_URL}/auth/callback"


def _create_oauth_state(user_id: str) -> str:
    now = datetime.now(timezone.utc)
    # Expired states are never used; clear them before adding a new one.
    supabase_admin.table("oauth_states").delete().lt("expires_at", now.isoformat()).execute()
    state = secrets.token_urlsafe(32)
    supabase_admin.table("oauth_states").insert({
        "state":      state,
        "user_id":    user_id,
        "expires_at": (now + OAUTH_STATE_TTL).isoformat(),
    }).execute()
    return state


def _consume_oauth_state(state: str) -> str | None:
    """Delete the state row and return its user id; None if unknown or expired."""
    res = supabase_admin.table("oauth_states").delete().eq("state", state).execute()
    if not res.data:
        return None
    row = res.data[0]
    if datetime.fromisoformat(row["expires_at"]) <= datetime.now(timezone.utc):
        return None
    return row["user_id"]


def _back_to_frontend(result: str) -> RedirectResponse:
    """The dashboard reads ?gmail=<result> and shows a message."""
    return RedirectResponse(f"{config.FRONTEND_URL}/?gmail={result}")


@app.post("/auth/google/start")
def auth_google_start(request: Request):
    """Return the Google consent URL for the signed-in user."""
    user  = get_user_from_token(request)
    # SEC-09: each call stores a state row; cap how many one user can make.
    since  = (datetime.now(timezone.utc) - OAUTH_STATE_TTL).isoformat()
    recent = (
        supabase_admin.table("oauth_states").select("state")
        .eq("user_id", user.id).gte("created_at", since).execute()
    ).data or []
    if len(recent) >= OAUTH_START_LIMIT:
        raise HTTPException(
            status_code=429,
            detail="Too many Gmail connection attempts. Try again in a few minutes.",
            headers={"Retry-After": str(int(OAUTH_STATE_TTL.total_seconds()))},
        )
    state = _create_oauth_state(user.id)
    params = {
        "client_id":     config.GOOGLE_CLIENT_ID,
        "redirect_uri":  _redirect_uri(),
        "response_type": "code",
        "scope":         GMAIL_SCOPE,
        "access_type":   "offline",
        "prompt":        "consent",
        "state":         state,
    }
    return {"url": "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)}


@app.get("/auth/callback")
def auth_callback(state: str | None = None, code: str | None = None, error: str | None = None):
    user_id = _consume_oauth_state(state) if state else None
    if not user_id:
        return _back_to_frontend("expired")
    if error or not code:
        return _back_to_frontend("denied")

    try:
        token_res = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code":          code,
                "client_id":     config.GOOGLE_CLIENT_ID,
                "client_secret": config.GOOGLE_CLIENT_SECRET,
                "redirect_uri":  _redirect_uri(),
                "grant_type":    "authorization_code",
            },
            timeout=15,
        )
        token_data = token_res.json()
    except (requests.RequestException, ValueError) as e:
        log.warning("[OAuth] Token exchange failed for user %s: %s", user_id, e)
        return _back_to_frontend("error")

    access_token = token_data.get("access_token")
    if "error" in token_data or not access_token:
        log.warning("[OAuth] Google returned no token for user %s: %s", user_id, token_data.get("error"))
        return _back_to_frontend("error")

    now = datetime.now(timezone.utc).isoformat()
    credentials = {"user_id": user_id, "access_token": access_token, "updated_at": now}
    refresh_token = token_data.get("refresh_token")
    if refresh_token:
        credentials["refresh_token_encrypted"] = token_crypto.encrypt(refresh_token)

    supabase_admin.table("gmail_credentials").upsert(credentials).execute()
    supabase_admin.table("gmail_sync").upsert({
        "user_id":         user_id,
        "updated_at":      now,
        "needs_reconnect": False,
    }).execute()

    return _back_to_frontend("connected")


# ─────────────────────────────────────────────────────────────────────────────
# Manual Gmail fetch — runs in background for current user
# ─────────────────────────────────────────────────────────────────────────────

def _claim_manual_sync(user_id: str) -> str:
    """
    Record a manual sync request in one conditional UPDATE.
    Returns "ok", "limited" (one was requested within MANUAL_SYNC_INTERVAL),
    "reconnect_required" (Google no longer accepts the stored grant) or
    "not_connected" (the user has no gmail_sync row).
    """
    now       = datetime.now(timezone.utc)
    threshold = (now - MANUAL_SYNC_INTERVAL).strftime("%Y-%m-%dT%H:%M:%SZ")
    res = (
        supabase_admin.table("gmail_sync")
        .update({"last_sync_requested_at": now.isoformat()})
        .eq("user_id", user_id)
        .eq("needs_reconnect", False)
        .or_(f"last_sync_requested_at.is.null,last_sync_requested_at.lt.{threshold}")
        .execute()
    )
    if res.data:
        return "ok"
    row = supabase_admin.table("gmail_sync").select("needs_reconnect").eq("user_id", user_id).execute()
    if not row.data:
        return "not_connected"
    return "reconnect_required" if row.data[0].get("needs_reconnect") else "limited"


@app.get("/fetch-gmail", status_code=202)
def fetch_gmail(request: Request, background_tasks: BackgroundTasks):
    """
    Kick off a Gmail fetch + ETL pipeline for the logged-in user.
    Returns 202 Accepted immediately with the sync_jobs id the dashboard polls.
    One manual sync per user per 10 minutes.
    """
    user  = get_user_from_token(request)
    claim = _claim_manual_sync(user.id)
    if claim == "not_connected":
        raise HTTPException(status_code=409, detail="Gmail is not connected. Connect Gmail first.")
    if claim == "reconnect_required":
        raise HTTPException(status_code=409, detail=RECONNECT_MESSAGE)
    if claim == "limited":
        raise HTTPException(
            status_code=429,
            detail="A sync already ran in the last 10 minutes. Try again later.",
            headers={"Retry-After": str(int(MANUAL_SYNC_INTERVAL.total_seconds()))},
        )

    job_id = create_sync_job(user.id, "manual")
    background_tasks.add_task(fetch_gmail_for_user_task, user.id, job_id)
    log.info("[API] Scheduled fetch_gmail_for_user_task for user %s (job %s)", user.id, job_id)

    return {
        "status":  "accepted",
        "message": "Gmail sync started.",
        "job_id":  job_id,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Cron endpoint — fan-out fetch for ALL users
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/cron/fetch-all", status_code=202)
def cron_fetch_all(background_tasks: BackgroundTasks, x_cron_secret: str | None = Header(default=None)):
    """
    Runs fetch_gmail_for_all_users_task in the background for every connected
    user. Requires the X-Cron-Secret header; CRON_SECRET is mandatory config.
    """
    if not x_cron_secret or not hmac.compare_digest(x_cron_secret.encode(), config.CRON_SECRET.encode()):
        raise HTTPException(status_code=403, detail="Invalid or missing cron secret")

    background_tasks.add_task(fetch_gmail_for_all_users_task)
    log.info("[API] Scheduled fetch_gmail_for_all_users_task")

    return {
        "status":  "accepted",
        "message": "Batch fetch scheduled — processing all connected users in background",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Account deletion
# ─────────────────────────────────────────────────────────────────────────────

def _revoke_google_access(user_id: str) -> None:
    """Best effort: ask Google to revoke the grant so no token outlives the account."""
    res = (
        supabase_admin.table("gmail_credentials")
        .select("access_token, refresh_token_encrypted")
        .eq("user_id", user_id)
        .execute()
    )
    if not res.data:
        return
    row   = res.data[0]
    token = None
    if row.get("refresh_token_encrypted"):
        try:
            token = token_crypto.decrypt(row["refresh_token_encrypted"])
        except token_crypto.InvalidToken:
            log.warning("[Account] Refresh token for user %s cannot be decrypted", user_id)
    token = token or row.get("access_token")
    if not token:
        return
    try:
        r = requests.post("https://oauth2.googleapis.com/revoke", data={"token": token}, timeout=10)
        if r.status_code != 200:
            log.warning("[Account] Google revoke returned %s for user %s", r.status_code, user_id)
    except requests.RequestException as e:
        log.warning("[Account] Google revoke failed for user %s: %s", user_id, e)


@app.delete("/account")
def delete_account(request: Request):
    """Revoke Gmail access, delete every row the user owns, then the auth user."""
    user = get_user_from_token(request)
    _revoke_google_access(user.id)
    for table in USER_TABLES:
        supabase_admin.table(table).delete().eq("user_id", user.id).execute()
    supabase_admin.auth.admin.delete_user(user.id)
    log.info("[Account] Deleted user %s", user.id)
    return {"status": "deleted"}


# ─────────────────────────────────────────────────────────────────────────────
# Model status
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/model-info")
def get_model_info(request: Request):
    """Signed-in users only (SEC-08); metadata was read once, when the model loaded."""
    get_user_from_token(request)
    return model_info()
