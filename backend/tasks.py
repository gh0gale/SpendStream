"""
tasks.py — Gmail sync and pipeline work, as plain Python functions.

No Celery. No Redis. Functions are called directly via FastAPI BackgroundTasks
or invoked inline from cron_runner.py. Every Gmail sync is recorded in
sync_jobs; the dashboard polls that row to know when the sync has finished.

Function catalogue
──────────────────
  create_sync_job(user_id, kind, runner)            — queued sync_jobs row; returns its id
  run_pipeline_task(user_id)                        — ETL + ML categorisation for one user
  fetch_gmail_for_user_task(user_id, job_id, ...)   — Gmail fetch → insert → run_pipeline
  fetch_gmail_for_all_users_task(runner)            — the above for every connected user
"""

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import config
import token_crypto
from etl import run_pipeline          # your existing ETL / ML entry-point
from gmail_parser import message_to_transaction

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Supabase admin client (service-role key — workers run server-side only)
# ─────────────────────────────────────────────────────────────────────────────

supabase_admin = config.admin_client()

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

GOOGLE_CLIENT_ID     = config.GOOGLE_CLIENT_ID
GOOGLE_CLIENT_SECRET = config.GOOGLE_CLIENT_SECRET
CRON_MAX_WORKERS     = config.CRON_MAX_WORKERS

GMAIL_API     = "https://gmail.googleapis.com/gmail/v1/users/me"
# Gmail's from: matches whole words, not prefixes. "hdfc" matches only a display name such as
# "HDFC Bank InstaAlerts"; HDFC also sends from a bare alerts@hdfcbank.bank.in, which needs
# "hdfcbank". The bare domain words are verified for HDFC only; the other banks' are unchecked.
BANK_QUERY    = ("from:(hdfc OR hdfcbank OR icici OR icicibank OR sbi OR axis OR axisbank OR kotak OR yesbank) "
                 "(debited OR spent OR txn OR transaction)")
PAGE_SIZE     = 100                  # message ids per list request (Gmail allows up to 500)
MAX_PAGES     = 50                   # 5,000 messages per sync; hitting it is logged, never silent
FETCH_WORKERS = 4                    # parallel message downloads per sync (8 hit the per-minute quota)
FETCH_OVERLAP = timedelta(hours=1)   # re-read the last hour; message ids make repeats harmless
IST           = timezone(timedelta(hours=5, minutes=30))   # India has no daylight saving
RETRY_STATUS  = {429, 500, 502, 503, 504}
MAX_ATTEMPTS  = 3                    # per Gmail request, for the statuses above and timeouts
RATE_LIMIT_WAIT = 10                 # seconds; x attempt number on a 403/429 rate limit
MAX_PENDING   = 500                  # message ids kept for the next sync to retry
STALE_JOB_AGE = timedelta(minutes=30)  # a job still running after this died with its process
EVENT_RETENTION = timedelta(days=90)   # app_events older than this are deleted on each scheduled run
UNIQUE_VIOLATION = "23505"           # Postgres error code; sync_jobs_one_active raises it
HISTORY_MONTHS = 12                  # how far before the current month "read an earlier month" can go
INTERRUPTED_MESSAGE = "Interrupted by a server restart. Sync again."

_sleep = time.sleep                  # tests replace it

# Each sync holds a pool thread, 4 download threads and its message ids in
# memory, on a 512 MB host. A waiting sync keeps its job row "queued".
# ponytail: in-process cap; a real queue only if waits regularly exceed a minute.
_SYNC_SLOTS = threading.BoundedSemaphore(config.MAX_CONCURRENT_SYNCS)

# Shown to the user in sync_jobs.error: no internal details.
RECONNECT_MESSAGE     = "Gmail access has expired. Reconnect Gmail to keep syncing."
NOT_CONNECTED_MESSAGE = "Gmail is not connected. Connect Gmail first."
FAILED_MESSAGE        = "Gmail sync failed. Try again later."
ACTIVE_MESSAGE        = "A sync is already running."
HISTORY_LIMIT_MESSAGE = f"Everything from the last {HISTORY_MONTHS} months has already been read."


class GmailReconnectRequired(RuntimeError):
    """Google no longer accepts the stored grant; only the user can restore it."""


class GmailNotConnected(RuntimeError):
    """The user has no stored Gmail tokens."""


class SyncAlreadyActive(RuntimeError):
    """The user already has a queued or running sync (sync_jobs_one_active)."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ═════════════════════════════════════════════════════════════════════════════
# Sync jobs
# ═════════════════════════════════════════════════════════════════════════════

def create_sync_job(user_id: str, kind: str, runner: str = "api") -> str:
    """
    Insert a queued sync_jobs row. kind: 'manual' | 'scheduled'; runner: 'api' | 'cron_runner'.
    Raises SyncAlreadyActive when the user has a live queued or running job: the
    database allows one (unique index sync_jobs_one_active). The user's own dead
    jobs are failed first, so a crashed sync cannot block the next one.
    """
    fail_stale_jobs(user_id)
    try:
        res = supabase_admin.table("sync_jobs").insert({
            "user_id": user_id,
            "kind":    kind,
            "runner":  runner,
        }).execute()
    except Exception as e:
        if getattr(e, "code", None) == UNIQUE_VIOLATION:
            raise SyncAlreadyActive(f"user {user_id} already has an active sync") from e
        raise
    return res.data[0]["id"]


def has_active_sync(user_id: str) -> bool:
    """True if the user has a queued or running sync young enough to be alive."""
    cutoff = (datetime.now(timezone.utc) - STALE_JOB_AGE).isoformat()
    res = (
        supabase_admin.table("sync_jobs").select("id")
        .eq("user_id", user_id).in_("status", ["queued", "running"])
        .gte("created_at", cutoff).limit(1).execute()
    )
    return bool(res.data)


def _update_job(job_id: str, **fields) -> None:
    supabase_admin.table("sync_jobs").update(fields).eq("id", job_id).execute()


def fail_stale_jobs(user_id: str | None = None) -> None:
    """
    Fail jobs (one user's, or everyone's) still queued or running after
    STALE_JOB_AGE. A host that sleeps
    (Render free) kills in-flight background work without a restart the API
    would notice, so the scheduled run cleans up instead. The dashboard then
    stops waiting, and the unmoved cursor makes the next sync read it again.
    """
    cutoff = (datetime.now(timezone.utc) - STALE_JOB_AGE).isoformat()
    try:
        query = supabase_admin.table("sync_jobs").update({
            "status":      "failed",
            "error":       INTERRUPTED_MESSAGE,
            "finished_at": _now_iso(),
        }).in_("status", ["queued", "running"]).lt("created_at", cutoff)
        if user_id:
            query = query.eq("user_id", user_id)
        query.execute()
    except Exception as e:
        log.warning("[Task] Could not fail stale sync jobs: %s", e)


def prune_old_rows() -> None:
    """Delete event rows past EVENT_RETENTION. Best effort: a failure only warns."""
    cutoff = (datetime.now(timezone.utc) - EVENT_RETENTION).isoformat()
    try:
        supabase_admin.table("app_events").delete().lt("created_at", cutoff).execute()
    except Exception as e:
        log.warning("[Task] Could not prune app_events: %s", type(e).__name__)


def _finish_job(job_id: str, counters: dict, **fields) -> None:
    """
    Close a job with its counters. A database without the counter columns
    (migration 20261001000000 not applied yet) still finishes the job, without them.
    """
    try:
        _update_job(job_id, **fields, **counters)
    except Exception as e:
        log.warning("[Task] Could not store sync counters: %s", type(e).__name__)
        _update_job(job_id, **fields)


# ═════════════════════════════════════════════════════════════════════════════
# Google tokens
# ═════════════════════════════════════════════════════════════════════════════

def _decrypt_refresh_token(user_id: str, encrypted: str | None) -> str | None:
    """Stored refresh tokens are Fernet ciphertext (token_crypto)."""
    if not encrypted:
        return None
    try:
        return token_crypto.decrypt(encrypted)
    except token_crypto.InvalidToken:
        raise GmailReconnectRequired(f"refresh token for user {user_id} cannot be decrypted")


def _refresh_google_token(user_id: str, refresh_token: str) -> str:
    """Exchange a refresh token for a new access token and persist it."""
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

    if data.get("error") == "invalid_grant":
        raise GmailReconnectRequired(f"Google rejected the refresh token for user {user_id}")
    if "error" in data or not data.get("access_token"):
        raise RuntimeError(f"Google token refresh failed for user {user_id}: {data.get('error')}")

    new_access_token = data["access_token"]

    supabase_admin.table("gmail_credentials").update({
        "access_token": new_access_token,
        "updated_at":   _now_iso(),
    }).eq("user_id", user_id).execute()

    log.info("[Gmail] Token refreshed for user %s", user_id)
    return new_access_token


def _gmail_get(url: str, access_token: str, params: dict | None = None):
    """
    GET a Gmail API url, retrying rate limits (429), server errors and
    timeouts up to MAX_ATTEMPTS times with backoff (Retry-After when Google
    sends it). Returns the last response; raises only if every attempt timed out.
    """
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            res = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, params=params, timeout=15)
        except requests.RequestException:
            if attempt == MAX_ATTEMPTS:
                raise
            _sleep(2 ** (attempt - 1))
            continue
        # Gmail also reports rate limits as 403 (rateLimitExceeded,
        # userRateLimitExceeded); seen on a real 153-message read 2026-09-27.
        rate_limited_403 = res.status_code == 403 and "ratelimitexceeded" in (getattr(res, "text", "") or "").lower()
        if (res.status_code not in RETRY_STATUS and not rate_limited_403) or attempt == MAX_ATTEMPTS:
            return res
        # Rate limits are per minute per user, so a second's pause does not
        # clear them; server errors usually do clear quickly.
        retry_after = (getattr(res, "headers", None) or {}).get("Retry-After")
        if str(retry_after or "").isdigit():
            _sleep(min(int(retry_after), 30))
        elif res.status_code in (403, 429):
            _sleep(RATE_LIMIT_WAIT * attempt)
        else:
            _sleep(2 ** (attempt - 1))
    return res


# ═════════════════════════════════════════════════════════════════════════════
# Gmail fetch
# ═════════════════════════════════════════════════════════════════════════════

def _gmail_query(last_fetched: str | None, now: datetime) -> str:
    """
    Bank-alert search since the last sync (minus FETCH_OVERLAP), or from the
    start of the current month in India on the first sync (user decision
    2026-09-27: a short first read stays inside Gmail's per-user rate limit).
    Gmail's after: takes epoch seconds.
    """
    if last_fetched:
        since = datetime.fromisoformat(last_fetched) - FETCH_OVERLAP
    else:
        since = now.astimezone(IST).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    return f"{BANK_QUERY} after:{int(since.timestamp())}"


def _month_start_ist(dt: datetime) -> datetime:
    """Midnight at the start of dt's calendar month in India."""
    return dt.astimezone(IST).replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _month_index(dt: datetime) -> int:
    local = dt.astimezone(IST)
    return local.year * 12 + local.month - 1


def next_history_window(read_from: datetime, now: datetime) -> tuple[datetime, datetime] | None:
    """
    The calendar month (India) before the oldest month already read, as
    (start, end), or None once it would be more than HISTORY_MONTHS before the
    current month. read_from is any moment inside the oldest month read.
    """
    end   = _month_start_ist(read_from)
    start = _month_start_ist(end - timedelta(days=1))
    if _month_index(start) < _month_index(now) - HISTORY_MONTHS:
        return None
    return start, end


def _gmail_window_query(start: datetime, end: datetime) -> str:
    """Bank-alert search for one closed window. Gmail's after:/before: take epoch seconds."""
    return f"{BANK_QUERY} after:{int(start.timestamp())} before:{int(end.timestamp())}"


def history_window(user_id: str, now: datetime | None = None, sync_row: dict | None = None):
    """
    The next earlier month to read for this user, or None at the limit. The
    oldest month read so far is gmail_sync.history_from; for users from before
    it existed it is the month of their earliest stored payment, else this month.
    """
    now = now or datetime.now(timezone.utc)
    if sync_row is None:
        res = supabase_admin.table("gmail_sync").select("*").eq("user_id", user_id).execute()
        sync_row = res.data[0] if res.data else {}
    read_from = sync_row.get("history_from")
    if not read_from:
        res = (
            supabase_admin.table("transactions").select("timestamp")
            .eq("user_id", user_id).order("timestamp").limit(1).execute()
        )
        read_from = res.data[0]["timestamp"] if res.data else None
    read_from = datetime.fromisoformat(str(read_from).replace("Z", "+00:00")) if read_from else now
    return next_history_window(read_from, now)


def _list_message_ids(
    user_id: str,
    access_token: str,
    refresh_token: str | None,
    query: str,
) -> tuple[list[str], str]:
    """Every matching message id, newest first, up to MAX_PAGES pages. Returns (ids, access_token)."""
    ids: list[str] = []
    page_token = None

    for _ in range(MAX_PAGES):
        params = {"q": query, "maxResults": PAGE_SIZE}
        if page_token:
            params["pageToken"] = page_token

        res = _gmail_get(f"{GMAIL_API}/messages", access_token, params)
        if res.status_code == 401:
            if not refresh_token:
                raise GmailReconnectRequired(f"Gmail returned 401 for user {user_id} and no refresh token is stored")
            access_token = _refresh_google_token(user_id, refresh_token)
            res = _gmail_get(f"{GMAIL_API}/messages", access_token, params)
        if res.status_code != 200:
            raise RuntimeError(f"Gmail message list returned {res.status_code} for user {user_id}")

        data = res.json()
        ids.extend(m["id"] for m in data.get("messages", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    else:
        if page_token:
            log.warning(
                "[Gmail] Stopped after %d pages (%d messages) for user %s; older messages in this window were not fetched",
                MAX_PAGES, len(ids), user_id,
            )

    log.info("[Gmail] Found %d messages for user %s", len(ids), user_id)
    return ids, access_token


class MessageDownloadFailed(RuntimeError):
    """A message could not be downloaded after retries; the next sync retries it."""


def _google_reason(res) -> str:
    """Google's machine-readable error reason (e.g. userRateLimitExceeded).
    A fixed code from Google, never mail content, so it is safe to log."""
    try:
        err = (res.json() or {}).get("error") or {}
        errors = err.get("errors") or [{}]
        return str(errors[0].get("reason") or err.get("status") or "unknown")[:60]
    except (ValueError, AttributeError, TypeError):
        return "unknown"


def _download_message(msg_id: str, access_token: str) -> dict | None:
    """The message, or None if Gmail no longer has it (404). Raises on any other failure."""
    res = _gmail_get(f"{GMAIL_API}/messages/{msg_id}", access_token, {"format": "full"})
    if res.status_code == 404:
        log.warning("[Gmail] Message %s no longer exists", msg_id)
        return None
    if res.status_code != 200:
        raise MessageDownloadFailed(f"message {msg_id} returned {res.status_code} ({_google_reason(res)})")
    return res.json()


def _download_transactions(user_id: str, message_ids: list[str], access_token: str) -> tuple[list[dict], list[str]]:
    """
    Download messages in parallel and keep the ones that parse as debit
    alerts. Returns (rows, failed_ids): the ids that could not be downloaded
    or read, which the caller keeps for the next sync to retry (DATA-07).
    """
    fetched_at = datetime.now(timezone.utc)

    def work(msg_id: str) -> dict | None:
        message = _download_message(msg_id, access_token)
        return message_to_transaction(user_id, message, fetched_at) if message else None

    transactions, failed = [], []
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        futures = {pool.submit(work, msg_id): msg_id for msg_id in message_ids}
        for future in as_completed(futures):
            try:
                row = future.result()
            except Exception as e:
                failed.append(futures[future])
                log.warning("[Gmail] Message %s for user %s not read: %s", futures[future], user_id, e)
                continue
            if row:
                transactions.append(row)
    return transactions, failed


# ═════════════════════════════════════════════════════════════════════════════
# Plain functions (previously Celery tasks)
# ═════════════════════════════════════════════════════════════════════════════

def run_pipeline_task(user_id: str) -> dict:
    """
    Run the ETL + ML categorisation pipeline for a single user.
    Called via FastAPI BackgroundTasks or directly from other functions.
    """
    log.info("[Task] run_pipeline_task started for user %s", user_id)

    try:
        run_pipeline(user_id)
        return {"status": "ok", "user_id": user_id}

    except Exception as exc:
        log.exception("[Task] run_pipeline_task failed for user %s: %s", user_id, exc)
        raise


def fetch_gmail_for_user_task(user_id: str, job_id: str | None = None, runner: str = "api") -> dict:
    """
    Gmail sync for one user, once one of MAX_CONCURRENT_SYNCS slots is free.
    A job created elsewhere (the /fetch-gmail route) is passed in; otherwise a
    'scheduled' job is created here. See _sync_user for the steps.
    """
    if job_id is None:
        try:
            job_id = create_sync_job(user_id, "scheduled", runner)
        except SyncAlreadyActive:
            log.info("[Task] Skipped user %s: a sync is already active", user_id)
            return {"status": "skipped", "user_id": user_id, "error": ACTIVE_MESSAGE}
    waiting = time.monotonic()
    with _SYNC_SLOTS:
        log.info("[Task] Sync slot for job %s after %.1f s", job_id, time.monotonic() - waiting)
        return _sync_user(user_id, job_id)


def backfill_gmail_for_user_task(user_id: str, job_id: str) -> dict:
    """
    Read one earlier month of bank alerts for a user (job created by POST
    /backfill-gmail), once a sync slot is free. Never moves the normal cursor.
    """
    waiting = time.monotonic()
    with _SYNC_SLOTS:
        log.info("[Task] Sync slot for backfill job %s after %.1f s", job_id, time.monotonic() - waiting)
        return _sync_user(user_id, job_id, backfill=True)


def _sync_user(user_id: str, job_id: str, backfill: bool = False) -> dict:
    """
    Gmail fetch + insert + pipeline for one user, recorded in sync_jobs.
    backfill=True reads the next earlier month instead of new mail: the
    window comes from history_window(), the read cursor and retry list are
    left alone (failed ids are added to it), and history_from moves back.

    Steps
    ─────
    1. Load the tokens (gmail_credentials) and the fetch cursor (gmail_sync)
    2. Validate the access token, refreshing it if needed
    3. List every matching message id since the cursor (minus FETCH_OVERLAP)
    4. Download and parse the messages in parallel
    5. Insert raw rows; a message already stored is skipped (user_id, message_id)
    6. Move the cursor to the moment this sync started, always, and keep the
       ids of messages that could not be read in gmail_sync.pending_message_ids;
       the next sync retries them along with the new mail (DATA-07). Holding
       the cursor back instead made every sync re-read the whole first window, and that burst
       is what tripped Gmail's rate limit (2026-09-27).
    7. Run the ETL pipeline

    """
    started = datetime.now(timezone.utc)
    _update_job(job_id, status="running", started_at=started.isoformat())
    log.info("[Task] fetch_gmail_for_user_task started for user %s (job %s)", user_id, job_id)

    try:
        # 1. Tokens and cursor
        cred_res = (
            supabase_admin.table("gmail_credentials")
            .select("access_token, refresh_token_encrypted")
            .eq("user_id", user_id)
            .execute()
        )
        if not cred_res.data:
            raise GmailNotConnected(f"no Gmail tokens for user {user_id}")

        cred          = cred_res.data[0]
        access_token  = cred["access_token"]
        refresh_token = _decrypt_refresh_token(user_id, cred.get("refresh_token_encrypted"))

        # "*": a database without the pending_message_ids migration still syncs.
        sync_res     = supabase_admin.table("gmail_sync").select("*").eq("user_id", user_id).execute()
        sync_row     = sync_res.data[0] if sync_res.data else {}
        last_fetched = sync_row.get("last_fetched")
        has_pending  = "pending_message_ids" in sync_row
        pending      = list(sync_row.get("pending_message_ids") or [])

        window = None
        if backfill:
            window = history_window(user_id, started, sync_row)
            if window is None:
                _update_job(job_id, status="failed", error=HISTORY_LIMIT_MESSAGE, finished_at=_now_iso())
                return {"status": "skipped", "user_id": user_id, "job_id": job_id, "error": HISTORY_LIMIT_MESSAGE}

        # 2. No separate token check: the list call below refreshes on a 401.

        # 3. List
        query = _gmail_window_query(*window) if window else _gmail_query(last_fetched, started)
        message_ids, access_token = _list_message_ids(user_id, access_token, refresh_token, query)
        if not window:
            message_ids = list(dict.fromkeys(message_ids + pending))   # new mail, then last sync's failures

        # 4. Download and parse
        transactions, failed = (_download_transactions(user_id, message_ids, access_token)
                                if message_ids else ([], []))

        # 5. Insert; the database drops messages it already has
        inserted = 0
        if transactions:
            res = (
                supabase_admin.table("transactions")
                .upsert(transactions, on_conflict="user_id,message_id", ignore_duplicates=True)
                .execute()
            )
            inserted = len(res.data or [])
            log.info("[Task] %d of %d parsed alerts were new for user %s", inserted, len(transactions), user_id)

        # 6. Cursor: the start of this sync, so mail arriving meanwhile is read
        #    next time. Messages not read are kept and retried (DATA-07).
        if window:
            # A backfill leaves the cursor alone; it records how far back it has read.
            update = {"history_from": window[0].isoformat()}
            if has_pending and failed:
                update["pending_message_ids"] = list(dict.fromkeys(pending + failed))[:MAX_PENDING]
        else:
            update = {"last_fetched": started.isoformat()}
            if has_pending:
                update["pending_message_ids"] = failed[:MAX_PENDING]
            # The first ever sync reads from the start of this month: remember it.
            if "history_from" in sync_row and not last_fetched and not sync_row["history_from"]:
                update["history_from"] = _month_start_ist(started).isoformat()
        supabase_admin.table("gmail_sync").update(update).eq("user_id", user_id).execute()
        if failed and has_pending:
            log.warning("[Task] %d message(s) not read for user %s; kept for the next sync", len(failed), user_id)
        elif failed:
            log.warning("[Task] %d message(s) not read for user %s and cannot be kept: apply the "
                        "pending_message_ids migration", len(failed), user_id)

        # 7. Pipeline
        run_pipeline_task(user_id)

        counters = {
            "messages_listed": len(message_ids),
            "messages_failed": len(failed),
            "alerts_parsed":   len(transactions),
        }
        _finish_job(job_id, counters, status="succeeded", transactions_found=inserted, finished_at=_now_iso())
        log.info("[Task] Sync finished for user %s", user_id)
        return {"status": "ok", "user_id": user_id, "job_id": job_id, "transactions_found": inserted}

    except GmailReconnectRequired as e:
        log.warning("[Task] Gmail reconnect required for user %s: %s", user_id, e)
        supabase_admin.table("gmail_sync").update({"needs_reconnect": True}).eq("user_id", user_id).execute()
        _update_job(job_id, status="failed", error=RECONNECT_MESSAGE, finished_at=_now_iso())
        return {"status": "skipped", "user_id": user_id, "job_id": job_id, "error": RECONNECT_MESSAGE}

    except GmailNotConnected as e:
        log.warning("[Task] Skipped user %s: %s", user_id, e)
        _update_job(job_id, status="failed", error=NOT_CONNECTED_MESSAGE, finished_at=_now_iso())
        return {"status": "skipped", "user_id": user_id, "job_id": job_id, "error": NOT_CONNECTED_MESSAGE}

    except Exception as exc:
        log.exception("[Task] fetch_gmail_for_user_task failed for user %s: %s", user_id, exc)
        _update_job(job_id, status="failed", error=FAILED_MESSAGE, finished_at=_now_iso())
        raise


def fetch_gmail_for_all_users_task(runner: str = "api") -> dict:
    """
    Sync every user whose Gmail connection works (needs_reconnect is false).
    Uses a thread pool to process multiple users concurrently.
    """
    log.info("[Task] fetch_gmail_for_all_users_task — loading user list")
    fail_stale_jobs()
    prune_old_rows()

    users_res = (
        supabase_admin.table("gmail_sync")
        .select("user_id")
        .eq("needs_reconnect", False)
        .execute()
    )
    user_ids = [row["user_id"] for row in users_res.data]

    if not user_ids:
        log.info("[Task] No users with Gmail connected — nothing to process")
        return {"users_processed": 0}

    results = []
    with ThreadPoolExecutor(max_workers=CRON_MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_gmail_for_user_task, uid, None, runner): uid for uid in user_ids}
        for future in as_completed(futures):
            uid = futures[future]
            try:
                result = future.result()
                results.append(result)
                log.info("[Task] Completed user %s: %s", uid, result.get("status"))
            except Exception as e:
                log.error("[Task] Failed user %s: %s", uid, e)
                results.append({"status": "error", "user_id": uid, "error": str(e)})

    log.info("[Task] Processed %d users", len(user_ids))
    _log_quality_report()
    return {"users_processed": len(user_ids), "results": results}


def _log_quality_report() -> None:
    """Last 7 days' quality numbers (uncategorised share, repeat corrections) into the log."""
    try:
        report = supabase_admin.rpc("quality_report", {}).execute().data
        log.info("[Quality] %s", report)
    except Exception as e:
        log.warning("[Quality] report failed: %s", type(e).__name__)
