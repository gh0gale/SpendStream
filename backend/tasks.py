"""
tasks.py — All heavy ETL / ML work runs here as plain Python functions.

No Celery. No Redis. Functions are called directly via FastAPI BackgroundTasks
or invoked inline from cron_runner.py.

Function catalogue
──────────────────
  run_pipeline_task(user_id)          — ETL + ML categorisation for one user
  fetch_gmail_for_user_task(user_id)  — Gmail fetch → insert → run_pipeline
  fetch_gmail_for_all_users_task()    — Loop: calls fetch_gmail_for_user for every connected user
  trigger_online_refit_task()         — Pulls recent feedback and refits the ML model in-place
"""

import os
import re
import base64
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from supabase import create_client
from dotenv import load_dotenv

from etl import run_pipeline          # your existing ETL / ML entry-point

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Supabase admin client (service-role key — workers run server-side only)
# ─────────────────────────────────────────────────────────────────────────────

supabase_admin = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)

# ─────────────────────────────────────────────────────────────────────────────
# Google OAuth config
# ─────────────────────────────────────────────────────────────────────────────

GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
CRON_MAX_WORKERS     = int(os.getenv("CRON_MAX_WORKERS", "5"))


# ═════════════════════════════════════════════════════════════════════════════
# Shared helpers (kept private — not exported to FastAPI)
# ═════════════════════════════════════════════════════════════════════════════

def _parse_date_safe(date_val):
    try:
        import pandas as pd
        return pd.to_datetime(date_val).to_pydatetime()
    except Exception:
        return None


def _extract_body(payload: dict) -> str | None:
    """Recursively extract the raw base64 body from a Gmail message payload."""
    if "parts" in payload:
        for part in payload["parts"]:
            mime = part.get("mimeType", "")
            if mime in ["text/plain", "text/html"]:
                data = part["body"].get("data")
                if data:
                    return data
            if "parts" in part:
                result = _extract_body(part)
                if result:
                    return result
    else:
        return payload.get("body", {}).get("data")
    return None


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
    """Proactively validate the access token; auto-refresh if expired."""
    test = requests.get(
        "https://gmail.googleapis.com/gmail/v1/users/me/profile",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=10,
    )
    if test.status_code != 401:
        return access_token

    if not refresh_token:
        raise RuntimeError(
            f"Access token expired for user {user_id} and no refresh token available"
        )

    log.info("[Gmail] Access token expired for user %s — refreshing", user_id)
    return _refresh_google_token(user_id, refresh_token)


def _fetch_gmail_messages(
    user_id: str,
    access_token: str,
    refresh_token: str | None,
    last_fetched: str | None,
) -> list[dict]:
    """Paginate Gmail and return raw message stubs."""
    if last_fetched:
        query = (
            "from:(hdfc OR icici OR sbi OR axis OR kotak OR yesbank) "
            f"(debited OR spent OR txn OR transaction) after:{last_fetched[:10]}"
        )
        log.info("[Gmail] Incremental fetch from %s for user %s", last_fetched[:10], user_id)
    else:
        first_day = datetime.utcnow().replace(day=1).strftime("%Y/%m/%d")
        query = (
            "from:(hdfc OR icici OR sbi OR axis OR kotak OR yesbank) "
            f"(debited OR spent OR txn OR transaction) after:{first_day}"
        )
        log.info("[Gmail] First-time fetch from %s for user %s", first_day, user_id)

    headers         = {"Authorization": f"Bearer {access_token}"}
    all_messages    = []
    next_page_token = None

    for _ in range(5):
        url = (
            "https://gmail.googleapis.com/gmail/v1/users/me/messages"
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
    """Fetch full email bodies and extract transaction data."""
    headers      = {"Authorization": f"Bearer {access_token}"}
    transactions = []

    for msg in messages:
        try:
            msg_id  = msg["id"]
            msg_res = requests.get(
                f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}",
                headers=headers,
                timeout=15,
            )
            msg_data = msg_res.json()
            payload  = msg_data.get("payload", {})
            raw_body = _extract_body(payload)

            if not raw_body:
                continue

            body = base64.urlsafe_b64decode(raw_body).decode("utf-8", errors="ignore")

            if not re.search(r"(?i)debited|spent|paid", body):
                continue

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
                    email_date = _parse_date_safe(header["value"])
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
            log.warning(
                "[Gmail] Email parse error (msg %s, user %s): %s",
                msg.get("id", "?"), user_id, e,
            )

    return transactions


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


def fetch_gmail_for_user_task(user_id: str) -> dict:
    """
    Gmail fetch + insert + pipeline for a single user.

    Steps
    ─────
    1. Load Gmail OAuth tokens from DB
    2. Validate / refresh access token
    3. Fetch message list (paginated, up to 50 messages)
    4. Parse emails → transaction dicts
    5. Insert into `transactions` table
    6. Update `last_fetched` timestamp
    7. Run the ETL pipeline inline
    """
    log.info("[Task] fetch_gmail_for_user_task started for user %s", user_id)

    try:
        # 1. Load tokens
        token_res = (
            supabase_admin.table("gmail_sync")
            .select("*")
            .eq("user_id", user_id)
            .execute()
        )
        if not token_res.data:
            raise RuntimeError("No Gmail tokens found — user has not connected Gmail")

        row           = token_res.data[0]
        access_token  = row["access_token"]
        refresh_token = row.get("refresh_token")
        last_fetched  = row.get("last_fetched")

        # 2. Validate / refresh
        access_token = _validate_and_refresh_token(user_id, access_token, refresh_token)

        # 3. Fetch message list
        messages = _fetch_gmail_messages(user_id, access_token, refresh_token, last_fetched)

        # 4. Parse
        transactions = _parse_gmail_messages(user_id, messages, access_token) if messages else []

        # 5. Insert
        if transactions:
            supabase_admin.table("transactions").insert(transactions).execute()
            log.info("[Task] Inserted %d transactions for user %s", len(transactions), user_id)

        # 6. Update last_fetched
        supabase_admin.table("gmail_sync").update({
            "last_fetched": datetime.utcnow().isoformat(),
        }).eq("user_id", user_id).execute()

        # 7. Run ETL pipeline inline
        run_pipeline_task(user_id)
        log.info("[Task] Pipeline completed for user %s", user_id)

        return {
            "status":             "ok",
            "user_id":            user_id,
            "transactions_found": len(transactions),
        }

    except RuntimeError as e:
        log.warning("[Task] Skipped user %s: %s", user_id, e)
        return {"status": "skipped", "user_id": user_id, "error": str(e)}

    except Exception as exc:
        log.exception("[Task] fetch_gmail_for_user_task failed for user %s: %s", user_id, exc)
        raise


def fetch_gmail_for_all_users_task() -> dict:
    """
    Loop through all users with Gmail connected and process each one.
    Uses a thread pool to process multiple users concurrently.
    """
    log.info("[Task] fetch_gmail_for_all_users_task — loading user list")

    users_res = supabase_admin.table("gmail_sync").select("user_id").execute()
    user_ids  = [row["user_id"] for row in users_res.data]

    if not user_ids:
        log.info("[Task] No users with Gmail connected — nothing to process")
        return {"users_processed": 0}

    results = []
    with ThreadPoolExecutor(max_workers=CRON_MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_gmail_for_user_task, uid): uid for uid in user_ids}
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
    return {"users_processed": len(user_ids), "results": results}


def trigger_online_refit_task() -> dict:
    """Pull recent feedback and refit the ML model in-place."""
    log.info("[Task] Extracting DB feedback for online refit")

    res = supabase_admin.table("category_feedback").select("*").order("corrected_at", desc=True).limit(50).execute()
    if not res.data:
        return {"status": "no data"}

    samples = []
    from ml.categoriser import _online_refit, extract_metadata
    for row in res.data:
        meta = extract_metadata(float(row.get("amount") or 0.0), row.get("corrected_at"))
        samples.append({
            "raw_text": row.get("raw_text") or row.get("merchant", ""),
            "category": row.get("corrected_category"),
            "metadata": meta
        })

    _online_refit(bulk_samples=samples)
    log.info("[Task] Refit complete")
    return {"status": "ok", "refit_samples": len(samples)}
