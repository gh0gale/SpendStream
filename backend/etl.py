import hashlib
import logging
import time
from datetime import datetime, timedelta, timezone

import config
from ml.categoriser import predict_batch
from merchant_identity import (   # noqa: F401  (re-exported for existing callers)
    MERCHANT_RULES,
    UPI_GATEWAYS,
    clean_merchant,
    extract_upi_merchant,
    looks_like_person,
    merchant_key,
)

supabase_admin = config.admin_client()
log = logging.getLogger("etl")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def make_fingerprint(user_id: str, amount: float, receiver: str, timestamp: str) -> str:
    """
    Legacy fingerprint for raw rows without a Gmail message id (rows from
    before 2026-09-15): amount, receiver and day. Two identical payments to
    the same receiver on the same day collapse into one. New rows use
    bronze_fingerprint(), which keys on the message id.
    """
    date_part = str(timestamp)[:10]  # just YYYY-MM-DD
    raw = f"{user_id}|{amount}|{receiver.strip().lower()}|{date_part}"
    return hashlib.sha256(raw.encode()).hexdigest()


# Merchant identity (names, the stable key, and who is a person) lives in
# merchant_identity.py, because ml/categoriser.py and correct_category() in
# supabase/migrations/ have to agree with this pipeline on all three.
# Re-exported here so `from etl import clean_merchant` keeps working.


# ─────────────────────────────────────────────────────────────────────────────
# Work queues
#
# Each stage reads a batch of rows whose marker column is still NULL, writes
# the next layer in one request, then marks the whole batch in one request.
# No stage loads a user's whole history, and no query can reach PostgREST's
# default 1,000-row response cap. Markers (supabase/migrations/
# 20260915000200_phase3_pipeline.sql): transactions.processed_at,
# bronze_transactions.processed_at, silver_transactions.predicted_at.
# Monthly totals are the gold_monthly_summary view; there is no gold stage.
# ─────────────────────────────────────────────────────────────────────────────

BATCH_SIZE = 200   # rows per request; keeps `id=in.(...)` query strings short
IST        = timezone(timedelta(hours=5, minutes=30))   # India has no daylight saving


def ist_date(timestamp) -> str | None:
    """
    The calendar date in India of a stored timestamp. The database returns
    timestamps in UTC, so taking the first ten characters put payments made
    between midnight and 05:30 IST on the previous day. None if unknown.
    """
    if not timestamp:
        return None
    try:
        dt = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except ValueError:
        return str(timestamp)[:10]
    if dt.tzinfo is None:
        return dt.date().isoformat()
    return dt.astimezone(IST).date().isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def bronze_fingerprint(row: dict) -> str:
    """
    Gmail rows are identified by their message id, so two identical payments
    on the same day stay separate. Rows without one (from before 2026-09-15)
    keep the legacy amount|receiver|day fingerprint.
    """
    if row.get("message_id"):
        raw = f"{row['user_id']}|gmail|{row['message_id']}"
        return hashlib.sha256(raw.encode()).hexdigest()
    return make_fingerprint(
        user_id   = row["user_id"],
        amount    = float(row["amount"]),
        receiver  = row.get("receiver") or "",
        timestamp = row.get("timestamp") or "",
    )


def _mark(table: str, column: str, ids: list[str]) -> None:
    """Set the batch's marker column; stop if the database did not record every row."""
    res  = supabase_admin.table(table).update({column: _now_iso()}).in_("id", ids).execute()
    done = len(res.data or [])
    if done != len(ids):
        raise RuntimeError(
            f"Marked {done} of {len(ids)} rows in {table}; stopping so the batch is not re-read forever"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Raw → Bronze
# ─────────────────────────────────────────────────────────────────────────────

def run_raw_to_bronze(user_id: str) -> int:
    """
    Copy pending raw rows into bronze, one batch at a time, and mark them processed.
    A row whose fingerprint is already in bronze is a duplicate: the insert
    skips it (ON CONFLICT DO NOTHING) and it is still marked processed.
    Returns the number of raw rows processed.
    """
    processed = 0

    while True:
        batch = (
            supabase_admin.table("transactions")
            .select("*")
            .eq("user_id", user_id)
            .is_("processed_at", "null")
            .order("id")
            .limit(BATCH_SIZE)
            .execute()
        ).data or []
        if not batch:
            break

        bronze_rows: dict[str, dict] = {}
        for row in batch:
            fingerprint = bronze_fingerprint(row)
            bronze_rows.setdefault(fingerprint, {
                "raw_id":           row["id"],
                "user_id":          user_id,
                "amount":           float(row["amount"]),
                "receiver":         row.get("receiver", ""),
                "transaction_type": row.get("transaction_type", "debit"),
                "timestamp":        row.get("timestamp"),
                "source":           row.get("source", "unknown"),
                "raw_text":         row.get("raw_text", ""),
                "fingerprint":      fingerprint,
                "message_id":       row.get("message_id"),
                "is_duplicate":     False,
                "created_at":       _now_iso(),
            })

        supabase_admin.table("bronze_transactions") \
            .upsert(list(bronze_rows.values()), on_conflict="fingerprint", ignore_duplicates=True) \
            .execute()
        _mark("transactions", "processed_at", [row["id"] for row in batch])
        processed += len(batch)

    return processed


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — Bronze → Silver
# ─────────────────────────────────────────────────────────────────────────────

def run_bronze_to_silver(user_id: str) -> int:
    """
    Copy pending bronze rows into silver with cleaned merchant names and
    dates, one batch at a time, and mark them processed. A bronze row already
    in silver is skipped (unique bronze_id). Category is left empty here,
    for the categorisation stage. Returns the number of bronze rows processed.
    """
    processed = 0

    while True:
        batch = (
            supabase_admin.table("bronze_transactions")
            .select("*")
            .eq("user_id", user_id)
            .is_("processed_at", "null")
            .order("id")
            .limit(BATCH_SIZE)
            .execute()
        ).data or []
        if not batch:
            break

        silver_rows = []
        for row in batch:
            if row.get("is_duplicate"):
                continue
            timestamp = row.get("timestamp")
            receiver  = row.get("receiver", "")
            merchant  = clean_merchant(receiver)
            silver_rows.append({
                "bronze_id":        row["id"],
                "user_id":          user_id,
                "amount":           float(row["amount"]),
                "merchant":         merchant,
                # 4.1: the stable key, computed once here and never again, so a
                # correction made on any row from this merchant is found by
                # every later one. NULL when there is nothing to key on.
                "merchant_key":     merchant_key(receiver, merchant) or None,
                # Payments to people stay personal: they never reach the
                # shared merchant directory (4.4).
                "merchant_is_person": looks_like_person(receiver),
                "transaction_type": row.get("transaction_type", "debit"),
                "transaction_date": ist_date(timestamp),   # date in India; NULL if unknown
                "source":           row.get("source", "unknown"),
                "category":         None,       # filled by ML stage
                "is_categorised":   False,
                "created_at":       _now_iso(),
            })

        if silver_rows:
            supabase_admin.table("silver_transactions") \
                .upsert(silver_rows, on_conflict="bronze_id", ignore_duplicates=True) \
                .execute()
        _mark("bronze_transactions", "processed_at", [row["id"] for row in batch])
        processed += len(batch)

    return processed


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3 — Silver categorisation
#
# 4.3 Prediction order, strongest first:
#   1. the user's own rule for this merchant   — absolute, never overruled
#   2. the shared merchant directory           — only merchants several
#                                                unrelated users agreed on
#   3. the model                               — everything else
#
# Matching is on the exact merchant_key and nothing else. A merely similar
# name never inherits a rule, so correcting "Sharma General Store" cannot
# touch "Sharma Medical".
# ─────────────────────────────────────────────────────────────────────────────

def _lookup_user_rules(user_id: str, keys: list) -> dict:
    """This user's corrections for the merchants in this batch: {key: category}."""
    wanted = sorted({k for k in keys if k})
    if not wanted:
        return {}
    rows = (
        supabase_admin.table("user_merchant_rules")
        .select("merchant_key, category")
        .eq("user_id", user_id)
        .in_("merchant_key", wanted)
        .execute()
    ).data or []
    return {r["merchant_key"]: r["category"] for r in rows if r.get("category")}


def _lookup_directory(keys: list) -> dict:
    """Shared merchant categories for the keys no user rule covers: {key: category}."""
    wanted = sorted({k for k in keys if k})
    if not wanted:
        return {}
    rows = (
        supabase_admin.table("merchant_directory")
        .select("merchant_key, category")
        .in_("merchant_key", wanted)
        .execute()
    ).data or []
    return {r["merchant_key"]: r["category"] for r in rows if r.get("category")}


def run_categorise_silver(user_id: str) -> int:
    """
    Predict categories for pending silver rows (uncategorised and never
    predicted), one batch at a time, and write each batch with one
    apply_predictions() call, which skips rows the user corrected meanwhile.
    A row the model is unsure about is stored with category NULL and is not
    predicted again. Returns the number of rows that received a category.
    """
    categorised_count = 0
    other_count       = 0
    rule_count        = 0      # decided by the user's own correction
    directory_count   = 0      # decided by shared consensus

    while True:
        batch = (
            supabase_admin.table("silver_transactions")
            .select("id, merchant, merchant_key, bronze_id, amount, transaction_date")
            .eq("user_id", user_id)
            .eq("is_categorised", False)
            .is_("predicted_at", "null")
            .order("id")
            .limit(BATCH_SIZE)
            .execute()
        ).data or []
        if not batch:
            break

        bronze_ids = [r["bronze_id"] for r in batch if r.get("bronze_id")]
        bronze_map = {}
        if bronze_ids:
            bronze_res = (
                supabase_admin.table("bronze_transactions")
                .select("id, receiver, timestamp")
                .in_("id", bronze_ids)
                .execute()
            )
            bronze_map = {r["id"]: r for r in bronze_res.data or []}

        amounts    = [float(r.get("amount", 0) or 0) for r in batch]
        timestamps = [
            bronze_map.get(r.get("bronze_id"), {}).get("timestamp") or r.get("transaction_date")
            for r in batch
        ]
        receivers  = [
            bronze_map.get(r.get("bronze_id"), {}).get("receiver") or r.get("merchant", "") or ""
            for r in batch
        ]

        # 4.3 Prediction order. A key the user has ruled on is decided here and
        # never reaches the model; next the shared directory, which only holds
        # merchants several people agreed on; only what is left is predicted.
        keys       = [r.get("merchant_key") for r in batch]
        user_rules = _lookup_user_rules(user_id, keys)
        directory  = _lookup_directory([k for k in keys if k and k not in user_rules])

        pending = [i for i, key in enumerate(keys)
                   if not key or (key not in user_rules and key not in directory)]

        predictions: dict[int, str] = {}
        if pending:
            # The categoriser cleans the receiver and adds the shop-QR marker
            # itself (ml/features.input_text), exactly as in training.
            predicted = predict_batch(
                receivers  = [receivers[i]  for i in pending],
                amounts    = [amounts[i]    for i in pending],
                timestamps = [timestamps[i] for i in pending],
            )
            predictions = {i: str(p[0]) for i, p in zip(pending, predicted)}

        rows = []
        for i, row in enumerate(batch):
            key = keys[i]
            if key and key in user_rules:
                category = user_rules[key]     # a rule of "Other" is a real answer (BUG-06)
                rule_count += 1
            elif key and key in directory:
                category = directory[key]
                directory_count += 1
            else:
                # From the model, "Other" means unsure: stored uncategorised.
                category = predictions[i] if predictions[i] != "Other" else None

            rows.append({"id": row["id"], "category": category})
            if category is not None:
                categorised_count += 1
            else:
                other_count += 1

        updated = supabase_admin.rpc(
            "apply_predictions", {"p_user_id": user_id, "p_rows": rows}
        ).execute().data
        if not updated:
            # Zero is fine when the user corrected every row meanwhile
            # (apply_predictions skips corrected rows, and they are no longer
            # pending). It is a defect only if the rows are still pending.
            still_pending = (
                supabase_admin.table("silver_transactions")
                .select("id")
                .in_("id", [r["id"] for r in rows])
                .eq("is_categorised", False)
                .is_("predicted_at", "null")
                .execute()
            ).data or []
            if still_pending:
                raise RuntimeError(
                    "apply_predictions updated no rows; stopping so the batch is not re-read forever"
                )

    log.info("categorise user=%s categorised=%d user_rule=%d directory=%d uncategorised=%d",
             user_id, categorised_count, rule_count, directory_count, other_count)
    return categorised_count


# ─────────────────────────────────────────────────────────────────────────────
# Master pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(user_id: str):
    """
    Full ETL pipeline triggered after every Gmail fetch:

    Raw transactions
        ↓  run_raw_to_bronze     — fingerprint (Gmail message id), drop duplicates
    Bronze
        ↓  run_bronze_to_silver  — clean merchant names, normalise dates
    Silver (uncategorised)
        ↓  run_categorise_silver — ML model assigns categories
    Silver (categorised)
        ↓  gold_monthly_summary  — a database view; nothing to run
    Gold
    """

    t0 = time.perf_counter()
    try:
        b = run_raw_to_bronze(user_id)
        s = run_bronze_to_silver(user_id)
        c = run_categorise_silver(user_id)
        log.info("pipeline user=%s raw=%d bronze=%d categorised=%d seconds=%.1f",
                 user_id, b, s, c, time.perf_counter() - t0)
    except Exception as e:
        # Type only: an exception message can carry row values.
        log.error("pipeline user=%s failed: %s", user_id, type(e).__name__)
        raise
