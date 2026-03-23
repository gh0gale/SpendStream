import os
import re
import hashlib
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv
from ml.categoriser import predict_batch

load_dotenv()

supabase_admin = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def make_fingerprint(user_id: str, amount: float, receiver: str, timestamp: str) -> str:
    """
    Creates a unique hash for a transaction.
    Same transaction from Gmail AND CSV will produce the same fingerprint
    so duplicates across sources get caught.
    We round timestamp to the day so minor time differences don't matter.
    """
    date_part = str(timestamp)[:10]  # just YYYY-MM-DD
    raw = f"{user_id}|{amount}|{receiver.strip().lower()}|{date_part}"
    return hashlib.sha256(raw.encode()).hexdigest()


# Payment gateways — these are the middlemen, NOT the actual merchant.
# When a UPI string contains one of these, we strip it and look further
# for the real business name that follows.
UPI_GATEWAYS = [
    "paytm", "phonepe", "gpay", "googlepay", "google pay",
    "okaxis", "okicici", "oksbi", "okhdfc", "okhdfcbank",
    "ybl", "axl", "ibl", "upi", "bhim",
]

# Known merchant rules applied AFTER gateway stripping.
# Order matters — more specific patterns first.
MERCHANT_RULES = [
    # Food delivery
    (r"(?i)swiggy",              "Swiggy"),
    (r"(?i)zomato",              "Zomato"),
    (r"(?i)dunzo",               "Dunzo"),
    (r"(?i)eatclub",             "EatClub"),
    (r"(?i)faasos",              "Faasos"),
    (r"(?i)box8",                "Box8"),
    (r"(?i)freshmenu",           "FreshMenu"),

    # Restaurants / QSR
    (r"(?i)kfc",                 "KFC"),
    (r"(?i)mcdonalds|mcdonald",  "McDonald's"),
    (r"(?i)dominos|domino",      "Domino's"),
    (r"(?i)subway",              "Subway"),
    (r"(?i)burger\s*king",       "Burger King"),
    (r"(?i)pizza\s*hut",         "Pizza Hut"),
    (r"(?i)starbucks",           "Starbucks"),
    (r"(?i)cafe\s*coffee|ccd",   "CCD"),

    # Groceries
    (r"(?i)blinkit|grofers",     "Blinkit"),
    (r"(?i)zepto",               "Zepto"),
    (r"(?i)bigbasket",           "BigBasket"),
    (r"(?i)dmart",               "DMart"),
    (r"(?i)jiomart",             "JioMart"),
    (r"(?i)milkbasket",          "Milkbasket"),

    # Shopping
    (r"(?i)amazon",              "Amazon"),
    (r"(?i)flipkart",            "Flipkart"),
    (r"(?i)myntra",              "Myntra"),
    (r"(?i)ajio",                "AJIO"),
    (r"(?i)nykaa",               "Nykaa"),
    (r"(?i)meesho",              "Meesho"),

    # Transport
    (r"(?i)uber",                "Uber"),
    (r"(?i)ola",             "Ola"),
    (r"(?i)rapido",              "Rapido"),
    (r"(?i)irctc",               "IRCTC"),
    (r"(?i)makemytrip|mmt",      "MakeMyTrip"),
    (r"(?i)redbus",              "RedBus"),
    (r"(?i)cleartrip",           "Cleartrip"),
    (r"(?i)indigo",              "IndiGo"),

    # Investments
    (r"(?i)groww",               "Groww"),
    (r"(?i)zerodha",             "Zerodha"),
    (r"(?i)kuvera",              "Kuvera"),
    (r"(?i)coin\.zerodha",       "Zerodha Coin"),
    (r"(?i)mutual\s*fund",       "Mutual Fund"),
    (r"(?i)iccl|nsccl",         "Stock Settlement"),

    # Entertainment / Subscriptions
    (r"(?i)netflix",             "Netflix"),
    (r"(?i)spotify",             "Spotify"),
    (r"(?i)hotstar|disney",      "Disney+ Hotstar"),
    (r"(?i)youtube|youtubepremium", "YouTube Premium"),
    (r"(?i)apple\s*media|apple\s*service", "Apple Services"),
    (r"(?i)prime\s*video|primevideo", "Amazon Prime"),
    (r"(?i)zee5",                "ZEE5"),
    (r"(?i)sonyliv",             "SonyLIV"),

    # Telecom / Utilities
    (r"(?i)jio",             "Jio"),
    (r"(?i)airtel",              "Airtel"),
    (r"(?i)bsnl",                "BSNL"),
    (r"(?i)vi|vodafone|idea",  "Vi"),
    (r"(?i)electricity|bescom|msedcl|tpddl|adani\s*electric", "Electricity"),
    (r"(?i)water\s*board|water\s*supply|bwssb",               "Water Bill"),
    (r"(?i)gas\s*(supply|bill)|mahanagar\s*gas|indraprastha",  "Gas Bill"),

    # Health / Pharmacy
    (r"(?i)apollo",              "Apollo Pharmacy"),
    (r"(?i)1mg",             "1mg"),
    (r"(?i)pharmeasy",           "PharmEasy"),
    (r"(?i)netmeds",             "Netmeds"),
    (r"(?i)medplus",             "MedPlus"),
    (r"(?i)global\s*medical",    "Medical Store"),

    # Finance / Payments
    (r"(?i)paytm",               "Paytm"),
    (r"(?i)phonepe",             "PhonePe"),
    (r"(?i)gpay|google\s*pay",   "Google Pay"),
    (r"(?i)mobikwik",            "MobiKwik"),
    (r"(?i)cred",              "CRED"),
]


def extract_upi_merchant(raw: str) -> str | None:
    """
    For UPI strings like:
      "VPA paytm.s11h0ar@pty NBC Vikhroli W"
      "VPA gpay-11256438096@okbizaxis Food Xpress"
      "VPA paytmqr557cs9@paytm GLOBAL MEDICAL AND G"

    Strategy:
      1. Strip the VPA prefix
      2. Remove the UPI handle (everything up to and including @xxx)
      3. Whatever text remains after the handle = actual merchant name
    """
    # Only process UPI-style strings
    if not re.search(r"@|VPA|UPI", raw, re.IGNORECASE):
        return None

    # Remove VPA prefix
    cleaned = re.sub(r"(?i)^vpa\s*", "", raw).strip()

    # Extract text AFTER the UPI handle (@gateway part)
    # Pattern: anything@anything MERCHANT NAME
    after_handle = re.sub(r"\S+@\S+\s*", "", cleaned).strip()

    if len(after_handle) >= 3:
        # Clean up trailing junk — truncated words, single chars
        after_handle = re.sub(r"\s+[A-Z]\s*$", "", after_handle).strip()
        return after_handle

    return None


def clean_merchant(raw_receiver: str) -> str:
    """
    Two-pass cleaning:
    Pass 1 — if it is a UPI string, extract the real merchant name
             from the text AFTER the @gateway handle, then apply rules to that.
    Pass 2 — apply MERCHANT_RULES to the full string as fallback.
    Finally — generic cleanup if nothing matched.
    """
    if not raw_receiver:
        return "Unknown"

    # Pass 1: UPI extraction — get real merchant name from after the handle
    upi_merchant = extract_upi_merchant(raw_receiver)
    if upi_merchant:
        # Try to match the extracted name against known merchants
        for pattern, clean_name in MERCHANT_RULES:
            if re.search(pattern, upi_merchant):
                return clean_name
        # No rule matched — return the extracted name cleaned up
        cleaned = re.sub(r"\s+", " ", upi_merchant).strip()
        return cleaned[:60] if cleaned else "Unknown"

    # Pass 2: No UPI handle — apply rules to full string
    for pattern, clean_name in MERCHANT_RULES:
        if re.search(pattern, raw_receiver):
            return clean_name

    # Pass 3: Generic cleanup
    cleaned = raw_receiver
    # Strip leading UPI username (e.g. "mukhtaransaria337" or "ss2002786kgn")
    cleaned = re.sub(r"^[a-z0-9]{6,}\s+", "", cleaned, flags=re.IGNORECASE)
    # Remove honorifics
    cleaned = re.sub(r"\b(Mr|Mrs|Miss|Dr|Shri)\b\.?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[/@]\S+", "", cleaned)
    cleaned = re.sub(r"\d{6,}", "", cleaned)
    cleaned = re.sub(r"\b(UPI|NEFT|IMPS|RTGS|VPA)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:60] if cleaned else "Unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Raw → Bronze
# ─────────────────────────────────────────────────────────────────────────────

def run_raw_to_bronze(user_id: str):
    """
    Reads unprocessed rows from raw transactions table.
    Generates a fingerprint for each row and skips it if:
      - raw_id already exists in bronze (already processed)
      - fingerprint already exists in bronze (duplicate transaction
        from a different source or re-fetch overlap)

    Duplicates are skipped entirely — they never enter Bronze.
    This keeps the table lean for automated nightly fetches where
    Gmail will return overlapping emails across runs.
    """
    print(f"[ETL] Raw → Bronze for user {user_id}")

    # Single query to get both already-processed raw_ids and existing fingerprints
    existing_res = supabase_admin.table("bronze_transactions") \
        .select("raw_id, fingerprint") \
        .eq("user_id", user_id) \
        .execute()

    already_processed      = {row["raw_id"]      for row in existing_res.data if row.get("raw_id")}
    existing_fingerprints  = {row["fingerprint"] for row in existing_res.data if row.get("fingerprint")}

    raw_res = supabase_admin.table("transactions") \
        .select("*") \
        .eq("user_id", user_id) \
        .execute()

    if not raw_res.data:
        print("[ETL] No raw transactions found")
        return 0

    bronze_rows  = []
    skipped_raw  = 0
    skipped_dupe = 0

    for row in raw_res.data:
        # Skip if this raw row was already processed in a previous pipeline run
        if row["id"] in already_processed:
            skipped_raw += 1
            continue

        fingerprint = make_fingerprint(
            user_id   = user_id,
            amount    = float(row["amount"]),
            receiver  = row.get("receiver", ""),
            timestamp = row.get("timestamp", "")
        )

        # Skip if same transaction already exists (duplicate from re-fetch or CSV+Gmail overlap)
        if fingerprint in existing_fingerprints:
            skipped_dupe += 1
            continue

        # New unique transaction — add to bronze
        existing_fingerprints.add(fingerprint)

        bronze_rows.append({
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
            "created_at":       datetime.utcnow().isoformat()
        })

    if bronze_rows:
        supabase_admin.table("bronze_transactions").insert(bronze_rows).execute()

    print(f"[ETL] Bronze: {len(bronze_rows)} inserted | {skipped_dupe} duplicates blocked | {skipped_raw} already processed")
    return len(bronze_rows)


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — Bronze → Silver
# ─────────────────────────────────────────────────────────────────────────────

def run_bronze_to_silver(user_id: str):
    """
    Reads non-duplicate bronze rows not yet in silver.
    Cleans merchant names, normalises dates, writes to silver_transactions.
    Category is left empty here — filled by ML model in next stage.
    """
    print(f"[ETL] Bronze → Silver for user {user_id}")

    # Get bronze IDs already in silver
    existing_bronze_ids = supabase_admin.table("silver_transactions") \
        .select("bronze_id") \
        .eq("user_id", user_id) \
        .execute()

    already_in_silver = {row["bronze_id"] for row in existing_bronze_ids.data}

    # Only process non-duplicate bronze rows
    bronze_res = supabase_admin.table("bronze_transactions") \
        .select("*") \
        .eq("user_id", user_id) \
        .eq("is_duplicate", False) \
        .execute()

    if not bronze_res.data:
        print("[ETL] No bronze rows to process")
        return 0

    silver_rows = []
    for row in bronze_res.data:
        if row["id"] in already_in_silver:
            continue

        # Clean merchant name
        merchant = clean_merchant(row.get("receiver", ""))

        # Normalise to date only (drop time)
        try:
            transaction_date = str(row["timestamp"])[:10]
        except Exception:
            transaction_date = datetime.utcnow().strftime("%Y-%m-%d")

        silver_rows.append({
            "bronze_id":        row["id"],
            "user_id":          user_id,
            "amount":           float(row["amount"]),
            "merchant":         merchant,
            "transaction_type": row.get("transaction_type", "debit"),
            "transaction_date": transaction_date,
            "source":           row.get("source", "unknown"),
            "category":         None,       # filled by ML stage
            "is_categorised":   False,
            "created_at":       datetime.utcnow().isoformat()
        })

    if silver_rows:
        supabase_admin.table("silver_transactions").insert(silver_rows).execute()
        print(f"[ETL] Silver: inserted {len(silver_rows)} rows")

    return len(silver_rows)


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3 — Silver categorisation (ML model)
# ─────────────────────────────────────────────────────────────────────────────

def run_categorise_silver(user_id: str) -> int:
    """
    Fetches all uncategorised silver rows for this user,
    runs them through the ML model in a single batch call,
    and updates each row with category + confidence + is_categorised.

    Rows with confidence < threshold get category="Other" and
    is_categorised=False so they can be reviewed or retrained later.
    """
    print(f"[ETL] Categorising silver rows for user {user_id}")

    uncategorised = supabase_admin.table("silver_transactions") \
        .select("id, merchant, bronze_id") \
        .eq("user_id", user_id) \
        .eq("is_categorised", False) \
        .execute()

    if not uncategorised.data:
        print("[ETL] No uncategorised silver rows")
        return 0

    # Fetch matching bronze raw_text for richer ML input
    bronze_ids = [r["bronze_id"] for r in uncategorised.data if r.get("bronze_id")]
    bronze_map = {}

    if bronze_ids:
        bronze_res = supabase_admin.table("bronze_transactions") \
            .select("id, receiver") \
            .in_("id", bronze_ids) \
            .execute()
        bronze_map = {r["id"]: r.get("receiver", "") for r in bronze_res.data}

    # Build input texts: use raw bronze receiver string when available
    # (more signal than cleaned merchant name alone)
    silver_rows = uncategorised.data
    input_texts = [
        bronze_map.get(row.get("bronze_id"), row.get("merchant", ""))
        for row in silver_rows
    ]

    # Batch predict — single vectorisation pass
    predictions = predict_batch(input_texts)

    # Update each row
    categorised_count = 0
    other_count       = 0

    for row, (category, confidence), raw_text in zip(silver_rows, predictions, input_texts):
        is_categorised = category != "Other"

        supabase_admin.table("silver_transactions").update({
            "category":       category,
            "is_categorised": is_categorised,
        }).eq("id", row["id"]).execute()

        if is_categorised:
            categorised_count += 1
        else:
            other_count += 1

    print(f"[ETL] Categorised: {categorised_count} rows | Low confidence (Other): {other_count} rows")
    return categorised_count


# ─────────────────────────────────────────────────────────────────────────────
# Stage 4 — Silver → Gold (aggregation)
# ─────────────────────────────────────────────────────────────────────────────

def run_silver_to_gold(user_id: str) -> int:
    """
    Aggregates all categorised silver rows into gold_monthly_summary.
    Groups by (user_id, month, category) — upserts so safe to re-run.
    Recalculates totals from scratch each time to stay accurate.
    """
    print(f"[ETL] Silver → Gold for user {user_id}")

    silver_res = supabase_admin.table("silver_transactions") \
        .select("amount, transaction_date, category") \
        .eq("user_id", user_id) \
        .eq("is_categorised", True) \
        .execute()

    if not silver_res.data:
        print("[ETL] No categorised silver rows — skipping gold")
        return 0

    # Aggregate: group by (month, category)
    summary: dict[tuple, dict] = {}

    for row in silver_res.data:
        try:
            month    = str(row["transaction_date"])[:7] + "-01"
            category = row.get("category") or "Other"
            amount   = float(row["amount"])
            key      = (month, category)

            if key not in summary:
                summary[key] = {"total_amount": 0.0, "txn_count": 0}

            summary[key]["total_amount"] += amount
            summary[key]["txn_count"]    += 1
        except Exception as e:
            print(f"[ETL] Gold aggregation row error: {e}")

    gold_rows = [
        {
            "user_id":      user_id,
            "month":        month,
            "category":     category,
            "total_amount": round(data["total_amount"], 2),
            "txn_count":    data["txn_count"],
            "updated_at":   datetime.utcnow().isoformat()
        }
        for (month, category), data in summary.items()
    ]

    if gold_rows:
        supabase_admin.table("gold_monthly_summary") \
            .upsert(gold_rows, on_conflict="user_id,month,category") \
            .execute()
        print(f"[ETL] Gold: upserted {len(gold_rows)} summary rows")

    return len(gold_rows)


# ─────────────────────────────────────────────────────────────────────────────
# Master pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(user_id: str):
    """
    Full ETL pipeline triggered after every Gmail fetch or file upload:

    Raw transactions
        ↓  run_raw_to_bronze    — fingerprint, flag duplicates
    Bronze
        ↓  run_bronze_to_silver — clean merchant names, normalise dates
    Silver (uncategorised)
        ↓  run_categorise_silver — ML model assigns category + confidence
    Silver (categorised)
        ↓  run_silver_to_gold   — aggregate into monthly summary
    Gold
    """
    print(f"\n[ETL] ── Starting pipeline for user {user_id} ──")
    try:
        b = run_raw_to_bronze(user_id)
        s = run_bronze_to_silver(user_id)
        c = run_categorise_silver(user_id)
        g = run_silver_to_gold(user_id)
        print(f"[ETL] ── Pipeline complete: {b} bronze | {s} silver | {c} categorised | {g} gold ──\n")
    except Exception as e:
        print(f"[ETL] ── Pipeline error: {e} ──\n")
        raise