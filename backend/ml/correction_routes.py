"""
correction_routes.py
────────────────────
FastAPI router for category corrections.

Endpoints:
  POST /correct-category   — user corrects a transaction category
  GET  /model-info         — current model status (health check)

What happens on correction:
  1. silver_transactions updated (category + user_corrected = true)
  2. Gold table recalculated for that month/category (both old and new)
  3. category_feedback upserted (permanent record for retraining)
  4. record_feedback() → in-memory override + history + online buffer
     If buffer hits ONLINE_UPDATE_THRESHOLD, background warm refit fires
     automatically. No manual python --train step needed.

Supabase SQL (run once):
────────────────────────
  alter table silver_transactions
    add column if not exists user_corrected boolean default false;

  create table if not exists category_feedback (
    id                 uuid primary key default gen_random_uuid(),
    user_id            uuid not null,
    silver_id          uuid not null,
    merchant           text,
    raw_text           text,
    original_category  text,
    corrected_category text not null,
    amount             numeric,
    corrected_at       timestamptz default now(),
    unique (user_id, silver_id)
  );
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone
import os

from supabase import create_client
from dotenv import load_dotenv
from ml.categoriser import record_feedback, get_categories, model_info

load_dotenv()
supabase_admin = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)

router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────────────────────

class CorrectionRequest(BaseModel):
    silver_id:          str
    merchant:           str
    raw_text:           Optional[str]   = None
    original_category:  Optional[str]   = None
    corrected_category: str
    amount:             Optional[float] = None
    transaction_date:   Optional[str]   = None   # "YYYY-MM-DD" used for gold update


# ─────────────────────────────────────────────────────────────────────────────
# Helper: recalculate gold for a specific user + month + category
# ─────────────────────────────────────────────────────────────────────────────

def _refresh_gold(user_id: str, month: str, category: str):
    """
    Recalculates gold_monthly_summary for ONE (user, month, category) bucket.
    Called after a category correction so Gold always reflects Silver.
    month format: "YYYY-MM-01"
    """
    res = supabase_admin.table("silver_transactions") \
        .select("amount") \
        .eq("user_id",        user_id) \
        .eq("is_categorised", True) \
        .eq("category",       category) \
        .gte("transaction_date", month) \
        .lt("transaction_date",  _next_month(month)) \
        .execute()

    rows  = res.data or []
    total = round(sum(float(r["amount"]) for r in rows), 2)
    count = len(rows)

    if count == 0:
        supabase_admin.table("gold_monthly_summary") \
            .delete() \
            .eq("user_id",  user_id) \
            .eq("month",    month) \
            .eq("category", category) \
            .execute()
    else:
        supabase_admin.table("gold_monthly_summary") \
            .upsert({
                "user_id":      user_id,
                "month":        month,
                "category":     category,
                "total_amount": total,
                "txn_count":    count,
                "updated_at":   datetime.now(timezone.utc).isoformat(),
            }, on_conflict="user_id,month,category") \
            .execute()


def _next_month(month_str: str) -> str:
    y, m, _ = month_str.split("-")
    y, m    = int(y), int(m) + 1
    if m > 12:
        m = 1; y += 1
    return f"{y:04d}-{m:02d}-01"


def _to_month(date_str: Optional[str]) -> str:
    if date_str:
        try:    return date_str[:7] + "-01"
        except: pass
    return datetime.now(timezone.utc).strftime("%Y-%m-01")


# ─────────────────────────────────────────────────────────────────────────────
# POST /correct-category
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/correct-category")
async def correct_category(body: CorrectionRequest, user_id: str):
    if body.corrected_category not in get_categories():
        raise HTTPException(400, f"Unknown category: {body.corrected_category}")

    # 1. Update silver
    updated = supabase_admin.table("silver_transactions") \
        .update({
            "category":       body.corrected_category,
            "is_categorised": True,
            "user_corrected": True,
        }) \
        .eq("id",      body.silver_id) \
        .eq("user_id", user_id) \
        .execute()

    if not updated.data:
        raise HTTPException(404, "Transaction not found or not owned by user")

    # 2. Recalculate Gold — both affected category buckets for that month
    month = _to_month(body.transaction_date)
    if body.original_category and body.original_category != body.corrected_category:
        _refresh_gold(user_id, month, body.original_category)   # old bucket decreases
    _refresh_gold(user_id, month, body.corrected_category)       # new bucket increases

    # 3. Persist to category_feedback
    supabase_admin.table("category_feedback").upsert({
        "user_id":            user_id,
        "silver_id":          body.silver_id,
        "merchant":           body.merchant,
        "raw_text":           body.raw_text or body.merchant,
        "original_category":  body.original_category,
        "corrected_category": body.corrected_category,
        "amount":             body.amount,
        "corrected_at":       datetime.now(timezone.utc).isoformat(),
    }, on_conflict="user_id,silver_id").execute()

    # 4. Immediate ML update + background warm refit if buffer full
    record_feedback(
        user_id            = user_id,
        receiver           = body.merchant,
        raw_text           = body.raw_text or body.merchant,
        corrected_category = body.corrected_category,
    )

    return {
        "ok":       True,
        "silver_id": body.silver_id,
        "category":  body.corrected_category,
        "month":     month,
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /model-info
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/model-info")
async def get_model_info():
    return model_info()