"""
correction_routes.py
────────────────────
FastAPI router for category corrections.

CHANGE: record_feedback() now receives amount + transaction_date so the
        in-memory history store can log amounts for subscription detection
        and the online buffer stores metadata for warm-refit.
        No other logic changed.
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


class CorrectionRequest(BaseModel):
    silver_id:          str
    merchant:           str
    raw_text:           Optional[str]   = None
    original_category:  Optional[str]   = None
    corrected_category: str
    amount:             Optional[float] = None
    transaction_date:   Optional[str]   = None


def _refresh_gold(user_id: str, month: str, category: str):
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


@router.post("/correct-category")
async def correct_category(body: CorrectionRequest, user_id: str):
    if body.corrected_category not in get_categories():
        raise HTTPException(400, f"Unknown category: {body.corrected_category}")

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

    month = _to_month(body.transaction_date)
    if body.original_category and body.original_category != body.corrected_category:
        _refresh_gold(user_id, month, body.original_category)
    _refresh_gold(user_id, month, body.corrected_category)

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

    # ── FIX: Fetch bronze_id to get the real raw text and receiver ──────────
    real_receiver = body.merchant
    real_raw = body.raw_text or body.merchant
    
    silver_row = supabase_admin.table("silver_transactions").select("bronze_id").eq("id", body.silver_id).execute()
    if silver_row.data and silver_row.data[0].get("bronze_id"):
        bronze_id = silver_row.data[0]["bronze_id"]
        bronze_row = supabase_admin.table("bronze_transactions").select("receiver, raw_text").eq("id", bronze_id).execute()
        if bronze_row.data:
            b_data = bronze_row.data[0]
            if b_data.get("receiver"):
                real_receiver = b_data["receiver"]
            if b_data.get("raw_text"):
                real_raw = b_data["raw_text"]

    record_feedback(
        user_id            = user_id,
        receiver           = real_receiver,
        raw_text           = real_raw,
        corrected_category = body.corrected_category,
        amount             = body.amount or 0.0,
        timestamp          = body.transaction_date,
    )

    return {
        "ok":        True,
        "silver_id": body.silver_id,
        "category":  body.corrected_category,
        "month":     month,
    }


@router.get("/model-info")
async def get_model_info():
    return model_info()