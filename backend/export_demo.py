"""
export_demo.py: write the real data behind the public site's demos.

The home page and How it works show the product's own components rendering
one real month of the developer's own account (design rule 18: real demos,
never mockups). This script reads that month with the service-role client and
writes frontend/src/data/demo-export.json. It only reads; it never writes to
the database. The frontend hides the demo sections while the file is absent.

Anonymised before writing: payments to people become "Person A", "Person B";
their UPI ids and the alert's account digits and reference numbers are
removed. Amounts and dates stay real (approved by the owner, 2026-09-27).
Read the JSON before committing it.

From the repo root (the extra volume lets the container reach frontend/):

    docker compose run --rm -v ./frontend/src/data:/frontend/src/data backend \
        python export_demo.py --email you@example.com [--month 2026-08]
"""

import argparse
import json
import logging
import os
import re
import string
import sys
from collections import defaultdict
from datetime import date

import config

log = logging.getLogger("export_demo")

OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "frontend", "src", "data", "demo-export.json")
SILVER_COLUMNS = ("id, bronze_id, amount, merchant, merchant_key, merchant_is_person, "
                  "transaction_date, category, is_categorised, user_corrected")
ALERT_MAX_CHARS = 320
CLEANING_EXAMPLES = 2

_MASKED_ACCOUNT = re.compile(r"(\*\*|XX|xx|X{2,}|x{2,})\d+")
# "account ending 9608", "A/c no. 1234", "account XX1234": digits after an account word.
_ACCOUNT_DIGITS = re.compile(r"((?:account|a/c|acct)\b[^\d.]{0,20}?[*Xx]*)\d{3,}", re.IGNORECASE)
_LONG_NUMBER    = re.compile(r"\d{8,}")
_EMAIL_OR_VPA   = re.compile(r"\S+@\S+")


def find_user_id(sb, email: str) -> str:
    target = email.strip().lower()
    for user in sb.auth.admin.list_users():
        if (user.email or "").lower() == target:
            return user.id
    raise SystemExit(f"No account with the email {email}.")


def read_silver(sb, user_id: str) -> list[dict]:
    rows, start, page = [], 0, 1000
    while True:
        batch = (sb.table("silver_transactions").select(SILVER_COLUMNS)
                 .eq("user_id", user_id).range(start, start + page - 1).execute().data)
        rows.extend(batch)
        if len(batch) < page:
            return rows
        start += page


def pick_month(rows: list[dict], wanted: str | None) -> str:
    """The requested month, else the fullest month before the current one."""
    months = defaultdict(int)
    for r in rows:
        if r["transaction_date"]:
            months[r["transaction_date"][:7]] += 1
    if wanted:
        if wanted not in months:
            raise SystemExit(f"No payments in {wanted}. Months with data: {sorted(months)}")
        return wanted
    current = date.today().isoformat()[:7]
    past = {m: n for m, n in months.items() if m < current} or months
    return max(past, key=lambda m: (past[m], m))


def person_names(rows: list[dict]) -> dict[str, str]:
    """merchant_key -> "Person A", in order of first appearance."""
    names: dict[str, str] = {}
    for r in sorted(rows, key=lambda r: r["transaction_date"] or ""):
        key = r["merchant_key"] or r["merchant"]
        if r["merchant_is_person"] and key not in names:
            i = len(names)
            suffix = string.ascii_uppercase[i % 26] + (str(i // 26) if i >= 26 else "")
            names[key] = f"Person {suffix}"
    return names


def display_name(row: dict, persons: dict[str, str]) -> str:
    if row["merchant_is_person"]:
        return persons[row["merchant_key"] or row["merchant"]]
    return row["merchant"] or "Unknown merchant"


def breakdown(month_rows: list[dict]) -> tuple[list[dict], dict]:
    totals = defaultdict(lambda: {"total": 0.0, "count": 0})
    unsure = {"total": 0.0, "count": 0}
    for r in month_rows:
        bucket = totals[r["category"]] if r["is_categorised"] and r["category"] else unsure
        bucket["total"] += float(r["amount"] or 0)
        bucket["count"] += 1
    rows = [{"category": c, "total": round(v["total"], 2), "count": v["count"]}
            for c, v in totals.items()]
    rows.sort(key=lambda x: -x["total"])
    return rows, {"total": round(unsure["total"], 2), "count": unsure["count"]}


def scrub_alert(text: str) -> str:
    """Keep the sentence that says what was debited; remove identifying numbers."""
    flat = " ".join(text.split())
    at = flat.lower().find("debited")
    if at > 0:
        start = flat.rfind(".", 0, max(0, at - 120))
        flat = flat[start + 1 if start >= 0 else 0:]
    flat = _ACCOUNT_DIGITS.sub(lambda m: m.group(1) + "0000", flat)
    flat = _MASKED_ACCOUNT.sub(lambda m: m.group(1) + "0000", flat)
    flat = _LONG_NUMBER.sub(lambda m: "0" * len(m.group()), flat)
    flat = flat[:ALERT_MAX_CHARS]
    end = flat.rfind(". ")          # stop at the last whole sentence
    return (flat[:end + 1] if end > 0 else flat).strip()


def fill_alert_text(bronze: dict[str, dict], raw: list[dict]) -> dict[str, dict]:
    """
    Bronze rows written before 2026-10-01 carry a copy of the alert text; newer
    ones do not (it stays on the raw row only). Fill a missing text from the raw
    row, keeping any copy bronze already has. Returns new dicts.
    """
    by_raw_id = {r["id"]: r.get("raw_text") for r in raw}
    return {bid: {**b, "raw_text": b.get("raw_text") or by_raw_id.get(b.get("raw_id"))}
            for bid, b in bronze.items()}


def bronze_rows(sb, ids: list[str]) -> dict[str, dict]:
    if not ids:
        return {}
    data = (sb.table("bronze_transactions").select("id, raw_id, receiver, raw_text")
            .in_("id", ids).execute().data)
    bronze = {b["id"]: b for b in data}
    raw_ids = [b["raw_id"] for b in data if b.get("raw_id") and not b.get("raw_text")]
    raw = (sb.table("transactions").select("id, raw_text").in_("id", raw_ids).execute().data
           if raw_ids else [])
    return fill_alert_text(bronze, raw)


def alert_example(sb, month_rows: list[dict]) -> dict | None:
    """One shop payment's real alert next to the row it became."""
    shops = [r for r in month_rows
             if not r["merchant_is_person"] and r["is_categorised"] and r["bronze_id"]]
    bronze = bronze_rows(sb, [r["bronze_id"] for r in shops[:20]])
    for r in shops[:20]:
        raw = (bronze.get(r["bronze_id"]) or {}).get("raw_text") or ""
        if "debited" in raw.lower():
            return {"text": scrub_alert(raw),
                    "row": {"date": r["transaction_date"], "merchant": r["merchant"],
                            "category": r["category"], "amount": float(r["amount"])}}
    return None


def cleaning_examples(sb, month_rows: list[dict]) -> list[dict]:
    """Shop receivers as the bank wrote them, next to the cleaned name."""
    shops = [r for r in month_rows if not r["merchant_is_person"] and r["bronze_id"]]
    bronze = bronze_rows(sb, [r["bronze_id"] for r in shops[:40]])
    seen, out = set(), []
    for r in shops[:40]:
        receiver = (bronze.get(r["bronze_id"]) or {}).get("receiver")
        if receiver and r["merchant"] and receiver.strip() != r["merchant"] \
                and r["merchant"] not in seen:
            seen.add(r["merchant"])
            out.append({"raw": receiver.strip(), "clean": r["merchant"]})
        if len(out) == CLEANING_EXAMPLES:
            break
    return out


def correction_example(sb, user_id: str, rows: list[dict],
                       persons: dict[str, str]) -> dict | None:
    """A real correction from category_feedback, and how many other payments
    to that merchant now follow its rule (rows the rule moved)."""
    feedback = (sb.table("category_feedback")
                .select("silver_id, merchant_key, original_category, corrected_category")
                .eq("user_id", user_id).order("corrected_at", desc=True).limit(50)
                .execute().data)
    by_id = {r["id"]: r for r in rows}
    best = None
    for f in feedback:
        row = by_id.get(f["silver_id"])
        if not row or row["merchant_is_person"] or not f["merchant_key"]:
            continue
        followers = [r for r in rows
                     if r["merchant_key"] == f["merchant_key"] and r["id"] != row["id"]
                     and not r["user_corrected"] and r["category"] == f["corrected_category"]]
        if best is None or len(followers) > best[2]:
            best = (f, row, len(followers))
    if best is None:
        return None
    f, row, also_updated = best
    return {"row": {"date": row["transaction_date"], "merchant": display_name(row, persons),
                    "amount": float(row["amount"]),
                    "category": f["original_category"]},
            "chosen": f["corrected_category"],
            "also_updated": also_updated}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--email", required=True, help="the account whose data is shown")
    parser.add_argument("--month", help="YYYY-MM; default: the fullest past month")
    args = parser.parse_args()

    sb = config.admin_client()
    user_id = find_user_id(sb, args.email)
    rows = read_silver(sb, user_id)
    if not rows:
        raise SystemExit("That account has no transactions yet.")

    month = pick_month(rows, args.month)
    month_rows = [r for r in rows if (r["transaction_date"] or "").startswith(month)]
    persons = person_names(rows)
    categories, unsure = breakdown(month_rows)

    export = {
        "month": f"{month}-01",
        "breakdown": categories,
        "unsure": unsure,
        "alert": alert_example(sb, month_rows),
        "cleaning": cleaning_examples(sb, month_rows),
        "correction": correction_example(sb, user_id, rows, persons),
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(export, fh, ensure_ascii=False, indent=2)
        fh.write("\n")

    log.info("Wrote %s", os.path.normpath(OUT_PATH))
    log.info("Month %s: %d payments, %d categories, %d unsure",
             month, len(month_rows), len(categories), unsure["count"])
    log.info("Alert example: %s. Correction example: %s.",
             "yes" if export["alert"] else "none found",
             "yes" if export["correction"] else "none found (correct one payment in the app first)")
    log.info("Read the file before committing it: it is published on the public site.")


if __name__ == "__main__":
    main()
