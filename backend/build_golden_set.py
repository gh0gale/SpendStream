"""
build_golden_set.py — turn the user's labelled dataset into files the model
tools read (docs/SYSTEM_AUDIT_AND_PLAN.md Phase 1).

Source: backend/ml/data/spendstream_dataset/ (gitignored). It holds the user's
real bank statement (205 rows, anonymised, hand-labelled) and ~26k rows
augmented or generated from it, in a 21-category taxonomy.

Writes (all gitignored):
  ml/data/golden_set.csv       REAL debits only, the honest test set: the
                               statement seed, plus 30% (by merchant) of
                               ml/data/1year_data.csv when it exists
  ml/data/private_train.csv    compare-run training: synthetic debits and the
                               other 70% of the year, with every merchant in
                               the golden set removed, so the test stays unseen
  ml/data/private_ship.csv     ship-run training: all_data.csv + 1year_data.csv

Rows are converted to what the Gmail pipeline produces: the receiver string
"VPA <payee id> <name>" that gmail_parser returns for a UPI alert, and the
app's 13 categories (decision A, 2026-09-27). Credits are dropped: the app
ingests debits only.

Run from backend/:
    python build_golden_set.py [--confirmed-only]
"""

import argparse
import csv
import os
import random
import sys

from merchant_identity import merchant_key

HERE     = os.path.dirname(os.path.abspath(__file__))
DATASET  = os.path.join(HERE, "ml", "data", "spendstream_dataset", "data")
OUT_DIR  = os.path.join(HERE, "ml", "data")

# 21 dataset categories -> the app's 13. Credit-only categories are absent
# because credits are dropped before mapping.
CATEGORY_MAP = {
    "Food & Dining":     "Food",
    "Groceries":         "Groceries",
    "Transport":         "Transport",
    "Travel":            "Transport",
    "Shopping":          "Shopping",
    "Bills & Utilities": "Utilities",
    "Subscriptions":     "Subscription",
    "Entertainment":     "Entertainment",
    "Health":            "Health",
    "Education":         "Education",
    "Investments":       "Investment",
    "Rent & Housing":    "Payments",
    "EMI & Loans":       "Payments",
    "Fees & Charges":    "Payments",
    "Transfers":         "Transfer",
    "Self Transfer":     "Transfer",
    "Personal Care":     "Other",
    "Cash & ATM":        "Other",
    "Other":             "Other",
}
FIELDS = ["raw_text", "category", "amount", "timestamp", "merchant", "source_category", "label_status"]


def receiver_text(row: dict) -> str:
    """The receiver as the Gmail parser returns it: "VPA <id> <name>" for UPI."""
    name = (row.get("merchant_raw") or "").strip()
    vpa  = (row.get("vpa") or "").strip().lower()
    return f"VPA {vpa} {name}".strip() if vpa else name


def timestamp(row: dict) -> str:
    """Date and time, or empty when the time is unknown (a date alone would read as midnight)."""
    return f"{row['date']}T{row['time']}+05:30" if row.get("time") else ""


def convert(row: dict) -> dict | None:
    if row.get("direction") != "debit":
        return None
    category = CATEGORY_MAP.get(row.get("category", ""))
    if category is None:
        raise SystemExit(f"unmapped category {row.get('category')!r} in {row.get('txn_id')}")
    return {
        "raw_text":        receiver_text(row),
        "category":        category,
        "amount":          row["amount"],
        "timestamp":       timestamp(row),
        "merchant":        row.get("merchant_normalized", ""),
        "source_category": row["category"],
        "label_status":    row.get("label_status", ""),
    }


def read(name: str) -> list[dict]:
    with open(os.path.join(DATASET, name), encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write(name: str, rows: list[dict]) -> None:
    with open(os.path.join(OUT_DIR, name), "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--confirmed-only", action="store_true",
                    help="leave out the seed rows the labeller marked needs_review")
    args = ap.parse_args()

    if not os.path.isdir(DATASET):
        print(f"No dataset at {DATASET}.")
        return 2

    seed = [r for r in read("statement_seed_labeled.csv")
            if not (args.confirmed_only and r.get("label_status") == "seed_needs_review")]
    golden = [c for c in map(convert, seed) if c]
    held_out = {r["merchant_normalized"] for r in seed}

    # Training rows: synthetic only. Augmentations of the seed and any
    # synthetic row sharing a seed merchant would leak the test set.
    train, dropped = [], 0
    for name in ("train.csv", "val.csv", "test.csv", "test_unseen_merchants.csv"):
        for r in read(name):
            if r.get("source") != "synthetic" or r.get("merchant_normalized") in held_out:
                dropped += 1
                continue
            c = convert(r)
            if c:
                train.append(c)

    # The user's own year of labelled debits (2026-09-27), if present: 30% by
    # merchant joins the golden set (only merchants the compare run never
    # trains on), the other 70% joins the compare training. Everything goes
    # into the ship training.
    year = read_year()
    ship = list(year)
    if year:
        golden_keys = {merchant_key(g["raw_text"]) for g in golden}
        year_train, year_test = split_by_merchant(year)
        golden += year_test
        golden_keys |= {merchant_key(r["raw_text"]) for r in year_test}
        kept = [r for r in year_train if merchant_key(r["raw_text"]) not in golden_keys]
        train += kept
        print(f"1year_data.csv     {len(year)} rows: {len(year_test)} to the golden set, "
              f"{len(kept)} to compare training ({len(year_train) - len(kept)} shared a golden merchant, left out)")

    all_path = os.path.join(OUT_DIR, "all_data.csv")
    if os.path.exists(all_path):
        with open(all_path, encoding="utf-8") as f:
            ship = [{k: r.get(k, "") for k in FIELDS} for r in csv.DictReader(f)] + ship

    write("golden_set.csv", golden)
    write("private_train.csv", train)
    write("private_ship.csv", ship)
    print(f"golden_set.csv     {len(golden)} real debits, {len({g['merchant'] for g in golden})} merchants")
    print(f"private_train.csv  {len(train)} rows for the compare run ({dropped} synthetic rows left out: seed-derived or seed merchant)")
    print(f"private_ship.csv   {len(ship)} rows for the ship run (all_data.csv + 1year_data.csv)")
    return 0


def read_year() -> list[dict]:
    """ml/data/1year_data.csv in golden-set columns; [] if absent."""
    path = os.path.join(OUT_DIR, "1year_data.csv")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return [{"raw_text": r["raw_text"], "category": r["category"], "amount": r.get("amount", ""),
             "timestamp": r.get("timestamp", ""), "merchant": r.get("merchant", ""),
             "source_category": r.get("source_category", ""), "label_status": "user_1year"}
            for r in rows if r.get("raw_text") and r.get("category")]


def split_by_merchant(rows: list[dict], share: float = 0.7, seed: int = 42) -> tuple[list[dict], list[dict]]:
    """Whole merchants to one side: ~share of rows to train, the rest to test."""
    keys = sorted({merchant_key(r["raw_text"]) for r in rows})
    random.Random(seed).shuffle(keys)
    train_keys, n = set(), 0
    for k in keys:
        if n >= share * len(rows):
            break
        train_keys.add(k)
        n += sum(1 for r in rows if merchant_key(r["raw_text"]) == k)
    return ([r for r in rows if merchant_key(r["raw_text"]) in train_keys],
            [r for r in rows if merchant_key(r["raw_text"]) not in train_keys])


if __name__ == "__main__":
    sys.exit(main())
