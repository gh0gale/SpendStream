"""
weekly_retrain.py — the automated retrain, run weekly by .github/workflows/retrain.yml.

It retrains only when there is enough new shared knowledge to learn from, and
ships only a model that scores at least as well on the golden set:

  1. refresh_merchant_directory(): merchants that at least 3 users labelled the
     same way (80% agreement), never payments to people.
  2. Skip unless the directory holds at least MIN_NEW_MERCHANTS merchants and
     MIN_NEW_ROWS transactions the live model was not trained on. A handful
     of new rows would move the model on noise (the old online refit did
     exactly that: DATA-06).
  3. Training rows = the stored private data (model_store data/) plus those
     transactions, labelled with the directory's category, at most
     MAX_ROWS_PER_MERCHANT per merchant so one busy merchant cannot tilt a
     category.
  4. Compare run (golden-set merchants left out) scored against the baseline
     by evaluate_model.py. Rejected: nothing changes, and the same merchants
     count as new again next week.
  5. Accepted: ship run on everything; live <- ship model (old live -> prev),
     baseline <- compare model, state.json updated, then the deploy hook
     restarts the backend, which downloads the new model.

A user's own corrections never train the shared model on their own: they are
rules for that user (correct_category), and reach the model only through the
directory's multi-user consensus.

Logs carry counts only: the workflow log of a public repository is public.

Run from backend/ (needs the Supabase values):
    python weekly_retrain.py            [--dry-run] stops after step 2
"""

import argparse
import csv
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone

import requests

import config
import model_store
from merchant_identity import merchant_key
from ml.categoriser import _local_time

log = logging.getLogger("weekly_retrain")

MIN_NEW_MERCHANTS     = 20
MIN_NEW_ROWS          = 100
MAX_ROWS_PER_MERCHANT = 20

HERE     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = model_store.DATA_DIR
FIELDS   = ["raw_text", "category", "amount", "timestamp"]
PAGE     = 1000     # PostgREST's default row cap per request


# ── Pure steps (tested by test_weekly_retrain.py) ────────────────────────────

def community_rows(directory: dict, silver: list[dict], bronze: dict,
                   cap: int = MAX_ROWS_PER_MERCHANT) -> list[dict]:
    """
    Training rows from other users' transactions with a directory merchant.
    directory {merchant_key: category}; silver [{bronze_id, merchant_key, amount}];
    bronze {id: {receiver, timestamp}}. At most `cap` rows per merchant.
    """
    rows, per_key = [], {}
    for s in sorted(silver, key=lambda r: str(r.get("bronze_id"))):
        key, b = s.get("merchant_key"), bronze.get(s.get("bronze_id"))
        if key not in directory or not b or not b.get("receiver"):
            continue
        if per_key.get(key, 0) >= cap:
            continue
        per_key[key] = per_key.get(key, 0) + 1
        rows.append({"raw_text": b["receiver"], "category": directory[key],
                     "amount": s.get("amount") or 0, "timestamp": _local_time(b.get("timestamp")) or "",
                     "merchant_key": key})
    return rows


def enough_new(rows: list[dict], trained_keys: set) -> tuple[bool, int, int]:
    """(go, new merchants, new rows): rows whose merchant the live model has not been trained on."""
    new = [r for r in rows if r["merchant_key"] not in trained_keys]
    merchants = len({r["merchant_key"] for r in new})
    return merchants >= MIN_NEW_MERCHANTS and len(new) >= MIN_NEW_ROWS, merchants, len(new)


def without_golden(rows: list[dict], golden_texts: list[str]) -> list[dict]:
    """Compare-run rows: drop merchants in the golden set so the test stays unseen."""
    golden = {merchant_key(t) for t in golden_texts}
    return [r for r in rows if r["merchant_key"] not in golden]


# ── Database and storage ─────────────────────────────────────────────────────

def _chunks(items: list, n: int):
    for i in range(0, len(items), n):
        yield items[i:i + n]


def fetch_community(client) -> tuple[dict, list[dict]]:
    client.rpc("refresh_merchant_directory", {}).execute()
    directory = {r["merchant_key"]: r["category"]
                 for r in client.table("merchant_directory").select("merchant_key, category").execute().data or []}
    silver = []
    for keys in _chunks(sorted(directory), 50):
        start = 0
        while True:
            page = (client.table("silver_transactions")
                    .select("bronze_id, merchant_key, amount")
                    .in_("merchant_key", keys).eq("merchant_is_person", False)
                    .order("bronze_id").range(start, start + PAGE - 1).execute().data or [])
            silver += page
            if len(page) < PAGE:
                break
            start += PAGE
    bronze = {}
    for ids in _chunks(sorted({s["bronze_id"] for s in silver if s.get("bronze_id")}), 200):
        for b in client.table("bronze_transactions").select("id, receiver, timestamp").in_("id", ids).execute().data or []:
            bronze[b["id"]] = b
    return directory, community_rows(directory, silver, bronze)


def _write_csv(name: str, base: str, extra: list[dict]) -> str:
    path = os.path.join(DATA_DIR, name)
    with open(os.path.join(DATA_DIR, base), encoding="utf-8") as fh:
        rows = [{k: r.get(k, "") for k in FIELDS} for r in csv.DictReader(fh)]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows + extra)
    return os.path.relpath(path, HERE)


def _run(*args: str) -> int:
    return subprocess.run([sys.executable, *args], cwd=HERE).returncode


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="report whether a retrain would run, then stop")
    args = ap.parse_args()

    client = config.admin_client()
    directory, rows = fetch_community(client)
    state = model_store.read_state(client)
    go, merchants, new_rows = enough_new(rows, set(state.get("trained_keys", [])))
    log.info("Directory: %d merchants. New since the live model: %d merchants, %d rows "
             "(retrain needs %d and %d).", len(directory), merchants, new_rows, MIN_NEW_MERCHANTS, MIN_NEW_ROWS)
    if not go or args.dry_run:
        log.info("No retrain this week." if not go else "Dry run: a retrain would run.")
        return 0

    os.makedirs(DATA_DIR, exist_ok=True)
    for name in model_store.DATA_FILES:
        with open(os.path.join(DATA_DIR, name), "wb") as fh:
            fh.write(model_store.download(f"data/{name}", client))
    for name, suffix in model_store.BASELINE.items():
        with open(os.path.join(HERE, "ml", "candidate_baseline" + suffix), "wb") as fh:
            fh.write(model_store.download(f"baseline/{name}", client))

    with open(os.path.join(DATA_DIR, "golden_set.csv"), encoding="utf-8") as fh:
        golden_texts = [r["raw_text"] for r in csv.DictReader(fh)]
    train_csv = _write_csv("weekly_train.csv", "private_train.csv", without_golden(rows, golden_texts))
    ship_csv  = _write_csv("weekly_ship.csv", "private_ship.csv", rows)

    if _run("train_model.py", "--no-smoke", "--extra-data", train_csv, "--out-prefix", "ml/candidate_compare"):
        log.error("Compare run failed.")
        return 1
    verdict = _run("evaluate_model.py",
                   "--model", "ml/candidate_compare_model.pkl", "--encoder", "ml/candidate_compare_encoder.pkl",
                   "--compare", "ml/candidate_baseline_model.pkl", "--compare-encoder", "ml/candidate_baseline_encoder.pkl")
    if verdict != 0:
        log.info("Candidate not shipped (evaluate_model exit %d). The live model stays.", verdict)
        return 0 if verdict == 1 else 1

    if _run("train_model.py", "--no-smoke", "--extra-data", ship_csv, "--out-prefix", "ml/candidate_ship"):
        log.error("Ship run failed. The live model stays.")
        return 1
    model_store.publish("ml/candidate_ship_model.pkl", "ml/candidate_ship_encoder.pkl", client)
    for name, suffix in model_store.BASELINE.items():
        with open(os.path.join(HERE, "ml", "candidate_compare" + suffix), "rb") as fh:
            model_store.upload(f"baseline/{name}", fh.read(), client)
    model_store.write_state({"trained_keys": sorted({r["merchant_key"] for r in rows}),
                             "community_rows": len(rows),
                             "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}, client)
    log.info("Shipped: live model replaced (previous kept as prev/).")

    if config.DEPLOY_HOOK_URL:
        r = requests.post(config.DEPLOY_HOOK_URL, timeout=30)
        log.info("Deploy hook: HTTP %d", r.status_code)
        return 0 if r.ok else 1
    log.warning("DEPLOY_HOOK_URL not set: restart the backend by hand to load the new model.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
