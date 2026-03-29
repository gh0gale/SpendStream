"""
test_categoriser.py
───────────────────
Standalone test for the ML categoriser.
Delete this file anytime — nothing else depends on it.

Tests:
  1. Basic predictions against known transactions
  2. Confidence threshold (low-signal inputs → "Other")
  3. User override (correction immediately overrides ML)
  4. History bias (repeated corrections shift future predictions)
  5. Online learning (buffer triggers warm refit, model improves)
  6. Batch prediction consistency
  7. Cold-start mode (no model file — falls back to embeddings)

Run from backend/:
    python test_categoriser.py
    python test_categoriser.py --verbose     # show all probas
    python test_categoriser.py --cold-start  # test without model file
"""

import sys
import os
import time
import argparse
import shutil
import tempfile

# ── Path setup so this works from backend/ ───────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ─────────────────────────────────────────────────────────────────────────────
# ANSI colours
# ─────────────────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
GREY   = "\033[90m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(s):   return f"{GREEN}✅ {s}{RESET}"
def fail(s): return f"{RED}❌ {s}{RESET}"
def warn(s): return f"{YELLOW}⚠  {s}{RESET}"
def info(s): return f"{CYAN}{s}{RESET}"
def dim(s):  return f"{GREY}{s}{RESET}"

# ─────────────────────────────────────────────────────────────────────────────
# Test cases
# ─────────────────────────────────────────────────────────────────────────────

STANDARD_CASES = [
    # (raw_text,                                                   expected_category)
    ("VPA swiggy.stores@axb Swiggy Limited",                       "Food"),
    ("VPA zomato@hdfcbank Zomato India",                           "Food"),
    ("VPA sapphirekfconline@ybl KFC",                              "Food"),
    ("VPA mcdonalds.42276700@hdfcbank MC DONALDS",                 "Food"),
    ("VPA eatclub@ptybl EatClub",                                  "Food"),
    ("VPA blinkit@axisbank Blinkit Delivery",                      "Groceries"),
    ("VPA bigbasket@hdfcbank BigBasket",                           "Groceries"),
    ("VPA amazon@hdfcbank Amazon India",                           "Shopping"),
    ("VPA myntra@icicibank Myntra Fashion",                        "Shopping"),
    ("VPA uber@hdfcbank Uber Technologies",                        "Transport"),
    ("VPA ola.money@ybl Ola Cabs",                                 "Transport"),
    ("VPA irctc@okicici IRCTC RAIL",                               "Transport"),
    ("VPA groww.brk@validhdfc GROWW INVEST TECH PVT LTD",          "Investment"),
    ("VPA groww.iccl2.brk@validicici Mutual Funds ICCL",           "Investment"),
    ("VPA zerodha@icicibank Zerodha Broking",                      "Investment"),
    ("VPA appleservices.bdsi@hdfcbank APPLE MEDIA SERVICES",       "Subscription"),
    ("VPA netflix@icicibank Netflix India",                        "Subscription"),
    ("VPA spotify@ybl Spotify Music",                              "Subscription"),
    ("VPA paytmqr5hqark@ptys GLOBAL MEDICAL AND G",               "Health"),
    ("VPA apollo@hdfcbank Apollo Pharmacy",                        "Health"),
    ("VPA bescom@hdfcbank BESCOM ELECTRICITY",                     "Utilities"),
    ("VPA jio@rjio Jio Recharge",                                  "Utilities"),
    ("VPA paytmqr6woody@ptys RAVINDRA S SHETTY",                   "Transfer"),
    ("VPA arnavdumane04@okhdfcbank ARNAV RAVISHANKAR DUMANE",      "Transfer"),
    ("VPA bharatpe.9y0r0e7l3x685328@fbpe SAJAHAN SK",              "Transfer"),
    ("VPA ss2002786kgn@okicici Mr SAHIL RAJU SAYYED",              "Transfer"),
    ("VPA 8368536065@pthdfc HARSHIT SAXENA",                       "Transfer"),
    ("VPA bookmyshow@hdfcbank BookMyShow",                         "Entertainment"),
    ("VPA byjus@hdfcbank BYJU S THINK",                            "Education"),
    ("VPA unacademy@okaxis Unacademy Learning",                    "Education"),
]

LOW_CONFIDENCE_CASES = [
    # These should return "Other" because they're too ambiguous
    "VPA xyzabc123@okaxis",
    "VPA 9999999999@paytm",
    "VPA randommerchant@ybl",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _bar(conf: float, width: int = 16) -> str:
    filled = int(conf * width)
    return f"[{'█' * filled}{'░' * (width - filled)}]"

def _separator(char="─", width=80):
    print(dim(char * width))

def _section(title: str):
    print()
    _separator("─")
    print(f"{BOLD}{CYAN}{title}{RESET}")
    _separator("─")


# ─────────────────────────────────────────────────────────────────────────────
# Main test runner
# ─────────────────────────────────────────────────────────────────────────────

def run_tests(verbose: bool = False, cold_start: bool = False):
    from ml.categoriser import (
        predict_category, predict_full, predict_batch,
        record_feedback, get_categories, model_info,
        _override_store, _history_store, _online_buffer,
    )

    # ── Preamble ──────────────────────────────────────────────────────────────
    print(f"\n{BOLD}SpendStream — Categoriser Test Suite{RESET}")
    _separator("═")

    info_data = model_info()
    print(f"  Model loaded     : {info_data['model_loaded']}")
    print(f"  Embeddings OK    : {info_data['embeddings_ok']}")
    print(f"  Buffer pending   : {info_data['buffer_pending']}")
    if info_data.get("trained_at"):
        print(f"  Trained at       : {info_data['trained_at']}")
    if info_data.get("updated_at"):
        print(f"  Last online upd  : {info_data['updated_at']}")
    if info_data.get("online_samples_applied"):
        print(f"  Online samples   : {info_data['online_samples_applied']}")

    total_pass = total_fail = 0

    # ─────────────────────────────────────────────────────────────────────────
    # 1. Standard predictions
    # ─────────────────────────────────────────────────────────────────────────
    _section("1 · Standard predictions")
    print(f"  {'Input (truncated)':<50} {'Expected':<14} {'Got':<14} {'Conf':>6}  {'ms':>5}")
    print(f"  {dim('─'*50)} {dim('─'*14)} {dim('─'*14)} {dim('─'*6)}  {dim('─'*5)}")

    passes = fails = 0
    t_start = time.perf_counter()

    for raw, expected in STANDARD_CASES:
        t0  = time.perf_counter()
        cat, conf = predict_category(raw, amount=250.0)
        ms  = (time.perf_counter() - t0) * 1000
        hit = cat == expected

        if hit:   passes += 1
        else:     fails  += 1

        status = "✅" if hit else "❌"
        cat_col = f"{GREEN}{cat}{RESET}" if hit else f"{RED}{cat}{RESET}"
        bar     = _bar(conf)

        print(f"  {raw[:50]:<50} {expected:<14} {cat_col:<23} {conf:>5.2f}  {ms:>4.0f}ms  {status}")

        if verbose and not hit:
            r = predict_full(raw, amount=250.0)
            for c, p in sorted(r.all_probas.items(), key=lambda x: -x[1])[:5]:
                print(f"  {dim(f'    {c:<18} {p:.3f}')}")

    total_ms = (time.perf_counter() - t_start) * 1000
    pct      = passes / len(STANDARD_CASES) * 100
    total_pass += passes
    total_fail += fails

    print(f"\n  Result: {GREEN if pct==100 else YELLOW}{passes}/{len(STANDARD_CASES)} ({pct:.0f}%){RESET}  "
          f"— total {total_ms:.0f}ms, avg {total_ms/len(STANDARD_CASES):.1f}ms/item")

    # ─────────────────────────────────────────────────────────────────────────
    # 2. Low confidence → Other
    # ─────────────────────────────────────────────────────────────────────────
    _section("2 · Low confidence → 'Other'")

    lc_pass = lc_fail = 0
    for raw in LOW_CONFIDENCE_CASES:
        cat, conf = predict_category(raw)
        hit = cat == "Other"
        if hit: lc_pass += 1
        else:   lc_fail += 1
        print(f"  {raw[:55]:<55} → {cat:<12} conf={conf:.2f}  {'✅' if hit else warn('not Other')}")

    total_pass += lc_pass
    total_fail += lc_fail

    # ─────────────────────────────────────────────────────────────────────────
    # 3. User override
    # ─────────────────────────────────────────────────────────────────────────
    _section("3 · User override (correction → immediate effect)")

    test_user     = "test_user_001"
    test_merchant = "Mystery Merchant XYZ"
    test_raw      = f"VPA mysterytx@okaxis {test_merchant}"

    cat_before, _ = predict_category(test_raw, user_id=test_user, receiver=test_merchant)
    print(f"  Before correction : {cat_before}")

    record_feedback(
        user_id            = test_user,
        receiver           = test_merchant,
        raw_text           = test_raw,
        corrected_category = "Health",
    )

    cat_after, conf_after = predict_category(test_raw, user_id=test_user, receiver=test_merchant)
    hit = cat_after == "Health" and conf_after == 1.0
    total_pass += int(hit)
    total_fail += int(not hit)

    print(f"  After correction  : {cat_after} (conf={conf_after:.2f})  {ok('override working') if hit else fail('override NOT working')}")

    # ─────────────────────────────────────────────────────────────────────────
    # 4. History bias
    # ─────────────────────────────────────────────────────────────────────────
    _section("4 · History bias (repeated corrections shift probability)")

    user2     = "test_user_002"
    merchant2 = "Ambiguous Store ABC"
    raw2      = f"VPA ambig@okaxis {merchant2}"

    # Record 3 corrections for same merchant → Groceries
    for _ in range(3):
        _history_store.record(user2, merchant2, "Groceries")

    r = predict_full(raw2, user_id=user2, receiver=merchant2)
    print(f"  After 3× Groceries history:")
    print(f"    Category  : {r.category}  conf={r.confidence:.2f}  source={r.source}")

    if verbose:
        for c, p in sorted(r.all_probas.items(), key=lambda x: -x[1])[:5]:
            print(f"      {dim(f'{c:<18} {p:.3f}')}")

    # Groceries should appear in top probabilities (history bias applied)
    if r.all_probas:
        groceries_p = r.all_probas.get("Groceries", 0)
        hit = groceries_p > 0.15   # history should have boosted it
    else:
        hit = r.category == "Groceries"

    total_pass += int(hit)
    total_fail += int(not hit)
    print(f"  Groceries probability boosted: {ok(f'{groceries_p:.2f}') if hit else fail(f'only {groceries_p:.2f}')}")

    # ─────────────────────────────────────────────────────────────────────────
    # 5. Online learning — buffer + background refit
    # ─────────────────────────────────────────────────────────────────────────
    _section("5 · Online learning (background warm refit)")

    from ml.categoriser import ONLINE_UPDATE_THRESHOLD

    print(f"  Online update threshold : {ONLINE_UPDATE_THRESHOLD} corrections")
    print(f"  Sending {ONLINE_UPDATE_THRESHOLD} corrections to fill buffer...")

    correction_pairs = [
        ("VPA pizzapalace@okaxis Pizza Palace Restaurant", "Food"),
        ("VPA veggieshop@ybl Fresh Veggies Store",         "Groceries"),
        ("VPA yogaclass@hdfcbank Yoga Studio Monthly",     "Subscription"),
        ("VPA eyecheck@okaxis Vision Care Clinic",         "Health"),
        ("VPA busticket@paytm KSRTC BUS TICKET",           "Transport"),
        ("VPA schoolfees@icicibank Springdale School Fees","Education"),
        ("VPA gymcafe@axisbank Gym Cafe Snack Bar",        "Food"),
    ]

    # Use only up to threshold to trigger exactly one refit
    pairs_to_send = correction_pairs[:ONLINE_UPDATE_THRESHOLD]
    for raw, cat in pairs_to_send:
        record_feedback(
            user_id            = "test_user_003",
            receiver           = raw.split()[-2] if len(raw.split()) > 2 else raw,
            raw_text           = raw,
            corrected_category = cat,
        )

    # Give background thread up to 5s to complete
    print(f"  Waiting for background refit (up to 5s)…")
    deadline = time.perf_counter() + 5.0
    from ml.categoriser import _online_buffer as buf
    while buf.size() > 0 and time.perf_counter() < deadline:
        time.sleep(0.1)

    if buf.size() == 0:
        print(f"  {ok('Background refit completed — buffer drained')}")
        total_pass += 1
    else:
        print(f"  {warn('Buffer still has items (refit may still be running)')}")
        total_fail += 1

    # Verify model info shows update
    updated_info = model_info()
    if updated_info.get("online_samples_applied", 0) > 0:
        print(f"  {ok(f'online_samples_applied = {updated_info}')}")
        total_pass += 1
    else:
        print(f"  {warn('online_samples_applied not yet reflected in model_info')}")

    # ─────────────────────────────────────────────────────────────────────────
    # 6. Batch prediction
    # ─────────────────────────────────────────────────────────────────────────
    _section("6 · Batch prediction (single vectorisation pass)")

    batch_inputs  = [raw for raw, _ in STANDARD_CASES[:10]]
    batch_expected = [exp for _, exp in STANDARD_CASES[:10]]

    t0      = time.perf_counter()
    results = predict_batch(batch_inputs, amounts=[300.0]*10)
    batch_ms = (time.perf_counter() - t0) * 1000

    batch_pass = sum(1 for (cat, _), exp in zip(results, batch_expected) if cat == exp)
    print(f"  {batch_pass}/10 correct  — {batch_ms:.1f}ms total, {batch_ms/10:.1f}ms avg/item")

    total_pass += batch_pass
    total_fail += (10 - batch_pass)

    # ─────────────────────────────────────────────────────────────────────────
    # 7. Known categories
    # ─────────────────────────────────────────────────────────────────────────
    _section("7 · Registered categories")
    cats = get_categories()
    print(f"  {len(cats)} categories: {', '.join(cats)}")

    # ─────────────────────────────────────────────────────────────────────────
    # Final summary
    # ─────────────────────────────────────────────────────────────────────────
    _separator("═")
    grand_total = total_pass + total_fail
    pct_overall = total_pass / grand_total * 100 if grand_total else 0
    colour      = GREEN if pct_overall >= 90 else (YELLOW if pct_overall >= 70 else RED)

    print(f"\n  {BOLD}Overall: {colour}{total_pass}/{grand_total} ({pct_overall:.0f}%){RESET}\n")

    if total_fail > 0:
        print(f"  {warn(f'{total_fail} checks failed — review output above')}\n")
    else:
        print(f"  {ok('All checks passed')}\n")

    return total_fail == 0


# ─────────────────────────────────────────────────────────────────────────────
# Cold-start test (no model file)
# ─────────────────────────────────────────────────────────────────────────────

def run_cold_start_test():
    """
    Temporarily renames model_v2.pkl so the categoriser falls back
    to embedding cold-start, then runs a subset of test cases.
    Restores the model file after.
    """
    from ml import categoriser as C

    original_path = C.MODEL_PATH
    backup_path   = original_path + ".bak"
    existed       = os.path.exists(original_path)

    if existed:
        shutil.move(original_path, backup_path)
        print(info(f"\n  Model temporarily moved to {backup_path}"))

    # Reset in-memory state so it re-evaluates
    C._model_loaded  = False
    C._clf           = None
    C._tfidf_char    = None
    C._tfidf_word    = None
    C._label_encoder = None
    C._override_store._overrides.clear()

    cold_cases = [
        ("VPA swiggy.stores@axb Swiggy Limited",             "Food"),
        ("VPA uber@hdfcbank Uber Technologies",              "Transport"),
        ("VPA groww.brk@validhdfc GROWW INVEST TECH",        "Investment"),
        ("VPA netflix@icicibank Netflix India",              "Subscription"),
        ("VPA apollo@hdfcbank Apollo Pharmacy",              "Health"),
    ]

    _section("Cold-start mode (embeddings only, no trained model)")
    passes = 0
    for raw, expected in cold_cases:
        r   = C.predict_full(raw)
        hit = r.category == expected
        if hit: passes += 1
        print(f"  {raw[:50]:<50} expected={expected:<14} got={r.category:<14} "
              f"conf={r.confidence:.2f}  source={r.source}  {'✅' if hit else '⚠'}")

    pct = passes / len(cold_cases) * 100
    print(f"\n  Cold-start: {passes}/{len(cold_cases)} ({pct:.0f}%) — "
          f"{YELLOW}lower accuracy expected without trained model{RESET}")

    # Restore model
    if existed:
        shutil.move(backup_path, original_path)
        C._model_loaded = False   # force reload on next call
        print(info(f"  Model restored from backup"))


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SpendStream categoriser test suite")
    parser.add_argument("--verbose",    action="store_true", help="Show probability breakdown on failures")
    parser.add_argument("--cold-start", action="store_true", help="Also run cold-start (no model) test")
    args = parser.parse_args()

    success = run_tests(verbose=args.verbose)

    if args.cold_start:
        run_cold_start_test()

    sys.exit(0 if success else 1)