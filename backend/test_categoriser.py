"""
test_categoriser.py  ─  SpendStream Categoriser Test Suite
═══════════════════════════════════════════════════════════
Tests every behaviour of the categoriser and prints a full accuracy report.

Run from backend/:
    python test_categoriser.py
    python test_categoriser.py --verbose        # show top-5 probas on failures
    python test_categoriser.py --cold-start     # also test cold-start fallback
    python test_categoriser.py --section basic  # run only one section
"""

import sys, os, time, argparse, shutil

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── ANSI ─────────────────────────────────────────────────────────────────────
G = "\033[92m"; R = "\033[91m"; Y = "\033[93m"
C = "\033[96m"; D = "\033[90m"; B = "\033[1m"; X = "\033[0m"
def ok(s):   return f"{G}✅ {s}{X}"
def fail(s): return f"{R}❌ {s}{X}"
def warn(s): return f"{Y}⚠  {s}{X}"
def dim(s):  return f"{D}{s}{X}"
def hdr(s):  return f"{B}{C}{s}{X}"
def bar(v, w=14): return f"[{'█'*int(v*w)}{'░'*(w-int(v*w))}]"

def sep(c="─", w=82): print(dim(c * w))
def section(title):
    print(); sep(); print(hdr(f"  {title}")); sep()

# ─────────────────────────────────────────────────────────────────────────────
# Test cases
# ─────────────────────────────────────────────────────────────────────────────

# 1. BASIC — well-known merchants that should score high confidence
BASIC = [
    # Food
    ("VPA swiggy.stores@axb Swiggy Limited",               "Food"),
    ("VPA zomato@hdfcbank Zomato India",                   "Food"),
    ("VPA sapphirekfconline@ybl KFC",                      "Food"),
    ("VPA mcdonalds.42276700@hdfcbank MC DONALDS",         "Food"),
    ("VPA eatclub@ptybl EatClub",                          "Food"),
    ("VPA dominospizza@hdfcbank Dominos Pizza",            "Food"),
    ("VPA burgerking@ybl Burger King",                     "Food"),
    ("VPA starbucks@hdfcbank Starbucks",                   "Food"),
    # Groceries
    ("VPA blinkit@axisbank Blinkit Delivery",              "Groceries"),
    ("VPA bigbasket@hdfcbank BigBasket",                   "Groceries"),
    ("VPA zepto@axisbank Zepto",                           "Groceries"),
    ("VPA jiomart@jio JioMart",                            "Groceries"),
    # Shopping
    ("VPA amazon@hdfcbank Amazon India",                   "Shopping"),
    ("VPA myntra@icicibank Myntra Fashion",                "Shopping"),
    ("VPA flipkart@axisbank Flipkart",                     "Shopping"),
    ("VPA nykaa@hdfcbank Nykaa",                           "Shopping"),
    # Transport
    ("VPA uber@hdfcbank Uber Technologies",                "Transport"),
    ("VPA ola.money@ybl Ola Cabs",                         "Transport"),
    ("VPA irctc@okicici IRCTC RAIL",                       "Transport"),
    ("VPA rapido@ybl Rapido Bike Taxi",                    "Transport"),
    # Investment
    ("VPA groww.brk@validhdfc GROWW INVEST TECH PVT LTD", "Investment"),
    ("VPA groww.iccl2.brk@validicici Mutual Funds ICCL",  "Investment"),
    ("VPA zerodha@icicibank Zerodha Broking",              "Investment"),
    ("VPA nsccl@axisbank NSE Clearing",                    "Investment"),
    # Subscription
    ("VPA appleservices.bdsi@hdfcbank APPLE MEDIA SERVICES","Subscription"),
    ("VPA netflix@icicibank Netflix India",                "Subscription"),
    ("VPA spotify@ybl Spotify Music",                      "Subscription"),
    ("VPA hotstar@axisbank Hotstar",                       "Subscription"),
    # Health
    ("VPA apollo@hdfcbank Apollo Pharmacy",                "Health"),
    ("VPA pharmeasy@axisbank PharmEasy",                   "Health"),
    ("VPA paytmqr5hqark@ptys GLOBAL MEDICAL AND G",       "Health"),
    ("VPA cultfit@icici Cult Fit",                         "Health"),
    # Utilities
    ("VPA bescom@hdfcbank BESCOM ELECTRICITY",             "Utilities"),
    ("VPA jio@rjio Jio Recharge",                          "Utilities"),
    ("VPA airtel@axisbank Airtel Prepaid",                 "Utilities"),
    ("VPA jiofiber@jio JioFiber",                          "Utilities"),
    # Transfer
    ("VPA paytmqr6woody@ptys RAVINDRA S SHETTY",          "Transfer"),
    ("VPA arnavdumane04@okhdfcbank ARNAV RAVISHANKAR DUMANE","Transfer"),
    ("VPA bharatpe.9y0r0e7l3x685328@fbpe SAJAHAN SK",     "Transfer"),
    ("VPA ss2002786kgn@okicici Mr SAHIL RAJU SAYYED",     "Transfer"),
    ("VPA 8368536065@pthdfc HARSHIT SAXENA",               "Transfer"),
    # Entertainment
    ("VPA bookmyshow@hdfcbank BookMyShow",                 "Entertainment"),
    ("VPA pvr@hdfcbank PVR Cinemas",                       "Entertainment"),
    ("VPA dream11@ybl Dream11 Fantasy",                    "Entertainment"),
    # Education
    ("VPA byjus@hdfcbank BYJU S THINK",                   "Education"),
    ("VPA unacademy@okaxis Unacademy Learning",            "Education"),
    ("VPA vedantu@axisbank Vedantu",                       "Education"),
    # Payments
    ("VPA cred@axisbank CRED Bill Payment",                "Payments"),
    ("VPA hdfcbank@hdfcbank HDFC Credit Card",             "Payments"),
    ("VPA lic@paytm LIC Premium",                          "Payments"),
]

# 2. TRICKY — ambiguous, obfuscated, or boundary cases
TRICKY = [
    # Swiggy Instamart looks like Groceries but is labelled Food in training
    ("VPA swiggy.instamart@icici Swiggy Instamart",        "Groceries"),  # debatable
    # Person paying for food — UPI handle is a person name
    ("VPA rahul.sharma@okicici Rahul Sharma",              "Transfer"),
    # Generic "payment" string — should stay Other
    ("VPA payment@upi",                                    "Other"),
    # Ambiguous short handle
    ("VPA xyzabc123@okaxis",                               "Other"),
    # Paytm QR for a medical merchant — tricky context
    ("VPA paytmqr5hqark@ptys GLOBAL MEDICAL AND G",       "Health"),
    # GROWW via ICCL clearing — looks like a clearing house
    ("VPA groww.iccl2.brk@validicici Mutual Funds ICCL",  "Investment"),
    # EMI payment — could be confused with Shopping
    ("VPA bajajfinserv@icici Bajaj Finserv EMI",           "Payments"),
    # IRCTC — transport, not shopping
    ("VPA irctc@okicici IRCTC RAIL",                       "Transport"),
    # Dream11 — entertainment not Transfer even though money moves
    ("VPA dream11@ybl Dream11 Fantasy",                    "Entertainment"),
    # Apple Media — Subscription, not Shopping
    ("VPA appleservices.bdsi@hdfcbank APPLE MEDIA SERVICES","Subscription"),
    # Tata Power — Utilities, not Investment
    ("VPA tatapower@hdfcbank TATA POWER",                  "Utilities"),
    # 9-digit phone number as handle — Other
    ("VPA 9999999999@paytm",                               "Other"),
    # Zerodha — Investment, not Payments (involves money movement)
    ("VPA zerodha@icicibank Zerodha Broking",              "Investment"),
    # Cult Fit — Health, not Subscription (even though monthly)
    ("VPA cultfit@icici Cult Fit Monthly",                 "Health"),
]

# 3. LOW-CONFIDENCE — should return "Other" (no recognisable signal)
LOW_CONF = [
    "VPA xyzabc123@okaxis",
    "VPA 9999999999@paytm",
    "VPA randommerchant@ybl",
    "VPA unknownhandle@upi",
    "VPA test@upi",
    "VPA zzz@okicici",
]

# 4. CORRECTION — test that record_feedback takes effect immediately
CORRECTION_CASES = [
    {
        "raw":        "VPA mysterytx@okaxis Mystery Merchant XYZ",
        "receiver":   "Mystery Merchant XYZ",
        "correction": "Health",
    },
    {
        "raw":        "VPA ambig123@paytm Some Ambiguous Store",
        "receiver":   "Some Ambiguous Store",
        "correction": "Groceries",
    },
]

# 5. HISTORY — repeated corrections should shift probability distribution
HISTORY_CASES = [
    {
        "receiver": "Shyam Provision Store",
        "category": "Groceries",
        "repeat":   3,
        "query":    "VPA shyamprov@upi Shyam Provision Store",
        "expect":   "Groceries",
    },
    {
        "receiver": "My Monthly Gym",
        "category": "Health",
        "repeat":   4,
        "query":    "VPA gymclub@upi My Monthly Gym",
        "expect":   "Health",
    },
]

# 6. BATCH — same as first 12 BASIC cases, tested via predict_batch
BATCH = BASIC[:12]

# ─────────────────────────────────────────────────────────────────────────────
# Result tracker
# ─────────────────────────────────────────────────────────────────────────────

class Results:
    def __init__(self):
        self.sections: dict[str, tuple[int, int]] = {}  # section → (pass, fail)

    def record(self, section: str, passed: bool):
        p, f = self.sections.get(section, (0, 0))
        if passed: self.sections[section] = (p + 1, f)
        else:      self.sections[section] = (p, f + 1)

    def totals(self):
        tp = sum(p for p, _ in self.sections.values())
        tf = sum(f for _, f in self.sections.values())
        return tp, tf

    def print_summary(self):
        sep("═")
        print(hdr("  ACCURACY REPORT"))
        sep("═")
        col_w = max(len(s) for s in self.sections) + 2
        for sec, (p, f) in self.sections.items():
            total = p + f
            pct   = p / total * 100 if total else 0
            colour = G if pct >= 90 else (Y if pct >= 70 else R)
            b     = bar(pct / 100, 18)
            print(f"  {sec:<{col_w}}  {b}  "
                  f"{colour}{p:>2}/{total} ({pct:.0f}%){X}")
        sep("─")
        tp, tf = self.totals()
        grand  = tp / (tp + tf) * 100 if (tp + tf) else 0
        colour = G if grand >= 90 else (Y if grand >= 70 else R)
        print(f"  {'TOTAL':<{col_w}}  {bar(grand/100, 18)}  "
              f"{B}{colour}{tp}/{tp+tf} ({grand:.0f}%){X}")
        sep("═")
        print()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _show_probas(r, n=5):
    top = sorted(r.all_probas.items(), key=lambda x: -x[1])[:n]
    for cat, p in top:
        print(f"      {dim(f'{cat:<18} {p:.3f}  {bar(p, 10)}')}")


# ─────────────────────────────────────────────────────────────────────────────
# Sections
# ─────────────────────────────────────────────────────────────────────────────

def run_basic(cat, verbose, results: Results):
    from ml.categoriser import predict_category, predict_full

    section("1 · Basic predictions  (well-known merchants)")
    print(f"  {'Input':<52} {'Expected':<14} {'Got':<14} {'Conf':>5}  {'ms':>4}")
    sep("─", 82)

    t_all = time.perf_counter()
    for raw, expected in BASIC:
        t0 = time.perf_counter()
        got, conf = predict_category(raw, amount=300.0)
        ms  = (time.perf_counter() - t0) * 1000
        hit = got == expected
        results.record("1·Basic", hit)
        col = f"{G}{got}{X}" if hit else f"{R}{got}{X}"
        icon = "✅" if hit else "❌"
        print(f"  {raw[:52]:<52} {expected:<14} {col:<23} {conf:>5.2f}  {ms:>3.0f}ms  {icon}")
        if verbose and not hit:
            _show_probas(predict_full(raw, amount=300.0))

    total_ms = (time.perf_counter() - t_all) * 1000
    p, f = results.sections.get("1·Basic", (0, 0))
    pct  = p / (p + f) * 100 if (p + f) else 0
    print(f"\n  {p}/{p+f} ({pct:.0f}%)  ·  total {total_ms:.0f}ms  ·  avg {total_ms/len(BASIC):.1f}ms/item")


def run_tricky(cat, verbose, results: Results):
    from ml.categoriser import predict_category, predict_full

    section("2 · Tricky / edge cases")
    print(f"  {'Input':<52} {'Expected':<14} {'Got':<14} {'Conf':>5}")
    sep("─", 82)

    for raw, expected in TRICKY:
        got, conf = predict_category(raw, amount=400.0)
        hit = got == expected
        results.record("2·Tricky", hit)
        col  = f"{G}{got}{X}" if hit else f"{R}{got}{X}"
        icon = "✅" if hit else f"{Y}⚠{X}"
        print(f"  {raw[:52]:<52} {expected:<14} {col:<23} {conf:>5.2f}  {icon}")
        if verbose and not hit:
            _show_probas(predict_full(raw, amount=400.0))

    p, f = results.sections.get("2·Tricky", (0, 0))
    pct  = p / (p + f) * 100 if (p + f) else 0
    print(f"\n  {p}/{p+f} ({pct:.0f}%)  — lower score expected for ambiguous inputs")


def run_low_conf(cat, verbose, results: Results):
    from ml.categoriser import predict_category

    section('3 · Low-confidence inputs  (expect "Other")')
    for raw in LOW_CONF:
        got, conf = predict_category(raw)
        hit = got == "Other"
        results.record("3·LowConf", hit)
        icon = "✅" if hit else warn("")
        print(f"  {raw[:60]:<60} → {got:<12} conf={conf:.2f}  {icon}")

    p, f = results.sections.get("3·LowConf", (0, 0))
    print(f"\n  {p}/{p+f} returned 'Other' as expected")


def run_correction(cat, verbose, results: Results):
    from ml.categoriser import predict_category, predict_full, record_feedback

    section("4 · User correction  (override takes effect immediately)")
    uid = "test_user_correction"

    for case in CORRECTION_CASES:
        raw, rcv, correction = case["raw"], case["receiver"], case["correction"]

        before, _ = predict_category(raw, user_id=uid, receiver=rcv)
        print(f"  Input   : {raw[:65]}")
        print(f"  Before  : {before}")

        record_feedback(user_id=uid, receiver=rcv, raw_text=raw,
                        corrected_category=correction, amount=300.0)

        after, conf = predict_category(raw, user_id=uid, receiver=rcv)
        hit  = after == correction
        results.record("4·Correction", hit)
        icon = ok("override working") if hit else fail("override NOT working")
        print(f"  After   : {after} (conf={conf:.2f})  {icon}")
        print()


def run_history(cat, verbose, results: Results):
    from ml.categoriser import predict_full, _history_store

    section("5 · History bias  (repeated category → boosted probability)")
    uid = "test_user_history"

    for case in HISTORY_CASES:
        rcv, cat_h, repeat = case["receiver"], case["category"], case["repeat"]
        query = case["query"]

        for _ in range(repeat):
            _history_store.record(uid, rcv, cat_h)

        r       = predict_full(query, user_id=uid, receiver=rcv, amount=500.0)
        boosted = r.all_probas.get(cat_h, 0)
        hit     = boosted > 0.10
        results.record("5·History", hit)
        icon = ok(f"P({cat_h})={boosted:.2f}") if hit else fail(f"P({cat_h})={boosted:.2f} too low")
        print(f"  Receiver: {rcv}")
        print(f"  Repeated: {cat_h} × {repeat}")
        print(f"  Got     : {r.category} conf={r.confidence:.2f}  {icon}")
        if verbose:
            _show_probas(r)
        print()


def run_online(cat, verbose, results: Results):
    from ml.categoriser import record_feedback, model_info, ONLINE_UPDATE_THRESHOLD
    from ml.categoriser import _online_buffer as buf

    section("6 · Online learning  (background warm refit)")
    print(f"  Threshold: {ONLINE_UPDATE_THRESHOLD} corrections")

    pairs = [
        ("VPA pizzapalace@okaxis Pizza Palace Restaurant", "Food"),
        ("VPA veggieshop@ybl Fresh Veggies Store",         "Groceries"),
        ("VPA yogaclass@hdfcbank Yoga Studio",             "Subscription"),
        ("VPA eyecheck@okaxis Vision Care Clinic",         "Health"),
        ("VPA busticket@paytm KSRTC BUS TICKET",           "Transport"),
        ("VPA schoolfees@icicibank Springdale School",      "Education"),
        ("VPA gymcafe@axisbank Gym Cafe Snack Bar",        "Food"),
    ][:ONLINE_UPDATE_THRESHOLD]

    for raw, cat2 in pairs:
        record_feedback(user_id="test_user_online",
                        receiver=raw.split()[-1],
                        raw_text=raw,
                        corrected_category=cat2,
                        amount=300.0)

    print(f"  Waiting for background refit (up to 8 s) …")
    deadline = time.perf_counter() + 8.0
    
    # Check BOTH the buffer size AND whether the thread is currently running
    while (buf.size() > 0 or buf._is_running) and time.perf_counter() < deadline:
        time.sleep(0.1)

    drained = buf.size() == 0
    results.record("6·Online", drained)
    print(f"  Buffer drained: {ok('yes') if drained else warn('still pending')}")

    info = model_info()
    applied = info.get("online_samples_applied", 0)
    results.record("6·Online", applied > 0)
    print(f"  Samples applied: {ok(str(applied)) if applied > 0 else warn('0 (may still be running)')}")


def run_batch(cat, verbose, results: Results):
    from ml.categoriser import predict_batch

    section("7 · Batch prediction")
    texts    = [r for r, _ in BATCH]
    expected = [e for _, e in BATCH]

    t0      = time.perf_counter()
    preds   = predict_batch(texts, amounts=[300.0] * len(texts))
    ms      = (time.perf_counter() - t0) * 1000

    for (got, conf), exp in zip(preds, expected):
        hit = got == exp
        results.record("7·Batch", hit)

    p, f = results.sections.get("7·Batch", (0, 0))
    pct  = p / (p + f) * 100 if (p + f) else 0
    print(f"  {p}/{p+f} ({pct:.0f}%)  in {ms:.0f}ms  ({ms/len(texts):.1f}ms/item)")


def run_latency(cat, verbose, results: Results):
    from ml.categoriser import predict_category

    section("8 · Latency  (p50 / p95 / p99)")
    test_inputs = [r for r, _ in BASIC[:20]]
    times = []
    for _ in range(3):   # 3 warmup
        predict_category(test_inputs[0], amount=200.0)
    for raw in test_inputs * 5:   # 100 timed calls
        t0 = time.perf_counter()
        predict_category(raw, amount=200.0)
        times.append((time.perf_counter() - t0) * 1000)

    times.sort()
    p50 = times[len(times) // 2]
    p95 = times[int(len(times) * 0.95)]
    p99 = times[int(len(times) * 0.99)]
    print(f"  p50={p50:.1f}ms  p95={p95:.1f}ms  p99={p99:.1f}ms  (n={len(times)})")
    hit = p95 < 150   # <150ms p95 is reasonable
    results.record("8·Latency", hit)
    print(f"  p95 < 150ms: {ok('yes') if hit else warn(f'no ({p95:.0f}ms)')}")


def run_cold_start(verbose):
    from ml import categoriser as C

    original = C.MODEL_PATH
    backup   = original + ".bak"
    existed  = os.path.exists(original)

    if existed:
        shutil.move(original, backup)

    C._model_loaded = False; C._clf = None
    C._tfidf_char   = None;  C._tfidf_word = None
    C._label_encoder = None
    C._override_store._overrides.clear()

    cases = [
        ("VPA swiggy.stores@axb Swiggy Limited",    "Food"),
        ("VPA uber@hdfcbank Uber Technologies",      "Transport"),
        ("VPA groww.brk@validhdfc GROWW INVEST",     "Investment"),
        ("VPA netflix@icicibank Netflix India",      "Subscription"),
        ("VPA apollo@hdfcbank Apollo Pharmacy",      "Health"),
    ]

    section("Cold-start  (embeddings only, no trained model)")
    passed = 0
    for raw, expected in cases:
        r   = C.predict_full(raw)
        hit = r.category == expected
        if hit: passed += 1
        icon = "✅" if hit else "⚠"
        print(f"  {raw[:50]:<50} got={r.category:<14} conf={r.confidence:.2f}  "
              f"src={r.source}  {icon}")

    pct = passed / len(cases) * 100
    print(f"\n  {passed}/{len(cases)} ({pct:.0f}%)  "
          f"— {Y}lower accuracy expected without trained model{X}")

    if existed:
        shutil.move(backup, original)
        C._model_loaded = False


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verbose",    action="store_true")
    ap.add_argument("--cold-start", action="store_true")
    ap.add_argument("--section",    default="all",
                    choices=["all","basic","tricky","lowconf",
                             "correction","history","online","batch","latency"])
    args = ap.parse_args()

    from ml.categoriser import model_info
    info = model_info()

    print(f"\n{B}SpendStream — Categoriser Test Suite{X}")
    sep("═")
    print(f"  Model loaded     : {info['model_loaded']}")
    print(f"  Embeddings OK    : {info['embeddings_ok']}")
    print(f"  Online threshold : {info['online_threshold']}")
    print(f"  Buffer pending   : {info['buffer_pending']}")
    if info.get("trained_at"):   print(f"  Trained at       : {info['trained_at']}")
    if info.get("updated_at"):   print(f"  Last updated     : {info['updated_at']}")
    if info.get("online_samples_applied"):
        print(f"  Online samples   : {info['online_samples_applied']}")

    results = Results()
    s = args.section

    if s in ("all", "basic"):       run_basic(s,      args.verbose, results)
    if s in ("all", "tricky"):      run_tricky(s,     args.verbose, results)
    if s in ("all", "lowconf"):     run_low_conf(s,   args.verbose, results)
    if s in ("all", "correction"):  run_correction(s, args.verbose, results)
    if s in ("all", "history"):     run_history(s,    args.verbose, results)
    if s in ("all", "online"):      run_online(s,     args.verbose, results)
    if s in ("all", "batch"):       run_batch(s,      args.verbose, results)
    if s in ("all", "latency"):     run_latency(s,    args.verbose, results)

    results.print_summary()

    if args.cold_start:
        run_cold_start(args.verbose)

    _, tf = results.totals()
    sys.exit(0 if tf == 0 else 1)


if __name__ == "__main__":
    main()