"""
test_unseen_merchants.py ─ does the model generalise past what it memorised?
════════════════════════════════════════════════════════════════════════════
Every other score in this project is measured on the training templates or on
near-copies of them, so it is inflated and says nothing about a merchant the
model has not seen. This script is the cheapest honest counterweight: real
Indian merchants that appear **nowhere** in RAW_CASES or SMOKE_CASES, with the
category a person would obviously pick.

    IT IS STILL SYNTHETIC. The labels here are written from general knowledge,
    not from anyone's real transactions, and the merchant list is chosen rather
    than sampled. It is a smoke detector for "the model only memorises", not a
    substitute for the real evaluation set (ml/data/golden_set.csv, scored by
    evaluate_model.py).

The script refuses to lie to itself: before scoring, it asserts that none of
these merchants appears in the training data. If one is added to RAW_CASES
later, this test fails loudly rather than quietly becoming a memorisation test.

    docker run --rm --network none -e PYTHONPYCACHEPREFIX=/tmp/pyc \
        -v "$(pwd)/backend:/app:ro" spendstream-backend python test_unseen_merchants.py

Exit code is non-zero if a merchant leaked into training, or if accuracy falls
below FLOOR.
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cryptography.fernet import Fernet   # noqa: E402

os.environ.update({
    "SUPABASE_URL":              "https://placeholder.supabase.co",
    "SUPABASE_ANON_KEY":         "placeholder.placeholder.placeholder",
    "SUPABASE_SERVICE_ROLE_KEY": "placeholder.placeholder.placeholder",
    "GOOGLE_CLIENT_ID":          "test-client-id",
    "GOOGLE_CLIENT_SECRET":      "test-client-secret",
    "CRON_SECRET":               "test-cron-secret",
    "TOKEN_ENCRYPTION_KEY":      Fernet.generate_key().decode(),
})

# A guard, not a target. Below this the model has stopped generalising and
# something is wrong; above it, this file still proves very little on its own.
FLOOR = 0.55

# (raw receiver as a bank alert would show it, amount, the obvious category)
UNSEEN = [
    # (raw receiver as a bank alert would show it, amount, the obvious category)
    ("VPA chaipoint@ybl CHAI POINT", 180, "Food"),
    ("VPA bluetokai@okaxis BLUE TOKAI COFFEE ROASTERS", 420, "Food"),
    ("VPA bikanervala@paytm BIKANERVALA FOODS", 560, "Food"),
    ("VPA lapinoz@ptys LA PINOZ PIZZA", 640, "Food"),
    ("VPA jumboking@icici JUMBOKING BURGERS", 150, "Food"),
    ("VPA otipy@ybl OTIPY FRESH", 390, "Groceries"),
    ("VPA ratnadeep@okhdfcbank RATNADEEP SUPER MARKET", 1450, "Groceries"),
    ("VPA reliancesmart@icici RELIANCE SMART", 2100, "Groceries"),
    ("VPA vishalmegamart@paytm VISHAL MEGA MART", 1750, "Groceries"),
    ("VPA limeroad@ybl LIMEROAD FASHION", 1250, "Shopping"),
    ("VPA shoppersstop@axisbank SHOPPERS STOP", 4300, "Shopping"),
    ("VPA pantaloons@okicici PANTALOONS FASHION", 2800, "Shopping"),
    ("VPA wildcraft@hdfcbank WILDCRAFT INDIA", 3100, "Shopping"),
    ("VPA vijaysales@paytm VIJAY SALES ELECTRONICS", 28000, "Shopping"),
    ("VPA meru@ybl MERU CABS", 430, "Transport"),
    ("VPA savaari@okaxis SAVAARI CAR RENTALS", 2600, "Transport"),
    ("VPA goibibo@icici GOIBIBO TRAVEL", 5400, "Transport"),
    ("VPA railyatri@paytm RAILYATRI BUS", 890, "Transport"),
    ("VPA motilaloswal@hdfcbank MOTILAL OSWAL BROKING", 25000, "Investment"),
    ("VPA sharekhan@icici SHAREKHAN SECURITIES", 18000, "Investment"),
    ("VPA scripbox@axisbank SCRIPBOX MUTUAL FUND", 10000, "Investment"),
    ("VPA dhan@ybl DHAN BROKING", 7500, "Investment"),
    ("VPA wynk@axisbank WYNK MUSIC", 99, "Subscription"),
    ("VPA hungama@paytm HUNGAMA MUSIC", 129, "Subscription"),
    ("VPA sunnxt@icici SUN NXT", 260, "Subscription"),
    ("VPA grammarly@hdfcbank GRAMMARLY PREMIUM", 999, "Subscription"),
    ("VPA drlalpathlabs@okaxis DR LAL PATHLABS", 1200, "Health"),
    ("VPA healthians@ybl HEALTHIANS LAB TEST", 980, "Health"),
    ("VPA pristyncare@icici PRISTYN CARE CLINIC", 6500, "Health"),
    ("VPA srldiagnostics@paytm SRL DIAGNOSTICS", 1600, "Health"),
    ("VPA mahadiscom@icici MAHADISCOM ELECTRICITY BILL", 2400, "Utilities"),
    ("VPA bharatgas@paytm BHARAT GAS CYLINDER", 920, "Utilities"),
    ("VPA excitel@ybl EXCITEL BROADBAND", 750, "Utilities"),
    ("VPA spectranet@hdfcbank SPECTRANET INTERNET", 1350, "Utilities"),
    ("VPA carnivalcinemas@icici CARNIVAL CINEMAS", 700, "Entertainment"),
    ("VPA wonderla@ybl WONDERLA AMUSEMENT PARK", 2900, "Entertainment"),
    ("VPA imagicaa@paytm IMAGICAA THEME PARK", 3400, "Entertainment"),
    ("VPA toppr@okaxis TOPPR LEARNING", 3800, "Education"),
    ("VPA doubtnut@ybl DOUBTNUT CLASSES", 1100, "Education"),
    ("VPA cuemath@icici CUEMATH TUITIONS", 4600, "Education"),
    ("VPA greatlearning@hdfcbank GREAT LEARNING COURSE", 22000, "Education"),
    ("VPA hdfcergo@hdfcbank HDFC ERGO INSURANCE", 14000, "Payments"),
    ("VPA bajajallianz@icici BAJAJ ALLIANZ PREMIUM", 11000, "Payments"),
    ("VPA sbilife@sbi SBI LIFE INSURANCE PREMIUM", 19000, "Payments"),
    ("VPA acko@ybl ACKO GENERAL INSURANCE", 3200, "Payments"),
    ("VPA 9765001122@ybl SNEHA VIKRAM DESHMUKH", 3000, "Transfer"),
    ("VPA farhan.qureshi@okicici FARHAN QURESHI", 1500, "Transfer"),
]

# Fixed timestamp: predictions must not depend on when this runs (4.14).
FIXED_TS = "2024-06-15T16:30:00"


def leaked_into_training() -> list[str]:
    """
    Any of these merchants present in the training data or the smoke set makes
    this a memorisation test instead of a generalisation test.
    """
    import train_model

    corpus = " ".join(r for r, _, _, _ in train_model.RAW_CASES).lower()
    corpus += " ".join(r for r, _, _, _ in train_model.SMOKE_CASES).lower()

    leaked = []
    for raw, _, _ in UNSEEN:
        # the distinctive token is the handle's local part, e.g. "chaayos"
        m = re.search(r"([A-Za-z][A-Za-z0-9.\-_]{3,})@", raw)
        token = (m.group(1) if m else raw.split()[-1]).lower()
        token = token.split(".")[0].split("-")[0]
        if token and token in corpus:
            leaked.append(token)
    return leaked


def main() -> int:
    leaked = leaked_into_training()
    if leaked:
        print("FAIL  these merchants are in the training data, so this test "
              "would measure memorisation, not generalisation:")
        for t in sorted(set(leaked)):
            print(f"        {t}")
        return 1

    from ml.categoriser import predict_category

    print(f"{len(UNSEEN)} merchants, none of them in the training data\n")
    correct = 0
    misses = []
    by_cat: dict[str, list[int]] = {}

    for raw, amount, expected in UNSEEN:
        got, conf = predict_category(raw, amount=amount, timestamp=FIXED_TS)
        hit = got == expected
        correct += hit
        by_cat.setdefault(expected, []).append(1 if hit else 0)
        if not hit:
            misses.append((raw, expected, got, conf))
        print(f"  {'ok ' if hit else 'MISS'}  {raw[:46]:<46} "
              f"{expected:<14} -> {got:<14} {conf:.2f}")

    acc = correct / len(UNSEEN)
    print(f"\nUnseen-merchant accuracy: {correct}/{len(UNSEEN)} = {acc:.1%}")

    print("\nBy category:")
    for cat, hits in sorted(by_cat.items()):
        print(f"  {cat:<15} {sum(hits)}/{len(hits)}")

    if misses:
        print("\nMissed:")
        for raw, exp, got, conf in misses:
            print(f"  {raw[:52]:<52} want {exp:<14} got {got:<14} ({conf:.2f})")

    print("\nThis number is a floor-check, not a quality bar: the merchants and "
          "labels here were written by hand, not sampled from real data.")
    print("The real measure is evaluate_model.py on the golden set (4.8).")

    if acc < FLOOR:
        print(f"\nFAIL  below the {FLOOR:.0%} floor.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
