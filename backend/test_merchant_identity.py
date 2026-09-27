"""
test_merchant_identity.py ─ offline checks for merchant_identity.py
═══════════════════════════════════════════════════════════════════
Covers the stable merchant key (DATA-05) and the person-name check (BUG-02).

Needs no network, no Supabase project, no .env and no model: merchant_identity
imports nothing project-local. Run in the dev image from the repo root
(Git Bash on Windows: prefix MSYS_NO_PATHCONV=1 and use "$(pwd -W)"):

    docker run --rm --network none -v "$(pwd)/backend:/app:ro" \
        spendstream-backend python test_merchant_identity.py

MERCHANT_KEY_CASES is also the list the SQL mirror (public.merchant_key_of in
supabase/migrations/20260916000000_phase4_corrections.sql) is checked against
in supabase/tests/security_test.sql. Add a case to both when you add one here.

Exit code is non-zero if any check fails.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from merchant_identity import (            # noqa: E402
    clean_merchant,
    is_shop_qr,
    looks_like_person,
    merchant_key,
    model_text,
)

FAILED: list[str] = []


def check(name: str, ok, detail="") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   [{detail}]"))
    if not ok:
        FAILED.append(name)


# ─────────────────────────────────────────────────────────────────────────────
# 4.1 merchant_key
#
# (raw receiver, silver merchant, expected key). The same table is asserted
# against the SQL mirror, so every row here must be reproducible in SQL from
# exactly these two inputs.
# ─────────────────────────────────────────────────────────────────────────────

MERCHANT_KEY_CASES = [
    # A UPI payee id wins: it survives the display name changing.
    ("VPA swiggy@icici SWIGGY",            "Swiggy",         "upi:swiggy@icici"),
    ("VPA swiggy@icici SWIGGY ORDER 4412", "Swiggy",         "upi:swiggy@icici"),
    ("VPA zomato@hdfcbank ZOMATO",         "Zomato",         "upi:zomato@hdfcbank"),
    # A per-shop QR code keeps its own key, which is the point: that shop is
    # a merchant in its own right.
    ("VPA paytmqr2njw85@ptys NBC Vikhroli", "NBC Vikhroli",  "upi:paytmqr2njw85@ptys"),
    ("VPA paytm.k42v9be@pty NBC Vikhroli W", "NBC Vikhroli W", "upi:paytm.k42v9be@pty"),
    ("VPA gpay-70418293355@okbizaxis Food Xpress", "Food Xpress", "upi:gpay-70418293355@okbizaxis"),
    # Case and surrounding text do not matter.
    ("VPA SWIGGY@ICICI Swiggy",             "Swiggy",        "upi:swiggy@icici"),
    # No handle: the cleaned name, letters and digits only.
    ("NETFLIX INDIA",                       "Netflix",       "name:netflix"),
    ("NBC Vikhroli W",                      "NBC Vikhroli W", "name:nbcvikhroliw"),
    ("nbc vikhroli-w",                      "NBC Vikhroli W", "name:nbcvikhroliw"),
    ("Sharma General Store",                "Sharma General Store", "name:sharmageneralstore"),
    ("Sharma Medical",                      "Sharma Medical", "name:sharmamedical"),
    # Nothing to key on.
    ("",                                    "",              ""),
    ("!!!",                                 "",              ""),
]


def test_merchant_key():
    for raw, cleaned, expected in MERCHANT_KEY_CASES:
        got = merchant_key(raw, cleaned)
        check(f"merchant_key({raw!r}) == {expected!r}", got == expected, got)

    # Without a cleaned name it falls back to clean_merchant()
    check(
        "merchant_key derives the name itself when not given one",
        merchant_key("NETFLIX INDIA") == "name:netflix",
        merchant_key("NETFLIX INDIA"),
    )

    # The whole point of 4.1: the raw Gmail text and the cleaned merchant name
    # produce the same key, so a correction stored under one is found by the
    # other. This is what made corrections repeat (DATA-05).
    raw = "VPA swiggy@icici SWIGGY"
    check(
        "raw text and its cleaned name agree on one key",
        merchant_key(raw, clean_merchant(raw)) == merchant_key(raw),
    )

    # Two different shops never collide.
    check(
        "Sharma General Store and Sharma Medical are different merchants",
        merchant_key("Sharma General Store", "Sharma General Store")
        != merchant_key("Sharma Medical", "Sharma Medical"),
    )

    # An amount or a stray fragment is not a payee id.
    check("an amount is not a UPI id", merchant_key("paid 60@2 to shop", "Shop") == "name:shop")


# ─────────────────────────────────────────────────────────────────────────────
# 4.5 looks_like_person (BUG-02)
# ─────────────────────────────────────────────────────────────────────────────

def test_looks_like_person():
    # The bug: stripping the handle left "VPA Amazon India", a plain
    # three-word name, so a merchant counted as a person and got the
    # Transfer boost.
    people = [
        "VPA rahul.sharma@okaxis RAHUL SHARMA",
        "Rahul Sharma",
        "PRIYA IYER",
        "Anil Kumar Verma",
    ]
    businesses = [
        "VPA amazon@hdfcbank Amazon India",
        "VPA swiggy@icici SWIGGY",
        "VPA zomato@hdfcbank ZOMATO",
        "Amazon India",
        "Sharma General Store",
        "Sharma Medical",
        "Kirana Shop",
        "Apollo Pharmacy",
        "NBC Vikhroli W",          # not two plain words
        "",
    ]

    for text in people:
        check(f"person: {text!r}", looks_like_person(text) is True)
    for text in businesses:
        check(f"business: {text!r}", looks_like_person(text) is False)


# ─────────────────────────────────────────────────────────────────────────────
# clean_merchant still behaves as it did before the move out of etl.py
# ─────────────────────────────────────────────────────────────────────────────

def test_clean_merchant_unchanged():
    cases = [
        ("VPA swiggy@icici SWIGGY",              "Swiggy"),
        ("VPA paytm.k42v9be@pty NBC Vikhroli W", "NBC Vikhroli"),
        ("NETFLIX INDIA",                        "Netflix"),
        ("",                                     "Unknown"),
        # 2026-09-27: short brand names matched inside people's names.
        ("VPA arjun.gupta@okaxis ARJUN RAVI GUPTA", "ARJUN RAVI GUPTA"),
        ("VPA aditysalvi-1@sbi ADITYA ADITI SALVI", "ADITYA ADITI SALVI"),
        ("VPA x@okaxis NIKOLA COLA",               "NIKOLA COLA"),
        ("VPA z@okaxis SHREE CREDIT SOCIETY",      "SHREE CREDIT SOCIETY"),
        ("VPA vodafoneidea@axb VI PREPAID",        "Vi"),
        ("VPA olacabs@ybl OLA",                    "Ola"),
        # Products are not collapsed into their brand.
        ("VPA swiggyone@okbizaxis AUTOPAY-SWIGGY ONE", "Swiggy One"),
        ("VPA swiggyinstamart@icici SWIGGY INSTAMART", "Swiggy Instamart"),
        ("VPA amznprime@apl AMAZON PRIME",         "Amazon Prime"),
        ("VPA amazon-pod@yapl AMAZON PAY",         "Amazon"),
    ]
    for raw, expected in cases:
        got = clean_merchant(raw)
        check(f"clean_merchant({raw!r}) == {expected!r}", got == expected, got)


# -----------------------------------------------------------------------------
# UPI handle families (added 2026-09-18 with the wider training data)
#
# The point of these is that the handle suffix must be irrelevant. Whatever app
# or bank issued the id, the key is the payee id and the category signal is the
# display name. A merchant must not be read as a person, and a person paying
# from a numeric or dotted handle must still be read as a person.
# -----------------------------------------------------------------------------

UPI_FAMILY_CASES = [
    # (raw receiver, expected merchant_key, is a person)
    # PhonePe
    ("VPA swiggy@ybl SWIGGY",                     "upi:swiggy@ybl",        False),
    ("VPA amazon@ibl AMAZON",                     "upi:amazon@ibl",        False),
    ("VPA rapido@axl",                            "upi:rapido@axl",        False),
    # Google Pay
    ("VPA zomato@okaxis ZOMATO",                  "upi:zomato@okaxis",     False),
    ("VPA groww@okhdfcbank GROWW",                "upi:groww@okhdfcbank",  False),
    # Paytm, including its QR ids
    ("VPA netflix@ptys",                          "upi:netflix@ptys",      False),
    ("VPA paytmqr281005050101@paytm SHREE GANESH KIRANA AND GENERAL STORES",
     "upi:paytmqr281005050101@paytm", False),
    ("VPA paytmqr5x8k2p@ptys ANNAPURNA TIFFIN SERVICE",
     "upi:paytmqr5x8k2p@ptys", False),
    # BHIM / generic
    ("VPA jio@upi JIO RECHARGE",                  "upi:jio@upi",           False),
    # Amazon Pay
    ("vpa amazon@apl amazon india",               "upi:amazon@apl",        False),
    # BharatPe and Google Pay merchant ids
    ("VPA bharatpe.8w2k1m7p390214@fbpe VARIETY SNACKS CENTRE",
     "upi:bharatpe.8w2k1m7p390214@fbpe", False),
    ("VPA gpay-30518274901@okbizaxis SUNRISE STATIONERY",
     "upi:gpay-30518274901@okbizaxis", False),
    # PhonePe merchant ids
    ("VPA q721904553@ybl BOMBAY CHAAT CORNER",    "upi:q721904553@ybl",    False),
    ("VPA q905412387@ybl CITY OPTICALS",          "upi:q905412387@ybl",    False),
    # Razorpay sub-handles
    ("VPA cultfit.rzp@hdfcbank CULT FITNESS",     "upi:cultfit.rzp@hdfcbank", False),
    # Newer neobank / wallet handles
    ("VPA netflix@jupiteraxis NETFLIX INDIA",     "upi:netflix@jupiteraxis", False),
    ("VPA uber@fifederal UBER INDIA",             "upi:uber@fifederal",    False),
    ("VPA spotify@slc SPOTIFY",                   "upi:spotify@slc",       False),
    ("VPA airtel@freecharge AIRTEL PREPAID",      "upi:airtel@freecharge", False),
    # WhatsApp Pay
    ("VPA bigbasket@waaxis BIGBASKET",            "upi:bigbasket@waaxis",  False),
    # Bank-native handles
    ("VPA apollo@barodampay APOLLO PHARMACY",     "upi:apollo@barodampay", False),
    ("VPA cred@idfcbank CRED BILL PAYMENT",       "upi:cred@idfcbank",     False),
    # People: numeric, dotted, hyphenated and underscored handles
    ("VPA 9812345670@ybl ANIL KUMAR VERMA",       "upi:9812345670@ybl",    True),
    ("VPA 8801234567@upi DEEPAK JOSHI",           "upi:8801234567@upi",    True),
    ("VPA priya.iyer.98@okhdfcbank PRIYA IYER",   "upi:priya.iyer.98@okhdfcbank", True),
    ("VPA karan-malhotra1@ybl KARAN MALHOTRA",    "upi:karan-malhotra1@ybl", True),
    ("VPA rohan_verma_07@okicici ROHAN VERMA",    "upi:rohan_verma_07@okicici", True),
]


def test_upi_handle_families():
    for raw, expected_key, is_person in UPI_FAMILY_CASES:
        got = merchant_key(raw)
        check("key for %r" % raw[:46], got == expected_key, got)
        check("person=%s for %r" % (is_person, raw[:40]),
              looks_like_person(raw) is is_person, looks_like_person(raw))

    # The same merchant seen through different apps is still one merchant to
    # the model, but a *different* key per handle - which is correct: the payee
    # id is what a correction is stored against, and the ids really are
    # different accounts.
    a = merchant_key("VPA swiggy@ybl SWIGGY")
    b = merchant_key("VPA swiggy@icici SWIGGY")
    check("different payee ids give different keys", a != b, (a, b))

    # Structural edge cases that used to be ambiguous.
    check("a handle with no display name still keys",
          merchant_key("VPA dmart@ybl") == "upi:dmart@ybl")
    check("no VPA prefix still keys",
          merchant_key("swiggy@okaxis SWIGGY") == "upi:swiggy@okaxis")
    check("a bare merchant name keys by name",
          merchant_key("PVR INOX LIMITED", "PVR INOX LIMITED") == "name:pvrinoxlimited")
    check("mixed-case handles fold to one key",
          merchant_key("VPA SwiGGy@YBL Swiggy Limited") == "upi:swiggy@ybl")
    check("legacy rails have no payee id, so they key by name",
          merchant_key("IMPS P2A TRANSFER TARUN KHANNA", "Tarun Khanna") == "name:tarunkhanna")


def test_shop_qr():
    # 2026-09-27: small shops take UPI through a QR registered under the
    # owner's name. The payee id says shop; the name must not win.
    shops = [
        "VPA paytmqr5cs26w@ptys RAVINDRA S SHETTY",
        "VPA getepay.svccblqr875749@icici KESTREL MANUFACTURIN",
        "VPA q123456789@ybl KIRAN",
        "VPA paytm.k42v9be@pty NBC Vikhroli W",
        "VPA paytm-54862057@ptybl IMPERIAL FAST FOOD C",
        "VPA bharatpe09876543@fbpe SHREE GANESH",
    ]
    people = ["VPA namanwagh@okicici NAMAN WAGH", "VPA rahul.sharma@okaxis RAHUL SHARMA", "Rahul Sharma"]
    for r in shops:
        check(f"shop QR: {r}", is_shop_qr(r) and not looks_like_person(r))
        check(f"shop QR marker in model text: {r}", model_text(r, clean_merchant(r)).endswith(" shopqr"))
    for r in people:
        check(f"not a shop QR: {r}", not is_shop_qr(r))
        check(f"no marker for a person: {r}", model_text(r, "X") == "X")
    check("still a person: RAHUL SHARMA at okaxis", looks_like_person("VPA rahul.sharma@okaxis RAHUL SHARMA"))


if __name__ == "__main__":
    test_merchant_key()
    test_looks_like_person()
    test_clean_merchant_unchanged()
    test_upi_handle_families()
    test_shop_qr()

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) failed:")
        for name in FAILED:
            print(f"  - {name}")
        sys.exit(1)
    print("ALL MERCHANT IDENTITY CHECKS PASSED")
