"""
merchant_identity.py — who was paid, as a stable key.

Pure functions: no network, no database, no settings. Tested by
test_merchant_identity.py.

Everything that has to agree on "the same merchant" lives here, because three
callers need the same answer:

  * etl.py         writes merchant_key onto every silver row at ingest;
  * correct_category() in supabase/migrations/ looks corrections up by it;
  * ml/categoriser.py asks whether a receiver is a person, to decide the
    Transfer boost and to keep personal payments out of the shared directory.

Before 2026-09-16 corrections were stored under the cleaned merchant name
("Swiggy") but looked up by the raw Gmail text ("VPA swiggy@icici SWIGGY"),
so they almost never matched and users had to correct the same merchant again
(DATA-05). merchant_key() is the fix: one key, computed once, from the most
stable thing in the string.

The SQL mirror of merchant_key() is public.merchant_key_of() in
supabase/migrations/20260916000000_phase4_corrections.sql. The two must agree;
test_merchant_identity.py lists the cases both are checked against.
"""

import re

# ─────────────────────────────────────────────────────────────────────────────
# Merchant name cleaning (moved here from etl.py on 2026-09-16, unchanged)
# ─────────────────────────────────────────────────────────────────────────────

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
    # Products before their brand: Swiggy One is a subscription and Instamart
    # is groceries. Collapsing both into "Swiggy" taught the model that
    # Swiggy means Subscription (found 2026-09-27 on the user's dataset).
    (r"(?i)swiggy\s*(one|membership)|swiggyone", "Swiggy One"),
    (r"(?i)instamart",           "Swiggy Instamart"),
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
    (r"(?i)cafe\s*coffee|\bccd\b", "CCD"),

    # Groceries
    (r"(?i)blinkit|grofers",     "Blinkit"),
    (r"(?i)zepto",               "Zepto"),
    (r"(?i)bigbasket",           "BigBasket"),
    (r"(?i)dmart",               "DMart"),
    (r"(?i)jiomart",             "JioMart"),
    (r"(?i)milkbasket",          "Milkbasket"),

    # Shopping
    (r"(?i)amazon\s*prime|amznprime|prime\s*video|primevideo", "Amazon Prime"),
    (r"(?i)amazon",              "Amazon"),
    (r"(?i)flipkart",            "Flipkart"),
    (r"(?i)myntra",              "Myntra"),
    (r"(?i)ajio",                "AJIO"),
    (r"(?i)nykaa",               "Nykaa"),
    (r"(?i)meesho",              "Meesho"),

    # Transport
    # Short names match whole words only: a bare "ola" matched COLA and KOLA.
    (r"(?i)\buber\b",            "Uber"),
    (r"(?i)\bola\b|olacabs",     "Ola"),
    (r"(?i)rapido",              "Rapido"),
    (r"(?i)irctc",               "IRCTC"),
    (r"(?i)makemytrip|\bmmt\b",  "MakeMyTrip"),
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
    (r"(?i)zee5",                "ZEE5"),
    (r"(?i)sonyliv",             "SonyLIV"),

    # Telecom / Utilities
    (r"(?i)\bjio\b",             "Jio"),
    (r"(?i)airtel",              "Airtel"),
    (r"(?i)bsnl",                "BSNL"),
    (r"(?i)\bvilpremum\b",       "Vi"),           # Vi premium recharge UPI handle
    # Whole words only: a bare "vi\b" matched RAVI, DEVI, SALVI, PALLAVI.
    (r"(?i)\bvi\b|vodafone|\bidea\b", "Vi"),
    (r"(?i)electricity|bescom|msedcl|tpddl|adani\s*electric", "Electricity"),
    (r"(?i)water\s*board|water\s*supply|bwssb",               "Water Bill"),
    (r"(?i)gas\s*(supply|bill)|mahanagar\s*gas|indraprastha",  "Gas Bill"),

    # Transport - Indian Railways UTS (local unreserved train tickets)
    (r"(?i)indian\s*railways?\s*(uts)?",  "Indian Railways UTS"),
    (r"(?i)\biruts\b",                    "Indian Railways UTS"),

    # Health / Pharmacy
    (r"(?i)apollo",              "Apollo Pharmacy"),
    (r"(?i)\b1mg\b",             "1mg"),
    (r"(?i)pharmeasy",           "PharmEasy"),
    (r"(?i)netmeds",             "Netmeds"),
    (r"(?i)medplus",             "MedPlus"),
    (r"(?i)global\s*medical",    "Medical Store"),

    # Finance / Payments
    (r"(?i)paytm",               "Paytm"),
    (r"(?i)phonepe",             "PhonePe"),
    (r"(?i)gpay|google\s*pay",   "Google Pay"),
    (r"(?i)mobikwik",            "MobiKwik"),
    (r"(?i)\bcred\b|cred\.club", "CRED"),   # not CREDIT or CREDITED
]


def extract_upi_merchant(raw: str) -> str | None:
    """
    For UPI strings like:
      "VPA paytm.k42v9be@pty NBC Vikhroli W"
      "VPA gpay-70418293355@okbizaxis Food Xpress"
      "VPA paytmqr319bd7@paytm GLOBAL MEDICAL AND G"

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
    # Strip leading UPI username (e.g. "vinodpatelkar219" or "nk2001452xyz")
    cleaned = re.sub(r"^[a-z0-9]{6,}\s+", "", cleaned, flags=re.IGNORECASE)
    # Remove honorifics
    cleaned = re.sub(r"\b(Mr|Mrs|Miss|Dr|Shri)\b\.?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[/@]\S+", "", cleaned)
    cleaned = re.sub(r"\d{6,}", "", cleaned)
    cleaned = re.sub(r"\b(UPI|NEFT|IMPS|RTGS|VPA)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:60] if cleaned else "Unknown"


# ─────────────────────────────────────────────────────────────────────────────
# 4.1 The stable key (DATA-05)
# ─────────────────────────────────────────────────────────────────────────────

# A UPI payee id: local-part@handle. The handle must start with a letter, so
# an amount ("60@2") or a stray email-like fragment does not match.
_VPA = re.compile(r"[A-Za-z0-9][A-Za-z0-9._\-]*@[A-Za-z][A-Za-z0-9.\-]*")


def merchant_key(raw_receiver: str, cleaned: str | None = None) -> str:
    """
    A stable identity for the party that was paid.

    Two forms, in order:
      "upi:<payee id>"   the UPI payee id, lowercased, when the raw text has
                         one. It is the most stable identifier there is: it
                         survives the display name changing, and a per-shop QR
                         code (paytmqr2njw85@ptys) keeps its own key.
      "name:<letters>"   otherwise the cleaned merchant name, lowercased with
                         everything but letters and digits removed, so
                         "NBC Vikhroli W" and "nbc vikhroli-w" agree.

    `cleaned` is clean_merchant(raw_receiver) when the caller already has it.
    Returns "" when there is nothing to key on; callers must treat "" as
    "no key" and never store it.
    """
    match = _VPA.search(raw_receiver or "")
    if match:
        return "upi:" + match.group(0).lower()

    name = cleaned if cleaned is not None else clean_merchant(raw_receiver or "")
    normalised = re.sub(r"[^a-z0-9]+", "", (name or "").lower())
    return "name:" + normalised if normalised else ""


# ─────────────────────────────────────────────────────────────────────────────
# Shop QR codes (2026-09-27)
#
# Small shops take UPI through a payment company's QR code, and the QR is often
# registered under the owner's own name ("AKSHAY FARHAN CHAVAN" at
# paytmqr...@ptys). The name reads as a person; the payee id says shop. The
# shapes below come from the user's labelled statement and dataset, where they
# are local businesses or person-named merchants, never personal transfers.
# ─────────────────────────────────────────────────────────────────────────────

_SHOP_QR_ID      = re.compile(r"^(paytmqr|bharatpe|getepay|paytm[.\-])", re.IGNORECASE)
_SHOP_QR_PHONEPE = re.compile(r"^q\d{5,}@ybl$", re.IGNORECASE)        # PhonePe merchant QR
_SHOP_QR_HANDLES = {"ptys", "fbpe", "okbizaxis", "pineaxis"}          # merchant-only handles

MODEL_SHOP_QR_TOKEN = "shopqr"


def is_shop_qr(raw_receiver: str) -> bool:
    """True when the payee id is a merchant QR code, whatever name is shown."""
    match = _VPA.search(raw_receiver or "")
    if not match:
        return False
    vpa = match.group(0).lower()
    handle = vpa.rsplit("@", 1)[1]
    return bool(_SHOP_QR_ID.match(vpa) or _SHOP_QR_PHONEPE.match(vpa) or handle in _SHOP_QR_HANDLES)


def model_text(raw_receiver: str, cleaned: str) -> str:
    """
    The text the categoriser model reads: the cleaned merchant name, plus a
    marker word when the payee is a shop QR, so a shop under a person's name
    is not read as a transfer. train_model.py (--pipeline-input),
    evaluate_model.py and etl.run_categorise_silver all call this, so training
    and prediction see the same text. Changing it needs a retrain.
    """
    return f"{cleaned} {MODEL_SHOP_QR_TOKEN}" if is_shop_qr(raw_receiver) else cleaned


# ─────────────────────────────────────────────────────────────────────────────
# 4.5 Is this a person or a business? (BUG-02)
# ─────────────────────────────────────────────────────────────────────────────

_HANDLE_PREFIX = re.compile(r"^VPA\s+", re.IGNORECASE)
_UPI_HANDLE    = re.compile(r"\S+@\S+\s*")

# Words that only a business puts in its name. The second line was added on
# 2026-09-16: small Indian shops are usually "<family name> <trade>", and
# without the trade word "Sharma Medical" reads as a person called Sharma.
_MERCHANT_KEYWORDS = re.compile(
    r"\b(store|stores|shop|mart|foods|kitchen|cafe|pvt|ltd|solutions|services|"
    r"enterprises|technologies|consultancy|agency|studio|lab|clinic|hospital|"
    r"pharmacy|pay|payments|bank|finance|credit|capital|ventures|"
    r"medical|medicals|kirana|traders|trading|hotel|restaurant|bakery|sweets|"
    r"dairy|provision|provisions|hardware|electricals|electronics|opticals|"
    r"salon|tailors|garments|fashion|stationery|general|"
    # Added 2026-09-18 alongside the wider training data: Indian shop signage is
    # usually "<name> <trade>", and without the trade word a shop like
    # "Bombay Chaat Corner" reads as a person and is kept out of the shared
    # directory it belongs in.
    r"corner|centre|center|stall|supermarket|market|hypermarket|departmental|"
    r"tiffin|dhaba|canteen|caterers|catering|juice|snacks|chaat|misal|"
    r"bhandar|bhavan|emporium|express|xpress|sons|brothers|agencies|"
    r"industries|motors|automobiles|petroleum|filling|cyber|xerox|"
    r"telecom|computers|mobiles|furniture|jewellers|jewellery|sarees|"
    r"footwear|nursery|florist|confectionery|poultry|mutton|chicken|"
    r"vegetable|vegetables|fruits|paints|tiles|cement|timber|"
    r"fitness|gym|spa|academy|institute|classes|tuitions|"
    r"apparel|collection|collections|readymade|novelty|hosiery)\b",
    re.IGNORECASE,
)
_PERSON_PATTERN = re.compile(r"^[A-Za-z]{2,}\s+[A-Za-z]{2,}(\s+[A-Za-z]{2,})?$")


def looks_like_person(receiver: str) -> bool:
    """
    True when the receiver text is a human being rather than a business.

    Used for two things: the Transfer boost in ml/categoriser.py, and keeping
    payments to people out of the shared merchant directory, where a friend's
    name has no business being.

    Three things make it False, in order:
      * a known merchant matches (MERCHANT_RULES). "VPA amazon@hdfcbank Amazon
        India" used to come out True, because stripping the handle left the
        plain two-word name "Amazon India" (BUG-02);
      * a business word appears (Kirana Store, Sharma Medical);
      * what remains is not a plain two- or three-word name.

    A person keeps their own name and handle: "VPA rahul.sharma@okaxis RAHUL
    SHARMA" is True.
    """
    if not receiver:
        return False

    # A shop QR code is a shop, even when it carries the owner's name.
    if is_shop_qr(receiver):
        return False

    # A merchant we can name is never a person, however plain the leftover text.
    for pattern, _clean_name in MERCHANT_RULES:
        if re.search(pattern, receiver):
            return False

    # "VPA " is a label from the bank, not part of the name. Before 2026-09-16
    # only the handle was stripped, so the leftover "VPA" counted as a word and
    # a three-word merchant name read as a person.
    cleaned = _HANDLE_PREFIX.sub("", receiver).strip()
    cleaned = _UPI_HANDLE.sub("", cleaned).strip()

    if _MERCHANT_KEYWORDS.search(cleaned):
        return False
    return bool(_PERSON_PATTERN.match(cleaned))
