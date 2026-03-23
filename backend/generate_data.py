"""
generate_data.py
────────────────
Generates synthetic training data that closely matches real Indian UPI
transaction strings as they appear in bank notification emails.

All strings follow the real pattern observed in production data:
    VPA <handle>@<gateway> <MERCHANT NAME>

Run once from the backend/ directory:
    python generate_data.py

Output: ml/data/synthetic.csv  (~2500+ rows)
"""

import os
import csv
import random
import re

random.seed(42)


# ─────────────────────────────────────────────────────────────────────────────
# Real gateway suffixes observed in Indian UPI transactions
# ─────────────────────────────────────────────────────────────────────────────

BANK_GATEWAYS = [
    "okhdfcbank", "okicici", "oksbi", "okaxis",
    "ybl",        "axl",     "ibl",   "okbizaxis",
    "hdfcbank",   "icici",   "sbi",   "axisbank",
]

PAYTM_GATEWAYS = [
    "ptys", "pty", "pthdfc", "paytm", "ptybl",
]

OTHER_GATEWAYS = [
    "fbpe",       # BharatPe
    "validhdfc",  # Groww/brokers
    "validicici", # Groww/brokers
    "axb",        # Axis bank merchants
    "apl",        # Amazon Pay
    "rapl",       # Razorpay
]

ALL_GATEWAYS    = BANK_GATEWAYS + PAYTM_GATEWAYS + OTHER_GATEWAYS


# ─────────────────────────────────────────────────────────────────────────────
# Handle generators — matching real patterns exactly
# ─────────────────────────────────────────────────────────────────────────────

def rand_digits(n):
    return "".join([str(random.randint(0, 9)) for _ in range(n)])

def rand_alpha(n):
    return "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=n))

def rand_alphanum(n):
    return "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=n))


def handle_paytmqr():
    """paytmqr6woody, paytmqr5wr2s9, paytmqr6yuz30"""
    return f"paytmqr{rand_digits(1)}{rand_alpha(random.randint(4,6))}"


def handle_paytm_dot():
    """paytm.s1uytc4, paytm.s11h0ar"""
    return f"paytm.{rand_alpha(1)}{rand_alphanum(random.randint(5,7))}"


def handle_phone_number():
    """8368536065, gpay-11256438096, phonepe-9876543210"""
    number = rand_digits(10)
    style  = random.randint(1, 3)
    if style == 1:
        return number
    elif style == 2:
        return f"gpay-{number}"
    else:
        return f"phonepe-{number}"


def handle_name_slug(name: str):
    """
    gawandkrrish5, siddhantpatel0712-1, mukhtaransaria337, arnavdumane04
    """
    parts = name.lower().split()
    style = random.randint(1, 4)

    if style == 1 and len(parts) >= 2:
        slug = parts[-1] + parts[0][:random.randint(3, 6)] + rand_digits(random.randint(2, 4))
    elif style == 2:
        slug = parts[0] + rand_digits(random.randint(2, 5))
    elif style == 3 and len(parts) >= 2:
        slug = parts[0] + parts[-1][:4] + rand_digits(3)
        if random.random() > 0.5:
            slug += f"-{random.randint(1, 5)}"
    else:
        slug = rand_alpha(2) + rand_digits(7) + rand_alpha(3)

    return slug


def handle_merchant_slug(merchant: str):
    """sapphirekfconline, eatclub, swiggy.stores, mcdonalds.42276700"""
    slug  = re.sub(r"[^a-z0-9]", "", merchant.lower())[:10]
    style = random.randint(1, 3)
    if style == 1:
        return slug
    elif style == 2:
        return f"{slug}.{rand_alpha(4)}"
    else:
        return f"{slug}.{rand_digits(8)}"


def handle_service_specific(service: str):
    """groww.brk, groww.iccl2.brk, appleservices.bdsi, bharatpe.9y0r0e7l3x685328"""
    slug  = re.sub(r"[^a-z0-9]", "", service.lower())[:8]
    style = random.randint(1, 3)
    if style == 1:
        return f"{slug}.{rand_alpha(3)}"
    elif style == 2:
        return f"{slug}.{rand_alphanum(4)}.{rand_alpha(3)}"
    else:
        return f"{slug}.{rand_digits(16)}"


# ─────────────────────────────────────────────────────────────────────────────
# Merchant name display — with realistic truncation
# ─────────────────────────────────────────────────────────────────────────────

def maybe_truncate(name: str) -> str:
    """
    Real bank strings often truncate long merchant names.
    e.g. "GLOBAL MEDICAL AND G"  "NBC Vikhroli W"
    """
    if len(name) <= 15 or random.random() > 0.25:
        return name
    words     = name.split()
    keep      = random.randint(max(1, len(words) - 2), len(words))
    truncated = " ".join(words[:keep])
    if random.random() > 0.6:
        truncated += " " + rand_alpha(1).upper()
    return truncated


def merchant_display(merchant: str) -> str:
    display = maybe_truncate(merchant)
    r = random.random()
    if r < 0.6:
        return display.upper()
    elif r < 0.85:
        return display.title()
    else:
        return display


# ─────────────────────────────────────────────────────────────────────────────
# Core VPA string builder
# ─────────────────────────────────────────────────────────────────────────────

def make_vpa(handle: str, gateway: str, display: str) -> str:
    return f"VPA {handle}@{gateway} {display}"


def generate_variants(merchant: str, category: str, n: int) -> list[dict]:
    rows = []
    for _ in range(n):
        style   = random.randint(1, 5)
        gateway = random.choice(ALL_GATEWAYS)
        display = merchant_display(merchant)

        if style == 1:
            handle  = handle_paytmqr()
            gateway = random.choice(PAYTM_GATEWAYS)
        elif style == 2:
            handle  = handle_paytm_dot()
            gateway = random.choice(PAYTM_GATEWAYS)
        elif style == 3:
            handle  = handle_phone_number()
            gateway = random.choice(BANK_GATEWAYS)
        elif style == 4:
            handle  = handle_merchant_slug(merchant)
            gateway = random.choice(BANK_GATEWAYS + OTHER_GATEWAYS)
        else:
            handle  = handle_service_specific(merchant)
            gateway = random.choice(BANK_GATEWAYS + OTHER_GATEWAYS)

        rows.append({
            "raw_text": make_vpa(handle, gateway, display),
            "merchant": merchant,
            "category": category
        })

    return rows


def generate_person_variants(n: int) -> list[dict]:
    """
    Matches real patterns like:
      VPA gawandkrrish5@okhdfcbank KRRISH GAJENDRA GAWAND
      VPA arnavdumane04@okhdfcbank ARNAV RAVISHANKAR DUMANE
      VPA ss2002786kgn@okicici Mr SAHIL RAJU SAYYED
      VPA 8368536065@pthdfc HARSHIT SAXENA
      VPA bharatpe.9y0r0e7l3x685328@fbpe SAJAHAN SK
    """
    FIRST = [
        "RAHUL","PRIYA","AMIT","SNEHA","VIKRAM","POOJA","ROHIT","NEHA",
        "AAKASH","DIVYA","SURESH","ANITA","KARAN","MEERA","ARJUN","KAVYA",
        "NIKHIL","SHREYA","RAJESH","ANJALI","SIDDHARTH","NAINA","MANISH",
        "RITU","DEEPAK","SIMRAN","ADITYA","PALLAVI","VARUN","SWATI",
        "HARISH","PREETI","VIVEK","SHWETA","GAURAV","MONIKA","SACHIN",
        "SUNITA","YASH","REKHA","KUNAL","MADHURI","SANDEEP","USHA",
        "TARUN","LATA","MANOJ","GEETA","VINOD","SONIA","ARNAV","BHERU",
        "GOPAL","MUKHTAR","RAVINDRA","SAJAHAN","KRRISH","HARSHIT",
    ]
    MIDDLE = [
        "KUMAR","SINGH","LAL","PRASAD","RAJU","GAFUR","RAVISHANKAR",
        "AJAYKUMAR","GAJENDRA","DILIP","RAMESH","","","","",
    ]
    LAST = [
        "SHARMA","PATEL","SINGH","KUMAR","GUPTA","JOSHI","MEHTA","SHAH",
        "VERMA","MISHRA","PANDEY","CHAUHAN","YADAV","TIWARI","DUBEY",
        "SAXENA","AGARWAL","KAPOOR","MALHOTRA","NAIR","REDDY","RAO",
        "PATIL","JADHAV","SHINDE","KULKARNI","JAIN","DESAI","NAIK",
        "SHETTY","DUMANE","GAWAND","ANSARI","SAYYED","SOLANKI","CHAUTALA",
    ]
    HONORIFICS = ["Mr ", "Mrs ", "Ms ", "Dr ", ""]

    rows = []
    for _ in range(n):
        first     = random.choice(FIRST)
        middle    = random.choice(MIDDLE)
        last      = random.choice(LAST)
        honorific = random.choice(HONORIFICS)

        name_parts = [p for p in [first, middle, last] if p]
        full_name  = " ".join(name_parts)

        style   = random.randint(1, 4)
        gateway = random.choice(BANK_GATEWAYS + PAYTM_GATEWAYS)

        if style == 1:
            handle  = handle_name_slug(full_name)
            gateway = random.choice(BANK_GATEWAYS)
        elif style == 2:
            handle  = handle_phone_number()
            gateway = random.choice(BANK_GATEWAYS + PAYTM_GATEWAYS)
        elif style == 3:
            handle  = rand_alpha(2) + rand_digits(7) + rand_alpha(3)
            gateway = random.choice(BANK_GATEWAYS)
        else:
            handle  = f"bharatpe.{rand_alphanum(16)}"
            gateway = "fbpe"

        raw = make_vpa(handle, gateway, f"{honorific}{full_name}")

        rows.append({
            "raw_text": raw.strip(),
            "merchant": full_name,
            "category": "Transfer"
        })

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Seed definitions — (merchant_name, category, n_variants)
# ─────────────────────────────────────────────────────────────────────────────

SEEDS = [
    # Food delivery
    ("Swiggy",                  "Food",          20),
    ("Zomato",                  "Food",          20),
    ("EatClub",                 "Food",          15),
    ("Dunzo",                   "Food",          12),
    ("Faasos",                  "Food",          10),
    ("Box8",                    "Food",          10),
    ("FreshMenu",               "Food",          10),
    # Restaurants
    ("KFC",                     "Food",          15),
    ("McDonalds",               "Food",          15),
    ("Dominos",                 "Food",          15),
    ("Subway",                  "Food",          12),
    ("Burger King",             "Food",          12),
    ("Pizza Hut",               "Food",          10),
    ("Starbucks",               "Food",          10),
    ("Cafe Coffee Day",         "Food",          10),
    ("NBC Vikhroli",            "Food",          12),
    ("Food Xpress",             "Food",          12),
    ("Misal Restaurant",        "Food",          10),
    ("Hotel Shiv Sagar",        "Food",          10),
    ("Haldirams",               "Food",          10),
    ("Barbeque Nation",         "Food",           8),
    # Groceries
    ("Blinkit",                 "Groceries",     15),
    ("Zepto",                   "Groceries",     15),
    ("BigBasket",               "Groceries",     15),
    ("DMart",                   "Groceries",     12),
    ("JioMart",                 "Groceries",     12),
    ("Milkbasket",              "Groceries",     10),
    ("Nature Basket",           "Groceries",      8),
    ("Spencers",                "Groceries",      8),
    # Shopping
    ("Amazon",                  "Shopping",      20),
    ("Flipkart",                "Shopping",      20),
    ("Myntra",                  "Shopping",      15),
    ("AJIO",                    "Shopping",      12),
    ("Nykaa",                   "Shopping",      12),
    ("Meesho",                  "Shopping",      12),
    ("Snapdeal",                "Shopping",      10),
    ("Tata Cliq",               "Shopping",      10),
    ("Reliance Digital",        "Shopping",      10),
    ("Croma",                   "Shopping",      10),
    # Transport
    ("Uber",                    "Transport",     15),
    ("Ola",                     "Transport",     15),
    ("Rapido",                  "Transport",     12),
    ("IRCTC",                   "Transport",     15),
    ("MakeMyTrip",              "Transport",     12),
    ("RedBus",                  "Transport",     10),
    ("Cleartrip",               "Transport",     10),
    ("IndiGo",                  "Transport",     12),
    ("Air India",               "Transport",     10),
    ("SpiceJet",                "Transport",     10),
    # Investment
    ("Groww",                   "Investment",    20),
    ("Groww Invest Tech",       "Investment",    15),
    ("Mutual Funds ICCL",       "Investment",    15),
    ("Next Billion Technology", "Investment",    12),
    ("Zerodha",                 "Investment",    15),
    ("Kuvera",                  "Investment",    10),
    ("Upstox",                  "Investment",    12),
    ("Angel Broking",           "Investment",    10),
    ("HDFC Securities",         "Investment",    10),
    ("SBI Mutual Fund",         "Investment",    12),
    ("ICICI Prudential",        "Investment",    10),
    ("Axis Mutual Fund",        "Investment",    10),
    # Subscription
    ("Netflix",                 "Subscription",  15),
    ("Spotify",                 "Subscription",  15),
    ("Disney Hotstar",          "Subscription",  12),
    ("YouTube Premium",         "Subscription",  12),
    ("Apple Media Services",    "Subscription",  15),
    ("Amazon Prime",            "Subscription",  12),
    ("ZEE5",                    "Subscription",  10),
    ("SonyLIV",                 "Subscription",  10),
    ("Jio Cinema",              "Subscription",  10),
    # Health
    ("Apollo Pharmacy",         "Health",        15),
    ("1mg",                     "Health",        12),
    ("PharmEasy",               "Health",        12),
    ("Netmeds",                 "Health",        10),
    ("MedPlus",                 "Health",        10),
    ("Global Medical",          "Health",        15),
    ("Medical Store",           "Health",        12),
    ("Practo",                  "Health",        10),
    # Utilities
    ("Electricity Bill",        "Utilities",     15),
    ("Water Bill",              "Utilities",     12),
    ("Gas Bill",                "Utilities",     12),
    ("Jio",                     "Utilities",     15),
    ("Airtel",                  "Utilities",     15),
    ("Vi Vodafone",             "Utilities",     10),
    ("BSNL",                    "Utilities",     10),
    ("Tata Sky",                "Utilities",     10),
    ("ACT Fibernet",            "Utilities",     10),
    # Entertainment
    ("BookMyShow",              "Entertainment", 12),
    ("PVR Cinemas",             "Entertainment", 12),
    ("INOX",                    "Entertainment", 10),
    ("Dream11",                 "Entertainment", 12),
    ("MPL",                     "Entertainment", 10),
    ("WinZo",                   "Entertainment",  8),
    # Education
    ("Byjus",                   "Education",     12),
    ("Unacademy",               "Education",     12),
    ("Vedantu",                 "Education",     10),
    ("Coursera",                "Education",     10),
    ("Udemy",                   "Education",     10),
    ("upGrad",                  "Education",     10),
    ("College Fees",            "Education",     12),
    ("School Fees",             "Education",     12),
    # Payments
    ("Paytm",                   "Payments",      20),
    ("PhonePe",                 "Payments",      20),
    ("Google Pay",              "Payments",      15),
    ("MobiKwik",                "Payments",      10),
    ("CRED",                    "Payments",      12),
    ("Razorpay",                "Payments",      12),
    ("BillDesk",                "Payments",      10),
]


# ─────────────────────────────────────────────────────────────────────────────
# Main generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_dataset() -> list[dict]:
    rows = []
    for merchant, category, n in SEEDS:
        rows.extend(generate_variants(merchant, category, n))
    rows.extend(generate_person_variants(400))
    random.shuffle(rows)
    return rows


def print_stats(rows: list[dict]):
    from collections import Counter
    counts = Counter(r["category"] for r in rows)
    total  = len(rows)
    print(f"\n{'─'*48}")
    print(f"  Total rows      : {total}")
    print(f"{'─'*48}")
    print(f"  {'Category':<22} {'Count':>6}  {'%':>5}")
    print(f"{'─'*48}")
    for cat, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {cat:<22} {count:>6}  {count/total*100:>4.1f}%")
    print(f"{'─'*48}\n")


def save_dataset(rows: list[dict], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["raw_text", "merchant", "category"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows)} rows → {path}")


if __name__ == "__main__":
    OUTPUT_PATH = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "ml", "data", "synthetic.csv"
    )

    print("Generating synthetic UPI transaction dataset...")
    rows = generate_dataset()
    print_stats(rows)
    save_dataset(rows, OUTPUT_PATH)

    print("\nSample rows:")
    print(f"  {'raw_text':<62} category")
    print(f"  {'─'*62} {'─'*14}")
    for row in random.sample(rows, 12):
        print(f"  {row['raw_text'][:60]:<62} {row['category']}")
    print("\nDone. Run train.py next.")