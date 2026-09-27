"""
train_model.py  ─  UPI Categoriser Training Script
════════════════════════════════════════════════════
Usage:
    python train_model.py                  # default 40 samples/case
    python train_model.py --samples 60    # more data = better accuracy
    python train_model.py --no-smoke      # skip smoke test

Output:
    ml/model_v2.pkl
    ml/label_encoder_v2.pkl
"""

from __future__ import annotations

import os
import re as _re
import time
import random
import logging
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import joblib

from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelEncoder
from sklearn.utils import resample
from sklearn.utils.class_weight import compute_class_weight
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from scipy.sparse import hstack


logging.basicConfig(level=logging.INFO, format="[train] %(message)s")
log = logging.getLogger("train")

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────

_HERE        = os.path.dirname(os.path.abspath(__file__))
_ML_DIR      = _HERE if os.path.basename(_HERE) == "ml" else os.path.join(_HERE, "ml")
MODEL_PATH   = os.path.join(_ML_DIR, "model_v2.pkl")
ENCODER_PATH = os.path.join(_ML_DIR, "label_encoder_v2.pkl")

# Categories, feature sizes, text cleaning and metadata: ml/features.py, shared
# with prediction and evaluation (2026-09-27; this file used to keep a copy).
from ml.features import (   # noqa: E402
    CATEGORIES, EMBEDDING_DIM, METADATA_DIM, TFIDF_CHAR_MAX, TFIDF_WORD_MAX,
    extract_metadata, input_text, preprocess_text,
)

# ─────────────────────────────────────────────────────────────────────────────
# RAW CASES
# (receiver_text, category, (amount_min, amount_max), (hour_min, hour_max))
# ─────────────────────────────────────────────────────────────────────────────

RAW_CASES = [

    # ══════════ FOOD (60 cases) ══════════════════════════════════════════════
    # Delivery platforms
    ("VPA Swiggy SWIGGY",                           "Food", (60, 800),   (9,  23)),
    ("VPA swiggy@icici SWIGGY",                     "Food", (80, 700),   (9,  23)),
    ("VPA swiggy.stores@axb SWIGGY STORES",         "Food", (90, 600),   (9,  23)),
    ("VPA swiggypop@ybl SWIGGYPOP",                 "Food", (50, 300),   (11, 15)),
    ("VPA Zomato ZOMATO",                           "Food", (80, 900),   (10, 23)),
    ("VPA zomato@kotak ZOMATO",                     "Food", (100, 800),  (10, 23)),
    ("VPA zomato@hdfcbank ZOMATO",                  "Food", (100, 750),  (10, 23)),
    ("VPA Zomato Gold ZOMATO GOLD",                 "Food", (80, 700),   (10, 23)),
    ("VPA EatSure EATSURE",                         "Food", (100, 600),  (11, 22)),
    ("VPA eatsure@razorpay EATSURE",                "Food", (100, 550),  (11, 22)),
    # QSR chains
    ("VPA Dominos Pizza DOMINOS PIZZA",             "Food", (200, 900),  (11, 23)),
    ("VPA dominospizza@hdfcbank DOMINOSPIZZA",      "Food", (250, 850),  (12, 23)),
    ("VPA dominos@okaxis DOMINOS",                  "Food", (200, 800),  (11, 23)),
    ("VPA KFC India KFC INDIA",                     "Food", (150, 700),  (11, 22)),
    ("VPA kfc@axisbank KFC",                        "Food", (200, 650),  (11, 22)),
    ("VPA sapphirekfconline@ybl SAPPHIREKFCONLINE", "Food", (150, 600),  (11, 22)),
    ("VPA McDonalds MCDONALDS",                     "Food", (100, 600),  (10, 22)),
    ("VPA mcdonalds@paytm MCDONALDS",               "Food", (150, 550),  (10, 22)),
    ("VPA mcdonalds.42276700@hdfcbank MC DONALDS",  "Food", (100, 500),  (10, 22)),
    ("VPA Burger King BURGER KING",                 "Food", (150, 500),  (11, 22)),
    ("VPA burgerking@ybl BURGERKING",               "Food", (180, 480),  (11, 22)),
    ("VPA Pizza Hut PIZZA HUT",                     "Food", (250, 900),  (11, 23)),
    ("VPA pizzahut@icici PIZZAHUT",                 "Food", (280, 850),  (11, 23)),
    ("VPA Subway SUBWAY",                           "Food", (150, 500),  (10, 22)),
    ("VPA subway@axisbank SUBWAY",                  "Food", (180, 480),  (10, 22)),
    # Cafes & beverages
    ("VPA Starbucks STARBUCKS",                     "Food", (200, 700),  (8,  21)),
    ("VPA starbucks@hdfcbank STARBUCKS",            "Food", (250, 650),  (8,  21)),
    ("VPA Chaayos CHAAYOS",                         "Food", (80, 300),   (8,  21)),
    ("VPA chaayos@paytm CHAAYOS",                   "Food", (90, 280),   (8,  21)),
    ("VPA Cafe Coffee Day CAFE COFFEE DAY",         "Food", (100, 400),  (8,  21)),
    ("VPA cafecoffeeday@icici CAFECOFFEEDAY",       "Food", (110, 380),  (8,  20)),
    ("VPA Blue Tokai Coffee BLUE TOKAI COFFEE",     "Food", (150, 500),  (8,  20)),
    ("VPA Third Wave Coffee THIRD WAVE COFFEE",     "Food", (150, 450),  (8,  20)),
    # Cloud kitchens
    ("VPA EatClub EATCLUB",                         "Food", (80, 400),   (11, 15)),
    ("VPA eatclub@ybl EATCLUB",                     "Food", (90, 380),   (11, 15)),
    ("VPA eatclub@ptybl EATCLUB",                   "Food", (90, 350),   (11, 15)),
    ("VPA Faasos FAASOS",                           "Food", (100, 500),  (11, 22)),
    ("VPA faasos@paytm FAASOS",                     "Food", (110, 450),  (11, 22)),
    ("VPA Box8 BOX8",                               "Food", (100, 400),  (11, 15)),
    ("VPA box8@icici BOX8",                         "Food", (100, 380),  (11, 15)),
    ("VPA Behrouz Biryani BEHROUZ BIRYANI",         "Food", (200, 800),  (11, 22)),
    ("VPA behrouzbiryani@icici BEHROUZBIRYANI",     "Food", (220, 750),  (11, 22)),
    ("VPA Oven Story Pizza OVEN STORY PIZZA",       "Food", (200, 700),  (11, 23)),
    ("VPA Rebel Foods REBEL FOODS",                 "Food", (150, 600),  (11, 22)),
    # Dining
    ("VPA Barbeque Nation BARBEQUE NATION",         "Food", (500, 2000), (12, 22)),
    ("VPA Theobroma THEOBROMA",                     "Food", (150, 700),  (9,  21)),
    ("VPA Haldirams HALDIRAMS",                     "Food", (100, 600),  (9,  22)),
    ("VPA haldirams@paytm HALDIRAMS",               "Food", (110, 580),  (9,  22)),
    ("VPA Paradise Biryani PARADISE BIRYANI",       "Food", (150, 600),  (11, 22)),
    ("VPA Hotel Saravana Bhavan HOTEL SARAVANA BHAVAN", "Food", (100, 500),  (7,  22)),
    ("VPA Sagar Ratna SAGAR RATNA",                 "Food", (120, 600),  (8,  22)),
    ("VPA Punjab Grill PUNJAB GRILL",               "Food", (400, 1800), (12, 22)),
    ("VPA Social SOCIAL",                           "Food", (400, 1500), (12, 23)),
    # Generic UPI handles
    ("VPA bakery@upi BAKERY",                       "Food", (50, 300),   (8,  21)),
    ("VPA restaurant@paytm RESTAURANT",             "Food", (100, 1200), (12, 22)),
    ("VPA fooddelivery@ybl FOODDELIVERY",           "Food", (80, 700),   (10, 22)),
    ("VPA dhabawala@upi DHABAWALA",                 "Food", (60, 300),   (7,  22)),
    ("VPA teashop@paytm TEASHOP",                   "Food", (20, 100),   (6,  20)),
    ("VPA canteen@okhdfc CANTEEN",                  "Food", (30, 200),   (8,  18)),
    ("VPA biryanihouse@okaxis BIRYANIHOUSE",        "Food", (150, 600),  (11, 22)),
    ("VPA lunchbox@icici LUNCHBOX",                 "Food", (100, 350),  (11, 15)),
    ("VPA snackbar@ybl SNACKBAR",                   "Food", (50, 250),   (10, 18)),

    # ══════════ GROCERIES (40 cases) ═════════════════════════════════════════
    ("VPA Blinkit BLINKIT",                         "Groceries", (100, 1500), (7,  23)),
    ("VPA blinkit@icici BLINKIT",                   "Groceries", (120, 1200), (7,  23)),
    ("VPA blinkit@axisbank BLINKIT",                "Groceries", (100, 1300), (7,  23)),
    ("VPA Zepto ZEPTO",                             "Groceries", (100, 1200), (6,  23)),
    ("VPA zepto@axisbank ZEPTO",                    "Groceries", (110, 1100), (6,  23)),
    ("VPA BigBasket BIGBASKET",                     "Groceries", (200, 3000), (8,  22)),
    ("VPA bigbasket@hdfcbank BIGBASKET",            "Groceries", (250, 2800), (8,  22)),
    ("VPA bigbasket@icici BIGBASKET",               "Groceries", (220, 2500), (8,  22)),
    ("VPA DMart DMART",                             "Groceries", (300, 5000), (9,  21)),
    ("VPA dmart@paytm DMART",                       "Groceries", (350, 4500), (9,  21)),
    ("VPA JioMart JIOMART",                         "Groceries", (200, 3000), (8,  22)),
    ("VPA jiomart@jio JIOMART",                     "Groceries", (220, 2800), (8,  22)),
    ("VPA Milkbasket MILKBASKET",                   "Groceries", (80, 800),   (4,  10)),
    ("VPA milkbasket@ybl MILKBASKET",               "Groceries", (80, 700),   (4,  10)),
    ("VPA Swiggy Instamart SWIGGY INSTAMART",       "Groceries", (100, 1500), (8,  23)),
    ("VPA swiggygrocery@icici SWIGGYGROCERY",       "Groceries", (120, 1300), (8,  23)),
    ("VPA Dunzo Daily DUNZO DAILY",                 "Groceries", (100, 1000), (8,  22)),
    ("VPA dunzo@razorpay DUNZO",                    "Groceries", (100, 900),  (8,  22)),
    ("VPA Grofers GROFERS",                         "Groceries", (150, 2000), (8,  22)),
    ("VPA Reliance Fresh RELIANCE FRESH",           "Groceries", (200, 2500), (9,  21)),
    ("VPA reliancefresh@jio RELIANCEFRESH",         "Groceries", (220, 2200), (9,  21)),
    ("VPA More Supermarket MORE SUPERMARKET",       "Groceries", (150, 2000), (9,  21)),
    ("VPA Spencer's Retail SPENCER'S RETAIL",       "Groceries", (200, 3000), (9,  21)),
    ("VPA Star Bazaar STAR BAZAAR",                 "Groceries", (200, 2500), (9,  21)),
    ("VPA starbazaar@tata STARBAZAAR",              "Groceries", (200, 2300), (9,  21)),
    ("VPA Nature's Basket NATURE'S BASKET",         "Groceries", (300, 3500), (9,  21)),
    ("VPA Licious LICIOUS",                         "Groceries", (200, 1500), (8,  20)),
    ("VPA licious@razorpay LICIOUS",                "Groceries", (200, 1400), (8,  20)),
    ("VPA FreshToHome FRESHTOHOME",                 "Groceries", (200, 1500), (8,  20)),
    ("VPA Country Delight COUNTRY DELIGHT",         "Groceries", (80, 600),   (5,  10)),
    ("VPA supermarket@paytm SUPERMARKET",           "Groceries", (200, 3000), (8,  21)),
    ("VPA kirana@upi KIRANA",                       "Groceries", (50, 1000),  (7,  21)),
    ("VPA provision@okicici PROVISION",             "Groceries", (100, 1500), (8,  21)),
    ("VPA grocery@ybl GROCERY",                     "Groceries", (100, 2000), (8,  21)),
    ("VPA vegetable@paytm VEGETABLE",               "Groceries", (50, 500),   (6,  12)),
    ("VPA dairy@upi DAIRY",                         "Groceries", (30, 200),   (5,  10)),
    ("VPA fruitsandveggies@upi FRUITSANDVEGGIES",   "Groceries", (100, 800),  (7,  13)),
    ("VPA wholesale market@paytm WHOLESALE MARKET", "Groceries", (500, 5000), (6,  12)),
    ("VPA superstore@okaxis SUPERSTORE",            "Groceries", (300, 3000), (9,  21)),
    ("VPA instantgrocery@ybl INSTANTGROCERY",       "Groceries", (100, 1200), (8,  22)),

    # ══════════ SHOPPING (50 cases) ══════════════════════════════════════════
    ("VPA Amazon AMAZON",                           "Shopping", (200, 50000),  (8,  23)),
    ("VPA amazon@okaxis AMAZON",                    "Shopping", (200, 45000),  (8,  23)),
    ("VPA amazon@hdfcbank AMAZON",                  "Shopping", (200, 40000),  (8,  23)),
    ("VPA Flipkart FLIPKART",                       "Shopping", (200, 50000),  (8,  23)),
    ("VPA flipkart@axisbank FLIPKART",              "Shopping", (250, 45000),  (8,  23)),
    ("VPA Myntra MYNTRA",                           "Shopping", (300, 8000),   (8,  23)),
    ("VPA myntra@kotak MYNTRA",                     "Shopping", (350, 7500),   (8,  23)),
    ("VPA myntra@icicibank MYNTRA",                 "Shopping", (300, 7000),   (8,  23)),
    ("VPA Ajio AJIO",                               "Shopping", (300, 7000),   (8,  23)),
    ("VPA ajio@icici AJIO",                         "Shopping", (350, 6500),   (8,  23)),
    ("VPA Nykaa NYKAA",                             "Shopping", (200, 5000),   (8,  23)),
    ("VPA nykaa@hdfcbank NYKAA",                    "Shopping", (220, 4500),   (8,  23)),
    ("VPA Meesho MEESHO",                           "Shopping", (100, 3000),   (8,  23)),
    ("VPA meesho@ybl MEESHO",                       "Shopping", (120, 2800),   (8,  23)),
    ("VPA Snapdeal SNAPDEAL",                       "Shopping", (100, 10000),  (8,  23)),
    ("VPA Tatacliq TATACLIQ",                       "Shopping", (300, 30000),  (8,  23)),
    ("VPA tatacliq@paytm TATACLIQ",                 "Shopping", (350, 28000),  (8,  23)),
    ("VPA Croma CROMA",                             "Shopping", (500, 80000),  (9,  21)),
    ("VPA croma@icici CROMA",                       "Shopping", (600, 75000),  (9,  21)),
    ("VPA Reliance Digital RELIANCE DIGITAL",       "Shopping", (500, 80000),  (9,  21)),
    ("VPA reliancedigital@jio RELIANCEDIGITAL",     "Shopping", (600, 75000),  (9,  21)),
    ("VPA Vijay Sales VIJAY SALES",                 "Shopping", (500, 60000),  (9,  21)),
    ("VPA IKEA IKEA",                               "Shopping", (500, 25000),  (9,  21)),
    ("VPA Pepperfry PEPPERFRY",                     "Shopping", (500, 30000),  (8,  22)),
    ("VPA pepperfry@kotak PEPPERFRY",               "Shopping", (600, 28000),  (8,  22)),
    ("VPA Lifestyle LIFESTYLE",                     "Shopping", (400, 8000),   (9,  21)),
    ("VPA Westside WESTSIDE",                       "Shopping", (400, 6000),   (9,  21)),
    ("VPA H&M India H&M INDIA",                     "Shopping", (500, 8000),   (9,  21)),
    ("VPA handm@hdfcbank HANDM",                    "Shopping", (500, 7500),   (9,  21)),
    ("VPA Zara India ZARA INDIA",                   "Shopping", (1000, 15000), (9,  21)),
    ("VPA zara@icici ZARA",                         "Shopping", (1000, 14000), (9,  21)),
    ("VPA Decathlon DECATHLON",                     "Shopping", (300, 15000),  (9,  21)),
    ("VPA decathlon@axisbank DECATHLON",            "Shopping", (350, 14000),  (9,  21)),
    ("VPA FirstCry FIRSTCRY",                       "Shopping", (200, 5000),   (8,  22)),
    ("VPA firstcry@paytm FIRSTCRY",                 "Shopping", (220, 4800),   (8,  22)),
    ("VPA LensKart LENSKART",                       "Shopping", (500, 5000),   (9,  21)),
    ("VPA lenskart@razorpay LENSKART",              "Shopping", (500, 4800),   (9,  21)),
    ("VPA Boat Store BOAT STORE",                   "Shopping", (500, 8000),   (8,  22)),
    ("VPA Bewakoof BEWAKOOF",                       "Shopping", (300, 3000),   (8,  23)),
    ("VPA bewakoof@razorpay BEWAKOOF",              "Shopping", (300, 2800),   (8,  23)),
    ("VPA Mamaearth MAMAEARTH",                     "Shopping", (200, 2000),   (8,  22)),
    ("VPA Nykaa Fashion NYKAA FASHION",             "Shopping", (500, 5000),   (8,  22)),
    ("VPA Purplle PURPLLE",                         "Shopping", (200, 3000),   (8,  22)),
    ("VPA Sugar Cosmetics SUGAR COSMETICS",         "Shopping", (300, 3000),   (8,  22)),
    ("VPA Clovia CLOVIA",                           "Shopping", (300, 3000),   (8,  22)),
    ("VPA online shopping@upi ONLINE SHOPPING",     "Shopping", (200, 20000),  (8,  23)),
    ("VPA ecommerce@razorpay ECOMMERCE",            "Shopping", (200, 30000),  (8,  23)),
    ("VPA electronics@okaxis ELECTRONICS",          "Shopping", (500, 50000),  (9,  21)),
    ("VPA fashion@ybl FASHION",                     "Shopping", (300, 8000),   (8,  22)),
    ("VPA mobileaccessory@paytm MOBILEACCESSORY",   "Shopping", (200, 5000),   (9,  22)),

    # ══════════ TRANSPORT (55 cases) ═════════════════════════════════════════
    # Ride-hailing
    ("VPA Uber UBER",                               "Transport", (50, 1500),   (4,  23)),
    ("VPA uber@hdfcbank UBER",                      "Transport", (60, 1300),   (4,  23)),
    ("VPA Ola OLA",                                 "Transport", (50, 1200),   (5,  23)),
    ("VPA ola@icici OLA",                           "Transport", (60, 1100),   (5,  23)),
    ("VPA ola.money@ybl OLA MONEY",                 "Transport", (50, 1000),   (5,  23)),
    ("VPA Rapido RAPIDO",                           "Transport", (30, 300),    (6,  23)),
    ("VPA rapido@ybl RAPIDO",                       "Transport", (30, 280),    (6,  23)),
    ("VPA BluSmart BLUSMART",                       "Transport", (60, 800),    (5,  23)),
    ("VPA blusmart@icici BLUSMART",                 "Transport", (70, 750),    (5,  23)),
    ("VPA Ola Electric OLA ELECTRIC",               "Transport", (50, 500),    (6,  22)),
    ("VPA Yulu YULU",                               "Transport", (20, 200),    (6,  22)),
    # Rail
    ("VPA IRCTC IRCTC",                             "Transport", (200, 5000),  (0,  23)),
    ("VPA irctc@axisbank IRCTC",                    "Transport", (250, 4800),  (0,  23)),
    ("VPA irctc@okicici IRCTC",                     "Transport", (200, 4500),  (0,  23)),
    # Flights
    ("VPA IndiGo INDIGO",                           "Transport", (1500, 15000),(8,  22)),
    ("VPA indigo@hdfcbank INDIGO",                  "Transport", (1600, 14000),(8,  22)),
    ("VPA Air India AIR INDIA",                     "Transport", (2000, 20000),(8,  22)),
    ("VPA SpiceJet SPICEJET",                       "Transport", (1500, 12000),(8,  22)),
    ("VPA Vistara VISTARA",                         "Transport", (2000, 18000),(8,  22)),
    ("VPA AirAsia India AIRASIA INDIA",             "Transport", (1200, 10000),(8,  22)),
    # Travel booking
    ("VPA MakeMyTrip MAKEMYTRIP",                   "Transport", (1000, 30000),(8,  22)),
    ("VPA makemytrip@icici MAKEMYTRIP",             "Transport", (1100, 28000),(8,  22)),
    ("VPA Yatra YATRA",                             "Transport", (800, 20000), (8,  22)),
    ("VPA Cleartrip CLEARTRIP",                     "Transport", (800, 25000), (8,  22)),
    ("VPA cleartrip@axisbank CLEARTRIP",            "Transport", (900, 24000), (8,  22)),
    ("VPA ixigo@paytm IXIGO",                       "Transport", (500, 15000), (8,  22)),
    # Bus
    ("VPA RedBus REDBUS",                           "Transport", (200, 2000),  (6,  22)),
    ("VPA redbus@ybl REDBUS",                       "Transport", (220, 1900),  (6,  22)),
    ("VPA Abhibus ABHIBUS",                         "Transport", (150, 1500),  (6,  22)),
    ("VPA KSRTC KSRTC",                             "Transport", (100, 1000),  (5,  22)),
    # Metro / local
    ("VPA DMRC DMRC",                               "Transport", (10, 100),    (5,  23)),
    ("VPA dmrc@paytm DMRC",                         "Transport", (10, 80),     (5,  23)),
    ("VPA Namma Metro NAMMA METRO",                 "Transport", (10, 60),     (5,  23)),
    ("VPA BMTC BMTC",                               "Transport", (5, 60),      (5,  22)),
    ("VPA metro@okhdfc METRO",                      "Transport", (10, 100),    (5,  23)),
    # Fuel
    ("VPA HPCL HPCL",                               "Transport", (500, 5000),  (6,  22)),
    ("VPA hpcl@okaxis HPCL",                        "Transport", (500, 4800),  (6,  22)),
    ("VPA BPCL BPCL",                               "Transport", (500, 5000),  (6,  22)),
    ("VPA bpcl@ybl BPCL",                           "Transport", (500, 4800),  (6,  22)),
    ("VPA Indian Oil INDIAN OIL",                   "Transport", (500, 5000),  (6,  22)),
    ("VPA indianoil@paytm INDIANOIL",               "Transport", (500, 4500),  (6,  22)),
    ("VPA petrolpump@upi PETROLPUMP",               "Transport", (500, 5000),  (6,  22)),
    # FASTag / misc
    ("VPA FASTag FASTAG",                           "Transport", (100, 500),   (0,  23)),
    ("VPA fastag@hdfcbank FASTAG",                  "Transport", (100, 500),   (0,  23)),
    ("VPA parking@upi PARKING",                     "Transport", (20, 200),    (7,  22)),
    ("VPA autorickshaw@paytm AUTORICKSHAW",         "Transport", (30, 300),    (6,  22)),
    ("VPA cabservice@upi CABSERVICE",               "Transport", (100, 1000),  (5,  23)),
    ("VPA taxifare@okaxis TAXIFARE",                "Transport", (100, 1000),  (5,  23)),
    ("VPA tollplaza@paytm TOLLPLAZA",               "Transport", (50, 300),    (0,  23)),
    ("VPA carservice@ybl CARSERVICE",               "Transport", (200, 3000),  (8,  18)),
    ("VPA flightticket@icici FLIGHTTICKET",         "Transport", (1500, 20000),(8,  22)),
    ("VPA trainticket@sbi TRAINTICKET",             "Transport", (200, 5000),  (0,  23)),
    ("VPA busticket@paytm BUSTICKET",               "Transport", (100, 1500),  (5,  22)),

    # ══════════ INVESTMENT (40 cases) ════════════════════════════════════════
    ("VPA Groww GROWW",                             "Investment", (500,  100000), (8,  18)),
    ("VPA groww@axisbank GROWW",                    "Investment", (500,  90000),  (8,  18)),
    ("VPA groww.brk@validhdfc GROWW BRK",           "Investment", (1000, 80000),  (9,  15)),
    ("VPA Zerodha ZERODHA",                         "Investment", (1000, 200000), (9,  15)),
    ("VPA zerodha@kotak ZERODHA",                   "Investment", (1000, 180000), (9,  15)),
    ("VPA zerodha@icicibank ZERODHA",               "Investment", (1000, 160000), (9,  15)),
    ("VPA Kuvera KUVERA",                           "Investment", (500,  50000),  (8,  18)),
    ("VPA kuvera@icici KUVERA",                     "Investment", (500,  48000),  (8,  18)),
    ("VPA Upstox UPSTOX",                           "Investment", (500,  100000), (9,  15)),
    ("VPA upstox@axisbank UPSTOX",                  "Investment", (800,  90000),  (9,  15)),
    ("VPA Coin by Zerodha COIN BY ZERODHA",         "Investment", (500,  100000), (9,  18)),
    ("VPA coin@zerodha COIN",                       "Investment", (500,  90000),  (9,  18)),
    ("VPA Paytm Money PAYTM MONEY",                 "Investment", (500,  50000),  (8,  18)),
    ("VPA paytmmoney@paytm PAYTMMONEY",             "Investment", (500,  48000),  (8,  18)),
    ("VPA ICICI Direct ICICI DIRECT",               "Investment", (1000, 200000), (9,  15)),
    ("VPA icicidirect@icici ICICIDIRECT",           "Investment", (1000, 180000), (9,  15)),
    ("VPA HDFC Securities HDFC SECURITIES",         "Investment", (1000, 200000), (9,  15)),
    ("VPA hdfcsec@hdfcbank HDFCSEC",                "Investment", (1000, 180000), (9,  15)),
    ("VPA SBI Securities SBI SECURITIES",           "Investment", (500,  100000), (9,  15)),
    ("VPA sbisec@sbi SBISEC",                       "Investment", (500,  90000),  (9,  15)),
    ("VPA Smallcase SMALLCASE",                     "Investment", (1000, 50000),  (9,  18)),
    ("VPA smallcase@hdfc SMALLCASE",                "Investment", (1000, 48000),  (9,  18)),
    ("VPA Angel One ANGEL ONE",                     "Investment", (500,  100000), (9,  15)),
    ("VPA angelone@axisbank ANGELONE",              "Investment", (500,  90000),  (9,  15)),
    ("VPA 5Paisa 5PAISA",                           "Investment", (500,  50000),  (9,  15)),
    ("VPA 5paisa@icici 5PAISA",                     "Investment", (500,  48000),  (9,  15)),
    ("VPA Motilal Oswal MOTILAL OSWAL",             "Investment", (1000, 200000), (9,  15)),
    ("VPA NSE Clearing NSE CLEARING",               "Investment", (1000, 500000), (9,  15)),
    ("VPA nsccl@axisbank NSCCL",                    "Investment", (1000, 500000), (9,  15)),
    ("VPA ICCL ICCL",                               "Investment", (1000, 500000), (9,  15)),
    ("VPA iccl@axisbank ICCL",                      "Investment", (1000, 450000), (9,  15)),
    ("VPA groww.iccl2.brk@validicici GROWW ICCL2 BRK", "Investment", (1000, 300000), (9,  15)),
    ("VPA ETMoney ETMONEY",                         "Investment", (500,  50000),  (8,  18)),
    ("VPA etmoney@icici ETMONEY",                   "Investment", (500,  48000),  (8,  18)),
    ("VPA WazirX WAZIRX",                           "Investment", (1000, 100000), (0,  23)),
    ("VPA wazirx@ybl WAZIRX",                       "Investment", (1000, 90000),  (0,  23)),
    ("VPA CoinSwitch COINSWITCH",                   "Investment", (500,  100000), (0,  23)),
    ("VPA mutualfund@paytm MUTUALFUND",             "Investment", (500,  50000),  (8,  18)),
    ("VPA sip@groww SIP",                           "Investment", (500,  25000),  (8,  18)),
    ("VPA stockbroker@axisbank STOCKBROKER",        "Investment", (1000, 200000), (9,  15)),

    # ══════════ SUBSCRIPTION (45 cases) ══════════════════════════════════════
    ("VPA Netflix NETFLIX",                         "Subscription", (149,  649),  (0,  23)),
    ("VPA netflix@icici NETFLIX",                   "Subscription", (149,  649),  (0,  23)),
    ("VPA netflix@icicibank NETFLIX",               "Subscription", (149,  649),  (0,  23)),
    ("VPA Spotify SPOTIFY",                         "Subscription", (59,   119),  (0,  23)),
    ("VPA spotify@icici SPOTIFY",                   "Subscription", (59,   119),  (0,  23)),
    ("VPA spotify@ybl SPOTIFY",                     "Subscription", (59,   119),  (0,  23)),
    ("VPA Hotstar HOTSTAR",                         "Subscription", (299,  899),  (0,  23)),
    ("VPA hotstar@axisbank HOTSTAR",                "Subscription", (299,  899),  (0,  23)),
    ("VPA Disney Hotstar DISNEY HOTSTAR",           "Subscription", (299,  899),  (0,  23)),
    ("VPA YouTube Premium YOUTUBE PREMIUM",         "Subscription", (129,  189),  (0,  23)),
    ("VPA youtube@google YOUTUBE",                  "Subscription", (129,  189),  (0,  23)),
    ("VPA Amazon Prime AMAZON PRIME",               "Subscription", (179, 1499),  (0,  23)),
    ("VPA primevideo@amazon PRIMEVIDEO",            "Subscription", (179, 1499),  (0,  23)),
    ("VPA Apple Music APPLE MUSIC",                 "Subscription", (99,   149),  (0,  23)),
    ("VPA apple@appleid APPLE",                     "Subscription", (99,   149),  (0,  23)),
    ("VPA appleservices.bdsi@hdfcbank APPLESERVICES BDSI", "Subscription", (99,   299),  (0,  23)),
    ("VPA iCloud ICLOUD",                           "Subscription", (75,   219),  (0,  23)),
    ("VPA Zee5 ZEE5",                               "Subscription", (99,   999),  (0,  23)),
    ("VPA zee5@paytm ZEE5",                         "Subscription", (99,   999),  (0,  23)),
    ("VPA SonyLIV SONYLIV",                         "Subscription", (299,  999),  (0,  23)),
    ("VPA sonyliv@icici SONYLIV",                   "Subscription", (299,  999),  (0,  23)),
    ("VPA JioCinema JIOCINEMA",                     "Subscription", (29,   999),  (0,  23)),
    ("VPA jiocinema@jio JIOCINEMA",                 "Subscription", (29,   999),  (0,  23)),
    ("VPA MX Player Pro MX PLAYER PRO",             "Subscription", (99,   199),  (0,  23)),
    ("VPA ALTBalaji ALTBALAJI",                     "Subscription", (99,   300),  (0,  23)),
    ("VPA Curiosity Stream CURIOSITY STREAM",       "Subscription", (199,  499),  (0,  23)),
    ("VPA Voot Select VOOT SELECT",                 "Subscription", (99,   599),  (0,  23)),
    ("VPA Lionsgate Play LIONSGATE PLAY",           "Subscription", (199,  399),  (0,  23)),
    ("VPA LinkedIn Premium LINKEDIN PREMIUM",       "Subscription", (1600, 3000), (0,  23)),
    ("VPA linkedin@razorpay LINKEDIN",              "Subscription", (1600, 3000), (0,  23)),
    ("VPA Adobe Creative Cloud ADOBE CREATIVE CLOUD", "Subscription", (1675, 4230), (0,  23)),
    ("VPA adobe@razorpay ADOBE",                    "Subscription", (1675, 4230), (0,  23)),
    ("VPA Microsoft 365 MICROSOFT 365",             "Subscription", (489,  999),  (0,  23)),
    ("VPA microsoft@hdfcbank MICROSOFT",            "Subscription", (489,  999),  (0,  23)),
    ("VPA Google One GOOGLE ONE",                   "Subscription", (130,  650),  (0,  23)),
    ("VPA googleone@okicici GOOGLEONE",             "Subscription", (130,  650),  (0,  23)),
    ("VPA Gaana GAANA",                             "Subscription", (99,   399),  (0,  23)),
    ("VPA JioSaavn Pro JIOSAAVN PRO",               "Subscription", (99,   399),  (0,  23)),
    ("VPA Audible AUDIBLE",                         "Subscription", (199,  499),  (0,  23)),
    ("VPA Duolingo Plus DUOLINGO PLUS",             "Subscription", (399,  799),  (0,  23)),
    ("VPA Canva Pro CANVA PRO",                     "Subscription", (499,  999),  (0,  23)),
    ("VPA subscription@razorpay SUBSCRIPTION",      "Subscription", (99,  1999),  (0,  23)),
    ("VPA membership@paytm MEMBERSHIP",             "Subscription", (99,  1999),  (0,  23)),
    ("VPA streaming@icici STREAMING",               "Subscription", (99,   999),  (0,  23)),
    ("VPA monthlyplan@ybl MONTHLYPLAN",             "Subscription", (99,  2999),  (0,  23)),

    # ══════════ HEALTH (45 cases) ═════════════════════════════════════════════
    ("VPA Apollo Pharmacy APOLLO PHARMACY",         "Health", (100, 5000),  (7,  22)),
    ("VPA apollopharmacy@hdfcbank APOLLOPHARMACY",  "Health", (120, 4800),  (7,  22)),
    ("VPA apollo@hdfcbank APOLLO",                  "Health", (100, 4500),  (7,  22)),
    ("VPA MedPlus MEDPLUS",                         "Health", (100, 3000),  (7,  22)),
    ("VPA medplus@icici MEDPLUS",                   "Health", (110, 2800),  (7,  22)),
    ("VPA 1mg 1MG",                                 "Health", (100, 5000),  (8,  22)),
    ("VPA 1mg@icici 1MG",                           "Health", (120, 4800),  (8,  22)),
    ("VPA PharmEasy PHARMEASY",                     "Health", (100, 5000),  (8,  22)),
    ("VPA pharmeasy@axisbank PHARMEASY",            "Health", (120, 4800),  (8,  22)),
    ("VPA NetMeds NETMEDS",                         "Health", (100, 4000),  (8,  22)),
    ("VPA netmeds@hdfcbank NETMEDS",                "Health", (110, 3800),  (8,  22)),
    ("VPA Tata 1mg TATA 1MG",                       "Health", (100, 5000),  (8,  22)),
    ("VPA Apollo Hospitals APOLLO HOSPITALS",       "Health", (500, 50000), (8,  20)),
    ("VPA apollohospitals@icici APOLLOHOSPITALS",   "Health", (500, 48000), (8,  20)),
    ("VPA Fortis Healthcare FORTIS HEALTHCARE",     "Health", (500, 50000), (8,  20)),
    ("VPA fortis@hdfcbank FORTIS",                  "Health", (500, 48000), (8,  20)),
    ("VPA Max Hospital MAX HOSPITAL",               "Health", (500, 50000), (8,  20)),
    ("VPA maxhospital@paytm MAXHOSPITAL",           "Health", (500, 48000), (8,  20)),
    ("VPA Medanta MEDANTA",                         "Health", (500, 60000), (8,  20)),
    ("VPA Manipal Hospitals MANIPAL HOSPITALS",     "Health", (500, 50000), (8,  20)),
    ("VPA Narayana Health NARAYANA HEALTH",         "Health", (500, 40000), (8,  20)),
    ("VPA Dr Lal PathLabs DR LAL PATHLABS",         "Health", (200, 5000),  (7,  18)),
    ("VPA lalpathlab@icici LALPATHLAB",             "Health", (200, 4800),  (7,  18)),
    ("VPA SRL Diagnostics SRL DIAGNOSTICS",         "Health", (200, 4000),  (7,  18)),
    ("VPA Thyrocare THYROCARE",                     "Health", (300, 3000),  (7,  18)),
    ("VPA Metropolis METROPOLIS",                   "Health", (200, 5000),  (7,  18)),
    ("VPA Practo PRACTO",                           "Health", (200, 2000),  (8,  22)),
    ("VPA practo@razorpay PRACTO",                  "Health", (200, 1800),  (8,  22)),
    ("VPA HealthifyMe HEALTHIFYME",                 "Health", (200, 2000),  (6,  22)),
    ("VPA healthifyme@razorpay HEALTHIFYME",        "Health", (200, 1800),  (6,  22)),
    ("VPA Cult Fit CULT FIT",                       "Health", (500, 4000),  (5,  22)),
    ("VPA cultfit@icici CULTFIT",                   "Health", (500, 3800),  (5,  22)),
    ("VPA Gold's Gym GOLD'S GYM",                   "Health", (500, 5000),  (5,  22)),
    ("VPA Fitness First FITNESS FIRST",             "Health", (1000, 8000), (5,  22)),
    ("VPA Anytime Fitness ANYTIME FITNESS",         "Health", (1000, 6000), (5,  22)),
    ("VPA gym@paytm GYM",                           "Health", (500, 4000),  (5,  22)),
    ("VPA pharmacy@upi PHARMACY",                   "Health", (50, 3000),   (7,  22)),
    ("VPA hospital@hdfcbank HOSPITAL",              "Health", (500, 50000), (7,  22)),
    ("VPA clinic@paytm CLINIC",                     "Health", (200, 2000),  (9,  20)),
    ("VPA doctor@upi DOCTOR",                       "Health", (200, 2000),  (9,  20)),
    ("VPA diagnostics@icici DIAGNOSTICS",           "Health", (200, 5000),  (7,  18)),
    ("VPA medicinedelivery@ybl MEDICINEDELIVERY",   "Health", (100, 3000),  (8,  22)),
    ("VPA labtest@okaxis LABTEST",                  "Health", (200, 5000),  (7,  18)),
    ("VPA healthcheckup@paytm HEALTHCHECKUP",       "Health", (500, 5000),  (7,  18)),
    ("VPA paytmqr8lotus@ptys PAYTMQR8LOTUS",        "Health", (200, 3000),  (8,  20)),

    # ══════════ UTILITIES (45 cases) ══════════════════════════════════════════
    ("VPA BESCOM BESCOM",                           "Utilities", (200, 6000),  (8,  20)),
    ("VPA bescom@paytm BESCOM",                     "Utilities", (200, 5500),  (8,  20)),
    ("VPA bescom@hdfcbank BESCOM",                  "Utilities", (200, 5000),  (8,  20)),
    ("VPA MSEDCL MSEDCL",                           "Utilities", (200, 8000),  (8,  20)),
    ("VPA msedcl@icici MSEDCL",                     "Utilities", (200, 7500),  (8,  20)),
    ("VPA BSES Delhi BSES DELHI",                   "Utilities", (300, 6000),  (8,  20)),
    ("VPA bses@axisbank BSES",                      "Utilities", (300, 5500),  (8,  20)),
    ("VPA TATA Power TATA POWER",                   "Utilities", (300, 8000),  (8,  20)),
    ("VPA tatapower@hdfcbank TATAPOWER",            "Utilities", (300, 7500),  (8,  20)),
    ("VPA Adani Electricity ADANI ELECTRICITY",     "Utilities", (200, 6000),  (8,  20)),
    ("VPA adanielectricity@icici ADANIELECTRICITY", "Utilities", (200, 5500),  (8,  20)),
    ("VPA TNEB TNEB",                               "Utilities", (200, 5000),  (8,  20)),
    ("VPA tneb@paytm TNEB",                         "Utilities", (200, 4800),  (8,  20)),
    ("VPA Airtel AIRTEL",                           "Utilities", (149, 3000),  (8,  22)),
    ("VPA airtel@axisbank AIRTEL",                  "Utilities", (149, 2800),  (8,  22)),
    ("VPA Jio JIO",                                 "Utilities", (149, 2999),  (8,  22)),
    ("VPA jio@paytm JIO",                           "Utilities", (149, 2999),  (8,  22)),
    ("VPA jio@rjio JIO",                            "Utilities", (149, 2999),  (8,  22)),
    ("VPA Vodafone Vi VODAFONE VI",                 "Utilities", (149, 2499),  (8,  22)),
    ("VPA vodafone@hdfcbank VODAFONE",              "Utilities", (149, 2200),  (8,  22)),
    ("VPA BSNL BSNL",                               "Utilities", (100, 1500),  (8,  20)),
    ("VPA bsnl@icici BSNL",                         "Utilities", (100, 1300),  (8,  20)),
    ("VPA Airtel Broadband AIRTEL BROADBAND",       "Utilities", (400, 1500),  (8,  20)),
    ("VPA airtelbroadband@axisbank AIRTELBROADBAND","Utilities", (400, 1400),  (8,  20)),
    ("VPA JioFiber JIOFIBER",                       "Utilities", (399, 1999),  (8,  20)),
    ("VPA jiofiber@jio JIOFIBER",                   "Utilities", (399, 1999),  (8,  20)),
    ("VPA ACT Fibernet ACT FIBERNET",               "Utilities", (400, 1500),  (8,  20)),
    ("VPA actfibernet@icici ACTFIBERNET",           "Utilities", (400, 1400),  (8,  20)),
    ("VPA Hathway HATHWAY",                         "Utilities", (300, 1200),  (8,  20)),
    ("VPA Tata Sky TATA SKY",                       "Utilities", (200, 1000),  (8,  20)),
    ("VPA tatasky@paytm TATASKY",                   "Utilities", (200, 1000),  (8,  20)),
    ("VPA Dish TV DISH TV",                         "Utilities", (200, 800),   (8,  20)),
    ("VPA Indane Gas INDANE GAS",                   "Utilities", (700, 1100),  (8,  18)),
    ("VPA indane@paytm INDANE",                     "Utilities", (700, 1000),  (8,  18)),
    ("VPA HP Gas HP GAS",                           "Utilities", (700, 1100),  (8,  18)),
    ("VPA hpgas@icici HPGAS",                       "Utilities", (700, 1000),  (8,  18)),
    ("VPA Bharat Gas BHARAT GAS",                   "Utilities", (700, 1100),  (8,  18)),
    ("VPA DJB Water DJB WATER",                     "Utilities", (100, 2000),  (8,  18)),
    ("VPA djb@paytm DJB",                           "Utilities", (100, 1800),  (8,  18)),
    ("VPA electricity bill@upi ELECTRICITY BILL",   "Utilities", (200, 6000),  (8,  20)),
    ("VPA mobile recharge@paytm MOBILE RECHARGE",   "Utilities", (149, 2999),  (8,  22)),
    ("VPA broadband@icici BROADBAND",               "Utilities", (300, 1500),  (8,  20)),
    ("VPA dth@axisbank DTH",                        "Utilities", (200, 800),   (8,  20)),
    ("VPA waterbill@upi WATERBILL",                 "Utilities", (100, 1500),  (8,  18)),
    ("VPA gasbill@paytm GASBILL",                   "Utilities", (700, 1100),  (8,  18)),

    # ══════════ TRANSFER (50 cases) ═══════════════════════════════════════════
    # Named individuals
    ("VPA Rahul Sharma RAHUL SHARMA",               "Transfer", (100, 10000),  (8,  22)),
    ("VPA rahulsharma@okicici RAHULSHARMA",         "Transfer", (100, 9000),   (8,  22)),
    ("VPA rahul.sharma@okicici RAHUL SHARMA",       "Transfer", (100, 8500),   (8,  22)),
    ("VPA Priya Singh PRIYA SINGH",                 "Transfer", (200, 15000),  (8,  22)),
    ("VPA priyasingh@ybl PRIYASINGH",               "Transfer", (200, 13000),  (8,  22)),
    ("VPA Amit Kumar AMIT KUMAR",                   "Transfer", (100, 20000),  (8,  22)),
    ("VPA amitkumar@paytm AMITKUMAR",               "Transfer", (100, 18000),  (8,  22)),
    ("VPA Neha Gupta NEHA GUPTA",                   "Transfer", (200, 10000),  (8,  22)),
    ("VPA nehagupta@okaxis NEHAGUPTA",              "Transfer", (200, 9000),   (8,  22)),
    ("VPA Vikram Patel VIKRAM PATEL",               "Transfer", (500, 50000),  (8,  22)),
    ("VPA vikrampatel@icici VIKRAMPATEL",           "Transfer", (500, 45000),  (8,  22)),
    ("VPA Anjali Mehta ANJALI MEHTA",               "Transfer", (100, 5000),   (8,  22)),
    ("VPA anjali.mehta@ybl ANJALI MEHTA",           "Transfer", (100, 4500),   (8,  22)),
    ("VPA Rohan Verma ROHAN VERMA",                 "Transfer", (500, 25000),  (8,  22)),
    ("VPA rohan.verma@okhdfcbank ROHAN VERMA",      "Transfer", (500, 22000),  (8,  22)),
    ("VPA Sunita Rao SUNITA RAO",                   "Transfer", (200, 10000),  (8,  22)),
    ("VPA sunita.rao@icici SUNITA RAO",             "Transfer", (200, 9000),   (8,  22)),
    ("VPA Deepak Joshi DEEPAK JOSHI",               "Transfer", (100, 30000),  (8,  22)),
    ("VPA deepak.j@ybl DEEPAK J",                   "Transfer", (100, 28000),  (8,  22)),
    ("VPA Kavya Nair KAVYA NAIR",                   "Transfer", (100, 8000),   (8,  22)),
    ("VPA kavya.nair@okicici KAVYA NAIR",           "Transfer", (100, 7000),   (8,  22)),
    ("VPA Arjun Reddy ARJUN REDDY",                 "Transfer", (200, 15000),  (8,  22)),
    ("VPA arjunreddy@axisbank ARJUNREDDY",          "Transfer", (200, 13000),  (8,  22)),
    ("VPA Pooja Iyer POOJA IYER",                   "Transfer", (100, 5000),   (8,  22)),
    ("VPA pooja.iyer@paytm POOJA IYER",             "Transfer", (100, 4500),   (8,  22)),
    ("VPA Karan Malhotra KARAN MALHOTRA",           "Transfer", (500, 30000),  (8,  22)),
    ("VPA karan.m@ybl KARAN M",                     "Transfer", (500, 28000),  (8,  22)),
    ("VPA Meera Pillai MEERA PILLAI",               "Transfer", (200, 10000),  (8,  22)),
    ("VPA meera.pillai@ybl MEERA PILLAI",           "Transfer", (200, 9000),   (8,  22)),
    ("VPA Suresh Babu SURESH BABU",                 "Transfer", (100, 20000),  (8,  22)),
    ("VPA suresh.babu@okaxis SURESH BABU",          "Transfer", (100, 18000),  (8,  22)),
    ("VPA Ravi Chandrasekhar RAVI CHANDRASEKHAR",   "Transfer", (500, 40000),  (8,  22)),
    ("VPA Lalitha Krishna LALITHA KRISHNA",         "Transfer", (200, 8000),   (8,  22)),
    # Real-looking UPI handles
    ("VPA ANANYA RAVISHANKAR IYER ANANYA RAVISHANKAR IYER", "Transfer", (200, 10000),  (8,  22)),
    ("VPA ananyaiyer07@okhdfcbank ANANYAIYER07",  "Transfer", (200, 9000),   (8,  22)),
    ("VPA TARUN KHANNA TARUN KHANNA",           "Transfer", (100, 5000),   (8,  22)),
    ("VPA 9000000001@pthdfc 9000000001",            "Transfer", (100, 5000),   (8,  22)),
    ("VPA Mr NIKHIL RAJU PAWAR MR NIKHIL RAJU PAWAR", "Transfer", (200, 8000),   (8,  22)),
    ("VPA nk2001452xyz@okicici NK2001452XYZ",       "Transfer", (200, 7000),   (8,  22)),
    ("VPA MAHESH S PRABHU MAHESH S PRABHU",     "Transfer", (100, 5000),   (8,  22)),
    ("VPA paytmqr4tulip@ptys PAYTMQR4TULIP",        "Transfer", (100, 5000),   (8,  22)),
    ("VPA bharatpe.3k7m2p9q4r182640@fbpe BHARATPE 3K7M2P9Q4R182640", "Transfer", (100, 10000),  (8,  22)),
    # Generic P2P
    ("VPA sent to friend@upi SENT TO FRIEND",       "Transfer", (100, 20000),  (8,  22)),
    ("VPA payment to@paytm PAYMENT TO",             "Transfer", (100, 30000),  (8,  22)),
    ("VPA friendsplit@upi FRIENDSPLIT",             "Transfer", (100, 5000),   (8,  22)),
    # Recurring P2P
    ("VPA rent@upi RENT",                           "Transfer", (5000, 30000), (1,  5)),
    ("VPA roommate@ybl ROOMMATE",                   "Transfer", (1000, 15000), (8,  22)),
    ("VPA housemaid@paytm HOUSEMAID",               "Transfer", (1000, 5000),  (7,  10)),
    ("VPA cook@upi COOK",                           "Transfer", (1000, 4000),  (7,  10)),
    ("VPA electrician@paytm ELECTRICIAN",           "Transfer", (200, 3000),   (9,  18)),
    ("VPA plumber@upi PLUMBER",                     "Transfer", (200, 3000),   (9,  18)),

    # ══════════ ENTERTAINMENT (40 cases) ══════════════════════════════════════
    ("VPA BookMyShow BOOKMYSHOW",                   "Entertainment", (100, 3000),  (10, 22)),
    ("VPA bookmyshow@icici BOOKMYSHOW",             "Entertainment", (120, 2800),  (10, 22)),
    ("VPA bookmyshow@hdfcbank BOOKMYSHOW",          "Entertainment", (120, 2500),  (10, 22)),
    ("VPA PVR Cinemas PVR CINEMAS",                 "Entertainment", (150, 600),   (10, 22)),
    ("VPA pvr@hdfcbank PVR",                        "Entertainment", (180, 580),   (10, 22)),
    ("VPA INOX INOX",                               "Entertainment", (150, 600),   (10, 22)),
    ("VPA inox@axisbank INOX",                      "Entertainment", (170, 580),   (10, 22)),
    ("VPA Cinepolis CINEPOLIS",                     "Entertainment", (150, 500),   (10, 22)),
    ("VPA cinepolis@paytm CINEPOLIS",               "Entertainment", (160, 480),   (10, 22)),
    ("VPA Carnival Cinemas CARNIVAL CINEMAS",       "Entertainment", (120, 450),   (10, 22)),
    ("VPA Dream11 DREAM11",                         "Entertainment", (100, 5000),  (0,  23)),
    ("VPA dream11@ybl DREAM11",                     "Entertainment", (100, 4500),  (0,  23)),
    ("VPA MPL Sports MPL SPORTS",                   "Entertainment", (50, 2000),   (0,  23)),
    ("VPA mpl@icici MPL",                           "Entertainment", (50, 1800),   (0,  23)),
    ("VPA Games24x7 GAMES24X7",                     "Entertainment", (50, 2000),   (0,  23)),
    ("VPA games24x7@razorpay GAMES24X7",            "Entertainment", (50, 1800),   (0,  23)),
    ("VPA WinZo WINZO",                             "Entertainment", (50, 1000),   (0,  23)),
    ("VPA winzo@paytm WINZO",                       "Entertainment", (50, 900),    (0,  23)),
    ("VPA Gameskraft GAMESKRAFT",                   "Entertainment", (50, 2000),   (0,  23)),
    ("VPA Paytm First Games PAYTM FIRST GAMES",     "Entertainment", (50, 1000),   (0,  23)),
    ("VPA Zupee ZUPEE",                             "Entertainment", (50, 500),    (0,  23)),
    ("VPA Rooter ROOTER",                           "Entertainment", (50, 300),    (0,  23)),
    ("VPA Loco LOCO",                               "Entertainment", (50, 500),    (0,  23)),
    ("VPA Steam STEAM",                             "Entertainment", (100, 5000),  (0,  23)),
    ("VPA PlayStation Store PLAYSTATION STORE",     "Entertainment", (200, 5000),  (0,  23)),
    ("VPA Xbox Game Pass XBOX GAME PASS",           "Entertainment", (500, 1500),  (0,  23)),
    ("VPA Nintendo eShop NINTENDO ESHOP",           "Entertainment", (200, 3000),  (0,  23)),
    ("VPA gaming@paytm GAMING",                     "Entertainment", (50, 2000),   (0,  23)),
    ("VPA onlinegame@razorpay ONLINEGAME",          "Entertainment", (50, 2000),   (0,  23)),
    ("VPA events@bookmyshow EVENTS",                "Entertainment", (200, 5000),  (10, 22)),
    ("VPA concert@razorpay CONCERT",                "Entertainment", (500, 5000),  (10, 22)),
    ("VPA sportsticket@icici SPORTSTICKET",         "Entertainment", (200, 3000),  (8,  22)),
    ("VPA IPL tickets@paytm IPL TICKETS",           "Entertainment", (500, 5000),  (8,  22)),
    ("VPA movieticket@upi MOVIETICKET",             "Entertainment", (150, 800),   (10, 22)),
    ("VPA gamestore@ybl GAMESTORE",                 "Entertainment", (100, 3000),  (0,  23)),
    ("VPA fantasysports@okaxis FANTASYSPORTS",      "Entertainment", (100, 3000),  (0,  23)),
    ("VPA esports@upi ESPORTS",                     "Entertainment", (100, 2000),  (0,  23)),
    ("VPA arcade@paytm ARCADE",                     "Entertainment", (50, 500),    (10, 22)),
    ("VPA Paytm Insider PAYTM INSIDER",             "Entertainment", (200, 5000),  (10, 22)),
    ("VPA entertainmenthub@paytm ENTERTAINMENTHUB", "Entertainment", (100, 3000),  (0,  23)),

    # ══════════ EDUCATION (40 cases) ══════════════════════════════════════════
    ("VPA BYJU'S BYJU'S",                           "Education", (1000, 80000),  (8,  20)),
    ("VPA byjus@razorpay BYJUS",                    "Education", (1000, 75000),  (8,  20)),
    ("VPA byjus@hdfcbank BYJUS",                    "Education", (1000, 70000),  (8,  20)),
    ("VPA Unacademy UNACADEMY",                     "Education", (500,  30000),  (8,  20)),
    ("VPA unacademy@icici UNACADEMY",               "Education", (500,  28000),  (8,  20)),
    ("VPA unacademy@okaxis UNACADEMY",              "Education", (500,  28000),  (8,  20)),
    ("VPA Vedantu VEDANTU",                         "Education", (500,  20000),  (8,  20)),
    ("VPA vedantu@axisbank VEDANTU",                "Education", (500,  18000),  (8,  20)),
    ("VPA WhiteHat Jr WHITEHAT JR",                 "Education", (500,  20000),  (8,  20)),
    ("VPA whitehatjr@icici WHITEHATJR",             "Education", (500,  18000),  (8,  20)),
    ("VPA Coursera COURSERA",                       "Education", (1000, 50000),  (8,  22)),
    ("VPA coursera@razorpay COURSERA",              "Education", (1000, 45000),  (8,  22)),
    ("VPA Udemy UDEMY",                             "Education", (300,  5000),   (8,  22)),
    ("VPA udemy@paytm UDEMY",                       "Education", (300,  4800),   (8,  22)),
    ("VPA Skillshare SKILLSHARE",                   "Education", (500,  3000),   (8,  22)),
    ("VPA upGrad UPGRAD",                           "Education", (5000, 100000), (8,  20)),
    ("VPA upgrad@icici UPGRAD",                     "Education", (5000, 90000),  (8,  20)),
    ("VPA Great Learning GREAT LEARNING",           "Education", (5000, 80000),  (8,  20)),
    ("VPA Simplilearn SIMPLILEARN",                 "Education", (1000, 50000),  (8,  20)),
    ("VPA simplilearn@razorpay SIMPLILEARN",        "Education", (1000, 48000),  (8,  20)),
    ("VPA Physics Wallah PHYSICS WALLAH",           "Education", (500,  10000),  (8,  20)),
    ("VPA pw@razorpay PW",                          "Education", (500,  9000),   (8,  20)),
    ("VPA IIT Madras IIT MADRAS",                   "Education", (1000, 100000), (8,  18)),
    ("VPA iitmadras@sbi IITMADRAS",                 "Education", (1000, 90000),  (8,  18)),
    ("VPA GATE coaching@upi GATE COACHING",         "Education", (1000, 20000),  (8,  20)),
    ("VPA NEET coaching@paytm NEET COACHING",       "Education", (1000, 25000),  (8,  20)),
    ("VPA college fees@axisbank COLLEGE FEES",      "Education", (5000, 200000), (8,  18)),
    ("VPA school fees@hdfcbank SCHOOL FEES",        "Education", (1000, 30000),  (8,  18)),
    ("VPA tuition@paytm TUITION",                   "Education", (500,  10000),  (8,  20)),
    ("VPA coaching@icici COACHING",                 "Education", (1000, 20000),  (8,  20)),
    ("VPA examination@upi EXAMINATION",             "Education", (500,  5000),   (8,  18)),
    ("VPA library@paytm LIBRARY",                   "Education", (100,  2000),   (8,  18)),
    ("VPA onlinecourse@razorpay ONLINECOURSE",      "Education", (500,  50000),  (8,  22)),
    ("VPA elearning@ybl ELEARNING",                 "Education", (300,  30000),  (8,  22)),
    ("VPA entranceexam@upi ENTRANCEEXAM",           "Education", (500,  3000),   (8,  18)),
    ("VPA schooladmission@hdfcbank SCHOOLADMISSION","Education", (5000, 50000),  (8,  18)),
    ("VPA semesterfees@icici SEMESTERFEES",         "Education", (5000, 100000), (8,  18)),
    ("VPA educationloan@sbi EDUCATIONLOAN",         "Education", (5000, 200000), (8,  18)),
    ("VPA Khan Academy KHAN ACADEMY",               "Education", (0,    500),    (8,  22)),
    ("VPA IIM Bangalore IIM BANGALORE",             "Education", (5000, 200000), (8,  18)),

    # ══════════ PAYMENTS (50 cases) ═══════════════════════════════════════════
    # Credit cards
    ("VPA HDFC Credit Card HDFC CREDIT CARD",       "Payments", (1000,  200000), (8,  20)),
    ("VPA hdfcbank@hdfcbank HDFCBANK",              "Payments", (1000,  180000), (8,  20)),
    ("VPA ICICI Credit Card ICICI CREDIT CARD",     "Payments", (1000,  200000), (8,  20)),
    ("VPA icicipay@icici ICICIPAY",                 "Payments", (1000,  180000), (8,  20)),
    ("VPA SBI Card SBI CARD",                       "Payments", (1000,  100000), (8,  20)),
    ("VPA sbicard@sbi SBICARD",                     "Payments", (1000,  90000),  (8,  20)),
    ("VPA Axis Bank Credit Card AXIS BANK CREDIT CARD", "Payments", (1000,  150000), (8,  20)),
    ("VPA axisPay@axisbank AXISPAY",                "Payments", (1000,  130000), (8,  20)),
    ("VPA CRED CRED",                               "Payments", (1000,  200000), (8,  22)),
    ("VPA cred@axisbank CRED",                      "Payments", (1000,  180000), (8,  22)),
    ("VPA Kotak Mahindra Bank KOTAK MAHINDRA BANK", "Payments", (1000,  100000), (8,  20)),
    ("VPA kotak@kotak KOTAK",                       "Payments", (1000,  90000),  (8,  20)),
    ("VPA AmEx India AMEX INDIA",                   "Payments", (2000,  200000), (8,  20)),
    ("VPA RBL Bank Card RBL BANK CARD",             "Payments", (1000,  100000), (8,  20)),
    # Insurance
    ("VPA LIC Premium LIC PREMIUM",                 "Payments", (1000,  50000),  (8,  18)),
    ("VPA lic@paytm LIC",                           "Payments", (1000,  45000),  (8,  18)),
    ("VPA HDFC Life Insurance HDFC LIFE INSURANCE", "Payments", (1000,  50000),  (8,  18)),
    ("VPA hdfclife@hdfcbank HDFCLIFE",              "Payments", (1000,  45000),  (8,  18)),
    ("VPA Max Life Insurance MAX LIFE INSURANCE",   "Payments", (1000,  30000),  (8,  18)),
    ("VPA ICICI Lombard ICICI LOMBARD",             "Payments", (2000,  30000),  (8,  18)),
    ("VPA icicilombard@icici ICICILOMBARD",         "Payments", (2000,  28000),  (8,  18)),
    ("VPA New India Assurance NEW INDIA ASSURANCE", "Payments", (1000,  20000),  (8,  18)),
    ("VPA Star Health Insurance STAR HEALTH INSURANCE", "Payments", (2000,  30000),  (8,  18)),
    ("VPA Niva Bupa NIVA BUPA",                     "Payments", (2000,  30000),  (8,  18)),
    # EMI / Loans
    ("VPA Bajaj Finserv EMI BAJAJ FINSERV EMI",     "Payments", (500,   50000),  (8,  20)),
    ("VPA bajajfinserv@icici BAJAJFINSERV",         "Payments", (500,   48000),  (8,  20)),
    ("VPA Home Loan EMI HOME LOAN EMI",             "Payments", (10000, 80000),  (1,  5)),
    ("VPA homeloan@hdfcbank HOMELOAN",              "Payments", (10000, 75000),  (1,  5)),
    ("VPA Car Loan EMI CAR LOAN EMI",               "Payments", (5000,  30000),  (1,  5)),
    ("VPA carloan@icici CARLOAN",                   "Payments", (5000,  28000),  (1,  5)),
    ("VPA Personal Loan EMI PERSONAL LOAN EMI",     "Payments", (2000,  30000),  (1,  5)),
    ("VPA personalloan@axisbank PERSONALLOAN",      "Payments", (2000,  28000),  (1,  5)),
    ("VPA NBFC@paytm NBFC",                         "Payments", (1000,  30000),  (8,  18)),
    # Tax & Govt
    ("VPA income tax@nsdl INCOME TAX",              "Payments", (1000,  500000), (8,  18)),
    ("VPA GST payment@upi GST PAYMENT",             "Payments", (1000,  100000), (8,  18)),
    ("VPA challan@paytm CHALLAN",                   "Payments", (100,   5000),   (8,  18)),
    ("VPA trafficfine@upi TRAFFICFINE",             "Payments", (500,   5000),   (8,  18)),
    ("VPA municipaltax@paytm MUNICIPALTAX",         "Payments", (1000,  20000),  (8,  18)),
    # Wallets & misc
    ("VPA Paytm Wallet PAYTM WALLET",               "Payments", (100,   10000),  (8,  22)),
    ("VPA paytm@paytm PAYTM",                       "Payments", (100,   10000),  (8,  22)),
    ("VPA PhonePe Wallet PHONEPE WALLET",           "Payments", (100,   10000),  (8,  22)),
    ("VPA creditcard@razorpay CREDITCARD",          "Payments", (1000,  200000), (8,  20)),
    ("VPA insurance@hdfcbank INSURANCE",            "Payments", (1000,  50000),  (8,  18)),
    ("VPA emi@axisbank EMI",                        "Payments", (500,   50000),  (1,  10)),
    ("VPA billpayment@upi BILLPAYMENT",             "Payments", (200,   50000),  (8,  20)),
    ("VPA loanrepayment@icici LOANREPAYMENT",       "Payments", (2000,  80000),  (1,  10)),
    ("VPA cardpayment@ybl CARDPAYMENT",             "Payments", (1000,  200000), (8,  20)),
    ("VPA premiumdue@paytm PREMIUMDUE",             "Payments", (1000,  50000),  (8,  18)),
    ("VPA emi payment HDFC EMI PAYMENT HDFC",       "Payments", (2000,  50000),  (1,  10)),

    # ══════════ OTHER (20 cases) ══════════════════════════════════════════════
    ("VPA unknown@upi UNKNOWN",                     "Other", (10,  50000), (0,  23)),
    ("VPA miscellaneous@paytm MISCELLANEOUS",       "Other", (10,  50000), (0,  23)),
    ("VPA general@icici GENERAL",                   "Other", (10,  50000), (0,  23)),
    ("VPA qr payment@upi QR PAYMENT",               "Other", (10,  10000), (8,  22)),
    ("VPA test@upi TEST",                           "Other", (1,   100),   (0,  23)),
    ("VPA payment@upi PAYMENT",                     "Other", (10,  50000), (0,  23)),
    ("VPA merchant@razorpay MERCHANT",              "Other", (10,  50000), (0,  23)),
    ("VPA random@ybl RANDOM",                       "Other", (10,  50000), (0,  23)),
    ("VPA others@icici OTHERS",                     "Other", (10,  50000), (0,  23)),
    ("VPA xyzabc123@okaxis XYZABC123",              "Other", (10,  50000), (0,  23)),
    ("VPA 9999999999@paytm 9999999999",             "Other", (10,  20000), (0,  23)),
    ("VPA randommerchant@ybl RANDOMMERCHANT",       "Other", (10,  50000), (0,  23)),
    ("VPA unnamed@okaxis UNNAMED",                  "Other", (10,  30000), (0,  23)),
    ("VPA noname@upi NONAME",                       "Other", (10,  20000), (0,  23)),
    ("VPA abc123@paytm ABC123",                     "Other", (10,  10000), (0,  23)),
    ("VPA temp@razorpay TEMP",                      "Other", (10,  10000), (0,  23)),
    ("VPA vpaxyz@ybl VPAXYZ",                       "Other", (10,  50000), (0,  23)),
    ("VPA justpay@upi JUSTPAY",                     "Other", (10,  50000), (0,  23)),
    ("VPA zzz@okicici ZZZ",                         "Other", (10,  10000), (0,  23)),
    ("VPA VPA unknown@upi VPA UNKNOWN",             "Other", (10,  50000), (0,  23)),

    # ══════════ REAL WORLD SAMPLES (From User Data) ══════════════════════════
    # Food & Groceries
    ("VPA paytmqr2njw85@ptys MAHARASHTRIYA MAMLEDAR MISAL RESTAURANT", "Food", (100, 800), (8, 22)),
    ("VPA q637205914@ybl SPICE EXPRESS",                           "Food", (100, 600), (11, 23)),
    ("VPA gpay-70418293355@okbizaxis Food Xpress",                 "Food", (100, 500), (11, 23)),
    ("VPA paytm.k42v9be@pty NBC Vikhroli W",                       "Food", (100, 500), (10, 22)),
    ("VPA paytmqr3azbn8@ptys Baba Mutton shop",                    "Groceries", (200, 1500), (8, 20)),
    
    # Entertainment & Health
    ("VPA district.movies@hdfcbank ORBGEN TECHNOLOGIES PRIVATE LIMITED DISTRICT MOVIE UPI", "Entertainment", (150, 1000), (10, 23)),
    ("VPA paytmqr8lotus@ptys GLOBAL MEDICAL AND G",                "Health", (200, 5000), (8, 22)),
    ("VPA paytmqrz8mm2kwnrc@paytm GLOBAL MEDICAL AND G",           "Health", (200, 5000), (8, 22)),
    
    # Investment & Subscriptions
    ("VPA groww.rzpiccl.brk@validhdfc Next Billion Technology Private Limited RZP", "Investment", (500, 50000), (9, 15)),
    ("VPA groww.brk@validhdfc GROWW INVEST TECH PVT LTD",          "Investment", (500, 50000), (9, 15)),
    ("VPA groww.iccl2.brk@validicici Mutual Funds ICCL",           "Investment", (500, 50000), (9, 15)),
    ("VPA appleservices.bdsi@hdfcbank APPLE MEDIA SERVICES",       "Subscription", (99, 999), (0, 23)),
    
    # Real-World Personal Transfers
    ("VPA datapointmeera@oksbi MEERA HITESH SHAH",                 "Transfer", (100, 15000), (8, 22)),
    ("VPA dianafonseca@okhdfcbank DIANA BRENDA FONSECA",       "Transfer", (100, 15000), (8, 22)),
    ("VPA kadamrohan5@okhdfcbank ROHAN GAJANAN KADAM",        "Transfer", (100, 15000), (8, 22)),
    ("VPA pratikdesai0413-1@okhdfcbank PRATIK ANILKUMAR DESAI", "Transfer", (100, 15000), (8, 22)),
    ("VPA vinodpatelkar219@oksbi IRFAN GAFUR SHAIKH",           "Transfer", (100, 15000), (8, 22)),
    ("VPA paytm.t7ophd2@pty SAGAR DILIP RANE",                     "Transfer", (100, 15000), (8, 22)),
    ("VPA paytmqr21krsn@ptys Mohan Singh Rathore",                 "Transfer", (100, 15000), (8, 22)),
    ("VPA paytmqr9de4t1@ptys Suresh Ramesh Bhandari",               "Transfer", (100, 15000), (8, 22)),
    ("VPA q504817360@ybl RASHID HABIBULLAH KHAN",                "Transfer", (100, 15000), (8, 22)),

    # ──── Additional real-world patterns from failing production data ──────────
    # Transport — Indian Railways UTS (local train unreserved tickets)
    ("VPA bdpg2.iruts@sbi Indian Railways UTS",                    "Transport", (4, 100), (6, 22)),
    ("Indian Railways UTS",                                        "Transport", (4, 100), (6, 22)),
    ("Indian Railways",                                            "Transport", (50, 5000), (6, 22)),

    # Utilities — Vi (Vodafone Idea) recharge patterns
    ("VPA vilpremum@ptybl",                                        "Utilities", (149, 2999), (8, 22)),
    ("vilpremum",                                                  "Utilities", (149, 2999), (8, 22)),
    ("Vi premium",                                                 "Utilities", (149, 2999), (8, 22)),
    ("VODAFONE IDEA LIM",                                          "Utilities", (50, 3000), (8, 22)),
    ("VPA viprev@habc VODAFONE IDEA LIM",                          "Utilities", (149, 2999), (8, 22)),
    ("Vodafone Idea",                                              "Utilities", (50, 3000), (8, 22)),
    ("Vi",                                                         "Utilities", (50, 2999), (8, 22)),

    # Food — NBC Vikhroli (local restaurant in Vikhroli, Mumbai)
    ("NBC Vikhroli",                                               "Food", (80, 600), (10, 22)),
    ("NBC Vikhroli W",                                             "Food", (80, 600), (10, 22)),
    ("Food Xpress",                                                "Food", (50, 400), (10, 22)),
    ("SPICE EXPRESS",                                              "Food", (80, 600), (11, 22)),
    ("MAHARASHTRIYA MAMLEDAR MISAL RESTAURANT",                    "Food", (80, 500), (8, 22)),
    ("Baba Mutton shop",                                           "Groceries", (200, 1500), (8, 20)),
    ("GLOBAL MEDICAL",                                             "Health", (100, 5000), (8, 22)),

    # Transfer — person names observed in failing data
    ("MAHESH S PRABHU",                                          "Transfer", (50, 20000), (8, 22)),
    ("VPA paytmqr4tulip@ptys RAVINDRA S",                          "Transfer", (50, 20000), (8, 22)),
    ("PRATIK ANILKUMAR DESAI",                                   "Transfer", (50, 20000), (8, 22)),
    ("AARTI KUNAL MEHTA",                                       "Transfer", (50, 10000), (8, 22)),
    ("VPA aartimehta3007@okidfcbank AARTI KUNAL MEHTA",      "Transfer", (50, 10000), (8, 22)),
    ("Mr NIKHIL RAJU PAWAR",                                       "Transfer", (50, 10000), (8, 22)),
    ("NIKHIL RAJU PAWAR",                                          "Transfer", (50, 10000), (8, 22)),
    ("VIJAYKUMAR RAMCHANDRA JOSH",                              "Transfer", (50, 20000), (8, 22)),


    # ---- 4.7/4.9 coverage: UPI handle families and edge cases --------------
    # Every handle suffix below appears on merchants from several different
    # categories on purpose: the handle must not be evidence, only the name.

    ("VPA swiggy@ybl SWIGGY", "Food", (80, 700), (9, 23)),
    ("VPA bigbasket@ybl BIGBASKET", "Groceries", (300, 3000), (8, 21)),
    ("VPA zomato@ibl ZOMATO", "Food", (80, 800), (10, 23)),
    ("VPA netflix@ibl NETFLIX INDIA", "Subscription", (149, 999), (0, 23)),
    ("VPA blinkit@axl BLINKIT", "Groceries", (150, 2000), (7, 23)),
    ("VPA unacademy@axl UNACADEMY", "Education", (500, 30000), (8, 22)),
    ("VPA bigbasket@yesg BIGBASKET", "Groceries", (300, 3000), (8, 21)),
    ("VPA flipkart@yesg FLIPKART", "Shopping", (200, 9000), (0, 23)),
    ("VPA amazon@okaxis AMAZON", "Shopping", (200, 8000), (0, 23)),
    ("VPA apollo@okaxis APOLLO PHARMACY", "Health", (100, 4000), (7, 22)),
    ("VPA flipkart@okhdfcbank FLIPKART", "Shopping", (200, 9000), (0, 23)),
    ("VPA swiggy@okhdfcbank SWIGGY", "Food", (80, 700), (9, 23)),
    ("VPA uber@okicici UBER INDIA", "Transport", (60, 1200), (0, 23)),
    ("VPA irctc@oksbi IRCTC RAIL", "Transport", (200, 4000), (6, 22)),
    ("VPA airtel@oksbi AIRTEL PREPAID", "Utilities", (99, 1500), (0, 23)),
    ("VPA groww@paytm GROWW", "Investment", (500, 50000), (9, 16)),
    ("VPA blinkit@paytm BLINKIT", "Groceries", (150, 2000), (7, 23)),
    ("VPA zerodha@ptys ZERODHA BROKING", "Investment", (500, 90000), (9, 16)),
    ("VPA netflix@ptybl NETFLIX INDIA", "Subscription", (149, 999), (0, 23)),
    ("VPA bookmyshow@ptybl BOOKMYSHOW", "Entertainment", (150, 2500), (9, 23)),
    ("VPA spotify@pthdfc SPOTIFY", "Subscription", (59, 399), (0, 23)),
    ("VPA amazon@pthdfc AMAZON", "Shopping", (200, 8000), (0, 23)),
    ("VPA apollo@ptaxis APOLLO PHARMACY", "Health", (100, 4000), (7, 22)),
    ("VPA spotify@ptaxis SPOTIFY", "Subscription", (59, 399), (0, 23)),
    ("VPA pharmeasy@upi PHARMEASY", "Health", (100, 3000), (7, 22)),
    ("VPA cred@upi CRED BILL PAYMENT", "Payments", (500, 60000), (0, 23)),
    ("VPA airtel@bhim AIRTEL PREPAID", "Utilities", (99, 1500), (0, 23)),
    ("VPA uber@bhim UBER INDIA", "Transport", (60, 1200), (0, 23)),
    ("VPA bescom@apl BESCOM ELECTRICITY", "Utilities", (300, 6000), (7, 22)),
    ("VPA pharmeasy@apl PHARMEASY", "Health", (100, 3000), (7, 22)),
    ("VPA bookmyshow@yapl BOOKMYSHOW", "Entertainment", (150, 2500), (9, 23)),
    ("VPA zomato@yapl ZOMATO", "Food", (80, 800), (10, 23)),
    ("VPA unacademy@fbpe UNACADEMY", "Education", (500, 30000), (8, 22)),
    ("VPA groww@fbpe GROWW", "Investment", (500, 50000), (9, 16)),
    ("VPA cred@jupiteraxis CRED BILL PAYMENT", "Payments", (500, 60000), (0, 23)),
    ("VPA bescom@jupiteraxis BESCOM ELECTRICITY", "Utilities", (300, 6000), (7, 22)),
    ("VPA swiggy@fifederal SWIGGY", "Food", (80, 700), (9, 23)),
    ("VPA bigbasket@fifederal BIGBASKET", "Groceries", (300, 3000), (8, 21)),
    ("VPA zomato@naviaxis ZOMATO", "Food", (80, 800), (10, 23)),
    ("VPA netflix@naviaxis NETFLIX INDIA", "Subscription", (149, 999), (0, 23)),
    ("VPA blinkit@slc BLINKIT", "Groceries", (150, 2000), (7, 23)),
    ("VPA unacademy@slc UNACADEMY", "Education", (500, 30000), (8, 22)),
    ("VPA bigbasket@ikwik BIGBASKET", "Groceries", (300, 3000), (8, 21)),
    ("VPA flipkart@ikwik FLIPKART", "Shopping", (200, 9000), (0, 23)),
    ("VPA amazon@freecharge AMAZON", "Shopping", (200, 8000), (0, 23)),
    ("VPA apollo@freecharge APOLLO PHARMACY", "Health", (100, 4000), (7, 22)),
    ("VPA flipkart@waaxis FLIPKART", "Shopping", (200, 9000), (0, 23)),
    ("VPA swiggy@waaxis SWIGGY", "Food", (80, 700), (9, 23)),
    ("VPA uber@waicici UBER INDIA", "Transport", (60, 1200), (0, 23)),
    ("VPA irctc@wahdfcbank IRCTC RAIL", "Transport", (200, 4000), (6, 22)),
    ("VPA airtel@wahdfcbank AIRTEL PREPAID", "Utilities", (99, 1500), (0, 23)),
    ("VPA groww@sbi GROWW", "Investment", (500, 50000), (9, 16)),
    ("VPA blinkit@sbi BLINKIT", "Groceries", (150, 2000), (7, 23)),
    ("VPA zerodha@hdfcbank ZERODHA BROKING", "Investment", (500, 90000), (9, 16)),
    ("VPA netflix@axisbank NETFLIX INDIA", "Subscription", (149, 999), (0, 23)),
    ("VPA bookmyshow@axisbank BOOKMYSHOW", "Entertainment", (150, 2500), (9, 23)),
    ("VPA spotify@icici SPOTIFY", "Subscription", (59, 399), (0, 23)),
    ("VPA amazon@icici AMAZON", "Shopping", (200, 8000), (0, 23)),
    ("VPA apollo@kotak APOLLO PHARMACY", "Health", (100, 4000), (7, 22)),
    ("VPA spotify@kotak SPOTIFY", "Subscription", (59, 399), (0, 23)),
    ("VPA pharmeasy@indus PHARMEASY", "Health", (100, 3000), (7, 22)),
    ("VPA cred@indus CRED BILL PAYMENT", "Payments", (500, 60000), (0, 23)),
    ("VPA airtel@yesbank AIRTEL PREPAID", "Utilities", (99, 1500), (0, 23)),
    ("VPA uber@yesbank UBER INDIA", "Transport", (60, 1200), (0, 23)),
    ("VPA bescom@idfcbank BESCOM ELECTRICITY", "Utilities", (300, 6000), (7, 22)),
    ("VPA pharmeasy@idfcbank PHARMEASY", "Health", (100, 3000), (7, 22)),
    ("VPA bookmyshow@federal BOOKMYSHOW", "Entertainment", (150, 2500), (9, 23)),
    ("VPA zomato@federal ZOMATO", "Food", (80, 800), (10, 23)),
    ("VPA unacademy@rbl UNACADEMY", "Education", (500, 30000), (8, 22)),
    ("VPA groww@rbl GROWW", "Investment", (500, 50000), (9, 16)),
    ("VPA cred@pnb CRED BILL PAYMENT", "Payments", (500, 60000), (0, 23)),
    ("VPA bescom@pnb BESCOM ELECTRICITY", "Utilities", (300, 6000), (7, 22)),
    ("VPA swiggy@unionbank SWIGGY", "Food", (80, 700), (9, 23)),
    ("VPA bigbasket@unionbank BIGBASKET", "Groceries", (300, 3000), (8, 21)),
    ("VPA zomato@barodampay ZOMATO", "Food", (80, 800), (10, 23)),
    ("VPA netflix@barodampay NETFLIX INDIA", "Subscription", (149, 999), (0, 23)),
    ("VPA blinkit@cnrb BLINKIT", "Groceries", (150, 2000), (7, 23)),
    ("VPA unacademy@cnrb UNACADEMY", "Education", (500, 30000), (8, 22)),
    ("VPA bigbasket@indianbank BIGBASKET", "Groceries", (300, 3000), (8, 21)),
    ("VPA flipkart@indianbank FLIPKART", "Shopping", (200, 9000), (0, 23)),
    ("VPA amazon@centralbank AMAZON", "Shopping", (200, 8000), (0, 23)),
    ("VPA apollo@centralbank APOLLO PHARMACY", "Health", (100, 4000), (7, 22)),
    ("VPA flipkart@dbs FLIPKART", "Shopping", (200, 9000), (0, 23)),
    ("VPA swiggy@dbs SWIGGY", "Food", (80, 700), (9, 23)),
    ("VPA uber@aubank UBER INDIA", "Transport", (60, 1200), (0, 23)),
    ("VPA irctc@equitas IRCTC RAIL", "Transport", (200, 4000), (6, 22)),
    ("VPA airtel@equitas AIRTEL PREPAID", "Utilities", (99, 1500), (0, 23)),
    ("VPA groww@idbi GROWW", "Investment", (500, 50000), (9, 16)),
    ("VPA blinkit@idbi BLINKIT", "Groceries", (150, 2000), (7, 23)),
    ("VPA zerodha@uco ZERODHA BROKING", "Investment", (500, 90000), (9, 16)),
    ("VPA paytmqr281005050101@paytm SHREE GANESH KIRANA AND GENERAL STORES", "Groceries", (100, 3000), (7, 22)),
    ("VPA paytmqr5x8k2p@ptys ANNAPURNA TIFFIN SERVICE", "Food", (60, 500), (7, 22)),
    ("VPA paytmqr1n4v7z@ptys SRI SAI MEDICALS", "Health", (50, 3000), (7, 22)),
    ("VPA paytmqr9k3m1t@ptys NEW STAR TAILORS", "Shopping", (100, 4000), (9, 21)),
    ("VPA q721904553@ybl BOMBAY CHAAT CORNER", "Food", (40, 400), (10, 23)),
    ("VPA q338271649@ybl SANJAY VEGETABLE STALL", "Groceries", (30, 800), (6, 21)),
    ("VPA q905412387@ybl CITY OPTICALS", "Health", (200, 6000), (9, 21)),
    ("VPA gpay-11409872263@okbizaxis GREEN LEAF SALAD BAR", "Food", (80, 600), (10, 22)),
    ("VPA gpay-30518274901@okbizaxis SUNRISE STATIONERY", "Shopping", (20, 900), (9, 21)),
    ("VPA bharatpe.8w2k1m7p390214@fbpe VARIETY SNACKS CENTRE", "Food", (30, 400), (7, 23)),
    ("VPA bharatpe.5t9n3r2q118745@fbpe DAILY NEEDS SUPERMARKET", "Groceries", (100, 2500), (7, 22)),
    ("VPA razorpay.rzp@icici ACME ONLINE SERVICES", "Payments", (100, 9000), (0, 23)),
    ("VPA cultfit.rzp@hdfcbank CULT FITNESS", "Health", (500, 6000), (5, 22)),
    ("VPA meesho.rzp@axisbank MEESHO ORDER", "Shopping", (100, 3000), (0, 23)),
    ("VPA netflix@ptys", "Subscription", (149, 999), (0, 23)),
    ("VPA dmart@ybl", "Groceries", (200, 4000), (8, 22)),
    ("VPA rapido@axl", "Transport", (30, 400), (0, 23)),
    ("swiggy@okaxis SWIGGY", "Food", (80, 700), (9, 23)),
    ("myntra@ptys MYNTRA", "Shopping", (300, 6000), (0, 23)),
    ("jio@upi JIO RECHARGE", "Utilities", (149, 1200), (0, 23)),
    ("BIGBASKET DAILY", "Groceries", (100, 2000), (6, 22)),
    ("PVR INOX LIMITED", "Entertainment", (150, 2000), (9, 23)),
    ("TATA 1MG", "Health", (100, 3000), (7, 22)),
    ("VPA SwiGGy@YBL Swiggy Limited", "Food", (80, 700), (9, 23)),
    ("vpa amazon@apl amazon india", "Shopping", (200, 8000), (0, 23)),
    ("VPA 9812345670@ybl ANIL KUMAR VERMA", "Transfer", (100, 20000), (7, 23)),
    ("VPA 9004587321@paytm SUNITA R RAO", "Transfer", (100, 15000), (7, 23)),
    ("VPA 8801234567@upi DEEPAK JOSHI", "Transfer", (100, 25000), (7, 23)),
    ("VPA 7700114488@axl KAVYA NAIR", "Transfer", (100, 12000), (7, 23)),
    ("VPA priya.iyer.98@okhdfcbank PRIYA IYER", "Transfer", (100, 9000), (7, 23)),
    ("VPA karan-malhotra1@ybl KARAN MALHOTRA", "Transfer", (200, 30000), (7, 23)),
    ("VPA rohan_verma_07@okicici ROHAN VERMA", "Transfer", (200, 22000), (7, 23)),
    ("VPA paytmqr7h2k9w@ptys SHREE BALAJI PROVISION AND G", "Groceries", (100, 2500), (7, 22)),
    ("VPA q114857392@ybl NEW ENGLAND RESTAURANT AND B", "Food", (150, 1500), (11, 23)),
    ("UPI AUTOPAY NETFLIX SUBSCRIPTION", "Subscription", (149, 999), (0, 23)),
    ("UPI MANDATE SPOTIFY PREMIUM", "Subscription", (59, 399), (0, 23)),
    ("VPA hdfcbank@hdfcbank UPI AUTOPAY SIP GROWW", "Investment", (500, 25000), (0, 23)),
    ("IMPS P2A TRANSFER TARUN KHANNA", "Transfer", (500, 50000), (0, 23)),
    ("NEFT CR HDFC MAHESH S PRABHU", "Transfer", (500, 90000), (0, 23)),
    ("RTGS TRANSFER VIJAY TRADERS", "Payments", (5000, 200000), (9, 18)),
    ("M/S SHARMA GENERAL STORE", "Groceries", (50, 2000), (7, 22)),
    ("SHREE GANESH KIRANA AND GEN STORES", "Groceries", (50, 2500), (7, 22)),
    ("D-MART AVENUE SUPERMARTS LTD", "Groceries", (300, 6000), (8, 22)),
    ("CAFE COFFEE DAY - VIKHROLI", "Food", (100, 800), (8, 22)),
    ("VPA zz9q7x2w@okaxis", "Other", (50, 3000), (0, 23)),
    ("VPA zz9q7x2w@ybl", "Other", (50, 3000), (0, 23)),
    ("VPA zz9q7x2w@paytm", "Other", (50, 3000), (0, 23)),
    ("VPA zz9q7x2w@upi", "Other", (50, 3000), (0, 23)),
    ("VPA zz9q7x2w@icici", "Other", (50, 3000), (0, 23)),
    ("VPA zz9q7x2w@hdfcbank", "Other", (50, 3000), (0, 23)),
    ("VPA zz9q7x2w@axl", "Other", (50, 3000), (0, 23)),
    ("VPA zz9q7x2w@ptys", "Other", (50, 3000), (0, 23)),
    ("VPA abcd1234efgh@upi", "Other", (50, 5000), (0, 23)),
    ("VPA 000000000@paytm", "Other", (50, 2000), (0, 23)),
    ("VPA temp.test.user@ybl", "Other", (50, 2000), (0, 23)),
    ("UNKNOWN MERCHANT PAYMENT", "Other", (50, 5000), (0, 23)),
]

# A payee id, for making augmented variants of a template (not a feature).
_UPI_HANDLE = _re.compile(r"\S+@\S+\s*")


# ─────────────────────────────────────────────────────────────────────────────
# Dataset generation
# ─────────────────────────────────────────────────────────────────────────────

_BASE_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)

_FREQ_RANGES = {
    "Subscription": (1, 4),
    "Utilities":    (1, 3),
    "Payments":     (1, 3),
    "Food":         (0, 15),
    "Transfer":     (0, 10),
}


def _random_ts(hr_lo: int, hr_hi: int, rng: random.Random) -> datetime:
    return _BASE_DATE + timedelta(
        days=rng.randint(0, 364),
        hours=rng.randint(hr_lo, hr_hi),
        minutes=rng.randint(0, 59),
    )


def _augment(text: str) -> list[str]:
    variants = [text, text.lower()]
    bare = _UPI_HANDLE.sub("", text).strip()
    if bare and bare != text:
        variants += [bare, bare.lower()]
    if not text.upper().startswith("VPA"):
        variants.append(f"VPA {bare or text}")
    if "@" in text:
        variants.append(text.replace(" ", ""))
    return list(dict.fromkeys(variants))


def build_dataset(samples_per_case: int = 40, seed: int = 42) -> pd.DataFrame:
    rng  = random.Random(seed)
    rows = []
    for (receiver, category, (alo, ahi), (hlo, hhi)) in RAW_CASES:
        variants = _augment(receiver)
        flo, fhi = _FREQ_RANGES.get(category, (0, 8))
        for i in range(samples_per_case):
            rows.append({
                "raw_text":  variants[i % len(variants)],
                "category":  category,
                "amount":    round(rng.uniform(alo, ahi), 2),
                "timestamp": _random_ts(hlo, hhi, rng),
                "freq_30d":  rng.randint(flo, fhi),
            })
    df = pd.DataFrame(rows).sample(frac=1, random_state=seed).reset_index(drop=True)
    log.info(f"Dataset: {len(df)} rows, {df['category'].nunique()} categories")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Embeddings
# ─────────────────────────────────────────────────────────────────────────────

_emb_model = None


def _load_emb() -> bool:
    global _emb_model
    if _emb_model:
        return True
    try:
        from sentence_transformers import SentenceTransformer
        _emb_model = SentenceTransformer("all-MiniLM-L6-v2")
        log.info("Embeddings: all-MiniLM-L6-v2 loaded")
        return True
    except ImportError:
        log.warning("sentence-transformers not installed — zero embeddings")
        return False


def get_embeddings(texts: list[str]) -> np.ndarray:
    if not _emb_model:
        return np.zeros((len(texts), EMBEDDING_DIM), dtype=np.float32)
    return _emb_model.encode(texts, batch_size=128, show_progress_bar=True)


# ─────────────────────────────────────────────────────────────────────────────
# Training
# ─────────────────────────────────────────────────────────────────────────────

def load_extra(path: str) -> pd.DataFrame:
    """
    Extra labelled rows (for example ml/data/private_train.csv from
    build_golden_set.py): columns raw_text, category, amount, timestamp.
    raw_text is the receiver as gmail_parser returns it. A blank timestamp is
    unknown, not midnight. Frequency is unknown for these rows, so 0.
    """
    extra = pd.read_csv(path, dtype={"raw_text": str, "category": str, "timestamp": str})
    unknown = sorted(set(extra["category"]) - set(CATEGORIES))
    if unknown:
        raise SystemExit(f"{path}: categories not in CATEGORIES: {unknown}")
    extra = extra.dropna(subset=["raw_text"]).drop_duplicates()
    return pd.DataFrame({
        "raw_text":  extra["raw_text"],
        "category":  extra["category"],
        "amount":    pd.to_numeric(extra["amount"], errors="coerce").fillna(0.0),
        "timestamp": [t if isinstance(t, str) and t else None for t in extra["timestamp"]],
        "freq_30d":  0,
    })


def train(samples_per_case: int = 40, seed: int = 42, classifier: str = "logreg",
          extra_path: str | None = None, pipeline_input: bool = True,
          use_embeddings: bool = False):
    """
    Train from scratch. `classifier` is "sgd" (the original) or "logreg" (4.9).

    Which one ships is a decision for the evaluation set (4.8), not for the
    validation score printed below: that validation is a random split of
    augmented copies of the same templates, so both models are scored on
    near-duplicates of what they memorised and both look better than they are.
    Compare them with evaluate_model.py on real transactions.
    """
    t0 = time.perf_counter()
    log.info("=" * 62)
    log.info("UPI Categoriser — Training")
    log.info(f"Output  → {MODEL_PATH}")
    log.info(f"Cases   → {len(RAW_CASES)}  |  samples/case={samples_per_case}")
    log.info("=" * 62)

    df = build_dataset(samples_per_case, seed)
    if extra_path:
        extra = load_extra(extra_path)
        log.info(f"Extra data: {len(extra)} rows from {extra_path}")
        df = pd.concat([df, extra], ignore_index=True).sample(frac=1, random_state=seed)

    log.info("\nClass distribution (raw):")
    counts = df["category"].value_counts()
    for cat, cnt in counts.items():
        log.info(f"  {cat:<22} {cnt:>5}  {'█'*(cnt//20)}")

    # Balance — upsample any class below min_n
    min_n = max(300, counts.min())
    parts = [df]
    for cat, cnt in counts.items():
        if cnt < min_n:
            parts.append(resample(df[df["category"] == cat],
                                  replace=True, n_samples=min_n - cnt,
                                  random_state=seed))
    df = pd.concat(parts, ignore_index=True).sample(frac=1, random_state=seed)
    log.info(f"After balance: {len(df)} rows")

    # pipeline_input: learn from the cleaned merchant name, which is what
    # etl.run_categorise_silver feeds the model at inference, instead of the
    # raw receiver string.
    processed = [input_text(t) for t in df["raw_text"]] if pipeline_input \
                else [preprocess_text(t) for t in df["raw_text"]]

    le = LabelEncoder()
    le.fit(CATEGORIES)
    y  = le.transform(df["category"])
    log.info(f"Labels: {list(le.classes_)}")

    log.info("Fitting TF-IDF …")
    tfidf_char = TfidfVectorizer(
        analyzer="char_wb", ngram_range=(2, 5), max_features=TFIDF_CHAR_MAX,
        sublinear_tf=True, strip_accents="unicode", lowercase=True, min_df=2,
    )
    tfidf_word = TfidfVectorizer(
        analyzer="word", ngram_range=(1, 2), max_features=TFIDF_WORD_MAX,
        sublinear_tf=True, strip_accents="unicode", lowercase=True, min_df=2,
    )
    char_f      = tfidf_char.fit_transform(processed)
    word_f      = tfidf_word.fit_transform(processed)
    text_sparse = hstack([char_f, word_f]) # <-- Kept as sparse matrix!

    # use_embeddings=False trains with the MiniLM block zeroed, so inference
    # can skip torch entirely (the bundle records it; categoriser.py obeys).
    emb_ok = use_embeddings and _load_emb()
    emb    = get_embeddings(processed) if emb_ok else \
             np.zeros((len(processed), EMBEDDING_DIM), dtype=np.float32)

    log.info("Building metadata …")
    meta = np.zeros((len(df), METADATA_DIM), dtype=np.float32)
    for j, row in enumerate(df.itertuples()):
        # Blank timestamps come through pandas as NaN/NaT: unknown, like None.
        ts = row.timestamp if isinstance(row.timestamp, (str, datetime)) and not pd.isna(row.timestamp) else None
        meta[j] = extract_metadata(float(row.amount), ts, int(row.freq_30d))

    log.info("Stacking features into sparse matrix (for memory efficiency)...")
    # We use sparse here to split the data without OOM, but we MUST
    # train on DENSE arrays to match categoriser.py inference format
    X_sparse = hstack([text_sparse, emb, meta]).tocsr() 

    log.info(f"Feature matrix: {X_sparse.shape}  "
             f"(tfidf={text_sparse.shape[1]}, emb={EMBEDDING_DIM}, meta={METADATA_DIM})")

    X_tr, X_val, y_tr, y_val = train_test_split(
        X_sparse, y, test_size=0.15, random_state=seed, stratify=y
    )
    log.info(f"Train: {X_tr.shape[0]}  |  Val: {X_val.shape[0]}")

    classes_arr   = np.arange(len(le.classes_))
    class_weights = compute_class_weight("balanced", classes=classes_arr, y=y_tr)
    cw_dict       = dict(zip(classes_arr, class_weights))
    sw            = np.array([cw_dict[c] for c in y_tr], dtype=np.float32)

    batch_size = 2000

    if classifier == "logreg":
        # 4.9: one fit over the whole training set, on the sparse matrix, with
        # no dense copy. The mini-batch alternative below makes a single pass
        # over each batch, which is likely undertrained — that is the thing
        # being compared. Sparse training is safe for inference: the model only
        # needs the same number of features, and sparse and dense predict_proba
        # were measured to agree to 7e-6 on this feature stack (2026-09-15).
        log.info("Training LogisticRegression (multinomial) on sparse features …")
        clf = LogisticRegression(max_iter=1000, class_weight="balanced",
                                 n_jobs=-1, random_state=seed)
        clf.fit(X_tr, y_tr)
    else:
        log.info("Training SGDClassifier (log_loss) in dense mini-batches …")
        clf = SGDClassifier(loss="log_loss", max_iter=1000, tol=1e-4,
                            random_state=seed, n_jobs=-1)

        # ── CRITICAL: We must train using dense arrays (not sparse) so inference works ──
        for i in range(0, X_tr.shape[0], batch_size):
            X_batch = X_tr[i:i+batch_size].toarray().astype(np.float32)
            y_batch = y_tr[i:i+batch_size]
            sw_batch = sw[i:i+batch_size]
            clf.partial_fit(X_batch, y_batch, classes=classes_arr, sample_weight=sw_batch)

    log.info("Evaluating on validation set...")
    # Evaluate using dense array as well
    preds = []
    for i in range(0, X_val.shape[0], batch_size):
        X_v_batch = X_val[i:i+batch_size].toarray().astype(np.float32)
        preds.extend(clf.predict(X_v_batch))
    y_pred = np.array(preds)
    
    print()
    print(classification_report(y_val, y_pred, target_names=le.classes_, zero_division=0))

    # No "online samples" are carried forward. The online refit was removed on
    # 2026-09-15, so nothing updates a model after training; copying the old
    # file's counter onto a freshly trained model made it claim corrections it
    # had never seen, and /model-info published that claim.

    os.makedirs(_ML_DIR, exist_ok=True)
    joblib.dump({
        "clf":                    clf,
        "classifier":             classifier,     # 4.9: which one produced this file
        "tfidf_char":             tfidf_char,
        "tfidf_word":             tfidf_word,
        "embedding_dim":          EMBEDDING_DIM,
        "uses_embeddings":        bool(emb_ok),
        "metadata_dim":           METADATA_DIM,
        "trained_at":             datetime.now(timezone.utc).isoformat(),
        "training_rows":          len(df),
        "cases_count":            len(RAW_CASES),
    }, MODEL_PATH)
    joblib.dump(le, ENCODER_PATH)

    log.info(f"\n✅  {MODEL_PATH}")
    log.info(f"✅  {ENCODER_PATH}")
    log.info(f"⏱   {time.perf_counter()-t0:.1f}s")


# ─────────────────────────────────────────────────────────────────────────────
# Smoke test
# ─────────────────────────────────────────────────────────────────────────────

SMOKE_CASES = [
    # (raw_text,                              amount,  hour, expected)
    ("Swiggy",                                250,     13,   "Food"),
    ("zomato@kotak",                          180,     20,   "Food"),
    ("VPA swiggy.stores@axb Swiggy Limited",  320,     12,   "Food"),
    ("dominospizza@hdfcbank",                 450,     21,   "Food"),
    ("KFC India",                             300,     19,   "Food"),
    ("eatclub@ptybl EatClub",                 180,     12,   "Food"),
    ("sapphirekfconline@ybl KFC",             250,     13,   "Food"),
    ("mcdonalds.42276700@hdfcbank MC DONALDS",200,     14,   "Food"),
    ("Blinkit",                               850,     10,   "Groceries"),
    ("bigbasket@hdfcbank",                    1200,    9,    "Groceries"),
    ("DMart",                                 2200,    18,   "Groceries"),
    ("zepto@axisbank",                        600,     20,   "Groceries"),
    ("blinkit@axisbank Blinkit Delivery",     900,     11,   "Groceries"),
    ("amazon@okaxis",                         1299,    15,   "Shopping"),
    ("Flipkart",                              1599,    14,   "Shopping"),
    ("myntra@icicibank Myntra Fashion",        2200,    21,   "Shopping"),
    ("Croma",                                 42000,   13,   "Shopping"),
    ("Uber",                                  220,     9,    "Transport"),
    ("irctc@okicici IRCTC RAIL",              1800,    11,   "Transport"),
    ("IndiGo",                                5500,    12,   "Transport"),
    ("rapido@ybl",                            90,      8,    "Transport"),
    ("hpcl@okaxis",                           2500,    20,   "Transport"),
    ("ola.money@ybl Ola Cabs",                350,     9,    "Transport"),
    ("groww@axisbank",                        5000,    10,   "Investment"),
    ("zerodha@kotak",                         10000,   11,   "Investment"),
    ("groww.iccl2.brk@validicici",            50000,   10,   "Investment"),
    ("groww.brk@validhdfc GROWW INVEST TECH", 8000,    10,   "Investment"),
    ("netflix@icici",                         499,     21,   "Subscription"),
    ("spotify@ybl",                           119,     18,   "Subscription"),
    ("Amazon Prime",                          1499,    12,   "Subscription"),
    ("hotstar@axisbank",                      899,     20,   "Subscription"),
    ("appleservices.bdsi@hdfcbank",           149,     3,    "Subscription"),
    ("apollopharmacy@hdfcbank",               350,     11,   "Health"),
    ("1mg@icici",                             450,     20,   "Health"),
    ("Fortis Healthcare",                     5000,    11,   "Health"),
    ("cultfit@icici",                         2000,    7,    "Health"),
    ("paytmqr8lotus@ptys GLOBAL MEDICAL",     500,     14,   "Health"),
    ("bescom@paytm",                          1800,    10,   "Utilities"),
    ("airtel@axisbank",                       599,     11,   "Utilities"),
    ("jio@rjio Jio Recharge",                 239,     14,   "Utilities"),
    ("JioFiber",                              999,     10,   "Utilities"),
    ("bescom@hdfcbank BESCOM ELECTRICITY",    2200,    10,   "Utilities"),
    ("Rahul Sharma",                          500,     18,   "Transfer"),
    ("ananyaiyer07@okhdfcbank",              2000,    19,   "Transfer"),
    ("rent@upi",                              12000,   2,    "Transfer"),
    ("paytmqr4tulip@ptys MAHESH S PRABHU",  300,     17,   "Transfer"),
    ("9000000001@pthdfc TARUN KHANNA",       1000,    15,   "Transfer"),
    ("bharatpe.3k7m2p9q4r182640@fbpe IMRAN", 400,    16,   "Transfer"),
    ("bookmyshow@hdfcbank BookMyShow",         600,    19,   "Entertainment"),
    ("dream11@ybl",                           500,     21,   "Entertainment"),
    ("PVR Cinemas",                           1200,    19,   "Entertainment"),
    ("byjus@hdfcbank BYJU S THINK",           3000,    16,   "Education"),
    ("unacademy@okaxis Unacademy Learning",    5000,    14,   "Education"),
    ("college fees@axisbank",                 25000,   10,   "Education"),
    ("cred@axisbank",                         15000,   10,   "Payments"),
    ("hdfcbank@hdfcbank",                     45000,   9,    "Payments"),
    ("LIC Premium",                           12000,   10,   "Payments"),
    ("unknown@upi",                           300,     14,   "Other"),
    ("xyzabc123@okaxis",                      500,     11,   "Other"),
]


def smoke_test():
    """Hand-written cases through the app's own prediction code (ml/categoriser.py). Inflated: see evaluate_model.py."""
    from ml import categoriser
    log.info("\n── Smoke test ──────────────────────────────────────────────────")
    model = categoriser.load(MODEL_PATH, ENCODER_PATH)
    if model is None:
        log.info("Smoke test skipped: the model could not be loaded by the app (see the log above).")
        return

    passed = failed = 0
    for raw, amount, hour, expected in SMOKE_CASES:
        ts    = f"2024-06-15T{hour:02d}:30:00+05:30"
        proba = categoriser.predict_proba(model, [raw], [amount], [ts])[0]
        pred  = model.classes[int(np.argmax(proba))]
        conf  = proba.max()
        icon  = "✅" if pred == expected else "❌"
        if pred == expected: passed += 1
        else:                failed += 1
        log.info(f"  {icon} {raw[:38]:<38} → {pred:<14} ({conf:.2f})  [want: {expected}]")

    pct = passed / (passed + failed) * 100
    log.info(f"\nSmoke: {passed}/{passed+failed} = {pct:.0f}%")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--samples",  type=int, default=40)
    p.add_argument("--seed",     type=int, default=42)
    p.add_argument("--no-smoke", action="store_true")
    p.add_argument("--extra-data", help="CSV of extra labelled rows (raw_text,category,amount,timestamp)")
    # Defaults are what ships (2026-09-27): logistic regression, no embeddings,
    # the app's input text (ml/features.input_text).
    p.add_argument("--embeddings", action="store_true",
                   help="also train on MiniLM embeddings (needs sentence-transformers; the app refuses such a model)")
    p.add_argument("--raw-input", action="store_true",
                   help="train on the raw receiver text instead of the app's input text (experiments only)")
    p.add_argument("--out-prefix", help="write <prefix>_model.pkl and <prefix>_encoder.pkl instead of "
                                        "replacing ml/model_v2.pkl (experiments)")
    p.add_argument("--classifier", choices=["sgd", "logreg"], default="logreg",
                   help="which model to fit (4.9). The winner is decided by "
                        "evaluate_model.py on real data, not by the validation "
                        "score printed here.")
    args = p.parse_args()
    if args.out_prefix:
        MODEL_PATH, ENCODER_PATH = f"{args.out_prefix}_model.pkl", f"{args.out_prefix}_encoder.pkl"
    train(samples_per_case=args.samples, seed=args.seed, classifier=args.classifier,
          extra_path=args.extra_data, pipeline_input=not args.raw_input,
          use_embeddings=args.embeddings)
    if not args.no_smoke:
        smoke_test()