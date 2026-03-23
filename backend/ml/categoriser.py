"""
categoriser.py
──────────────
Loads the trained model and exposes a clean predict interface.

Used by etl.py to categorise silver_transactions rows.

Usage:
    from ml.categoriser import predict_category, predict_batch

    category, confidence = predict_category("VPA swiggy.stores@axb Swiggy Limited")
    # → ("Food", 0.97)

    results = predict_batch([
        "VPA paytmqr6woody@ptys RAVINDRA S SHETTY",
        "VPA groww.brk@validhdfc GROWW INVEST TECH PVT LTD",
    ])
    # → [("Transfer", 0.94), ("Investment", 0.99)]
"""

import os
import re
import joblib
import numpy as np

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────

_BASE    = os.path.dirname(os.path.abspath(__file__))
_MODEL   = os.path.join(_BASE, "model.pkl")
_ENCODER = os.path.join(_BASE, "label_encoder.pkl")

# ─────────────────────────────────────────────────────────────────────────────
# Lazy load — model is loaded once on first use, not at import time
# ─────────────────────────────────────────────────────────────────────────────

_pipeline      = None
_label_encoder = None


def _load():
    global _pipeline, _label_encoder
    if _pipeline is None:
        if not os.path.exists(_MODEL):
            raise FileNotFoundError(
                f"Model not found at {_MODEL}. "
                "Run train.py first to generate the model."
            )
        _pipeline      = joblib.load(_MODEL)
        _label_encoder = joblib.load(_ENCODER)


# ─────────────────────────────────────────────────────────────────────────────
# Preprocessing — must match train.py exactly
# ─────────────────────────────────────────────────────────────────────────────

_HANDLE_PREFIX  = re.compile(r"^VPA\s+", re.IGNORECASE)
_UPI_HANDLE     = re.compile(r"\S+@\S+\s*")


def _preprocess(text: str) -> str:
    if not isinstance(text, str):
        return ""

    text = _HANDLE_PREFIX.sub("", text).strip()

    after_handle = _UPI_HANDLE.sub("", text).strip()
    working      = after_handle if len(after_handle) >= 3 else text

    working = re.sub(r"[/@.]",   " ", working)
    working = re.sub(r"\b\d{4,}\b", " ", working)
    working = re.sub(r"\b(pvt|ltd|pte|inc|llp|llc|private|limited)\b", " ", working, flags=re.IGNORECASE)
    working = re.sub(r"\b(payment|online|india|tech|services|w|g)\b",   " ", working, flags=re.IGNORECASE)
    working = re.sub(r"\s+", " ", working).strip()

    return working.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Confidence threshold
# Below this → category marked as "Other" and is_categorised stays False
# so a human or future model can review it
# ─────────────────────────────────────────────────────────────────────────────

CONFIDENCE_THRESHOLD = 0.55


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def predict_category(raw_text: str) -> tuple[str, float]:
    """
    Predict the category for a single transaction string.

    Returns:
        (category, confidence)
        category   — one of the 12 trained categories, or "Other"
        confidence — float 0.0–1.0

    If confidence < CONFIDENCE_THRESHOLD the category is "Other"
    and the row will be flagged for review (is_categorised = False).
    """
    _load()

    processed  = _preprocess(raw_text)
    proba      = _pipeline.predict_proba([processed])[0]
    top_idx    = int(np.argmax(proba))
    confidence = float(proba[top_idx])
    category   = _label_encoder.inverse_transform([top_idx])[0]

    if confidence < CONFIDENCE_THRESHOLD:
        return "Other", confidence

    return category, confidence


def predict_batch(raw_texts: list[str]) -> list[tuple[str, float]]:
    """
    Predict categories for a list of transaction strings.
    More efficient than calling predict_category() in a loop —
    the model processes all texts in a single vectorisation pass.

    Returns:
        List of (category, confidence) tuples in the same order as input.
    """
    _load()

    if not raw_texts:
        return []

    processed = [_preprocess(t) for t in raw_texts]
    probas    = _pipeline.predict_proba(processed)

    results = []
    for proba in probas:
        top_idx    = int(np.argmax(proba))
        confidence = float(proba[top_idx])
        category   = _label_encoder.inverse_transform([top_idx])[0]

        if confidence < CONFIDENCE_THRESHOLD:
            results.append(("Other", confidence))
        else:
            results.append((category, confidence))

    return results


def get_categories() -> list[str]:
    """Returns the full list of trained category names."""
    _load()
    return list(_label_encoder.classes_) + ["Other"]


# ─────────────────────────────────────────────────────────────────────────────
# Quick test — run directly to verify model is working
# python ml/categoriser.py
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_cases = [
        # (raw_text, expected_category)
        ("VPA swiggy.stores@axb Swiggy Limited",                    "Food"),
        ("VPA paytmqr6woody@ptys RAVINDRA S SHETTY",                "Transfer"),
        ("VPA groww.brk@validhdfc GROWW INVEST TECH PVT LTD",       "Investment"),
        ("VPA gpay-11256438096@okbizaxis Food Xpress",               "Food"),
        ("VPA paytmqr5hqark@ptys GLOBAL MEDICAL AND G",             "Health"),
        ("VPA paytm.s11h0ar@pty NBC Vikhroli W",                    "Food"),
        ("VPA sapphirekfconline@ybl KFC",                           "Food"),
        ("VPA mcdonalds.42276700@hdfcbank MC DONALDS",              "Food"),
        ("VPA eatclub@ptybl EatClub",                               "Food"),
        ("VPA arnavdumane04@okhdfcbank ARNAV RAVISHANKAR DUMANE",   "Transfer"),
        ("VPA groww.iccl2.brk@validicici Mutual Funds ICCL",        "Investment"),
        ("VPA appleservices.bdsi@hdfcbank APPLE MEDIA SERVICES",    "Subscription"),
        ("VPA bharatpe.9y0r0e7l3x685328@fbpe SAJAHAN SK",           "Transfer"),
        ("VPA ss2002786kgn@okicici Mr SAHIL RAJU SAYYED",           "Transfer"),
        ("VPA 8368536065@pthdfc HARSHIT SAXENA",                    "Transfer"),
    ]

    print("\nRunning categoriser tests on your actual transaction data...\n")
    print(f"  {'Raw text':<52} {'Expected':<14} {'Got':<14} {'Conf':>6}  {'✓'}")
    print(f"  {'─'*52} {'─'*14} {'─'*14} {'─'*6}  {'─'}")

    correct = 0
    for raw, expected in test_cases:
        category, conf = predict_category(raw)
        ok = "✅" if category == expected else "❌"
        if category == expected:
            correct += 1
        print(f"  {raw[:50]:<52} {expected:<14} {category:<14} {conf:>5.2f}   {ok}")

    print(f"\n  Result: {correct}/{len(test_cases)} correct ({correct/len(test_cases)*100:.0f}%)\n")