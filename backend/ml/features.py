"""
features.py — the one definition of what the categoriser model sees.

Training (train_model.py), evaluation (evaluate_model.py) and prediction
(ml/categoriser.py) all import from here, so the three can never drift
(before 2026-09-27 each kept its own copy). Pure: no settings, no database,
no network. Changing anything here needs a retrain.
"""

from __future__ import annotations

import re
from datetime import datetime

import numpy as np
from scipy.sparse import csr_matrix, hstack

from merchant_identity import clean_merchant, model_text

# Must match the list in correct_category() (supabase/migrations/) and the
# frontend's CATEGORY_COLORS / ALL_CATEGORIES (.claude/rules/general.md).
CATEGORIES = [
    "Education", "Entertainment", "Food", "Groceries", "Health",
    "Investment", "Payments", "Shopping", "Subscription",
    "Transfer", "Transport", "Utilities", "Other",
]

TFIDF_CHAR_MAX = 40_000
TFIDF_WORD_MAX = 20_000
EMBEDDING_DIM  = 384      # width of the embedding block; zeros when a model has none
METADATA_DIM   = 5

_HANDLE_PREFIX = re.compile(r"^VPA\s+", re.IGNORECASE)
_UPI_HANDLE    = re.compile(r"\S+@\S+\s*")
_NOISE_WORDS   = re.compile(
    r"\b(pvt|ltd|pte|inc|llp|llc|private|limited|"
    r"payment|online|india|tech|services|w|g|rzp|upi)\b",
    re.IGNORECASE,
)


def preprocess_text(text: str) -> str:
    """Lowercased merchant words: no VPA label, payee id, gateway prefix, long digit runs or noise words."""
    if not isinstance(text, str):
        return ""
    text    = _HANDLE_PREFIX.sub("", text).strip()
    after   = _UPI_HANDLE.sub("", text).strip()
    working = after if len(after) >= 3 else text
    working = re.sub(r"\b(paytmqr|gpay-|paytm\.)[a-zA-Z0-9-]+\b", " ", working, flags=re.IGNORECASE)
    working = re.sub(r"[/@.]",      " ", working)
    working = re.sub(r"\b\d{4,}\b", " ", working)
    working = _NOISE_WORDS.sub(" ", working)
    working = re.sub(r"\s+",        " ", working).strip()
    return working.lower()


def input_text(receiver: str) -> str:
    """What the model reads for a receiver: model_text(receiver, clean_merchant(receiver)), preprocessed."""
    receiver = receiver or ""
    return preprocess_text(model_text(receiver, clean_merchant(receiver)))


def extract_metadata(amount: float, timestamp=None, tx_frequency_30d: int = 0) -> np.ndarray:
    """
    5 features: log amount, hour as sine/cosine, day of week, frequency.

    A missing or unparseable timestamp means unknown: zeros for the three time
    features, never the current clock (TEST-01). Zero sine and zero cosine is a
    point no real hour produces, so the model can tell unknown from midnight.
    """
    log_amount = float(np.log1p(max(amount or 0.0, 0)))
    ts = None
    if isinstance(timestamp, str):
        try:
            ts = datetime.fromisoformat(timestamp[:19])
        except ValueError:
            ts = None
    elif isinstance(timestamp, datetime):
        ts = timestamp

    if ts is None:
        hour_sin = hour_cos = dow_norm = 0.0
    else:
        hour_sin = float(np.sin(2 * np.pi * ts.hour / 24))
        hour_cos = float(np.cos(2 * np.pi * ts.hour / 24))
        dow_norm = ts.weekday() / 6.0

    freq_norm = float(np.log1p(tx_frequency_30d)) / np.log1p(30)
    return np.array([log_amount, hour_sin, hour_cos, dow_norm, freq_norm], dtype=np.float32)


def build_features(bundle: dict, texts: list[str], meta: np.ndarray, embeddings=None):
    """
    Sparse feature matrix for a trained bundle: TF-IDF char + word, the
    embedding block (zeros unless `embeddings` is given), metadata. Sparse and
    dense inputs give the same probabilities; sparse is far smaller and faster.
    """
    n = len(texts)
    emb = csr_matrix((n, EMBEDDING_DIM), dtype=np.float32) if embeddings is None \
        else csr_matrix(np.asarray(embeddings, dtype=np.float32))
    return hstack([
        bundle["tfidf_char"].transform(texts),
        bundle["tfidf_word"].transform(texts),
        emb,
        csr_matrix(np.asarray(meta, dtype=np.float32).reshape(n, METADATA_DIM)),
    ]).tocsr()
