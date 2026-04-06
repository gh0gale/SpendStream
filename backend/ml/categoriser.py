"""
categoriser.py  ─  ML-powered UPI Transaction Categorisation Engine
─────────────────────────────────────────────────────────────────────
Architecture:
  f(raw_text, metadata, user_history) → (category, confidence)

Learning modes:
  ① Cold start  — embeddings-only model, no training data needed.
                  Cosine similarity against category anchor phrases.
                  Works immediately with zero labelled data.

  ② Online      — every user correction calls online_update() which
                  does a warm_start partial refit of the LogReg clf
                  on the accumulated corrections buffer. No manual
                  retraining step required. Triggers automatically
                  when buffer reaches ONLINE_UPDATE_THRESHOLD.

  ③ Full retrain (optional) — python ml/categoriser.py --train
                  Use only if you want to rebuild TF-IDF vocabulary
                  from a large corpus. Not required for normal use.

Synthetic data: NOT required. The model bootstraps from embeddings
and learns purely from user corrections.

CHANGE LOG (pattern-aware upgrade):
  - Metadata (amount, hour, dow, frequency) now included in ML features
  - Pattern-based probability boost layer added in _postprocess()
  - Hard override replaced with soft override (strong bias, not absolute)
  - History weighting now decays older transactions (recency-weighted)
"""



from __future__ import annotations

import os
import re
import time
import logging
import threading
import numpy as np
import pandas as pd

from datetime import datetime, timezone
from collections import defaultdict, Counter
from typing import Optional
from dataclasses import dataclass, field

from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelEncoder, normalize
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.utils import resample
from scipy.sparse import hstack

import joblib

from supabase import create_client
import os

supabase_admin = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)


print("[categoriser] LOADING LOGIC FROM:", __file__)

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="[%(name)s] %(message)s")
log = logging.getLogger("categoriser")

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────



_BASE        = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH   = os.path.join(_BASE, "model_v2.pkl")
ENCODER_PATH = os.path.join(_BASE, "label_encoder_v2.pkl")
SYNTHETIC_PATH = os.path.join(_BASE, "data", "synthetic.csv")
REAL_PATH      = os.path.join(_BASE, "data", "real_labels.csv")

# ─────────────────────────────────────────────────────────────────────────────
# Categories  (must match your Supabase + frontend)
# ─────────────────────────────────────────────────────────────────────────────

CATEGORIES = [
    "Education", "Entertainment", "Food", "Groceries", "Health",
    "Investment", "Payments", "Shopping", "Subscription",
    "Transfer", "Transport", "Utilities", "Other",
]

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

CONFIDENCE_THRESHOLD    = 0.40
HISTORY_LAMBDA          = 0.40
ONLINE_UPDATE_THRESHOLD = 2
TFIDF_CHAR_MAX          = 40_000
TFIDF_WORD_MAX          = 20_000
EMBEDDING_DIM           = 384

# ── CHANGE 1: Metadata dimension constant ─────────────────────────────────────
# extract_metadata() returns 5 features; declaring here keeps feature-dim
# arithmetic in one place and prevents silent shape mismatches.
METADATA_DIM = 5

# ── CHANGE 2: Pattern-boost constants ─────────────────────────────────────────
# Tune these to adjust how aggressively pattern rules fire.
# All boosts are additive deltas on the raw probability vector
# (renormalised afterward), so they cannot hard-override ML.

FOOD_AMOUNT_MAX        = 500     # ₹ — meals rarely exceed this
FOOD_HOUR_RANGES       = [(11, 15), (19, 22)]  # lunch window, dinner window
FOOD_BOOST             = 0.25    # added to P(Food) when rule fires

SUBSCRIPTION_FREQ_MIN  = 3       # tx count in 30 d to suspect subscription
SUBSCRIPTION_AMOUNT_CV = 0.15    # coefficient of variation (std/mean < this → fixed amount)
SUBSCRIPTION_BOOST     = 0.30

TRANSFER_AMOUNT_MAX    = 5_000   # amounts above this are rarely person-to-person food
PERSON_BOOST           = 0.20    # boost Transfer for receiver that looks like a person name

# ── CHANGE 3: Soft-override weight ────────────────────────────────────────────
# Instead of returning immediately on override, we blend override as a
# strong prior into the probability vector.  ML + patterns can still win
# if evidence is very strong (e.g., model confidence ≥ 0.85 for a different
# category), but in practice the override dominates.
SOFT_OVERRIDE_WEIGHT   = 0.75    # weight given to the override category

# ── CHANGE 4: Recency decay for history ───────────────────────────────────────
HISTORY_RECENCY_DECAY  = 0.80    # each older transaction is worth this × previous

# ─────────────────────────────────────────────────────────────────────────────
# Anchor phrases for cold-start embedding similarity
# ─────────────────────────────────────────────────────────────────────────────

CATEGORY_ANCHORS: dict[str, list[str]] = {
    "Food":          ["swiggy zomato restaurant food delivery kfc mcdonalds dominos pizza burger cafe",
                      "eatclub faasos box8 freshmenu starbucks chaayos subway"],
    "Groceries":     ["blinkit zepto bigbasket dmart jiomart milkbasket grofers supermarket grocery",
                      "vegetables fruits milk dairy provisions"],
    "Shopping":      ["amazon flipkart myntra ajio nykaa meesho snapdeal shopping retail clothes",
                      "fashion electronics gadgets accessories"],
    "Transport":     ["uber ola rapido irctc makemytrip redbus indigo flight train cab auto",
                      "travel ticket booking commute fare"],
    "Investment":    ["groww zerodha kuvera mutual fund iccl nsccl stocks equity sip investment",
                      "broker demat portfolio smallcase"],
    "Subscription":  ["netflix spotify hotstar youtube apple prime zee5 sonyliv subscription",
                      "streaming membership plan monthly"],
    "Health":        ["apollo pharmacy medplus 1mg pharmeasy netmeds hospital clinic doctor",
                      "medicine health medical"],
    "Utilities":     ["electricity bescom msedcl airtel jio vodafone vi bsnl gas water bill",
                      "recharge broadband internet utility"],
    "Transfer":      ["sent received person name transfer upi imps neft payment to friend",
                      "personal money send"],
    "Entertainment": ["bookmyshow pvr inox cinema movie event ticket game dream11",
                      "entertainment sports gaming"],
    "Education":     ["byjus unacademy vedantu coursera udemy college school fees tuition",
                      "education learning course"],
    "Payments":      ["credit card loan emi insurance premium tax challan fine payment bill",
                      "cred paytm wallet recharge topup"],
    "Other":         ["miscellaneous unknown other general"],
}

# ─────────────────────────────────────────────────────────────────────────────
# Transformer embeddings — lazy loaded, graceful fallback
# ─────────────────────────────────────────────────────────────────────────────

_embedding_model      = None
_embedding_lock       = threading.Lock()
_embeddings_available = False
_anchor_matrix        = None


def _load_embeddings():
    global _embedding_model, _embeddings_available, _anchor_matrix
    with _embedding_lock:
        if _embedding_model is not None:
            return
        try:
            from sentence_transformers import SentenceTransformer
            _embedding_model      = SentenceTransformer("all-MiniLM-L6-v2")
            _embeddings_available = True
            log.info("Transformer embeddings loaded (all-MiniLM-L6-v2, 384-dim)")
            _precompute_anchors()
        except ImportError:
            log.warning("sentence-transformers not installed — cold-start disabled")
            _embeddings_available = False
        except Exception as e:
            log.warning(f"Embedding model load failed: {e}")
            _embeddings_available = False


def _precompute_anchors():
    global _anchor_matrix
    vecs = []
    for cat in CATEGORIES:
        phrases = CATEGORY_ANCHORS.get(cat, [cat])
        embs    = _embedding_model.encode(phrases, show_progress_bar=False)
        vecs.append(embs.mean(axis=0))
    _anchor_matrix = normalize(np.stack(vecs), norm="l2")
    log.info(f"Anchor matrix built: {_anchor_matrix.shape}")


def get_embeddings(texts: list[str]) -> np.ndarray:
    _load_embeddings()
    if not _embeddings_available or not texts:
        return np.zeros((len(texts), EMBEDDING_DIM), dtype=np.float32)
    try:
        return _embedding_model.encode(texts, batch_size=64, show_progress_bar=False)
    except Exception as e:
        log.warning(f"Embedding error: {e}")
        return np.zeros((len(texts), EMBEDDING_DIM), dtype=np.float32)


# ─────────────────────────────────────────────────────────────────────────────
# Text preprocessing
# ─────────────────────────────────────────────────────────────────────────────

_HANDLE_PREFIX = re.compile(r"^VPA\s+", re.IGNORECASE)
_UPI_HANDLE    = re.compile(r"\S+@\S+\s*")
_NOISE_WORDS   = re.compile(
    r"\b(pvt|ltd|pte|inc|llp|llc|private|limited|"
    r"payment|online|india|tech|services|w|g|rzp|upi)\b",
    re.IGNORECASE
)
# ── CHANGE 5: Person-name heuristic ───────────────────────────────────────────
# A UPI receiver that looks like a person (first + last name, no digits,
# no known merchant keywords) is treated as a personal transfer candidate.
_MERCHANT_KEYWORDS = re.compile(
    r"\b(store|shop|mart|foods|kitchen|cafe|pvt|ltd|solutions|services|"
    r"enterprises|technologies|consultancy|agency|studio|lab|clinic|hospital|"
    r"pharmacy|pay|payments|bank|finance|credit|capital|ventures)\b",
    re.IGNORECASE,
)
_PERSON_PATTERN = re.compile(r"^[A-Za-z]{2,}\s+[A-Za-z]{2,}(\s+[A-Za-z]{2,})?$")


def _looks_like_person(receiver: str) -> bool:
    """True if receiver text resembles a human name (not a merchant)."""
    cleaned = _UPI_HANDLE.sub("", receiver).strip()
    if _MERCHANT_KEYWORDS.search(cleaned):
        return False
    return bool(_PERSON_PATTERN.match(cleaned.strip()))


def preprocess_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text    = _HANDLE_PREFIX.sub("", text).strip()
    after   = _UPI_HANDLE.sub("", text).strip()
    working = after if len(after) >= 3 else text

    # --- NEW: Strip gateway prefixes that bleed into the name ---
    working = re.sub(r"\b(paytmqr|gpay-|paytm\.)[a-zA-Z0-9-]+\b", " ", working, flags=re.IGNORECASE)

    working = re.sub(r"[/@.]",        " ", working)
    working = re.sub(r"\b\d{4,}\b",   " ", working)
    working = _NOISE_WORDS.sub(" ",       working)
    working = re.sub(r"\s+",          " ", working).strip()
    return working.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Metadata feature extraction  (UNCHANGED in signature)
# ─────────────────────────────────────────────────────────────────────────────

def extract_metadata(amount: float, timestamp=None, tx_frequency_30d: int = 0) -> np.ndarray:
    log_amount = float(np.log1p(max(amount, 0)))
    if timestamp is not None:
        if isinstance(timestamp, str):
            try: 
                ts = datetime.fromisoformat(timestamp[:19])
            except Exception: 
                ts = datetime.now(timezone.utc)
        else:
            ts = timestamp
    else:
        ts = datetime.now(timezone.utc)
        
    # Standardize hour to sine/cosine for cyclical time
    hour_sin = float(np.sin(2 * np.pi * ts.hour / 24))
    hour_cos = float(np.cos(2 * np.pi * ts.hour / 24))
    # Standardize Day of Week (0 to 1)
    dow_norm = ts.weekday() / 6.0
    # Standardize Frequency
    freq_norm = float(np.log1p(tx_frequency_30d)) / np.log1p(30)
    
    return np.array([log_amount, hour_sin, hour_cos, dow_norm, freq_norm], dtype=np.float32)




# ─────────────────────────────────────────────────────────────────────────────
# History feature extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_history_features(
    category_dist: dict[str, int],
    last_category: Optional[str] = None,
) -> np.ndarray:
    vec   = np.zeros(len(CATEGORIES), dtype=np.float32)
    total = sum(category_dist.values())
    if total > 0:
        for i, cat in enumerate(CATEGORIES):
            vec[i] = category_dist.get(cat, 0) / total
    if last_category and last_category in CATEGORIES:
        idx    = CATEGORIES.index(last_category)
        vec[idx] = min(vec[idx] + 0.10, 1.0)
        s = vec.sum()
        if s > 0:
            vec /= s
    return vec


# ─────────────────────────────────────────────────────────────────────────────
# Override store
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class UserOverrideStore:
    _overrides: dict = field(default_factory=dict)

    def set(self, user_id: str, receiver: str, category: str):
        self._overrides[(_norm(user_id), _norm(receiver))] = category

    def get(self, user_id: str, receiver: str) -> Optional[str]:
        return self._overrides.get((_norm(user_id), _norm(receiver)))

@dataclass
class HistoryStore:
    _entries: dict = field(default_factory=lambda: defaultdict(list))
    _last:    dict = field(default_factory=dict)
    
    def record(self, user_id: str, receiver: str, category: str):
        key = (_norm(user_id), _norm(receiver))
        self._entries[key].append((category, time.time()))
        self._last[key] = category

    def get(self, user_id: str, receiver: str) -> tuple[dict, Optional[str]]:
        key     = (_norm(user_id), _norm(receiver))
        entries = self._entries.get(key, [])
        if not entries:
            return {}, self._last.get(key)
        sorted_entries = sorted(entries, key=lambda x: x[1], reverse=True)
        weighted: dict[str, float] = {}
        for pos, (cat, _ts) in enumerate(sorted_entries):
            w = 0.85 ** pos # decay
            weighted[cat] = weighted.get(cat, 0.0) + w
        return weighted, self._last.get(key)

    def frequency(self, user_id: str, receiver: str) -> int:
        key = (_norm(user_id), _norm(receiver))
        return len(self._entries.get(key, []))

    def record_amount(self, user_id: str, receiver: str, amount: float):
        key = (_norm(user_id), _norm(receiver))
        if not hasattr(self, "_amounts"):
            self._amounts = defaultdict(list)
        self._amounts[key].append(amount)

    def get_amounts(self, user_id: str, receiver: str) -> list[float]:
        if not hasattr(self, "_amounts"):
            return []
        key = (_norm(user_id), _norm(receiver))
        return self._amounts.get(key, [])

def _load_history_from_db(user_ids: list[str]):
    """
    Pulls recent category_feedback from Supabase to populate
    the _history_store and _override_store dynamically for the current predict batch!
    Fixes the cross-process Celery worker disconnect!
    """
    if not user_ids: return
    
    unique_users = list(set(user_ids))
    try:
        res = supabase_admin.table("category_feedback") \
            .select("user_id, merchant, corrected_category, amount, corrected_at") \
            .in_("user_id", unique_users) \
            .order("corrected_at", desc=True) \
            .limit(1000) \
            .execute()
        
        if not res.data:
            return
            
        # Repopulate singleton caches with DB truth
        _history_store._entries.clear()
        _history_store._last.clear()
        if hasattr(_history_store, "_amounts"):
            _history_store._amounts.clear()
        _override_store._overrides.clear()
        
        for row in reversed(res.data): # oldest to newest so newest is .last
            u = row.get("user_id", "")
            m = row.get("merchant", "")
            c = row.get("corrected_category", "")
            a = float(row.get("amount") or 0.0)
            
            _history_store.record(u, m, c)
            if a > 0:
                _history_store.record_amount(u, m, a)
            _override_store.set(u, m, c)
            
    except Exception as e:
        log.error(f"[DB History Load] Failed: {e}")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower())


_override_store = UserOverrideStore()
_history_store  = HistoryStore()

# ─────────────────────────────────────────────────────────────────────────────
# Online learning buffer
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class OnlineBuffer:
    _is_running: bool = False
    # ── CHANGE 8: store metadata alongside text so online refit can use it ────
    _samples:   list = field(default_factory=list)   # [{raw_text, category, metadata}]
    _lock:      threading.Lock = field(default_factory=threading.Lock)

    def add(self, raw_text: str, category: str, metadata: Optional[np.ndarray] = None):
        with self._lock:
            self._samples.append({
                "raw_text": raw_text,
                "category": category,
                "metadata": metadata,   # may be None for legacy callers
            })
            count = len(self._samples)


    def drain(self) -> list[dict]:
        with self._lock:
            samples = list(self._samples)
            self._samples.clear()
        return samples

    def size(self) -> int:
        return len(self._samples)


_online_buffer = OnlineBuffer()


# ─────────────────────────────────────────────────────────────────────────────
# Core model state
# ─────────────────────────────────────────────────────────────────────────────

_model_lock    = threading.Lock()
_clf           = None
_tfidf_char    = None
_tfidf_word    = None
_label_encoder = None
_model_loaded  = False


import os.path
_last_model_mtime = 0

def _load_model():
    global _last_model_mtime
    global _clf, _tfidf_char, _tfidf_word, _label_encoder, _model_loaded
    with _model_lock:
        if _model_loaded and os.path.getmtime(MODEL_PATH) == _last_model_mtime:
            return
            
        if not os.path.exists(MODEL_PATH):
            log.warning("No trained model found on disk.")
            return

        bundle = joblib.load(MODEL_PATH)
        saved_meta_dim = bundle.get("metadata_dim", 0)

        # DIMENSION GUARD
        if saved_meta_dim != METADATA_DIM:
            log.warning(f"Metadata mismatch: saved={saved_meta_dim}, current={METADATA_DIM}. Re-initializing.")
            try:
                if os.path.exists(MODEL_PATH):
                    os.remove(MODEL_PATH)
                if os.path.exists(ENCODER_PATH):
                    os.remove(ENCODER_PATH)
            except OSError as e:
                log.error(f"Could not delete stale model files: {e}")
            
            _model_loaded = False
            return  # Exit so the app can attempt to bootstrap or wait for new files

        # Load the real model
        _clf = bundle["clf"]
        _tfidf_char = bundle["tfidf_char"]
        _tfidf_word = bundle["tfidf_word"]
        _label_encoder = joblib.load(ENCODER_PATH)
        _model_loaded = True
        _last_model_mtime = os.path.getmtime(MODEL_PATH)
        log.info(f"Model loaded from disk (metadata_dim={saved_meta_dim})")


def _ensure_loaded():
    if not _model_loaded:
        _load_model()
    _load_embeddings()


# ─────────────────────────────────────────────────────────────────────────────
# Feature builder
# ── CHANGE 10: metadata is now a required parameter (defaults to zeros) ───────
# All three callers (predict_full, predict_batch, _online_refit) pass
# metadata explicitly.  The function signature is backward-compatible
# (meta_arr defaults to None → zeros).
# ─────────────────────────────────────────────────────────────────────────────

# def _build_text_features(
#     raw_texts: list[str],
#     meta_arrs: Optional[list[np.ndarray]] = None,
# ) -> np.ndarray:
#     """
#     Returns feature matrix: TF-IDF (char+word) + embeddings + metadata.
#     meta_arrs: list of METADATA_DIM arrays, one per text.
#                If None, zeros are used (cold/legacy path).
#     """
#     processed   = [preprocess_text(t) for t in raw_texts]
#     char_feats  = _tfidf_char.transform(processed)
#     word_feats  = _tfidf_word.transform(processed)
#     text_dense  = hstack([char_feats, word_feats]).toarray().astype(np.float32)
#     emb         = get_embeddings(processed)

#     # Build metadata block
#     N = len(raw_texts)
#     if meta_arrs and len(meta_arrs) == N:
#         meta_block = np.stack(meta_arrs).astype(np.float32)
#     else:
#         meta_block = np.zeros((N, METADATA_DIM), dtype=np.float32)

#     return np.concatenate([text_dense, emb, meta_block], axis=1)

def _build_text_features(raw_texts: list[str], meta_arrs: Optional[list[np.ndarray]] = None):
    processed = [preprocess_text(t) for t in raw_texts]
    
    # 1. Transform text
    char_feats = _tfidf_char.transform(processed)
    word_feats = _tfidf_word.transform(processed)
    text_sparse = hstack([char_feats, word_feats])
    
    # 2. Get embeddings
    emb = get_embeddings(processed)

    # 3. Build metadata block
    N = len(raw_texts)
    if meta_arrs and len(meta_arrs) == N:
        meta_block = np.stack(meta_arrs).astype(np.float32)
    else:
        meta_block = np.zeros((N, METADATA_DIM), dtype=np.float32)

    # 4. Return as CSR Sparse Matrix (CRITICAL for model compatibility)
    return hstack([text_sparse, emb, meta_block]).tocsr()


# ─────────────────────────────────────────────────────────────────────────────
# Cold-start: embedding cosine similarity against anchors
# ─────────────────────────────────────────────────────────────────────────────

def _cold_start_predict(raw_texts: list[str]) -> list[tuple[str, float]]:
    _load_embeddings()
    if not _embeddings_available or _anchor_matrix is None:
        return [("Other", 0.0)] * len(raw_texts)

    processed = [preprocess_text(t) for t in raw_texts]
    embs      = normalize(get_embeddings(processed), norm="l2")
    sims      = embs @ _anchor_matrix.T

    def softmax(x):
        e = np.exp((x - x.max()) * 10)
        return e / e.sum()

    results = []
    for row in sims:
        proba      = softmax(row)
        top_idx    = int(np.argmax(proba))
        confidence = float(proba[top_idx])
        category   = CATEGORIES[top_idx]
        if confidence < CONFIDENCE_THRESHOLD:
            results.append(("Other", confidence))
        else:
            results.append((category, confidence))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Online refit
# ── CHANGE 11: includes metadata in feature construction ──────────────────────
# ─────────────────────────────────────────────────────────────────────────────

def _online_refit(bulk_samples=None):
    global _clf, _model_loaded

    samples = bulk_samples or _online_buffer.drain()
    if not samples:
        return

    _ensure_loaded()
    if not _model_loaded:
        log.info(f"[online] No base model yet — buffering {len(samples)} corrections")
        for s in samples:
            _online_buffer._samples.append(s)
        return

    log.info(f"[online] Warm-start refit on {len(samples)} corrections...")
    t0 = time.perf_counter()

    texts     = [s["raw_text"] for s in samples]
    labels    = [s["category"] for s in samples]
    # ── CHANGE 12: use stored metadata if available, else zeros ──────────────
    meta_arrs = [
        s.get("metadata") if s.get("metadata") is not None else np.zeros(METADATA_DIM, dtype=np.float32)
        for s in samples
    ]

    X_new = _build_text_features(texts, meta_arrs=meta_arrs)

    known = set(_label_encoder.classes_)
    
    # Use index-based filtering which works perfectly with Sparse Matrices
    valid_indices = [i for i, y in enumerate(labels) if y in known]
    
    if len(valid_indices) != len(labels):
        log.warning(f"[online] Dropped {len(labels) - len(valid_indices)} samples due to unknown labels")

    if not valid_indices:
        log.warning("[online] No valid labels in correction batch — skipping")
        return

    # Safely slice the sparse matrix
    X_new = X_new[valid_indices]
    y_new = _label_encoder.transform([labels[i] for i in valid_indices])

    if X_new.shape[0] < 10:
        from scipy.sparse import vstack
        # Use scipy's vstack for sparse matrices, NOT np.concatenate
        X_new = vstack([X_new] * 3) 
        y_new = np.concatenate([y_new] * 3)

    with _model_lock:
        _clf.warm_start = True
        _clf.max_iter   = 200
        try:
            _clf.partial_fit(X_new, y_new)
        except Exception as e:
            log.error(f"[online] Refit failed: {e}")
            return

        bundle = joblib.load(MODEL_PATH)
        bundle["clf"]        = _clf
        bundle["updated_at"] = datetime.now(timezone.utc).isoformat()
        bundle["online_samples_applied"] = bundle.get("online_samples_applied", 0) + len(samples)
        joblib.dump(bundle, MODEL_PATH)

    elapsed = (time.perf_counter() - t0) * 1000
    log.info(
        f"[online] Refit complete in {elapsed:.0f}ms — "
        f"{len(samples)} corrections applied, model saved"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pattern-boost layer
# ── CHANGE 13: NEW FUNCTION ───────────────────────────────────────────────────
#
# Applies heuristic probability boosts based on amount + time + frequency.
# Returns a modified probability vector (same length as CATEGORIES).
# Always renormalises so the output is a valid distribution.
#
# Rules fire as soft boosts, not hard overrides — if ML is confident (e.g.,
# 0.9 for "Shopping"), a 0.25 food boost still won't flip the result.
# ─────────────────────────────────────────────────────────────────────────────

def _apply_pattern_boosts(
    proba:    np.ndarray,              # (n_categories,) raw ML probabilities
    receiver: str,
    amount:   float,
    hour:     int,
    freq_30d: int,
    past_amounts: list[float],         # from _history_store.get_amounts()
    label_classes: list[str],
) -> np.ndarray:
    """
    Returns a renormalised probability vector after applying pattern rules.
    All modifications are additive deltas — never zeroing out categories.
    """
    p = proba.copy()

    def _idx(cat: str) -> int:
        return label_classes.index(cat) if cat in label_classes else -1

    # ── Rule 1: Meal-time + small amount → boost Food ─────────────────────────
    in_meal_window = any(lo <= hour <= hi for lo, hi in FOOD_HOUR_RANGES)
    if amount <= FOOD_AMOUNT_MAX and in_meal_window:
        i = _idx("Food")
        if i >= 0:
            p[i] += FOOD_BOOST
            log.debug(f"[pattern] Food boost ({FOOD_BOOST}) | amount={amount} hour={hour}")

    # ── Rule 2: High frequency + stable amount → boost Subscription ───────────
    if freq_30d >= SUBSCRIPTION_FREQ_MIN and len(past_amounts) >= SUBSCRIPTION_FREQ_MIN:
        mean_amt = np.mean(past_amounts)
        std_amt  = np.std(past_amounts)
        cv       = std_amt / mean_amt if mean_amt > 0 else 1.0
        if cv < SUBSCRIPTION_AMOUNT_CV:
            i = _idx("Subscription")
            if i >= 0:
                p[i] += SUBSCRIPTION_BOOST
                log.debug(
                    f"[pattern] Subscription boost ({SUBSCRIPTION_BOOST}) "
                    f"| freq={freq_30d} cv={cv:.3f}"
                )

    # ── Rule 3: Person-name receiver + small amount → boost Transfer ──────────
    if _looks_like_person(receiver) and amount <= TRANSFER_AMOUNT_MAX:
        i = _idx("Transfer")
        if i >= 0:
            p[i] += PERSON_BOOST
            log.debug(f"[pattern] Transfer boost ({PERSON_BOOST}) | receiver='{receiver}'")

    # Renormalise
    s = p.sum()
    if s > 0:
        p /= s
    return p


# ─────────────────────────────────────────────────────────────────────────────
# Post-processing: soft override + history bias + pattern boosts
# ── CHANGE 14: replaces original _postprocess() entirely ─────────────────────
#
# Processing order (each step modifies the probability vector):
#   1. Pattern boosts   — metadata-driven heuristic deltas
#   2. History bias     — recency-weighted category distribution blending
#   3. Soft override    — strong (but not absolute) prior from user correction
#   4. Threshold guard  — low-confidence → "Other"
#
# Returning (cat, conf, final_proba) — same tuple shape as before.
# ─────────────────────────────────────────────────────────────────────────────

def _postprocess(
    ml_probas:     np.ndarray,
    user_ids:      list[str],
    receivers:     list[str],
    label_classes: list[str],
    amounts:       Optional[list[float]]      = None,
    timestamps:    Optional[list]             = None,
    freq_30ds:     Optional[list[int]]        = None,
) -> list[tuple[str, float, np.ndarray]]:
    results = []
    C       = len(label_classes)
    N       = len(user_ids)

    _amounts    = amounts    or [0.0] * N
    _timestamps = timestamps or [None] * N
    _freq_30ds  = freq_30ds  or [0]   * N

    for i, (user_id, receiver) in enumerate(zip(user_ids, receivers)):
        proba    = ml_probas[i].copy()
        amount   = float(_amounts[i] or 0)
        ts       = _timestamps[i]
        freq_30d = int(_freq_30ds[i] or 0)

        # Resolve hour from timestamp
        if ts is not None:
            if isinstance(ts, str):
                try:   dt = datetime.fromisoformat(ts[:19])
                except Exception: dt = datetime.now(timezone.utc)
            elif isinstance(ts, datetime):
                dt = ts
            else:
                dt = datetime.now(timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
        hour = dt.hour

        # ── Step 1: pattern boosts ────────────────────────────────────────────
        past_amounts = _history_store.get_amounts(user_id, receiver)
        proba = _apply_pattern_boosts(
            proba        = proba,
            receiver     = receiver,
            amount       = amount,
            hour         = hour,
            freq_30d     = freq_30d,
            past_amounts = past_amounts,
            label_classes= label_classes,
        )

        # ── Step 2: recency-weighted history bias ─────────────────────────────
        dist, last = _history_store.get(user_id, receiver)
        if dist:
            hist_vec = np.zeros(C, dtype=np.float32)
            total    = sum(dist.values())
            for j, cat in enumerate(label_classes):
                if cat in dist:
                    hist_vec[j] = dist[cat] / total

            proba = (1 - HISTORY_LAMBDA) * proba + HISTORY_LAMBDA * hist_vec
            s = proba.sum()
            if s > 0:
                proba /= s
            log.debug(f"[history] bias applied (λ={HISTORY_LAMBDA})")

        # ── Step 3: soft override (was: hard early return) ────────────────────
        override = _override_store.get(user_id, receiver)
        if override and override in label_classes:
            override_idx = label_classes.index(override)
            # Build one-hot prior for override category
            override_vec = np.zeros(C, dtype=np.float32)
            override_vec[override_idx] = 1.0
            # Blend: give override SOFT_OVERRIDE_WEIGHT, ML gets the rest
            proba = SOFT_OVERRIDE_WEIGHT * override_vec + (1 - SOFT_OVERRIDE_WEIGHT) * proba
            s = proba.sum()
            if s > 0:
                proba /= s
            log.debug(
                f"[override] soft prior for '{override}' "
                f"(weight={SOFT_OVERRIDE_WEIGHT})"
            )

        # ── Step 4: threshold guard ───────────────────────────────────────────
        top_idx    = int(np.argmax(proba))
        confidence = float(proba[top_idx])
        category   = label_classes[top_idx]

        # Strong history still forces confidence floor
        if dist and sum(dist.values()) >= 3:
            results.append((category, max(confidence, 0.6), proba.copy()))
            continue

        if confidence < CONFIDENCE_THRESHOLD:
            results.append(("Other", confidence, proba.copy()))
        else:
            results.append((category, confidence, proba.copy()))

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

from dataclasses import dataclass as _dc


@_dc
class PredictionResult:
    category:   str
    confidence: float
    all_probas: dict
    latency_ms: float
    source:     str   # "override" | "ml+history" | "cold_start" | "low_confidence"


def predict_category(
    raw_text:  str,
    amount:    float         = 0.0,
    timestamp: Optional[str] = None,
    user_id:   Optional[str] = None,
    receiver:  Optional[str] = None,
) -> tuple[str, float]:
    """Drop-in replacement for old categoriser.predict_category()."""
    r = predict_full(raw_text, amount, timestamp, user_id, receiver)
    return r.category, r.confidence


def predict_full(
    raw_text:  str,
    amount:    float         = 0.0,
    timestamp: Optional[str] = None,
    user_id:   Optional[str] = None,
    receiver:  Optional[str] = None,
) -> PredictionResult:
    t0  = time.perf_counter()
    _ensure_loaded()
    _load_history_from_db([user_id])

    uid = user_id or "__anon__"
    rcv = receiver or raw_text

    # ── CHANGE 15: no more fast-path hard override at the top ─────────────────
    # Override is now handled as a soft prior inside _postprocess().
    # The only "fast path" remaining is cold-start (no model on disk).

    # Cold start path
    if not _model_loaded:
        results   = _cold_start_predict([raw_text])
        cat, conf = results[0]
        dist, last = _history_store.get(uid, rcv)
        if dist:
            cat_from_hist = max(dist, key=dist.get)
            if dist[cat_from_hist] >= 2:
                cat, conf = cat_from_hist, min(conf + 0.15, 0.99)
        return PredictionResult(
            category   = cat,
            confidence = conf,
            all_probas = {cat: conf},
            latency_ms = (time.perf_counter() - t0) * 1000,
            source     = "cold_start",
        )

    # ── CHANGE 16: build metadata and pass into features + postprocess ────────
    freq_30d = _history_store.frequency(uid, rcv)
    meta     = extract_metadata(amount, timestamp, tx_frequency_30d=freq_30d)

    X     = _build_text_features([raw_text], meta_arrs=[meta])
    proba = _clf.predict_proba(X)

    label_classes = list(_label_encoder.classes_)
    post_results  = _postprocess(
        ml_probas  = proba,
        user_ids   = [uid],
        receivers  = [rcv],
        label_classes = label_classes,
        amounts    = [amount],
        timestamps = [timestamp],
        freq_30ds  = [freq_30d],
    )
    (cat, conf, final_proba) = post_results[0]

    all_probas = {label_classes[j]: float(final_proba[j]) for j in range(len(label_classes))}
    source     = "low_confidence" if cat == "Other" else "ml+history"

    return PredictionResult(
        category   = cat,
        confidence = conf,
        all_probas = all_probas,
        latency_ms = (time.perf_counter() - t0) * 1000,
        source     = source,
    )


def predict_batch(
    raw_texts:  list[str],
    amounts:    Optional[list[float]] = None,
    timestamps: Optional[list]        = None,
    user_ids:   Optional[list[str]]   = None,
    receivers:  Optional[list[str]]   = None,
) -> list[tuple[str, float]]:
    """Batch prediction. Drop-in for old categoriser.predict_batch()."""
    if not raw_texts:
        return []

    _ensure_loaded()
    _load_history_from_db(user_ids)
    N          = len(raw_texts)
    _user_ids  = user_ids  or ["__anon__"] * N
    _receivers = receivers or raw_texts
    _amounts   = amounts   or [0.0] * N

    if not _model_loaded:
        return _cold_start_predict(raw_texts)

    # ── CHANGE 17: build per-row metadata for batch ───────────────────────────
    freq_30ds = [_history_store.frequency(uid, rcv)
                 for uid, rcv in zip(_user_ids, _receivers)]
    meta_arrs = [
        extract_metadata(_amounts[j], timestamps[j] if timestamps else None, freq_30ds[j])
        for j in range(N)
    ]

    X             = _build_text_features(raw_texts, meta_arrs=meta_arrs)
    probas        = _clf.predict_proba(X)
    label_classes = list(_label_encoder.classes_)

    results = _postprocess(
        ml_probas  = probas,
        user_ids   = _user_ids,
        receivers  = _receivers,
        label_classes = label_classes,
        amounts    = _amounts,
        timestamps = timestamps,
        freq_30ds  = freq_30ds,
    )
    return [(cat, conf) for (cat, conf, _) in results]


def record_feedback(
    user_id:            str,
    receiver:           str,
    raw_text:           str,
    corrected_category: str,
    amount:             float = 0.0,
    timestamp:          Optional[str] = None,
):
    """
    Called on every user correction.
      1. Override store  → soft prior for future predictions
      2. History store   → recency-weighted distribution + amount log
      3. Online buffer   → triggers background warm refit (with metadata)
    """
    _override_store.set(user_id, receiver, corrected_category)
    _history_store.record(user_id, receiver, corrected_category)
    # ── CHANGE 18: record amount for subscription CV detection ────────────────
    if amount:
        _history_store.record_amount(user_id, receiver, amount)

    # ── CHANGE 19: pass metadata into online buffer ───────────────────────────
    freq_30d = _history_store.frequency(user_id, receiver)
    meta     = extract_metadata(amount, timestamp, tx_frequency_30d=freq_30d)
    _online_buffer.add(raw_text, corrected_category, metadata=meta)

    log.info(
        f"Feedback: user={user_id[:8]}… receiver={receiver[:25]} "
        f"→ {corrected_category} | buffer={_online_buffer.size()}"
    )


def get_categories() -> list[str]:
    return CATEGORIES[:]


def model_info() -> dict:
    _ensure_loaded()
    info = {
        "model_loaded":     _model_loaded,
        "embeddings_ok":    _embeddings_available,
        "buffer_pending":   _online_buffer.size(),
        "online_threshold": ONLINE_UPDATE_THRESHOLD,
        "metadata_dim":     METADATA_DIM,
    }
    if _model_loaded and os.path.exists(MODEL_PATH):
        bundle = joblib.load(MODEL_PATH)
        info["trained_at"]             = bundle.get("trained_at")
        info["updated_at"]             = bundle.get("updated_at")
        info["online_samples_applied"] = bundle.get("online_samples_applied", 0)
    return info


# ─────────────────────────────────────────────────────────────────────────────
# ETL integration
# ── CHANGE 20: pass amount to record_feedback so amount history is built ──────
# ─────────────────────────────────────────────────────────────────────────────

def run_categorise_silver(user_id: str, supabase_admin) -> int:
    log.info(f"Categorising silver rows for user {user_id}")

    uncategorised = supabase_admin.table("silver_transactions") \
        .select("id, merchant, bronze_id, amount, transaction_date") \
        .eq("user_id", user_id) \
        .eq("is_categorised", False) \
        .execute()

    if not uncategorised.data:
        log.info("No uncategorised silver rows")
        return 0

    bronze_ids = [r["bronze_id"] for r in uncategorised.data if r.get("bronze_id")]
    bronze_map: dict = {}
    if bronze_ids:
        bronze_res = supabase_admin.table("bronze_transactions") \
            .select("id, receiver, timestamp") \
            .in_("id", bronze_ids) \
            .execute()
        bronze_map = {r["id"]: r for r in bronze_res.data}

    silver_rows = uncategorised.data
    raw_texts, amounts, timestamps, receivers = [], [], [], []
    for row in silver_rows:
        bronze = bronze_map.get(row.get("bronze_id"), {})
        raw_texts.append(bronze.get("receiver") or row.get("merchant", ""))
        amounts.append(float(row.get("amount", 0) or 0))
        timestamps.append(bronze.get("timestamp") or row.get("transaction_date"))
        receivers.append(bronze.get("receiver") or row.get("merchant", ""))

    predictions = predict_batch(
        raw_texts  = raw_texts,
        amounts    = amounts,
        timestamps = timestamps,
        user_ids   = [user_id] * len(silver_rows),
        receivers  = receivers,
    )

    categorised = other_count = 0
    for row, (category, confidence), receiver, amount in zip(
        silver_rows, predictions, receivers, amounts
    ):
        is_categorised = category != "Other"
        supabase_admin.table("silver_transactions").update({
            "category":       category,
            "is_categorised": is_categorised,
        }).eq("id", row["id"]).execute()

        if is_categorised:
            _history_store.record(user_id, receiver, category)
            _history_store.record_amount(user_id, receiver, amount)  # CHANGE 21
            categorised += 1
        else:
            other_count += 1

    log.info(f"Categorised: {categorised} | Other: {other_count}")
    return categorised


# ─────────────────────────────────────────────────────────────────────────────
# Optional full retrain
# ── CHANGE 22: metadata block appended to training features ───────────────────
# ─────────────────────────────────────────────────────────────────────────────

def train(include_real: bool = False):
    t0 = time.perf_counter()

    if not os.path.exists(SYNTHETIC_PATH):
        log.warning("No synthetic data — skipping full train. Use online learning.")
        return

    df = pd.read_csv(SYNTHETIC_PATH)
    log.info(f"Synthetic rows: {len(df)}")

    if include_real and os.path.exists(REAL_PATH):
        real_df = pd.read_csv(REAL_PATH)
        real_df = pd.concat([real_df] * 3, ignore_index=True)
        df      = pd.concat([df, real_df], ignore_index=True)
        log.info(f"Combined rows (real weighted 3×): {len(df)}")

    assert "raw_text" in df.columns and "category" in df.columns
    df = df.dropna(subset=["raw_text", "category"])
    df["raw_text"] = df["raw_text"].astype(str)
    df["category"] = df["category"].astype(str).str.strip()

    counts = df["category"].value_counts()
    parts  = [df]
    for cat, count in counts.items():
        if count < 100:
            parts.append(resample(df[df["category"]==cat], replace=True, n_samples=100, random_state=42))
    df = pd.concat(parts, ignore_index=True).sample(frac=1, random_state=42)

    log.info("Class distribution:")
    for cat, cnt in df["category"].value_counts().items():
        log.info(f"  {cat:<22} {cnt}")

    processed = [preprocess_text(t) for t in df["raw_text"]]

    le = LabelEncoder()
    y  = le.fit_transform(df["category"])

    tfidf_char = TfidfVectorizer(
        analyzer="char_wb", ngram_range=(2,5), max_features=TFIDF_CHAR_MAX,
        sublinear_tf=True, strip_accents="unicode", lowercase=True, min_df=2,
    )
    tfidf_word = TfidfVectorizer(
        analyzer="word", ngram_range=(1,2), max_features=TFIDF_WORD_MAX,
        sublinear_tf=True, strip_accents="unicode", lowercase=True, min_df=2,
    )

    char_feats = tfidf_char.fit_transform(processed)
    word_feats = tfidf_word.fit_transform(processed)
    text_feats = hstack([char_feats, word_feats]).toarray().astype(np.float32)

    _load_embeddings()
    if _embeddings_available:
        log.info("Generating embeddings...")
        emb = get_embeddings(processed)
    else:
        emb = np.zeros((len(processed), EMBEDDING_DIM), dtype=np.float32)

    # ── CHANGE 22: zero metadata for training rows (no amount data in CSV) ────
    # If synthetic.csv gains amount/hour columns later, populate these instead.
    meta_block = np.zeros((len(processed), METADATA_DIM), dtype=np.float32)
    if "amount" in df.columns:
        for j, row in enumerate(df.itertuples()):
            meta_block[j] = extract_metadata(float(row.amount or 0))

    X = np.concatenate([text_feats, emb, meta_block], axis=1)
    log.info(f"Feature matrix: {X.shape}")

    clf = SGDClassifier(loss="log_loss", class_weight="balanced", random_state=42)
    clf.partial_fit(X, y, classes=np.unique(y))

    global _clf
    _clf = clf

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump({
        "clf":                    clf,
        "tfidf_char":             tfidf_char,
        "tfidf_word":             tfidf_word,
        "embedding_dim":          EMBEDDING_DIM,
        "metadata_dim":           METADATA_DIM,
        "trained_at":             datetime.now(timezone.utc).isoformat(),
        "online_samples_applied": 0,
    }, MODEL_PATH)
    joblib.dump(le, ENCODER_PATH)

    log.info(f"Model saved → {MODEL_PATH}  ({time.perf_counter()-t0:.1f}s)")
    global _model_loaded
    _model_loaded = False


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--train",        action="store_true")
    p.add_argument("--include-real", action="store_true")
    p.add_argument("--info",         action="store_true", help="Show model status")
    args = p.parse_args()

    if args.train:
        train(include_real=args.include_real)
    if args.info:
        import json
        print(json.dumps(model_info(), indent=2, default=str))