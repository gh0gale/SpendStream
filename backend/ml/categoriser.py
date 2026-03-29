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

print("🔥 USING categoriser.py FROM:", __file__)

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
# kept for optional full retrain — no longer required for normal operation
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

CONFIDENCE_THRESHOLD    = 0.45   # below → "Other"
HISTORY_LAMBDA          = 0.40   # weight of history bias
ONLINE_UPDATE_THRESHOLD = 5      # trigger warm refit after N new corrections
TFIDF_CHAR_MAX          = 40_000
TFIDF_WORD_MAX          = 20_000
EMBEDDING_DIM           = 384

# ─────────────────────────────────────────────────────────────────────────────
# Anchor phrases for cold-start embedding similarity
# Used when no trained classifier exists yet.
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
_anchor_matrix        = None   # (n_categories, 384) — precomputed anchor embeddings


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
    """Build (n_cats, 384) anchor matrix for cold-start similarity."""
    global _anchor_matrix
    vecs = []
    for cat in CATEGORIES:
        phrases = CATEGORY_ANCHORS.get(cat, [cat])
        embs    = _embedding_model.encode(phrases, show_progress_bar=False)
        vecs.append(embs.mean(axis=0))
    _anchor_matrix = normalize(np.stack(vecs), norm="l2")
    log.info(f"Anchor matrix built: {_anchor_matrix.shape}")


def get_embeddings(texts: list[str]) -> np.ndarray:
    """Returns (N, 384) float32. Falls back to zeros if unavailable."""
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
    r"payment|online|india|tech|services|w|g)\b",
    re.IGNORECASE
)


def preprocess_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text    = _HANDLE_PREFIX.sub("", text).strip()
    after   = _UPI_HANDLE.sub("", text).strip()
    working = after if len(after) >= 3 else text
    working = re.sub(r"[/@.]",        " ", working)
    working = re.sub(r"\b\d{4,}\b",   " ", working)
    working = _NOISE_WORDS.sub(" ",       working)
    working = re.sub(r"\s+",          " ", working).strip()
    return working.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Metadata feature extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_metadata(
    amount:           float,
    timestamp:        Optional[str | datetime] = None,
    tx_frequency_30d: int = 0,
) -> np.ndarray:
    log_amount = float(np.log1p(max(amount, 0)))
    if timestamp is not None:
        if isinstance(timestamp, str):
            try:   ts = datetime.fromisoformat(timestamp[:19])
            except Exception: ts = datetime.now(timezone.utc)
        else:
            ts = timestamp
    else:
        ts = datetime.now(timezone.utc)
    hour     = ts.hour
    dow      = ts.weekday()
    hour_sin = float(np.sin(2 * np.pi * hour / 24))
    hour_cos = float(np.cos(2 * np.pi * hour / 24))
    dow_norm = dow / 6.0
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


# ─────────────────────────────────────────────────────────────────────────────
# History store
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class HistoryStore:
    _dist: dict = field(default_factory=lambda: defaultdict(Counter))
    _last: dict = field(default_factory=dict)

    def record(self, user_id: str, receiver: str, category: str):
        key = (_norm(user_id), _norm(receiver))
        self._dist[key][category] += 1
        self._last[key] = category

    def get(self, user_id: str, receiver: str) -> tuple[dict, Optional[str]]:
        key = (_norm(user_id), _norm(receiver))
        return dict(self._dist.get(key, {})), self._last.get(key)

    def frequency(self, user_id: str, receiver: str) -> int:
        key = (_norm(user_id), _norm(receiver))
        return sum(self._dist.get(key, {}).values())


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower())


_override_store = UserOverrideStore()
_history_store  = HistoryStore()

# ─────────────────────────────────────────────────────────────────────────────
# Online learning buffer
# Accumulates corrections → triggers warm refit automatically
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class OnlineBuffer:
    """
    Stores correction samples for online warm-start retraining.
    When len >= ONLINE_UPDATE_THRESHOLD, triggers _online_refit().
    """
    #update1
    _is_running: bool = False
    _samples:   list = field(default_factory=list)   # [{raw_text, category}]
    _lock:      threading.Lock = field(default_factory=threading.Lock)

    def add(self, raw_text: str, category: str):
        with self._lock:
            self._samples.append({"raw_text": raw_text, "category": category})
            count = len(self._samples)
        #upadate1
        # if count >= ONLINE_UPDATE_THRESHOLD:
        #     # Run refit in background so API call returns instantly
        #     threading.Thread(target=_online_refit, daemon=True).start()
        if count >= ONLINE_UPDATE_THRESHOLD and not self._is_running:
            self._is_running = True

            def run():
                try:
                    _online_refit()
                finally:
                    self._is_running = False

            threading.Thread(target=run, daemon=True).start()

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


def _load_model():
    global _clf, _tfidf_char, _tfidf_word, _label_encoder, _model_loaded
    with _model_lock:
        if _model_loaded:
            return
        if not os.path.exists(MODEL_PATH):
            log.warning("No trained model found — initializing new SGD model")

            

            # Minimal bootstrap data
            texts = ["init"]
            labels = ["Other"]

            processed = [preprocess_text(t) for t in texts]

            _tfidf_char = TfidfVectorizer(analyzer="char_wb", ngram_range=(2,5))
            _tfidf_word = TfidfVectorizer(analyzer="word", ngram_range=(1,2))

            char_feats = _tfidf_char.fit_transform(processed)
            word_feats = _tfidf_word.fit_transform(processed)
            text_feats = hstack([char_feats, word_feats]).toarray().astype(np.float32)

            emb = get_embeddings(processed)
            X = np.concatenate([text_feats, emb], axis=1)

            _label_encoder = LabelEncoder()
            _label_encoder.fit(CATEGORIES)

            y = _label_encoder.transform(labels)

            _clf = SGDClassifier(
                loss="log_loss",
                random_state=42
            )

            # IMPORTANT: initialize with ALL classes
            _clf.partial_fit(X, y, classes=np.arange(len(_label_encoder.classes_)))

            # Save immediately
            joblib.dump({
                "clf": _clf,
                "tfidf_char": _tfidf_char,
                "tfidf_word": _tfidf_word,
                "embedding_dim": EMBEDDING_DIM,
                "trained_at": datetime.now(timezone.utc).isoformat(),
                "online_samples_applied": 0,
            }, MODEL_PATH)

            joblib.dump(_label_encoder, ENCODER_PATH)

            _model_loaded = True
            log.info("✅ Initialized new model (no synthetic data)")
            return
            return
        bundle         = joblib.load(MODEL_PATH)
        _clf           = bundle["clf"]
        _tfidf_char    = bundle["tfidf_char"]
        _tfidf_word    = bundle["tfidf_word"]
        _label_encoder = joblib.load(ENCODER_PATH)
        _model_loaded  = True
        log.info("Model loaded from disk")


def _ensure_loaded():
    if not _model_loaded:
        _load_model()
    _load_embeddings()


# ─────────────────────────────────────────────────────────────────────────────
# Feature builder (text + embeddings only — consistent with training)
# ─────────────────────────────────────────────────────────────────────────────

def _build_text_features(raw_texts: list[str]) -> np.ndarray:
    processed   = [preprocess_text(t) for t in raw_texts]
    char_feats  = _tfidf_char.transform(processed)
    word_feats  = _tfidf_word.transform(processed)
    text_dense  = hstack([char_feats, word_feats]).toarray().astype(np.float32)
    emb         = get_embeddings(processed)
    return np.concatenate([text_dense, emb], axis=1)


# ─────────────────────────────────────────────────────────────────────────────
# Cold-start: embedding cosine similarity against anchors
# Used when no trained classifier exists
# ─────────────────────────────────────────────────────────────────────────────

def _cold_start_predict(raw_texts: list[str]) -> list[tuple[str, float]]:
    """
    Predicts purely from embedding similarity to category anchor phrases.
    No training data required. Accuracy ~70-80% for clear merchants.
    """
    _load_embeddings()
    if not _embeddings_available or _anchor_matrix is None:
        return [("Other", 0.0)] * len(raw_texts)

    processed = [preprocess_text(t) for t in raw_texts]
    embs      = normalize(get_embeddings(processed), norm="l2")   # (N, 384)
    sims      = embs @ _anchor_matrix.T                           # (N, n_cats)

    # Softmax over similarities to get probabilities
    def softmax(x):
        e = np.exp((x - x.max()) * 10)   # scale=10 sharpens distribution
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
# Online refit — warm_start LogReg update on correction buffer
# Runs in a background thread, triggered automatically
# ─────────────────────────────────────────────────────────────────────────────

def _online_refit():
    
    """
    Warm-start refit of the existing classifier on buffered corrections.

    Strategy:
      - Drain the correction buffer
      - Build feature vectors for corrections only
      - Call clf.fit() with warm_start=True — re-uses existing weights
        as starting point, converges faster, preserves existing knowledge
      - Overwrite model on disk atomically
      - Reload in-memory model

    This runs in a background daemon thread so predictions are never blocked.
    """
    global _clf, _model_loaded

    samples = _online_buffer.drain()
    if not samples:
        return

    _ensure_loaded()
    if not _model_loaded:
        log.info(f"[online] No base model yet — buffering {len(samples)} corrections")
        # Put them back so they accumulate for the first full train
        for s in samples:
            _online_buffer._samples.append(s)
        return

    log.info(f"[online] Warm-start refit on {len(samples)} corrections...")
    t0 = time.perf_counter()

    texts  = [s["raw_text"]  for s in samples]
    labels = [s["category"]  for s in samples]

    # Build features using existing (frozen) TF-IDF + embeddings
    processed  = [preprocess_text(t) for t in texts]
    char_feats = _tfidf_char.transform(processed)
    word_feats = _tfidf_word.transform(processed)
    text_dense = hstack([char_feats, word_feats]).toarray().astype(np.float32)
    emb        = get_embeddings(processed)
    X_new      = np.concatenate([text_dense, emb], axis=1)

    # Encode labels — must use existing encoder to preserve class indices
    known   = set(_label_encoder.classes_)
    #upadate1
    # valid   = [(x, y) for x, y in zip(X_new, labels) if y in known]
    valid = [(x, y) for x, y in zip(X_new, labels) if y in known]
    if len(valid) != len(labels):
        log.warning(f"[online] Dropped {len(labels) - len(valid)} samples due to unknown labels")

    if not valid:
        log.warning("[online] No valid labels in correction batch — skipping")
        return

    X_new = np.array([v[0] for v in valid])
    y_new = _label_encoder.transform([v[1] for v in valid])

    # Upsample corrections 3× to give them more weight vs original training
    if len(X_new) < 10:
        X_new = np.concatenate([X_new] * 3)
        y_new = np.concatenate([y_new] * 3)

    with _model_lock:
        _clf.warm_start = True
        _clf.max_iter   = 200   # fewer iters — just nudge, not full retrain
        try:
            _clf.partial_fit(X_new, y_new)
        except Exception as e:
            log.error(f"[online] Refit failed: {e}")
            return

        # Save updated model to disk
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
# Post-processing: history bias + override priority
# ─────────────────────────────────────────────────────────────────────────────
def _postprocess(
    ml_probas:     np.ndarray,
    user_ids:      list[str],
    receivers:     list[str],
    label_classes: list[str],
) -> list[tuple[str, float, np.ndarray]]:
    results = []
    C       = len(label_classes)

    for i, (user_id, receiver) in enumerate(zip(user_ids, receivers)):
        # Priority 1 — user override
        override = _override_store.get(user_id, receiver)
        if override:
            # create dummy proba (one-hot)
            proba_override = np.zeros(C, dtype=np.float32)
            if override in label_classes:
                idx = label_classes.index(override)
                proba_override[idx] = 1.0
            results.append((override, 1.0, proba_override))
            continue

        proba = ml_probas[i].copy()

        # Priority 2 — history bias
        dist, last = _history_store.get(user_id, receiver)
        if dist:
            hist_vec = np.zeros(C, dtype=np.float32)
            total = sum(dist.values())

            for j, cat in enumerate(label_classes):
                if cat in dist:
                    hist_vec[j] = dist[cat] / total

            proba = (1 - HISTORY_LAMBDA) * proba + HISTORY_LAMBDA * hist_vec
            print("DEBUG: HISTORY APPLIED", HISTORY_LAMBDA)

            s = proba.sum()
            if s > 0:
                proba /= s

        top_idx    = int(np.argmax(proba))
        confidence = float(proba[top_idx])
        category   = label_classes[top_idx]

        # 🔥 If strong history, trust it
        if dist and sum(dist.values()) >= 3:
            results.append((category, max(confidence, 0.6), proba.copy()))
            continue

        # fallback to normal threshold
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

    uid = user_id or "__anon__"
    rcv = receiver or raw_text

    # Fast path — override
    override = _override_store.get(uid, rcv)
    if override:
        return PredictionResult(
            category   = override,
            confidence = 1.0,
            all_probas = {override: 1.0},
            latency_ms = (time.perf_counter() - t0) * 1000,
            source     = "override",
        )

    # Cold start path — no trained model
    if not _model_loaded:
        results = _cold_start_predict([raw_text])
        cat, conf = results[0]
        # Apply history bias manually
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

    # Normal ML path
    X     = _build_text_features([raw_text])
    proba = _clf.predict_proba(X)
    label_classes = list(_label_encoder.classes_)
    #update1
    # (cat, conf),  = _postprocess(proba, [uid], [rcv], label_classes)
    post_results = _postprocess(proba, [uid], [rcv], label_classes)
    # (cat, conf) = post_results[0]
    (cat, conf, final_proba) = post_results[0]

    # all_probas = {label_classes[j]: float(proba[0][j]) for j in range(len(label_classes))}
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
    N          = len(raw_texts)
    _user_ids  = user_ids  or ["__anon__"] * N
    _receivers = receivers or raw_texts

    # Cold start
    if not _model_loaded:
        results = _cold_start_predict(raw_texts)
        return results

    X             = _build_text_features(raw_texts)
    probas        = _clf.predict_proba(X)
    label_classes = list(_label_encoder.classes_)
    results = _postprocess(probas, _user_ids, _receivers, label_classes)
    return [(cat, conf) for (cat, conf, _) in results]


def record_feedback(
    user_id:            str,
    receiver:           str,
    raw_text:           str,
    corrected_category: str,
):
    """
    Called on every user correction. Does three things instantly:
      1. Override store  → exact match returns corrected_category immediately
      2. History store   → blends into future probability calculations
      3. Online buffer   → triggers background warm refit when threshold reached
    No manual retraining step needed.
    """
    _override_store.set(user_id, receiver, corrected_category)
    _history_store.record(user_id, receiver, corrected_category)
    _online_buffer.add(raw_text, corrected_category)
    log.info(
        f"Feedback: user={user_id[:8]}… receiver={receiver[:25]} "
        f"→ {corrected_category} | buffer={_online_buffer.size()}"
    )


def get_categories() -> list[str]:
    return CATEGORIES[:]


def model_info() -> dict:
    """Returns current model status — useful for health checks."""
    _ensure_loaded()
    info = {
        "model_loaded":    _model_loaded,
        "embeddings_ok":   _embeddings_available,
        "buffer_pending":  _online_buffer.size(),
        "online_threshold": ONLINE_UPDATE_THRESHOLD,
    }
    if _model_loaded and os.path.exists(MODEL_PATH):
        bundle = joblib.load(MODEL_PATH)
        info["trained_at"]             = bundle.get("trained_at")
        info["updated_at"]             = bundle.get("updated_at")
        info["online_samples_applied"] = bundle.get("online_samples_applied", 0)
    return info


# ─────────────────────────────────────────────────────────────────────────────
# ETL integration
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
    for row, (category, confidence), receiver in zip(silver_rows, predictions, receivers):
        is_categorised = category != "Other"
        supabase_admin.table("silver_transactions").update({
            "category":       category,
            "is_categorised": is_categorised,
        }).eq("id", row["id"]).execute()

        if is_categorised:
            _history_store.record(user_id, receiver, category)
            categorised += 1
        else:
            other_count += 1

    log.info(f"Categorised: {categorised} | Other: {other_count}")
    return categorised


# ─────────────────────────────────────────────────────────────────────────────
# Optional full retrain (only needed to rebuild TF-IDF vocabulary)
# python ml/categoriser.py --train
# ─────────────────────────────────────────────────────────────────────────────

def train(include_real: bool = False):
    """
    Full training pipeline. Only needed if:
      - First time setup with synthetic.csv
      - You want to rebuild the TF-IDF vocabulary from a large corpus

    Online learning via record_feedback() handles ongoing adaptation
    without requiring this to be run manually.
    """
    
    t0 = time.perf_counter()

    

    # Load data
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

    # Balance
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
    log.info(f"Categories ({len(le.classes_)}): {list(le.classes_)}")

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
    log.info(f"Text features: {text_feats.shape[1]} dims")

    _load_embeddings()
    if _embeddings_available:
        log.info("Generating embeddings...")
        emb = get_embeddings(processed)
    else:
        emb = np.zeros((len(processed), EMBEDDING_DIM), dtype=np.float32)

    X = np.concatenate([text_feats, emb], axis=1)
    log.info(f"Feature matrix: {X.shape}")

    #update1
    # clf = LogisticRegression(
    #     C=5, max_iter=1000, class_weight="balanced",
    #     solver="lbfgs", random_state=42, warm_start=True,
    # )
    # clf.fit(X, y)
    clf = SGDClassifier(
    loss="log_loss",
    class_weight="balanced",
    random_state=42
    )
    clf.partial_fit(X, y, classes=np.unique(y))

    global _clf
    _clf = clf

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump({
        "clf": clf,
        "tfidf_char": tfidf_char,
        "tfidf_word": tfidf_word,
        "embedding_dim": EMBEDDING_DIM,
        "trained_at": datetime.now(timezone.utc).isoformat(),
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