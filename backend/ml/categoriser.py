"""
categoriser.py — predict a category for transactions the rules did not settle.

In the pipeline the model is the last resort: a user's own rule and the shared
merchant directory are applied first (etl.run_categorise_silver), and only the
rest reaches predict_batch(). Features come from ml/features.py, the same code
training and evaluation use.

Pure and stateless apart from the loaded model: no database, no settings, no
per-user memory. User corrections live in Postgres as rules, never here, and
never change the model file (only train_model.py writes it).

Rewritten 2026-09-27. Removed: the in-memory history and override stores and
their per-batch database reload (keyed on the display name, so they rarely
matched: BUG-04), the soft override, the 0.6 confidence floor, the embedding
cold start (sentence-transformers is no longer installed), a second unused
run_categorise_silver, a train() that needed a file that never existed, and a
guard that deleted the model files on a dimension mismatch.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import joblib
import numpy as np

from ml.features import (   # noqa: F401  (CATEGORIES re-exported for callers)
    CATEGORIES,
    METADATA_DIM,
    build_features,
    extract_metadata,
    input_text,
)

log = logging.getLogger("categoriser")

_BASE        = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH   = os.path.join(_BASE, "model_v2.pkl")
ENCODER_PATH = os.path.join(_BASE, "label_encoder_v2.pkl")

# Below this the row is left uncategorised ("Other" here). Measured on the
# golden set 2026-09-27 (compare model): 0.25 -> precision 0.81, coverage 0.985;
# 0.50 -> precision 0.92, coverage 0.82. 0.50 since 2026-09-27 (plan Phase 6),
# when the dashboard started showing uncategorised spend and a Needs review list.
CONFIDENCE_THRESHOLD = 0.50

# The pattern boosts (meal-time Food, person Transfer, fixed-amount
# Subscription) were removed 2026-09-27: on the golden set accuracy was the
# same with and without them (0.795) and coverage fell with them. The model
# already sees amount and time, and the shop-QR marker does the person work.


@dataclass(frozen=True)
class Model:
    clf:            object
    bundle:         dict
    classes:        tuple
    uses_embeddings: bool


_lock  = threading.Lock()
_model: Optional[Model] = None     # loaded once; restart the process after retraining


def load(model_path: str = MODEL_PATH, encoder_path: str = ENCODER_PATH) -> Optional[Model]:
    """Load a model bundle, or None when the files are missing or do not match the features."""
    if not os.path.exists(model_path) or not os.path.exists(encoder_path):
        log.warning("No trained model at %s", model_path)
        return None
    bundle = joblib.load(model_path)
    if bundle.get("metadata_dim") != METADATA_DIM:
        log.error("Model %s has metadata_dim=%s, code expects %s: retrain it. Predictions disabled.",
                  model_path, bundle.get("metadata_dim"), METADATA_DIM)
        return None
    uses_embeddings = bundle.get("uses_embeddings", True)
    if uses_embeddings:
        log.error("Model %s needs sentence embeddings, which this build does not install. "
                  "Retrain with --no-embeddings. Predictions disabled.", model_path)
        return None
    encoder = joblib.load(encoder_path)
    return Model(clf=bundle["clf"], bundle=bundle, classes=tuple(encoder.classes_),
                 uses_embeddings=False)


def _get_model() -> Optional[Model]:
    global _model
    if _model is None:
        with _lock:
            if _model is None:
                _model = load()
    return _model


IST = timezone(timedelta(hours=5, minutes=30))


def _local_time(timestamp) -> Optional[str]:
    """
    The timestamp as an India-time ISO string, the form training data uses.
    The database returns UTC, so before 2026-09-27 the hour features and the
    meal-time boost read UTC hours, 5.5 h off. None if unknown or a bare date;
    a naive value is taken as already local.
    """
    if not timestamp or len(str(timestamp)) <= 10:
        return None
    try:
        dt = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    return (dt.astimezone(IST) if dt.tzinfo else dt).isoformat()




def predict_proba(model: Model, receivers: list[str], amounts: list[float],
                  timestamps: list) -> np.ndarray:
    """Raw model probabilities (no boosts), columns in model.classes order."""
    texts = [input_text(r) for r in receivers]
    meta  = np.stack([extract_metadata(float(a or 0), _local_time(t)) for a, t in zip(amounts, timestamps)]) \
        if texts else np.zeros((0, METADATA_DIM), dtype=np.float32)
    return model.clf.predict_proba(build_features(model.bundle, texts, meta))


def predict_batch(receivers: list[str], amounts: Optional[list[float]] = None,
                  timestamps: Optional[list] = None, model: Optional[Model] = None) -> list[tuple[str, float]]:
    """
    (category, confidence) per receiver. "Other" means unsure: the pipeline
    stores it as uncategorised. Receivers are raw bank-alert receivers
    ("VPA x@y NAME"); cleaning and the shop-QR marker happen in ml/features.py.
    """
    if not receivers:
        return []
    model = model or _get_model()
    if model is None:
        return [("Other", 0.0)] * len(receivers)
    n          = len(receivers)
    amounts    = amounts or [0.0] * n
    timestamps = timestamps or [None] * n
    probas     = predict_proba(model, receivers, amounts, timestamps)
    classes    = list(model.classes)

    results = []
    for proba in probas:
        i = int(np.argmax(proba))
        confidence = float(proba[i])
        results.append((str(classes[i]) if confidence >= CONFIDENCE_THRESHOLD else "Other", confidence))
    return results


def predict_category(receiver: str, amount: float = 0.0, timestamp=None) -> tuple[str, float]:
    """One receiver; see predict_batch."""
    return predict_batch([receiver], [amount], [timestamp])[0]


def model_info() -> dict:
    """What is loaded, from the bundle's own metadata (read once, at load)."""
    model = _get_model()
    if model is None:
        return {"model_loaded": False}
    b = model.bundle
    return {
        "model_loaded":    True,
        "classifier":      b.get("classifier"),
        "uses_embeddings": model.uses_embeddings,
        "trained_at":      b.get("trained_at"),
        "training_rows":   b.get("training_rows"),
    }


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(model_info(), indent=2))
