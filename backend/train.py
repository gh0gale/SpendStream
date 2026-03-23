"""
train.py
────────
Trains a transaction category classifier on the synthetic dataset.

Pipeline:
  1. Load synthetic.csv
  2. Feature engineering — extract signals from raw VPA strings
  3. TF-IDF on cleaned text
  4. Logistic Regression classifier
  5. Evaluate with cross-validation + classification report
  6. Save model to ml/model.pkl

Run from backend/ directory:
    pip install scikit-learn pandas joblib
    python train.py

To retrain later with real data added:
    python train.py --include-real
"""

import os
import re
import sys
import argparse
import joblib
import numpy as np
import pandas as pd

from collections import Counter
from sklearn.pipeline         import Pipeline
from sklearn.linear_model     import LogisticRegression
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing    import LabelEncoder
from sklearn.model_selection  import StratifiedKFold, cross_validate
from sklearn.metrics          import classification_report
from sklearn.utils            import resample

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
SYNTHETIC_PATH = os.path.join(BASE_DIR, "ml", "data", "synthetic.csv")
REAL_PATH      = os.path.join(BASE_DIR, "ml", "data", "real_labels.csv")
MODEL_PATH     = os.path.join(BASE_DIR, "ml", "model.pkl")
ENCODER_PATH   = os.path.join(BASE_DIR, "ml", "label_encoder.pkl")

# ─────────────────────────────────────────────────────────────────────────────
# Text preprocessing
# Mirrors the logic in etl.py clean_merchant() so features are consistent
# ─────────────────────────────────────────────────────────────────────────────

# Gateways to strip — these are noise, not merchant signals
GATEWAY_PATTERN = re.compile(
    r"@(pty|ptys|pthdfc|paytm|ptsbi|pticici|"
    r"okaxis|okicici|oksbi|okhdfcbank|okbizaxis|"
    r"ybl|axl|ibl|upi|"
    r"hdfcbank|icicibank|sbi|axisbank|kotak|"
    r"fbpe|bharatpe|"
    r"validhdfc|validicici|validsbi)\S*",
    re.IGNORECASE
)

HANDLE_PREFIXES = re.compile(
    r"^VPA\s+", re.IGNORECASE
)

UPI_HANDLE_PATTERN = re.compile(
    r"\S+@\S+\s*"
)


def preprocess(text: str) -> str:
    """
    Clean a raw VPA string into a normalised token string for TF-IDF.

    Steps:
      1. Strip VPA prefix
      2. Extract text AFTER the @gateway handle (real merchant name)
      3. Remove remaining noise (numbers, special chars)
      4. Lowercase

    Example:
      "VPA paytmqr6woody@ptys RAVINDRA S SHETTY"
      → "ravindra s shetty"

      "VPA groww.brk@validhdfc GROWW INVEST TECH PVT LTD"
      → "groww invest tech pvt ltd"
    """
    if not isinstance(text, str):
        return ""

    # Step 1: strip VPA prefix
    text = HANDLE_PREFIXES.sub("", text).strip()

    # Step 2: extract merchant name after handle
    after_handle = UPI_HANDLE_PATTERN.sub("", text).strip()

    # Use after_handle if meaningful, else full text
    working = after_handle if len(after_handle) >= 3 else text

    # Step 3: remove remaining noise
    working = re.sub(r"[/@.]", " ", working)          # punctuation → space
    working = re.sub(r"\b\d{4,}\b", " ", working)     # long numbers
    working = re.sub(r"\b(pvt|ltd|pte|inc|llp|llc|private|limited)\b", " ", working, flags=re.IGNORECASE)
    working = re.sub(r"\b(payment|online|india|tech|services|w|g)\b", " ", working, flags=re.IGNORECASE)
    working = re.sub(r"\s+", " ", working).strip()

    return working.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

def load_data(include_real: bool = False) -> pd.DataFrame:
    print(f"[Train] Loading synthetic data from {SYNTHETIC_PATH}")
    df = pd.read_csv(SYNTHETIC_PATH)
    print(f"[Train] Synthetic rows: {len(df)}")

    if include_real and os.path.exists(REAL_PATH):
        real_df = pd.read_csv(REAL_PATH)
        print(f"[Train] Real label rows: {len(real_df)}")
        # Real data gets 3x weight by duplicating rows
        real_df = pd.concat([real_df] * 3, ignore_index=True)
        df = pd.concat([df, real_df], ignore_index=True)
        print(f"[Train] Combined rows (real weighted 3x): {len(df)}")

    # Validate required columns
    assert "raw_text"  in df.columns, "Missing column: raw_text"
    assert "category"  in df.columns, "Missing column: category"

    df = df.dropna(subset=["raw_text", "category"])
    df["raw_text"] = df["raw_text"].astype(str)
    df["category"] = df["category"].astype(str).str.strip()

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Class balancing
# Oversample minority classes to reduce bias toward Food/Transfer
# ─────────────────────────────────────────────────────────────────────────────

def balance_classes(df: pd.DataFrame, target_min: int = 100) -> pd.DataFrame:
    """
    Oversample any class with fewer than target_min examples
    up to target_min using random resampling with replacement.
    """
    counts = df["category"].value_counts()
    parts  = [df]

    for cat, count in counts.items():
        if count < target_min:
            subset    = df[df["category"] == cat]
            upsampled = resample(subset, replace=True, n_samples=target_min, random_state=42)
            parts.append(upsampled)

    balanced = pd.concat(parts, ignore_index=True).sample(frac=1, random_state=42)
    return balanced


# ─────────────────────────────────────────────────────────────────────────────
# Model pipeline
# ─────────────────────────────────────────────────────────────────────────────

def build_pipeline() -> Pipeline:
    """
    TF-IDF + Logistic Regression pipeline.

    TF-IDF settings:
      - char n-grams (2-4) to capture subword patterns
        e.g. "swig" matches swiggy, "eatcl" matches eatclub
      - word n-grams (1-2) for phrase matching
        e.g. "mutual fund", "electricity bill"
      - Combined via FeatureUnion equivalent (two TF-IDF fitted separately
        then we use sublinear_tf to handle frequency imbalance)

    Logistic Regression:
      - C=5 — moderate regularisation
      - max_iter=1000 — enough for convergence
      - class_weight='balanced' — handles any remaining class imbalance
      - solver='lbfgs' — efficient for multiclass
    """
    return Pipeline([
        ("tfidf", TfidfVectorizer(
            analyzer        = "char_wb",   # character n-grams within word boundaries
            ngram_range     = (2, 5),      # 2 to 5 char grams
            max_features    = 50000,
            sublinear_tf    = True,        # log(tf) to reduce effect of repeated terms
            strip_accents   = "unicode",
            lowercase       = True,
            min_df          = 2,           # ignore terms appearing in only 1 doc
        )),
        ("clf", LogisticRegression(
            C             = 5,
            max_iter      = 1000,
            class_weight  = "balanced",
            solver        = "lbfgs",
            random_state  = 42,
        )),
    ])


# ─────────────────────────────────────────────────────────────────────────────
# Evaluation
# ─────────────────────────────────────────────────────────────────────────────

def evaluate(pipeline: Pipeline, X: list, y: np.ndarray, label_encoder: LabelEncoder):
    """
    5-fold stratified cross-validation.
    Reports per-class precision, recall, F1 and overall accuracy.
    """
    print("\n[Train] Running 5-fold stratified cross-validation...")

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_validate(
        pipeline, X, y, cv=cv,
        scoring=["accuracy", "f1_macro", "f1_weighted"],
        n_jobs=-1
    )

    print(f"\n{'─'*50}")
    print(f"  Cross-validation results (5 folds)")
    print(f"{'─'*50}")
    print(f"  Accuracy     : {scores['test_accuracy'].mean():.4f}  ± {scores['test_accuracy'].std():.4f}")
    print(f"  F1 macro     : {scores['test_f1_macro'].mean():.4f}  ± {scores['test_f1_macro'].std():.4f}")
    print(f"  F1 weighted  : {scores['test_f1_weighted'].mean():.4f}  ± {scores['test_f1_weighted'].std():.4f}")
    print(f"{'─'*50}")

    # Full classification report on 80/20 split for readability
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )
    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)

    print(f"\n[Train] Classification report (20% holdout):\n")
    print(classification_report(
        y_test, y_pred,
        target_names=label_encoder.classes_,
        digits=3
    ))

    return pipeline


# ─────────────────────────────────────────────────────────────────────────────
# Save
# ─────────────────────────────────────────────────────────────────────────────

def save_model(pipeline: Pipeline, label_encoder: LabelEncoder):
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(pipeline,     MODEL_PATH)
    joblib.dump(label_encoder, ENCODER_PATH)
    print(f"\n[Train] Model saved    → {MODEL_PATH}")
    print(f"[Train] Encoder saved  → {ENCODER_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--include-real", action="store_true",
        help="Include real_labels.csv in training (weighted 3x over synthetic)"
    )
    args = parser.parse_args()

    # 1. Load
    df = load_data(include_real=args.include_real)

    # 2. Balance
    df = balance_classes(df, target_min=100)

    counts = df["category"].value_counts()
    print(f"\n[Train] Class distribution after balancing:")
    for cat, count in counts.items():
        print(f"  {cat:<22} {count}")

    # 3. Preprocess text
    print("\n[Train] Preprocessing text...")
    X = [preprocess(t) for t in df["raw_text"]]
    print(f"[Train] Sample preprocessed texts:")
    for raw, processed in zip(df["raw_text"].head(5), X[:5]):
        print(f"  {raw[:55]:<55} → {processed}")

    # 4. Encode labels
    le = LabelEncoder()
    y  = le.fit_transform(df["category"])
    print(f"\n[Train] Categories ({len(le.classes_)}): {list(le.classes_)}")

    # 5. Build pipeline
    pipeline = build_pipeline()

    # 6. Evaluate with cross-validation
    pipeline = evaluate(pipeline, X, y, le)

    # 7. Final fit on ALL data
    print("\n[Train] Final fit on full dataset...")
    pipeline.fit(X, y)

    # 8. Save
    save_model(pipeline, le)

    print("\n[Train] Done. Model ready for use in categoriser.py")


if __name__ == "__main__":
    main()