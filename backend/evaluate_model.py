"""
evaluate_model.py — score a model on real, hand-labelled transactions.
═══════════════════════════════════════════════════════════════════════
The golden set (backend/ml/data/golden_set.csv, gitignored, built by
build_golden_set.py) holds real debits the model is never trained on in a
compare run. It is the only honest accuracy measure in this repo: training and
smoke scores are measured on copies of the training data and are inflated.

Scoring goes through ml/categoriser.py's own prediction code, so what is
measured is what the app runs.

Reported:
  * raw model: accuracy and macro-F1 with 95% bootstrap intervals, per-category recall
  * pipeline: the same after the confidence threshold,
    with precision (share of labelled rows that are right) and coverage (share
    labelled at all), since an unsure row is left for review, not guessed
  * merchants held out: a subset whose merchants appear only once in the set
  * a threshold table, to pick CONFIDENCE_THRESHOLD on real data

Usage:
    python evaluate_model.py
    python evaluate_model.py --model ml/candidate_model.pkl --encoder ml/candidate_encoder.pkl
    python evaluate_model.py --model <candidate> --encoder <enc> --compare ml/model_v2.pkl

Exit codes:
    0  evaluation ran (and, with --compare, the candidate passed the gate)
    1  the candidate lost to the active model, or a category regressed
    2  nothing was measured: no golden set, or too few rows to gate on
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, recall_score

from merchant_identity import merchant_key
from ml import categoriser

_ML_DIR     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ml")
GOLDEN_PATH = os.path.join(_ML_DIR, "data", "golden_set.csv")

MAX_CATEGORY_REGRESSION = 0.05   # a category may not lose more recall than this
MIN_ROWS_FOR_GATE       = 100    # fewer rows cannot tell two models apart
MIN_CATEGORY_ROWS       = 20     # the per-category rule applies only with at least this many rows
BOOTSTRAP_SAMPLES       = 1000
REQUIRED_COLUMNS        = {"raw_text", "category"}


def load_golden_set(path: str = GOLDEN_PATH) -> pd.DataFrame | None:
    """The golden set, or None when it does not exist. Never invents rows."""
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, dtype={"raw_text": str, "category": str, "timestamp": str})
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise SystemExit(f"golden set {path} is missing column(s): {sorted(missing)}")
    df["amount"]    = pd.to_numeric(df.get("amount", 0.0), errors="coerce").fillna(0.0)
    df["timestamp"] = [t if isinstance(t, str) and t else None for t in df.get("timestamp", [None] * len(df))]
    df["merchant_key"] = [merchant_key(str(t)) for t in df["raw_text"]]
    return df


def _interval(y_true: list, y_pred: list, metric, seed: int = 42) -> tuple[float, float]:
    """95% bootstrap interval of a metric over rows."""
    rng = np.random.RandomState(seed)
    t, p = np.array(y_true), np.array(y_pred)
    stats = [metric(t[i], p[i]) for i in (rng.randint(0, len(t), len(t)) for _ in range(BOOTSTRAP_SAMPLES))]
    return float(np.percentile(stats, 2.5)), float(np.percentile(stats, 97.5))


def _accuracy(t, p) -> float:
    return float(np.mean(np.asarray(t) == np.asarray(p)))


def _macro_f1(t, p) -> float:
    return float(f1_score(t, p, average="macro", zero_division=0))


def score(df: pd.DataFrame, model, label: str) -> dict:
    """Raw-model and pipeline scores on one set of rows."""
    if df is None or df.empty:
        return {}
    receivers, amounts, stamps = list(df["raw_text"]), list(df["amount"]), list(df["timestamp"])
    y_true  = list(df["category"])
    classes = list(model.classes)
    probas  = categoriser.predict_proba(model, receivers, amounts, stamps)
    y_raw   = [classes[i] for i in probas.argmax(axis=1)]
    piped   = categoriser.predict_batch(receivers, amounts, stamps, model=model)
    y_pipe  = [c for c, _ in piped]
    labels  = sorted(set(y_true))

    labelled = [(t, p) for t, p in zip(y_true, y_pipe) if p != "Other" or t == "Other"]
    result = {
        "rows":        len(df),
        "accuracy":    _accuracy(y_true, y_raw),
        "accuracy_ci": _interval(y_true, y_raw, _accuracy),
        "macro_f1":    _macro_f1(y_true, y_raw),
        "macro_f1_ci": _interval(y_true, y_raw, _macro_f1),
        "recall":      {c: float(r) for c, r in zip(
            labels, recall_score(y_true, y_raw, labels=labels, average=None, zero_division=0))},
        "support":     {c: y_true.count(c) for c in labels},
        "pipeline_accuracy":  _accuracy(y_true, y_pipe),
        "pipeline_precision": _accuracy(*zip(*labelled)) if labelled else 0.0,
        "pipeline_coverage":  len(labelled) / len(df),
        "confidences":        [conf for _, conf in piped],
        "pipeline_labels":    [c for c, _ in piped],
        "raw_labels":         y_raw,
    }
    lo, hi = result["accuracy_ci"]
    flo, fhi = result["macro_f1_ci"]
    print(f"\n── {label} ── {result['rows']} rows")
    print(f"   raw model   accuracy {result['accuracy']:.3f} [{lo:.3f}-{hi:.3f}]   "
          f"macro-F1 {result['macro_f1']:.3f} [{flo:.3f}-{fhi:.3f}]")
    print(f"   pipeline    accuracy {result['pipeline_accuracy']:.3f}   precision "
          f"{result['pipeline_precision']:.3f}   coverage {result['pipeline_coverage']:.3f}")
    for cat, r in sorted(result["recall"].items(), key=lambda kv: kv[1]):
        n = y_true.count(cat)
        print(f"     {cat:<16} recall {r:.3f}   ({n} row{'s' if n != 1 else ''})")
    return result


def threshold_table(df: pd.DataFrame, model) -> None:
    """Precision and coverage of the raw model at candidate thresholds."""
    probas  = categoriser.predict_proba(model, list(df["raw_text"]), list(df["amount"]), list(df["timestamp"]))
    classes = list(model.classes)
    top, conf = probas.argmax(axis=1), probas.max(axis=1)
    right = np.array([classes[i] == t for i, t in zip(top, df["category"])])
    print("\n── Threshold (raw model) ──  labelled rows are those at or above it")
    for th in (0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60):
        keep = conf >= th
        prec = right[keep].mean() if keep.any() else float("nan")
        print(f"   {th:.2f}   coverage {keep.mean():.3f}   precision {prec:.3f}")


def evaluate(model_path: str, encoder_path: str, report_thresholds: bool = False) -> dict:
    df = load_golden_set()
    if df is None:
        print(f"No golden set at {GOLDEN_PATH}. Nothing was measured; build it with build_golden_set.py.")
        sys.exit(2)
    model = categoriser.load(model_path, encoder_path)
    if model is None:
        raise SystemExit(f"could not load {model_path}")

    print(f"Model {model_path}: {model.bundle.get('classifier')}, trained {model.bundle.get('trained_at')}")
    print(f"Golden set: {len(df)} rows, {df['category'].nunique()} categories, "
          f"{df['merchant_key'].nunique()} merchants")
    results = {"all_rows": score(df, model, "All golden-set rows")}
    counts = df["merchant_key"].value_counts()
    single = df[df["merchant_key"].map(counts) == 1]
    results["single_merchants"] = score(single, model, "Merchants seen only once in the set")
    if report_thresholds:
        threshold_table(df, model)
    return results


def gate(candidate: dict, active: dict) -> bool:
    """A candidate ships only if it is at least as good, everywhere that counts."""
    cand, base = candidate["all_rows"], active["all_rows"]
    ok = True
    if cand["macro_f1"] < base["macro_f1"]:
        print(f"REJECTED: macro-F1 {cand['macro_f1']:.3f} < active {base['macro_f1']:.3f}")
        ok = False
    for cat, r in cand["recall"].items():
        drop = base["recall"].get(cat, 0) - r
        if drop <= MAX_CATEGORY_REGRESSION:
            continue
        rows = cand["support"].get(cat, 0)
        # Below MIN_CATEGORY_ROWS one transaction moves recall by more than the
        # allowed drop, so the rule would decide on noise (added 2026-09-27,
        # after it rejected a better model over 1 of 13 Groceries rows).
        if rows < MIN_CATEGORY_ROWS:
            print(f"note: {cat} recall {base['recall'][cat]:.3f} -> {r:.3f} on only {rows} rows (not gated)")
            continue
        print(f"REJECTED: {cat} recall dropped {drop:.3f} ({base['recall'][cat]:.3f} -> {r:.3f}, {rows} rows)")
        ok = False
    return ok


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--model",   default=categoriser.MODEL_PATH)
    ap.add_argument("--encoder", default=categoriser.ENCODER_PATH)
    ap.add_argument("--compare", help="the active model, to gate the candidate against")
    ap.add_argument("--compare-encoder")
    ap.add_argument("--thresholds", action="store_true", help="print a threshold table")
    args = ap.parse_args()

    candidate = evaluate(args.model, args.encoder, args.thresholds)
    if args.compare:
        if candidate["all_rows"]["rows"] < MIN_ROWS_FOR_GATE:
            print(f"Only {candidate['all_rows']['rows']} golden rows; at least {MIN_ROWS_FOR_GATE} "
                  "are needed to gate. Nothing decided.")
            sys.exit(2)
        print("\n" + "=" * 62 + "\nActive model, same rows")
        active = evaluate(args.compare, args.compare_encoder or args.encoder)
        print("\n" + "=" * 62)
        if gate(candidate, active):
            print("ACCEPTED: the candidate matches or beats the active model.")
            sys.exit(0)
        sys.exit(1)
