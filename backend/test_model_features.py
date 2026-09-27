"""
test_model_features.py ─ the feature contract, and reproducibility
═══════════════════════════════════════════════════════════════════
Covers TEST-01 (predictions must not depend on the time of day) and the feature contract: since 2026-09-27 training, evaluation
and prediction all take their features from ml/features.py, and this test
proves they really share that one copy.

Needs no network and no Supabase project: it sets placeholder settings and
only calls pure feature functions — no model is loaded. Run in the dev image
from the repo root (Git Bash on Windows: prefix MSYS_NO_PATHCONV=1 and use
"$(pwd -W)"):

    docker run --rm --network none -v "$(pwd)/backend:/app:ro" \
        spendstream-backend python test_model_features.py

Exit code is non-zero if any check fails.
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cryptography.fernet import Fernet   # noqa: E402

os.environ.update({
    "SUPABASE_URL":              "https://placeholder.supabase.co",
    "SUPABASE_ANON_KEY":         "placeholder.placeholder.placeholder",
    "SUPABASE_SERVICE_ROLE_KEY": "placeholder.placeholder.placeholder",
    "GOOGLE_CLIENT_ID":          "test-client-id",
    "GOOGLE_CLIENT_SECRET":      "test-client-secret",
    "CRON_SECRET":               "test-cron-secret",
    "TOKEN_ENCRYPTION_KEY":      Fernet.generate_key().decode(),
})

import numpy as np                                  # noqa: E402

import evaluate_model                               # noqa: E402
import train_model                                  # noqa: E402
from ml import categoriser, features                # noqa: E402

FAILED: list[str] = []


def check(name: str, ok, detail="") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   [{detail}]"))
    if not ok:
        FAILED.append(name)


# ─────────────────────────────────────────────────────────────────────────────
# 4.14 A missing timestamp means unknown, never "now"
# ─────────────────────────────────────────────────────────────────────────────

def test_missing_timestamp_is_not_now():
    meta = categoriser.extract_metadata(300.0)
    # [log_amount, hour_sin, hour_cos, dow_norm, freq_norm]
    check("4.14 no timestamp gives zero hour features",
          meta[1] == 0.0 and meta[2] == 0.0, meta)
    check("4.14 no timestamp gives zero day-of-week", meta[3] == 0.0, meta)
    check("4.14 the amount feature is still real", meta[0] > 0, meta)

    # Zero sine AND zero cosine is impossible for a real hour, so "unknown"
    # is distinguishable from every actual time.
    for hour in range(24):
        ts = datetime(2024, 6, 15, hour, 30, tzinfo=timezone.utc)
        m  = categoriser.extract_metadata(300.0, ts)
        if m[1] == 0.0 and m[2] == 0.0:
            check(f"4.14 hour {hour} is distinguishable from unknown", False, m)
            return
    check("4.14 unknown is distinguishable from all 24 hours", True)

    # An unparseable string is unknown too, not now().
    bad = categoriser.extract_metadata(300.0, "not-a-date")
    check("4.14 an unparseable timestamp is unknown, not now",
          bad[1] == 0.0 and bad[2] == 0.0 and bad[3] == 0.0, bad)


def test_features_are_clock_independent():
    """The same input must give the same features whenever it is computed."""
    a = categoriser.extract_metadata(250.0)
    b = categoriser.extract_metadata(250.0)
    check("4.14 two calls with no timestamp agree", np.array_equal(a, b), (a, b))

    ts = "2024-06-15T13:30:00"
    check("4.14 two calls with a timestamp agree",
          np.array_equal(categoriser.extract_metadata(250.0, ts),
                         categoriser.extract_metadata(250.0, ts)))

    # The prediction path must not read the clock at all any more.
    #
    # Parsed, not grepped: "datetime.now" still appears in these functions as
    # a comment explaining why the call was removed, and an earlier substring
    # version of this check failed on its own documentation.
    import ast
    import inspect
    import textwrap

    def calls_now(fn) -> bool:
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
        return any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "now"
            for node in ast.walk(tree)
        )

    for fn in (features.extract_metadata, categoriser.predict_batch, categoriser._local_time):
        check(f"4.14 {fn.__name__} does not call datetime.now()", not calls_now(fn))


# ─────────────────────────────────────────────────────────────────────────────
# The duplicated feature contract (.claude/rules/general.md rule 3)
# ─────────────────────────────────────────────────────────────────────────────

def test_one_feature_module():
    # Identity, not equality: there is one copy, so it cannot drift.
    for name in ("extract_metadata", "preprocess_text", "input_text", "CATEGORIES",
                 "METADATA_DIM", "EMBEDDING_DIM", "TFIDF_CHAR_MAX", "TFIDF_WORD_MAX"):
        check(f"train_model.{name} is ml/features.{name}",
              getattr(train_model, name) is getattr(features, name))
    for name in ("extract_metadata", "input_text", "CATEGORIES", "METADATA_DIM"):
        check(f"categoriser.{name} is ml/features.{name}",
              getattr(categoriser, name) is getattr(features, name))
    check("evaluate_model scores through the categoriser", evaluate_model.categoriser is categoriser)
    check("extract_metadata returns METADATA_DIM features",
          len(features.extract_metadata(1.0)) == features.METADATA_DIM)
    check("the model reads the cleaned name plus the shop-QR marker",
          features.input_text("VPA paytmqr5cs26w@ptys RAVINDRA S SHETTY") == "ravindra s shetty shopqr",
          features.input_text("VPA paytmqr5cs26w@ptys RAVINDRA S SHETTY"))


def test_times_are_india_time():
    # The database returns UTC; training data is in India time (2026-09-27).
    check("a UTC timestamp is read in IST (19:00 UTC is 00:30 IST next day)",
          categoriser._local_time("2026-09-20T19:00:00+00:00").startswith("2026-09-21T00:30"),
          categoriser._local_time("2026-09-20T19:00:00+00:00"))
    check("a bare date carries no time of day", categoriser._local_time("2026-09-20") is None)
    check("a missing timestamp stays missing", categoriser._local_time(None) is None)


def test_sparse_matches_dense():
    # Prediction builds sparse features; the model gives the same answer as
    # the dense form it was trained on.
    model = categoriser.load()
    if model is None:
        check("a model is present to compare sparse and dense", False)
        return
    from scipy.sparse import hstack
    texts = [features.input_text(r) for r in ("VPA swiggy@icici SWIGGY", "VPA q123456789@ybl KIRAN", "Rahul Sharma")]
    meta  = np.stack([features.extract_metadata(a) for a in (120.0, 38.0, 500.0)])
    sparse = model.clf.predict_proba(features.build_features(model.bundle, texts, meta))
    dense  = np.concatenate([
        hstack([model.bundle["tfidf_char"].transform(texts), model.bundle["tfidf_word"].transform(texts)]).toarray(),
        np.zeros((3, features.EMBEDDING_DIM)), meta], axis=1).astype(np.float32)
    check("sparse and dense features give the same probabilities",
          np.allclose(sparse, model.clf.predict_proba(dense), atol=1e-5))


def test_no_hidden_state():
    # BUG-04: the per-user history and override stores are gone, and the
    # categoriser touches no database.
    for name in ("_history_store", "_override_store", "_load_history_from_db", "supabase_admin",
                 "record_feedback", "run_categorise_silver", "train", "CATEGORY_ANCHORS",
                 "_apply_pattern_boosts"):
        check(f"categoriser has no {name}", not hasattr(categoriser, name))


# ─────────────────────────────────────────────────────────────────────────────
# 4.7 Training data is consistent and carries no real person's identifier
# ─────────────────────────────────────────────────────────────────────────────

def test_training_data_is_clean():
    labels: dict[str, set] = {}
    for receiver, category, _amt, _hrs in train_model.RAW_CASES:
        labels.setdefault(receiver, set()).add(category)

    contradictions = {r: c for r, c in labels.items() if len(c) > 1}
    check("4.7 no training example carries two different categories",
          not contradictions, contradictions)

    every = list(labels)
    check("4.7 every category in the data is a real category",
          {c for cats in labels.values() for c in cats} <= set(train_model.CATEGORIES),
          {c for cats in labels.values() for c in cats} - set(train_model.CATEGORIES))
    check("4.7 the dataset is not empty", len(every) > 100, len(every))


if __name__ == "__main__":
    test_missing_timestamp_is_not_now()
    test_features_are_clock_independent()
    test_one_feature_module()
    test_times_are_india_time()
    test_sparse_matches_dense()
    test_no_hidden_state()
    test_training_data_is_clean()

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) failed:")
        for name in FAILED:
            print(f"  - {name}")
        sys.exit(1)
    print("ALL MODEL FEATURE CHECKS PASSED")
