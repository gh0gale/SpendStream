"""
Offline checks for weekly_retrain.py (when to retrain, what it trains on) and
model_store.ensure_local (how a fresh deploy gets its model).
No database, network or .env. Run from backend/: python test_weekly_retrain.py
"""

import os
import sys
import tempfile

import model_store
import weekly_retrain
from weekly_retrain import community_rows, enough_new, without_golden

DIRECTORY = {"upi:swiggy@ybl": "Food", "upi:uber@axl": "Transport"}


def silver(n, key, start=0):
    return [{"bronze_id": f"{key}-{i:03}", "merchant_key": key, "amount": 100} for i in range(start, start + n)]


def bronze_for(rows):
    return {r["bronze_id"]: {"receiver": f"VPA {r['merchant_key'][4:]} X", "timestamp": "2026-09-01T10:00:00Z"}
            for r in rows}


def test_rows_are_capped_per_merchant_so_one_merchant_cannot_tilt_a_category():
    s = silver(50, "upi:swiggy@ybl") + silver(3, "upi:uber@axl")
    rows = community_rows(DIRECTORY, s, bronze_for(s), cap=20)
    assert sum(r["merchant_key"] == "upi:swiggy@ybl" for r in rows) == 20
    assert sum(r["merchant_key"] == "upi:uber@axl" for r in rows) == 3


def test_only_directory_merchants_with_a_receiver_become_rows():
    s = silver(2, "upi:swiggy@ybl") + silver(2, "upi:unknown@ybl")
    b = bronze_for(s)
    b[s[0]["bronze_id"]]["receiver"] = ""
    rows = community_rows(DIRECTORY, s, b)
    assert [r["merchant_key"] for r in rows] == ["upi:swiggy@ybl"]
    assert rows[0]["category"] == "Food"


def test_timestamps_become_india_time_like_the_training_data():
    s = silver(1, "upi:uber@axl")
    rows = community_rows(DIRECTORY, s, bronze_for(s))
    assert rows[0]["timestamp"].startswith("2026-09-01T15:30:00"), rows[0]["timestamp"]


def test_retrain_waits_for_enough_new_merchants_and_rows():
    few = [{"merchant_key": f"k{i}"} for i in range(weekly_retrain.MIN_NEW_MERCHANTS - 1) for _ in range(10)]
    assert enough_new(few, set())[0] is False
    many = [{"merchant_key": f"k{i}"} for i in range(weekly_retrain.MIN_NEW_MERCHANTS) for _ in range(5)]
    assert enough_new(many, set()) == (True, weekly_retrain.MIN_NEW_MERCHANTS, len(many))


def test_merchants_already_trained_on_do_not_count_as_new():
    rows = [{"merchant_key": f"k{i}"} for i in range(40) for _ in range(5)]
    go, merchants, new = enough_new(rows, {f"k{i}" for i in range(30)})
    assert (go, merchants, new) == (False, 10, 50)


def test_compare_rows_leave_out_golden_set_merchants():
    rows = [{"merchant_key": "upi:swiggy@ybl"}, {"merchant_key": "upi:uber@axl"}]
    kept = without_golden(rows, ["VPA swiggy@ybl SWIGGY"])
    assert kept == [{"merchant_key": "upi:uber@axl"}]


class FakeBucket:
    def __init__(self, files):
        self.files = files

    def download(self, path):
        if path not in self.files:
            raise RuntimeError("not found")
        return self.files[path]


class FakeClient:
    def __init__(self, files):
        self.storage = type("S", (), {"from_": lambda _s, _b: FakeBucket(files)})()


def with_ml_dir(fn):
    def run():
        old = model_store.ML_DIR
        with tempfile.TemporaryDirectory() as d:
            model_store.ML_DIR = d
            try:
                fn(d)
            finally:
                model_store.ML_DIR = old
    run.__name__ = fn.__name__
    return run


@with_ml_dir
def test_fresh_deploy_downloads_the_live_model(d):
    client = FakeClient({"live/model_v2.pkl": b"m", "live/label_encoder_v2.pkl": b"e"})
    assert model_store.ensure_local(client) is True
    assert open(os.path.join(d, "model_v2.pkl"), "rb").read() == b"m"
    assert model_store.ensure_local(client) is False      # files present: no second download


@with_ml_dir
def test_missing_model_in_bucket_stops_startup_and_writes_nothing(d):
    client = FakeClient({"live/model_v2.pkl": b"m"})
    try:
        model_store.ensure_local(client)
        raise AssertionError("expected ModelStoreError")
    except model_store.ModelStoreError:
        pass
    assert os.listdir(d) == []


if __name__ == "__main__":
    tests = [v for k, v in dict(globals()).items() if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
    print(f"{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
