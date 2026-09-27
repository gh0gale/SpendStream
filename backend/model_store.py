"""
model_store.py — the model files live in a private Supabase Storage bucket, not in git.

The model is trained on the owner's real transactions, so its vocabulary holds
real payee names; it must never be pushed. The API downloads the live model at
startup when ml/ has none (a fresh deploy), and weekly_retrain.py replaces it.

Bucket "models" (private, service role only):
    live/     model_v2.pkl, label_encoder_v2.pkl   what the API loads
    prev/     the same two files, the model before  rollback target
    baseline/ model.pkl, encoder.pkl               last accepted compare-run model (the gate)
    data/     golden_set.csv, private_train.csv, private_ship.csv  from build_golden_set.py
    state.json                                     merchant keys the live model was trained on

Run from backend/ (needs the Supabase values in backend/.env):
    python model_store.py push-data        upload ml/data/{golden_set,private_train,private_ship}.csv
    python model_store.py push-model       upload ml/model_v2.pkl as live (old live -> prev)
                          [--prefix ml/candidate_ship] [--baseline ml/candidate_baseline]
    python model_store.py pull             download the live model into ml/
    python model_store.py rollback         prev -> live (restart the backend afterwards)
"""

import argparse
import json
import logging
import os
import sys

import config

log = logging.getLogger("model_store")

BUCKET  = "models"
HERE    = os.path.dirname(os.path.abspath(__file__))
ML_DIR  = os.path.join(HERE, "ml")
DATA_DIR = os.path.join(ML_DIR, "data")

MODEL_FILES = ("model_v2.pkl", "label_encoder_v2.pkl")
DATA_FILES  = ("golden_set.csv", "private_train.csv", "private_ship.csv")
BASELINE    = {"model.pkl": "_model.pkl", "encoder.pkl": "_encoder.pkl"}   # bucket name -> prefix suffix
STATE_PATH  = "state.json"


class ModelStoreError(RuntimeError):
    """The bucket is missing, unreachable, or lacks a file that must be there."""


def _bucket(client=None):
    return (client or config.admin_client()).storage.from_(BUCKET)


def download(path: str, client=None) -> bytes:
    try:
        return _bucket(client).download(path)
    except Exception as e:
        raise ModelStoreError(f"could not download {BUCKET}/{path} ({type(e).__name__})") from e


def upload(path: str, data: bytes, client=None) -> None:
    _bucket(client).upload(path, data, {"upsert": "true", "content-type": "application/octet-stream"})


def exists(path: str, client=None) -> bool:
    folder, _, name = path.rpartition("/")
    return any(f.get("name") == name for f in _bucket(client).list(folder or None))


def ensure_local(client=None) -> bool:
    """
    Download the live model into ml/ unless both files are already there.
    Returns True if it downloaded. Raises ModelStoreError when there is no
    local model and the bucket cannot supply one: without a model every row
    would be marked predicted and left uncategorised for good, so the API
    must not start.
    """
    local = [os.path.join(ML_DIR, f) for f in MODEL_FILES]
    if all(os.path.exists(p) for p in local):
        return False
    blobs = [download(f"live/{f}", client) for f in MODEL_FILES]
    for path, blob in zip(local, blobs):
        tmp = path + ".part"
        with open(tmp, "wb") as fh:
            fh.write(blob)
        os.replace(tmp, path)
    log.info("Downloaded the live model from %s/live (%d bytes)", BUCKET, sum(len(b) for b in blobs))
    return True


def publish(model_path: str, encoder_path: str, client=None) -> None:
    """Make these files the live model; the current live model becomes prev."""
    client = client or config.admin_client()
    for name in MODEL_FILES:
        if exists(f"live/{name}", client):
            upload(f"prev/{name}", download(f"live/{name}", client), client)
    for name, path in zip(MODEL_FILES, (model_path, encoder_path)):
        with open(path, "rb") as fh:
            upload(f"live/{name}", fh.read(), client)


def read_state(client=None) -> dict:
    """{"trained_keys": [...], ...}; empty when no automated retrain has run yet."""
    if not exists(STATE_PATH, client):
        return {}
    return json.loads(download(STATE_PATH, client))


def write_state(state: dict, client=None) -> None:
    upload(STATE_PATH, json.dumps(state, indent=1).encode(), client)


def _ensure_bucket(client) -> None:
    if not any(b.name == BUCKET for b in client.storage.list_buckets()):
        client.storage.create_bucket(BUCKET, options={"public": False})
        print(f"Created private bucket {BUCKET!r}")


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("push-data")
    pm = sub.add_parser("push-model")
    pm.add_argument("--prefix", help="use <prefix>_model.pkl / <prefix>_encoder.pkl instead of ml/model_v2.pkl")
    pm.add_argument("--baseline", help="also upload <prefix>_model.pkl / _encoder.pkl as the gate's baseline")
    sub.add_parser("pull")
    sub.add_parser("rollback")
    args = ap.parse_args()
    client = config.admin_client()

    if args.cmd == "push-data":
        _ensure_bucket(client)
        for name in DATA_FILES:
            with open(os.path.join(DATA_DIR, name), "rb") as fh:
                upload(f"data/{name}", fh.read(), client)
            print(f"uploaded data/{name}")
    elif args.cmd == "push-model":
        _ensure_bucket(client)
        if args.prefix:
            paths = (f"{args.prefix}_model.pkl", f"{args.prefix}_encoder.pkl")
        else:
            paths = tuple(os.path.join(ML_DIR, f) for f in MODEL_FILES)
        publish(*paths, client=client)
        print(f"live <- {paths[0]} (previous live kept as prev/)")
        if args.baseline:
            for name, suffix in BASELINE.items():
                with open(args.baseline + suffix, "rb") as fh:
                    upload(f"baseline/{name}", fh.read(), client)
            print(f"baseline <- {args.baseline}_*")
    elif args.cmd == "pull":
        for name in MODEL_FILES:
            with open(os.path.join(ML_DIR, name), "wb") as fh:
                fh.write(download(f"live/{name}", client))
        print("ml/model_v2.pkl and ml/label_encoder_v2.pkl <- live")
    elif args.cmd == "rollback":
        blobs = {name: download(f"prev/{name}", client) for name in MODEL_FILES}
        for name, blob in blobs.items():
            upload(f"live/{name}", blob, client)
        print("live <- prev. Restart the backend to load it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
