"""
loadtest_sync.py: how does the sync path behave when many users sync at once?
(docs/scalability/01-engineering.md section 9, scenario L3, and L7 memory)

Runs N concurrent Gmail syncs through the real tasks.fetch_gmail_for_user_task
(job rows, the sync slots, downloads with 4 threads per sync, parsing, the
raw insert) against an in-memory database and a stub Gmail that answers with
simple bank alerts after a fixed delay. The ETL pipeline is replaced by one
categoriser.predict_batch() call per sync, so the real model is loaded and
used and the memory figure includes it. No network, no Supabase, no .env:
it cannot touch a real project. Not part of run_tests.sh; it is a manual tool.

What it does NOT measure: database latency, Gmail's own rate limits, Render's
CPU share. Treat the timings as a floor and the memory figure as the useful
number. Run it under the host's memory limit:

    docker run --rm --memory 512m --network none -v "$(pwd)/backend:/app:ro" \\
        spendstream-backend python loadtest_sync.py --users 30 --alerts 100 --slots 3

Needs the model files in ml/ (python model_store.py pull, or train_model.py).
Exit code is non-zero if a sync failed, the cap was exceeded, or peak memory
went over --max-rss-mb.
"""

import argparse
import base64
import logging
import resource
import statistics
import sys
import threading
import time

import test_pipeline as tp              # placeholder settings, the fakes and alert_message()
from ml import categoriser

tasks = tp.tasks
PAGE = tasks.PAGE_SIZE


def receiver_for(n: int) -> str:
    return f"SHOP{n % 40}"


def build_gmail_stub(alerts: int, latency_s: float):
    """A Gmail that lists `alerts` messages per user and returns one alert per message."""
    ids = [f"m{i}" for i in range(alerts)]
    bodies = {i: tp.alert_message(i, f"You have paid ₹ {50 + n} to {receiver_for(n)} on 15 Sep")
              for n, i in enumerate(ids)}

    def handler(method, url, params):
        time.sleep(latency_s)
        if method == "POST":                       # token refresh is never needed here
            return tp.FakeResponse(200, {"access_token": "acc"})
        if url.endswith("/messages"):
            start = int(params.get("pageToken") or 0)
            page = ids[start:start + PAGE]
            body = {"messages": [{"id": i} for i in page]}
            if start + PAGE < len(ids):
                body["nextPageToken"] = str(start + PAGE)
            return tp.FakeResponse(200, body)
        return tp.FakeResponse(200, bodies[url.rsplit("/", 1)[1]])

    return handler


class Gauge:
    """How many syncs are running at once (inside a sync slot)."""

    def __init__(self):
        self.lock, self.now, self.peak = threading.Lock(), 0, 0

    def enter(self):
        with self.lock:
            self.now += 1
            self.peak = max(self.peak, self.now)

    def leave(self):
        with self.lock:
            self.now -= 1


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))] if ordered else 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--users", type=int, default=30, help="concurrent syncs (one per user)")
    ap.add_argument("--alerts", type=int, default=100, help="alert emails per sync")
    ap.add_argument("--slots", type=int, default=3, help="MAX_CONCURRENT_SYNCS to test")
    ap.add_argument("--latency-ms", type=int, default=30, help="stub Gmail delay per request")
    ap.add_argument("--max-rss-mb", type=int, default=400, help="fail above this peak memory")
    args = ap.parse_args()

    logging.disable(logging.CRITICAL)                     # the run prints its own summary
    if categoriser.load() is None:
        print("No model in ml/ (python model_store.py pull, or python train_model.py --no-smoke).")
        return 2

    gauge = Gauge()
    tasks._SYNC_SLOTS = threading.BoundedSemaphore(args.slots)
    tasks.requests = tp.FakeHTTP(build_gmail_stub(args.alerts, args.latency_ms / 1000))

    real_sync_user = tasks._sync_user

    def counted(*a, **kw):                                # runs only while a slot is held
        gauge.enter()
        try:
            return real_sync_user(*a, **kw)
        finally:
            gauge.leave()

    tasks._sync_user = counted
    tasks.supabase_admin = tp.FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": None}],
        ("gmail_sync", "select"): [{"last_fetched": "2026-09-15T08:00:00+00:00", "pending_message_ids": []}],
        ("transactions", "upsert"): lambda call: [{"id": r["message_id"]} for r in call["payload"]],
    })

    in_pipeline = {"now": 0, "peak": 0}
    lock = threading.Lock()

    def pipeline(user_id):                                # one real model call per sync
        with lock:
            in_pipeline["now"] += 1
            in_pipeline["peak"] = max(in_pipeline["peak"], in_pipeline["now"])
        try:
            n = args.alerts
            categoriser.predict_batch([receiver_for(i) for i in range(n)], [50.0 + i for i in range(n)], [None] * n)
        finally:
            with lock:
                in_pipeline["now"] -= 1

    tasks.run_pipeline_task = pipeline

    durations, results = [], []

    def one(i: int):
        t0 = time.monotonic()
        try:
            results.append(tasks.fetch_gmail_for_user_task(f"user-{i}", f"job-{i}")["status"])
        except Exception as e:                            # a failed sync is a finding, not a crash
            results.append(f"error:{type(e).__name__}")
        durations.append(time.monotonic() - t0)

    t0 = time.monotonic()
    threads = [threading.Thread(target=one, args=(i,)) for i in range(args.users)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.monotonic() - t0

    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024      # KB on Linux
    ok = results.count("ok")
    print(f"users={args.users} alerts={args.alerts} slots={args.slots} latency={args.latency_ms}ms")
    print(f"syncs ok:            {ok}/{args.users}  {sorted(set(results) - {'ok'}) or ''}")
    print(f"wall time:           {wall:.1f} s")
    print(f"per sync (incl wait): p50 {statistics.median(durations):.1f} s   p95 {percentile(durations, 0.95):.1f} s   max {max(durations):.1f} s")
    print(f"peak running syncs:  {gauge.peak} (cap {args.slots})")
    print(f"peak in model step:  {in_pipeline['peak']}")
    print(f"peak memory (RSS):   {rss_mb:.0f} MB (limit {args.max_rss_mb} MB)")

    failed = ok != args.users or gauge.peak > args.slots or rss_mb > args.max_rss_mb
    print("RESULT:", "FAIL" if failed else "PASS")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
