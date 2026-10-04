"""
test_pipeline.py  ─  offline checks for the Phase 3 Gmail pipeline
═══════════════════════════════════════════════════════════════════
Covers message-id dedup, fetching every message, reconnect, one write per
batch, work queues and sync jobs. The SQL side (constraints, apply_predictions, backfill) is
tested by supabase/tests/run_local.sh; the email parser by test_gmail_parser.py.

Needs no network, no Supabase project and no .env: the script sets
placeholder settings, the database is an in-memory fake, Google and Gmail
are a fake HTTP client, and the model is a stub. Run in the dev image from
the repo root (Git Bash on Windows: prefix MSYS_NO_PATHCONV=1 and use
"$(pwd -W)"):

    docker run --rm --network none -v "$(pwd)/backend:/app:ro" \
        spendstream-backend python test_pipeline.py

Exit code is non-zero if any check fails.
"""

import base64
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cryptography.fernet import Fernet

# Placeholder settings, set before config.py reads them.
os.environ.update({
    "SUPABASE_URL":              "https://placeholder.supabase.co",
    "SUPABASE_ANON_KEY":         "placeholder.placeholder.placeholder",
    "SUPABASE_SERVICE_ROLE_KEY": "placeholder.placeholder.placeholder",
    "GOOGLE_CLIENT_ID":          "test-client-id",
    "GOOGLE_CLIENT_SECRET":      "test-client-secret",
    "CRON_SECRET":               "test-cron-secret",
    "TOKEN_ENCRYPTION_KEY":      Fernet.generate_key().decode(),
})

import requests                 # noqa: E402

import token_crypto             # noqa: E402
import etl                      # noqa: E402
import tasks                    # noqa: E402

SLEEPS: list[float] = []
tasks._sleep = SLEEPS.append    # retries back off without waiting

FAILED: list[str] = []
U = "11111111-1111-1111-1111-111111111111"


def check(name: str, ok, detail="") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   [{detail}]"))
    if not ok:
        FAILED.append(name)


def raises(fn, exc) -> bool:
    try:
        fn()
    except exc:
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Fakes
# ─────────────────────────────────────────────────────────────────────────────

class FakeResult:
    def __init__(self, data):
        self.data = data


class Queue:
    """Successive responses for successive calls; [] once exhausted."""

    def __init__(self, *items):
        self.items = list(items)

    def next(self):
        return self.items.pop(0) if self.items else []


class FakeQuery:
    def __init__(self, db, table):
        self.db, self.table = db, table
        self.op, self.payload, self.kwargs, self.filters = None, None, {}, []

    def _set(self, op, payload=None, **kwargs):
        self.op, self.payload, self.kwargs = op, payload, kwargs
        return self

    def select(self, *cols):          return self._set("select", cols)
    def insert(self, data):           return self._set("insert", data)
    def upsert(self, data, **kwargs): return self._set("upsert", data, **kwargs)
    def update(self, data):           return self._set("update", data)
    def delete(self):                 return self._set("delete")

    def _filter(self, *f):
        self.filters.append(f)
        return self

    def eq(self, col, val):     return self._filter("eq", col, val)
    def lt(self, col, val):     return self._filter("lt", col, val)
    def gte(self, col, val):    return self._filter("gte", col, val)
    def is_(self, col, val):    return self._filter("is", col, val)
    def in_(self, col, val):    return self._filter("in", col, list(val))
    def order(self, col, **kw): return self._filter("order", col)
    def limit(self, n):         return self._filter("limit", n)

    def execute(self):
        call = {"table": self.table, "op": self.op, "payload": self.payload,
                "kwargs": self.kwargs, "filters": self.filters}
        self.db.calls.append(call)
        return FakeResult(self.db.respond(call))


class FakeRPC:
    def __init__(self, db, name, params):
        self.db, self.name, self.params = db, name, params

    def execute(self):
        call = {"table": "rpc", "op": self.name, "payload": self.params, "kwargs": {}, "filters": []}
        self.db.calls.append(call)
        return FakeResult(self.db.respond(call))


class FakeDB:
    """
    Stands in for supabase_admin. responses: {(table, op): value}; for RPCs
    the key is ("rpc", name). A Queue answers successive calls, a callable
    receives the call, anything else is returned as is.
    """

    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    def table(self, name):
        return FakeQuery(self, name)

    def rpc(self, name, params):
        return FakeRPC(self, name, params)

    def respond(self, call):
        value = self.responses.get((call["table"], call["op"]))
        if isinstance(value, Queue):
            return value.next()
        if callable(value):
            return value(call)
        return [] if value is None else value

    def find(self, table, op):
        return [c for c in self.calls if c["table"] == table and c["op"] == op]


def echo_ids(call):
    """An UPDATE ... WHERE id IN (...) that updates every listed row."""
    return [{"id": i} for f in call["filters"] if f[0] == "in" for i in f[2]]


class FakeResponse:
    def __init__(self, status, body, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}

    def json(self):
        return self._body

    @property
    def text(self):
        return str(self._body)


class FakeHTTP:
    """Stands in for the requests module inside tasks.py."""
    RequestException = requests.RequestException

    def __init__(self, handler):
        self.handler, self.calls = handler, []

    def get(self, url, headers=None, params=None, timeout=None):
        self.calls.append(("GET", url, dict(params or {})))
        return self.handler("GET", url, params or {})

    def post(self, url, data=None, timeout=None):
        self.calls.append(("POST", url, dict(data or {})))
        return self.handler("POST", url, data or {})


def alert_message(msg_id: str, body: str) -> dict:
    return {"id": msg_id, "payload": {
        "mimeType": "text/plain",
        "body":     {"data": base64.urlsafe_b64encode(body.encode()).decode()},
        "headers":  [{"name": "Date", "value": "Tue, 15 Sep 2026 10:05:33 +0530"}],
    }}


ALERT = "You have paid ₹ 60 to RAHUL SHARMA on 15 Sep"


# ─────────────────────────────────────────────────────────────────────────────
# etl.py
# ─────────────────────────────────────────────────────────────────────────────

def test_fingerprints():
    base = {"user_id": U, "amount": 120.0, "receiver": "VPA zomato@hdfcbank ZOMATO",
            "timestamp": "2026-09-15T12:00:00+05:30"}
    a = etl.bronze_fingerprint({**base, "message_id": "m1"})
    b = etl.bronze_fingerprint({**base, "message_id": "m2"})
    check("3.1 two identical payments in different emails stay separate", a != b)
    check("3.1 the same email always gets the same fingerprint",
          a == etl.bronze_fingerprint({**base, "message_id": "m1"}))
    check("3.1 rows without a message id keep the legacy fingerprint",
          etl.bronze_fingerprint({**base, "message_id": None})
          == etl.make_fingerprint(U, 120.0, base["receiver"], base["timestamp"]))


def test_raw_to_bronze():
    row = {"user_id": U, "amount": 100, "receiver": "VPA a@okaxis A", "timestamp": "2026-09-15T10:00:00+05:30",
           "source": "gmail", "raw_text": "x", "transaction_type": "debit"}
    raw = [
        {**row, "id": "r1", "message_id": "m1"},
        {**row, "id": "r2", "message_id": "m2"},
        {**row, "id": "r3", "message_id": None},
        {**row, "id": "r4", "message_id": None},
    ]
    db = FakeDB({("transactions", "select"): Queue(raw, []), ("transactions", "update"): echo_ids})
    etl.supabase_admin = db
    n = etl.run_raw_to_bronze(U)

    upserts = db.find("bronze_transactions", "upsert")
    rows    = upserts[0]["payload"] if upserts else []
    marks   = db.find("transactions", "update")
    select  = db.find("transactions", "select")[0]["filters"]
    check("3.5 raw→bronze processes every pending row", n == 4, n)
    check("3.4 one bronze write for the batch", len(upserts) == 1, len(upserts))
    check("WP11 bronze does not copy the alert text; it stays only on the raw row",
          bool(rows) and all(not r.get("raw_text") for r in rows), rows)
    check("3.1 identical payments in two emails both reach bronze",
          {"m1", "m2"} <= {r["message_id"] for r in rows}, rows)
    check("3.1 identical legacy rows in one batch collapse to one",
          sum(1 for r in rows if r["message_id"] is None) == 1, rows)
    check("3.1 rows already in bronze are skipped by the database",
          bool(upserts) and upserts[0]["kwargs"] == {"on_conflict": "fingerprint", "ignore_duplicates": True},
          upserts[0]["kwargs"] if upserts else None)
    check("3.5 the whole batch is marked processed in one write",
          len(marks) == 1 and sorted(marks[0]["filters"][0][2]) == ["r1", "r2", "r3", "r4"]
          and "processed_at" in marks[0]["payload"], marks)
    check("3.5 only this user's pending rows are read, a batch at a time",
          ("eq", "user_id", U) in select and ("is", "processed_at", "null") in select
          and ("limit", etl.BATCH_SIZE) in select, select)

    db = FakeDB({("transactions", "select"): Queue(raw[:1], raw[:1]), ("transactions", "update"): lambda call: []})
    etl.supabase_admin = db
    check("3.5 a batch that cannot be marked stops the stage instead of looping",
          raises(lambda: etl.run_raw_to_bronze(U), RuntimeError))


def test_bronze_to_silver():
    bronze = [
        {"id": "b1", "user_id": U, "amount": 120, "receiver": "VPA swiggy@icici SWIGGY",
         "timestamp": "2026-09-15T12:00:00+05:30", "source": "gmail", "transaction_type": "debit", "is_duplicate": False},
        {"id": "b2", "user_id": U, "amount": 30, "receiver": "VPA x@okaxis XYZ STORE",
         "timestamp": None, "source": "gmail", "transaction_type": "debit", "is_duplicate": False},
    ]
    db = FakeDB({("bronze_transactions", "select"): Queue(bronze, []), ("bronze_transactions", "update"): echo_ids})
    etl.supabase_admin = db
    n = etl.run_bronze_to_silver(U)

    upserts = db.find("silver_transactions", "upsert")
    rows    = upserts[0]["payload"] if upserts else []
    check("3.5 bronze→silver processes every pending row", n == 2, n)
    check("3.4 one silver write for the batch", len(upserts) == 1, len(upserts))
    check("3.5 a bronze row already in silver is skipped by the database",
          bool(upserts) and upserts[0]["kwargs"] == {"on_conflict": "bronze_id", "ignore_duplicates": True})
    check("3.5 merchant names are cleaned as before",
          bool(rows) and rows[0]["merchant"] == etl.clean_merchant("VPA swiggy@icici SWIGGY"), rows)
    check("4.1 the stable merchant key is written at ingest",
          bool(rows) and rows[0]["merchant_key"] == "upi:swiggy@icici", rows)
    check("4.1 a row with no UPI id still gets a key from its name",
          len(rows) == 2 and rows[1]["merchant_key"] == "upi:x@okaxis", rows)
    check("4.4 a shop is not recorded as a person",
          bool(rows) and rows[0]["merchant_is_person"] is False, rows)
    check("3.5 the date comes from the timestamp", bool(rows) and rows[0]["transaction_date"] == "2026-09-15", rows)
    check("3.5 a missing timestamp gives a NULL date, not a made-up one",
          len(rows) == 2 and rows[1]["transaction_date"] is None, rows)
    check("3.5 the bronze batch is marked processed",
          len(db.find("bronze_transactions", "update")) == 1)


def test_categorise():
    silver = [
        {"id": "s1", "merchant": "Swiggy", "bronze_id": "b1", "amount": 120, "transaction_date": "2026-09-15"},
        {"id": "s2", "merchant": "Mystery", "bronze_id": None, "amount": 5, "transaction_date": "2026-09-15"},
    ]
    seen = []

    def fake_predict(**kwargs):
        seen.append(kwargs)
        return [("Food", 0.9), ("Other", 0.1)]

    real_predict = etl.predict_batch
    etl.predict_batch = fake_predict
    try:
        db = FakeDB({
            ("silver_transactions", "select"): Queue(silver, []),
            ("bronze_transactions", "select"): [{"id": "b1", "receiver": "VPA swiggy@icici SWIGGY",
                                                 "timestamp": "2026-09-15T12:00:00+05:30"}],
            ("rpc", "apply_predictions"): 2,
        })
        etl.supabase_admin = db
        n = etl.run_categorise_silver(U)

        rpcs   = db.find("rpc", "apply_predictions")
        select = db.find("silver_transactions", "select")[0]["filters"]
        check("3.4 one apply_predictions call for the batch", len(rpcs) == 1, len(rpcs))
        check("3.4 unsure predictions are written as NULL, confident ones as the category",
              bool(rpcs) and rpcs[0]["payload"] == {"p_user_id": U, "p_rows": [
                  {"id": "s1", "category": "Food"}, {"id": "s2", "category": None}]},
              rpcs[0]["payload"] if rpcs else None)
        check("3.5 only uncategorised rows never predicted are read",
              ("eq", "is_categorised", False) in select and ("is", "predicted_at", "null") in select, select)
        check("3.5 the model sees the bronze receiver, else the merchant",
              bool(seen) and seen[0]["receivers"] == ["VPA swiggy@icici SWIGGY", "Mystery"], seen)
        check("3.5 the stage returns the rows that got a category", n == 1, n)

        db = FakeDB({("silver_transactions", "select"): Queue(silver, silver), ("rpc", "apply_predictions"): 0})
        etl.supabase_admin = db
        check("3.5 a batch nothing was written for stops the stage instead of looping",
              raises(lambda: etl.run_categorise_silver(U), RuntimeError))
    finally:
        etl.predict_batch = real_predict


def test_prediction_order():
    """
    4.3: the user's own rule beats the shared directory, which beats the
    model. A row settled by either never reaches the model at all.
    """
    silver = [
        {"id": "s1", "merchant": "Swiggy",  "merchant_key": "upi:swiggy@icici",
         "bronze_id": None, "amount": 120, "transaction_date": "2026-09-15"},
        {"id": "s2", "merchant": "Netflix", "merchant_key": "name:netflix",
         "bronze_id": None, "amount": 199, "transaction_date": "2026-09-15"},
        {"id": "s3", "merchant": "Mystery", "merchant_key": "name:mystery",
         "bronze_id": None, "amount": 50,  "transaction_date": "2026-09-15"},
        {"id": "s4", "merchant": "Unknown", "merchant_key": None,
         "bronze_id": None, "amount": 10,  "transaction_date": "2026-09-15"},
    ]
    seen = []

    def fake_predict(**kwargs):
        seen.append(kwargs)
        return [("Transport", 0.9)] * len(kwargs["receivers"])

    real_predict = etl.predict_batch
    etl.predict_batch = fake_predict
    try:
        db = FakeDB({
            ("silver_transactions", "select"): Queue(silver, []),
            ("user_merchant_rules", "select"): [
                {"merchant_key": "upi:swiggy@icici", "category": "Shopping"}],
            ("merchant_directory", "select"): [
                {"merchant_key": "name:netflix", "category": "Subscription"}],
            ("rpc", "apply_predictions"): 4,
        })
        etl.supabase_admin = db
        etl.run_categorise_silver(U)

        written = db.find("rpc", "apply_predictions")[0]["payload"]["p_rows"]
        by_id   = {r["id"]: r["category"] for r in written}

        check("4.3 the user's own correction decides the category",
              by_id.get("s1") == "Shopping", by_id)
        check("4.3 the shared directory decides when the user has no rule",
              by_id.get("s2") == "Subscription", by_id)
        check("4.3 the model decides everything else",
              by_id.get("s3") == "Transport" and by_id.get("s4") == "Transport", by_id)
        check("4.3 a settled row never reaches the model",
              len(seen) == 1 and seen[0]["receivers"] == ["Mystery", "Unknown"], seen)

        # Only the user's rules are read for this user, and the directory is
        # asked only about what the user has not already ruled on.
        rule_calls = db.find("user_merchant_rules", "select")
        check("4.2 rules are read for this user only",
              len(rule_calls) == 1
              and ("eq", "user_id", U) in rule_calls[0]["filters"], rule_calls)

        asked = [f[2] for c in db.find("merchant_directory", "select")
                 for f in c["filters"] if f[0] == "in"]
        check("4.3 a merchant the user has ruled on is not looked up in the directory",
              asked == [["name:mystery", "name:netflix"]], asked)
    finally:
        etl.predict_batch = real_predict


def test_other_rule_and_raced_batch():
    """BUG-06: a user's rule of "Other" is an answer; a batch the user corrected meanwhile is not an error."""
    silver = [{"id": "s1", "merchant": "Chai", "merchant_key": "upi:chai@ybl",
               "bronze_id": None, "amount": 20, "transaction_date": "2026-09-15"}]
    db = FakeDB({
        ("silver_transactions", "select"): Queue(silver, [], []),   # batch, end, re-check: nothing pending
        ("user_merchant_rules", "select"): [{"merchant_key": "upi:chai@ybl", "category": "Other"}],
        ("rpc", "apply_predictions"): 0,                             # the user corrected it meanwhile
    })
    etl.supabase_admin = db
    raised = raises(lambda: etl.run_categorise_silver(U), RuntimeError)
    written = db.find("rpc", "apply_predictions")[0]["payload"]["p_rows"]
    check("BUG-06 a rule of 'Other' is written as 'Other', not left uncategorised",
          written == [{"id": "s1", "category": "Other"}], written)
    check("BUG-06 zero rows updated because the user corrected them is not an error", not raised)


def test_correction_does_not_leak():
    """
    4.2/4.4: with no rule and no directory entry, nothing about another
    user's correction can reach this user — every row falls to the model.
    """
    silver = [{"id": "s1", "merchant": "Amazon", "merchant_key": "name:amazon",
               "bronze_id": None, "amount": 500, "transaction_date": "2026-09-15"}]

    real_predict = etl.predict_batch
    etl.predict_batch = lambda **kw: [("Shopping", 0.9)] * len(kw["receivers"])
    try:
        db = FakeDB({
            ("silver_transactions", "select"): Queue(silver, []),
            # Another user corrected Amazon to Groceries; with no consensus it
            # never became a directory entry, so this user is unaffected.
            ("user_merchant_rules", "select"): [],
            ("merchant_directory", "select"):  [],
            ("rpc", "apply_predictions"): 1,
        })
        etl.supabase_admin = db
        etl.run_categorise_silver(U)

        written = db.find("rpc", "apply_predictions")[0]["payload"]["p_rows"]
        check("4.4 another user's lone correction does not change this user's prediction",
              written == [{"id": "s1", "category": "Shopping"}], written)
    finally:
        etl.predict_batch = real_predict


# ─────────────────────────────────────────────────────────────────────────────
# tasks.py
# ─────────────────────────────────────────────────────────────────────────────

def test_gmail_query():
    now = datetime(2026, 9, 15, 10, 0, tzinfo=timezone.utc)
    q = tasks._gmail_query("2026-09-15T08:00:00+00:00", now)
    check("3.2 an incremental query starts one hour before the last sync",
          q.endswith(f"after:{int(datetime(2026, 9, 15, 7, 0, tzinfo=timezone.utc).timestamp())}"), q)
    q = tasks._gmail_query(None, now)
    check("3.2 the first sync reads from the start of the current month, India time",
          q.endswith(f"after:{int(datetime(2026, 8, 31, 18, 30, tzinfo=timezone.utc).timestamp())}"), q)
    # 1 Oct 02:00 IST is still 30 Sep in UTC: the month is India's.
    q = tasks._gmail_query(None, datetime(2026, 9, 30, 20, 30, tzinfo=timezone.utc))
    check("3.2 the first-sync month follows India's calendar",
          q.endswith(f"after:{int(datetime(2026, 9, 30, 18, 30, tzinfo=timezone.utc).timestamp())}"), q)
    check("3.2 the query keeps the bank senders", q.startswith(tasks.BANK_QUERY), q)
    # Gmail's from: matches whole words: "hdfc" misses a bare alerts@hdfcbank.bank.in
    # sender (a Rs.360 alert was never ingested, 2026-10-02).
    check("3.2 the query matches HDFC's bare-address sender", "hdfcbank" in tasks.BANK_QUERY, tasks.BANK_QUERY)


def test_list_pagination():
    pages = {None: {"messages": [{"id": "a"}, {"id": "b"}], "nextPageToken": "t1"},
             "t1": {"messages": [{"id": "c"}]}}
    http = FakeHTTP(lambda method, url, params: FakeResponse(200, pages[params.get("pageToken")]))
    tasks.requests = http
    ids, _ = tasks._list_message_ids(U, "acc", None, "q")
    check("3.2 every page is read (no 50-message cap)", ids == ["a", "b", "c"], ids)
    check("3.2 each page asks for 100 ids", all(c[2].get("maxResults") == 100 for c in http.calls), http.calls)

    saved = tasks.MAX_PAGES
    tasks.MAX_PAGES = 2
    try:
        http = FakeHTTP(lambda m, u, p: FakeResponse(200, {"messages": [{"id": "x"}], "nextPageToken": "more"}))
        tasks.requests = http
        ids, _ = tasks._list_message_ids(U, "acc", None, "q")
        check("3.2 listing stops at MAX_PAGES", len(http.calls) == 2 and len(ids) == 2, http.calls)
    finally:
        tasks.MAX_PAGES = saved

    tasks.requests = FakeHTTP(lambda m, u, p: FakeResponse(401, {}))
    check("3.3 a 401 with no refresh token means reconnect",
          raises(lambda: tasks._list_message_ids(U, "acc", None, "q"), tasks.GmailReconnectRequired))


def test_refresh_rejected():
    tasks.supabase_admin = FakeDB()
    tasks.requests = FakeHTTP(lambda m, u, d: FakeResponse(400, {"error": "invalid_grant"}))
    check("3.3 invalid_grant from Google means reconnect",
          raises(lambda: tasks._refresh_google_token(U, "ref"), tasks.GmailReconnectRequired))
    tasks.requests = FakeHTTP(lambda m, u, d: FakeResponse(500, {"error": "backend_error"}))
    try:
        tasks._refresh_google_token(U, "ref")
        caught = None
    except Exception as e:
        caught = e
    check("3.3 other refresh errors are not reported as reconnect",
          isinstance(caught, RuntimeError) and not isinstance(caught, tasks.GmailReconnectRequired),
          repr(caught))


PIPELINE_RUNS: list[str] = []


def _sync(db, handler, job_id="job-1"):
    tasks.supabase_admin = db
    tasks.requests = FakeHTTP(handler)
    tasks.run_pipeline_task = lambda uid: PIPELINE_RUNS.append(uid)
    PIPELINE_RUNS.clear()
    return tasks.fetch_gmail_for_user_task(U, job_id)


def test_sync_success():
    messages = {"m1": alert_message("m1", ALERT), "m2": alert_message("m2", ALERT)}

    def handler(method, url, params):
        if url.endswith("/profile"):
            return FakeResponse(200, {})
        if url.endswith("/messages"):
            return FakeResponse(200, {"messages": [{"id": "m1"}, {"id": "m2"}]})
        return FakeResponse(200, messages[url.rsplit("/", 1)[1]])

    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": token_crypto.encrypt("ref")}],
        ("gmail_sync", "select"):        [{"last_fetched": "2026-09-15T08:00:00+00:00"}],
        ("transactions", "upsert"):      [{"id": "r1"}],     # one of the two was new
    })
    result = _sync(db, handler)

    jobs    = db.find("sync_jobs", "update")
    inserts = db.find("transactions", "upsert")
    cursor  = [c["payload"] for c in db.find("gmail_sync", "update")]
    check("3.7 the job is marked running first, with a start time",
          bool(jobs) and jobs[0]["payload"].get("status") == "running" and jobs[0]["payload"].get("started_at"), jobs)
    check("3.7 the job ends succeeded with the number of new rows",
          bool(jobs) and jobs[-1]["payload"].get("status") == "succeeded"
          and jobs[-1]["payload"].get("transactions_found") == 1, jobs[-1] if jobs else None)
    check("3.7 every job update targets this job", all(("eq", "id", "job-1") in c["filters"] for c in jobs), jobs)
    check("3.1 both alerts are inserted with their message ids; stored ones are skipped",
          bool(inserts) and inserts[0]["kwargs"] == {"on_conflict": "user_id,message_id", "ignore_duplicates": True}
          and sorted(r["message_id"] for r in inserts[0]["payload"]) == ["m1", "m2"], inserts)
    check("3.2 the cursor moves to the moment the sync started",
          bool(cursor) and bool(jobs) and cursor[0].get("last_fetched") == jobs[0]["payload"]["started_at"], cursor)
    check("3.7 the pipeline runs after the insert", PIPELINE_RUNS == [U], PIPELINE_RUNS)
    check("3.7 the task reports the new-row count", result.get("transactions_found") == 1, result)


def test_logs_hold_no_email_text():
    """Phase 7: logs carry ids and counts, never email bodies, names or amounts from mail."""
    import logging
    records = []

    class Keep(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    root = logging.getLogger()
    handler, old_level = Keep(), root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        body = "You have paid ₹ 60 to SECRETNAME PERSON on 15 Sep"
        messages = {"m1": alert_message("m1", body)}

        def handler_fn(method, url, params):
            if url.endswith("/messages"):
                return FakeResponse(200, {"messages": [{"id": "m1"}]})
            return FakeResponse(200, messages["m1"])

        db = FakeDB({
            ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": token_crypto.encrypt("REFRESH-XYZ")}],
            ("gmail_sync", "select"):        [{"last_fetched": "2026-09-15T08:00:00+00:00"}],
            ("transactions", "upsert"):      [{"id": "r1"}],
        })
        _sync(db, handler_fn, "job-12")
    finally:
        root.removeHandler(handler)
        root.setLevel(old_level)
    text = "\n".join(records)
    check("Phase 7 sync logs contain no email text or names", "SECRETNAME" not in text and "paid" not in text, text[-500:])
    check("Phase 7 sync logs contain no tokens", "REFRESH-XYZ" not in text and "acc" not in text.split(), text[-500:])
    check("Phase 7 sync logs say what happened", "Sync finished" in text, text[-500:])


def test_sync_reconnect():
    def handler(method, url, params):
        if method == "GET" and url.endswith("/messages"):
            return FakeResponse(401, {})
        if method == "POST":
            return FakeResponse(400, {"error": "invalid_grant"})
        return FakeResponse(500, {})

    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "old", "refresh_token_encrypted": token_crypto.encrypt("ref")}],
        ("gmail_sync", "select"):        [{"last_fetched": None}],
    })
    result = _sync(db, handler, "job-2")
    flags = [c["payload"] for c in db.find("gmail_sync", "update")]
    last  = db.find("sync_jobs", "update")[-1]["payload"]
    check("3.3 a rejected grant marks the user needs_reconnect", {"needs_reconnect": True} in flags, flags)
    check("3.3 the job fails with the reconnect message",
          last.get("status") == "failed" and last.get("error") == tasks.RECONNECT_MESSAGE, last)
    check("3.3 nothing is inserted or run after a rejected grant",
          PIPELINE_RUNS == [] and not db.find("transactions", "upsert"))
    check("3.3 the task reports the skip", result.get("status") == "skipped", result)

    db = FakeDB({("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": "not-ciphertext"}]})
    _sync(db, handler, "job-3")
    flags = [c["payload"] for c in db.find("gmail_sync", "update")]
    check("3.3 a refresh token that cannot be decrypted also means reconnect",
          {"needs_reconnect": True} in flags, flags)


def test_sync_not_connected_and_failure():
    db = FakeDB({("gmail_credentials", "select"): []})
    _sync(db, lambda m, u, p: FakeResponse(200, {}), "job-4")
    last = db.find("sync_jobs", "update")[-1]["payload"]
    check("3.7 no stored tokens fails the job as not connected",
          last.get("status") == "failed" and last.get("error") == tasks.NOT_CONNECTED_MESSAGE, last)
    check("3.7 not connected is not reported as reconnect", not db.find("gmail_sync", "update"))

    def broken(method, url, params):
        if url.endswith("/profile"):
            return FakeResponse(200, {})
        return FakeResponse(503, {})

    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": None}],
        ("gmail_sync", "select"):        [{"last_fetched": None}],
    })
    raised = raises(lambda: _sync(db, broken, "job-5"), RuntimeError)
    last = db.find("sync_jobs", "update")[-1]["payload"]
    check("3.7 an unexpected error fails the job with a generic message and is re-raised",
          raised and last.get("status") == "failed" and last.get("error") == tasks.FAILED_MESSAGE, last)


def test_download_failures():
    """DATA-07: a message that cannot be downloaded is kept and retried; the cursor still moves."""
    calls = {"m1": 0, "m2": 0}

    def flaky(method, url, params):
        if url.endswith("/messages"):
            return FakeResponse(200, {"messages": [{"id": "m1"}, {"id": "m2"}]})
        msg = url.rsplit("/", 1)[1]
        calls[msg] += 1
        if msg == "m2":
            return FakeResponse(429, {}, {"Retry-After": "2"})   # rate limited every time
        return FakeResponse(200, alert_message("m1", ALERT))

    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": None}],
        ("gmail_sync", "select"):        [{"last_fetched": "2026-09-15T08:00:00+00:00", "pending_message_ids": []}],
        ("transactions", "upsert"):      [{"id": "r1"}],
    })
    SLEEPS.clear()
    result = _sync(db, flaky, "job-7")
    cursor = [c["payload"] for c in db.find("gmail_sync", "update") if "last_fetched" in c["payload"]]
    jobs = db.find("sync_jobs", "update")
    inserted = db.find("transactions", "upsert")
    check("DATA-07 a rate-limited message is retried before giving up",
          calls["m2"] == tasks.MAX_ATTEMPTS, calls)
    check("DATA-07 Retry-After is honoured", 2 in SLEEPS, SLEEPS)
    check("DATA-07 the messages that did download are still stored",
          bool(inserted) and [r["message_id"] for r in inserted[0]["payload"]] == ["m1"], inserted)
    check("DATA-07 the cursor moves to the sync's start even when a message was not read",
          len(cursor) == 1 and cursor[0]["last_fetched"] == jobs[0]["payload"]["started_at"], cursor)
    check("DATA-07 the message that was not read is kept for the next sync",
          len(cursor) == 1 and cursor[0].get("pending_message_ids") == ["m2"], cursor)
    check("DATA-07 the sync still succeeds and runs the pipeline",
          result.get("status") == "ok" and PIPELINE_RUNS == [U], result)

    # The next sync retries the kept message even though it is outside the new window.
    fetched = []

    def next_sync(method, url, params):
        if url.endswith("/messages"):
            return FakeResponse(200, {"messages": [{"id": "m3"}]})
        msg = url.rsplit("/", 1)[1]
        fetched.append(msg)
        return FakeResponse(200, alert_message(msg, ALERT))

    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": None}],
        ("gmail_sync", "select"):        [{"last_fetched": "2026-09-27T08:00:00+00:00", "pending_message_ids": ["m2"]}],
        ("transactions", "upsert"):      [{"id": "r2"}, {"id": "r3"}],
    })
    _sync(db, next_sync, "job-7b")
    cursor = [c["payload"] for c in db.find("gmail_sync", "update") if "last_fetched" in c["payload"]]
    check("DATA-07 kept messages are retried with the new mail", sorted(fetched) == ["m2", "m3"], fetched)
    check("DATA-07 the kept list empties once they are read",
          len(cursor) == 1 and cursor[0].get("pending_message_ids") == [], cursor)

    # A database without the pending_message_ids column still moves the cursor.
    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": None}],
        ("gmail_sync", "select"):        [{"last_fetched": "2026-09-15T08:00:00+00:00"}],
        ("transactions", "upsert"):      [{"id": "r1"}],
    })
    _sync(db, flaky, "job-7c")
    cursor = [c["payload"] for c in db.find("gmail_sync", "update") if "last_fetched" in c["payload"]]
    check("DATA-07 without the migration the cursor moves and no unknown column is written",
          len(cursor) == 1 and "pending_message_ids" not in cursor[0], cursor)

    # A transient 503 that recovers is invisible to the user.
    state = {"n": 0}

    def recovers(method, url, params):
        if url.endswith("/messages"):
            return FakeResponse(200, {"messages": [{"id": "m1"}]})
        state["n"] += 1
        return FakeResponse(503, {}) if state["n"] == 1 else FakeResponse(200, alert_message("m1", ALERT))

    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": None}],
        ("gmail_sync", "select"):        [{"last_fetched": "2026-09-15T08:00:00+00:00"}],
        ("transactions", "upsert"):      [{"id": "r1"}],
    })
    _sync(db, recovers, "job-8")
    cursor = [c["payload"] for c in db.find("gmail_sync", "update") if "last_fetched" in c["payload"]]
    check("DATA-07 a transient 503 is retried and the cursor then moves", len(cursor) == 1, cursor)

    # Gmail reports rate limits as 403 too.
    state = {"n": 0}

    def limited(method, url, params):
        if url.endswith("/messages"):
            return FakeResponse(200, {"messages": [{"id": "m1"}]})
        state["n"] += 1
        return (FakeResponse(403, {"error": {"errors": [{"reason": "userRateLimitExceeded"}]}}) if state["n"] == 1
                else FakeResponse(200, alert_message("m1", ALERT)))

    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": None}],
        ("gmail_sync", "select"):        [{"last_fetched": "2026-09-15T08:00:00+00:00"}],
        ("transactions", "upsert"):      [{"id": "r1"}],
    })
    _sync(db, limited, "job-11")
    cursor = [c["payload"] for c in db.find("gmail_sync", "update") if "last_fetched" in c["payload"]]
    check("DATA-07 a 403 rate limit is retried like a 429", state["n"] == 2 and len(cursor) == 1, (state, cursor))

    # A message Gmail deleted (404) is not a failure.
    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": None}],
        ("gmail_sync", "select"):        [{"last_fetched": None}],
    })
    _sync(db, lambda m, u, p: FakeResponse(200, {"messages": [{"id": "gone"}]}) if u.endswith("/messages")
          else FakeResponse(404, {}), "job-10")
    cursor = [c["payload"] for c in db.find("gmail_sync", "update") if "last_fetched" in c["payload"]]
    check("DATA-07 a deleted message (404) does not hold the cursor back", len(cursor) == 1, cursor)


def test_ist_dates():
    check("dates are Indian dates: 00:30 IST is stored as 19:00 UTC the day before",
          etl.ist_date("2026-09-20T19:00:00+00:00") == "2026-09-21", etl.ist_date("2026-09-20T19:00:00+00:00"))
    check("an IST timestamp keeps its date", etl.ist_date("2026-09-21T10:05:33+05:30") == "2026-09-21")
    check("a missing timestamp stays missing", etl.ist_date(None) is None)


def test_stale_jobs():
    db = FakeDB()
    tasks.supabase_admin = db
    tasks.fail_stale_jobs()
    ups = db.find("sync_jobs", "update")
    check("a job still queued or running after 30 minutes is failed with the interrupted message",
          len(ups) == 1 and ups[0]["payload"]["status"] == "failed"
          and ups[0]["payload"]["error"] == tasks.INTERRUPTED_MESSAGE
          and ("in", "status", ["queued", "running"]) in ups[0]["filters"]
          and any(f[0] == "lt" and f[1] == "created_at" for f in ups[0]["filters"]), ups)


def test_sync_slots():
    """At most MAX_CONCURRENT_SYNCS syncs run at once; a failed sync frees its slot."""
    real_slots = tasks._SYNC_SLOTS
    tasks._SYNC_SLOTS = threading.BoundedSemaphore(1)
    lock, state = threading.Lock(), {"now": 0, "peak": 0, "fail": False}

    def handler(method, url, params):
        if url.endswith("/messages"):
            with lock:
                state["now"] += 1
                state["peak"] = max(state["peak"], state["now"])
            time.sleep(0.05)
            with lock:
                state["now"] -= 1
            if state["fail"]:
                return FakeResponse(500, {})
            return FakeResponse(200, {"messages": []})
        return FakeResponse(200, {})

    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": token_crypto.encrypt("ref")}],
        ("gmail_sync", "select"):        [{"last_fetched": "2026-09-15T08:00:00+00:00"}],
    })
    tasks.supabase_admin = db
    tasks.requests = FakeHTTP(handler)
    tasks.run_pipeline_task = lambda uid: None
    try:
        threads = [threading.Thread(target=tasks.fetch_gmail_for_user_task, args=(U, f"job-{i}")) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        done = [c["payload"].get("status") for c in db.find("sync_jobs", "update")]
        check("three syncs with one slot never overlap", state["peak"] == 1, state)
        check("all three syncs still finish", done.count("succeeded") == 3, done)

        state["fail"] = True
        raises(lambda: tasks.fetch_gmail_for_user_task(U, "job-x"), Exception)
        got = tasks._SYNC_SLOTS.acquire(blocking=False)
        check("a failed sync releases its slot", got)
        if got:
            tasks._SYNC_SLOTS.release()
    finally:
        tasks._SYNC_SLOTS = real_slots


def test_sync_counters():
    """WP4: each sync records what it read; a database without the columns still syncs."""
    def handler(method, url, params):
        if url.endswith("/messages"):
            return FakeResponse(200, {"messages": [{"id": "m1"}, {"id": "m2"}, {"id": "m3"}]})
        msg = url.rsplit("/", 1)[1]
        if msg == "m3":
            return FakeResponse(200, {"id": "m3", "payload": {"mimeType": "text/plain",
                                      "body": {"data": base64.urlsafe_b64encode(b"Your OTP is 123456").decode()}}})
        return FakeResponse(200, alert_message(msg, ALERT))

    rows = {
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": token_crypto.encrypt("ref")}],
        ("gmail_sync", "select"):        [{"last_fetched": "2026-09-15T08:00:00+00:00"}],
        ("transactions", "upsert"):      [{"id": "r1"}, {"id": "r2"}],
    }
    db = FakeDB(dict(rows))
    _sync(db, handler, "job-c1")
    last = db.find("sync_jobs", "update")[-1]["payload"]
    check("WP4 the job records messages listed, failed and parsed",
          (last.get("messages_listed"), last.get("messages_failed"), last.get("alerts_parsed")) == (3, 0, 2), last)

    # Old schema: the update that carries the counters fails, the sync still finishes.
    def old_schema(call):
        if "messages_listed" in call["payload"]:
            raise RuntimeError("column sync_jobs.messages_listed does not exist")
        return []
    db = FakeDB({**rows, ("sync_jobs", "update"): old_schema})
    result = _sync(db, handler, "job-c2")
    finals = [c["payload"] for c in db.find("sync_jobs", "update") if c["payload"].get("status") == "succeeded"]
    plain = [f for f in finals if "messages_listed" not in f]
    check("WP4 a database without the counter columns still finishes the sync, without them",
          result.get("status") == "ok" and len(plain) == 1, (result, finals))


def test_one_active_sync():
    """WP4 / B3: one queued or running sync per user."""
    class UniqueViolation(Exception):
        code = "23505"

    def refuse(call):
        raise UniqueViolation("duplicate key value violates unique constraint sync_jobs_one_active")

    db = FakeDB({("sync_jobs", "insert"): refuse})
    tasks.supabase_admin = db
    check("WP4 a second active job for the user raises SyncAlreadyActive",
          raises(lambda: tasks.create_sync_job(U, "manual"), tasks.SyncAlreadyActive))
    stale = [c for c in db.find("sync_jobs", "update") if ("eq", "user_id", U) in c["filters"]]
    check("WP4 the user's own dead jobs are failed before the insert, so they cannot block a new sync",
          len(stale) == 1 and stale[0]["payload"]["status"] == "failed"
          and any(f[0] == "lt" and f[1] == "created_at" for f in stale[0]["filters"]), stale)

    def other_error(call):
        raise ConnectionError("db down")
    tasks.supabase_admin = FakeDB({("sync_jobs", "insert"): other_error})
    check("WP4 any other insert error is not mistaken for a busy user",
          raises(lambda: tasks.create_sync_job(U, "manual"), ConnectionError))

    tasks.supabase_admin = FakeDB({("sync_jobs", "select"): [{"id": "job-1"}]})
    check("WP4 has_active_sync is true while a recent job is queued or running", tasks.has_active_sync(U))
    sel = tasks.supabase_admin.find("sync_jobs", "select")[0]["filters"]
    check("WP4 has_active_sync looks only at this user's recent queued or running jobs",
          ("eq", "user_id", U) in sel and ("in", "status", ["queued", "running"]) in sel
          and any(f[0] == "gte" and f[1] == "created_at" for f in sel), sel)
    tasks.supabase_admin = FakeDB()
    check("WP4 has_active_sync is false with no active job", not tasks.has_active_sync(U))

    # A scheduled sync for a user who is already syncing is skipped, not failed.
    db = FakeDB({("sync_jobs", "insert"): refuse})
    tasks.supabase_admin = db
    result = tasks.fetch_gmail_for_user_task(U, None, "cron_runner")
    check("WP4 a scheduled sync for a busy user is skipped without touching Gmail",
          result.get("status") == "skipped" and not db.find("gmail_credentials", "select"), result)


def test_prune_old_rows():
    """WP6: event rows older than 90 days are deleted; a failure only warns."""
    db = FakeDB()
    tasks.supabase_admin = db
    tasks.prune_old_rows()
    deletes = db.find("app_events", "delete")
    check("WP6 events older than 90 days are deleted",
          len(deletes) == 1 and any(f[0] == "lt" and f[1] == "created_at" for f in deletes[0]["filters"]), deletes)

    class Broken(FakeDB):
        def table(self, name):
            raise ConnectionError("db down")
    tasks.supabase_admin = Broken()
    check("WP6 a failed prune does not stop the scheduled run", not raises(tasks.prune_old_rows, Exception))


def test_history_window():
    """WP9: earlier months are read one calendar month (India) at a time, up to a limit."""
    ist, now = tasks.IST, datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    w = tasks.next_history_window(datetime(2026, 9, 1, tzinfo=ist), now)
    check("WP9 the next window is the month before the oldest month read",
          w == (datetime(2026, 8, 1, tzinfo=ist), datetime(2026, 9, 1, tzinfo=ist)), w)
    w = tasks.next_history_window(datetime(2026, 1, 1, tzinfo=ist), now)
    check("WP9 the window steps back over a year boundary", w and w[0] == datetime(2025, 12, 1, tzinfo=ist), w)
    w = tasks.next_history_window(datetime(2026, 9, 17, tzinfo=ist), now)
    check("WP9 a date inside a month counts as that month's start",
          w == (datetime(2026, 8, 1, tzinfo=ist), datetime(2026, 9, 1, tzinfo=ist)), w)
    check("WP9 the last month inside the limit can still be read",
          tasks.next_history_window(datetime(2025, 10, 1, tzinfo=ist), now) is not None)
    check("WP9 nothing older than the limit is offered",
          tasks.next_history_window(datetime(2025, 9, 1, tzinfo=ist), now) is None)

    start, end = datetime(2026, 8, 1, tzinfo=ist), datetime(2026, 9, 1, tzinfo=ist)
    q = tasks._gmail_window_query(start, end)
    check("WP9 a window query keeps the bank senders and bounds both ends",
          q.startswith(tasks.BANK_QUERY) and q.endswith(f"after:{int(start.timestamp())} before:{int(end.timestamp())}"), q)

    # Unknown oldest month (users from before this feature): the earliest stored payment's month.
    db = FakeDB({("gmail_sync", "select"): [{"history_from": None}],
                 ("transactions", "select"): [{"timestamp": "2026-06-17T10:00:00+00:00"}]})
    tasks.supabase_admin = db
    w = tasks.history_window(U, now)
    check("WP9 without a recorded start, the window follows the earliest stored payment",
          w == (datetime(2026, 5, 1, tzinfo=ist), datetime(2026, 6, 1, tzinfo=ist)), w)
    tasks.supabase_admin = FakeDB({("gmail_sync", "select"): [{"history_from": None}]})
    w = tasks.history_window(U, now)
    check("WP9 with no payments at all, the window is the month before the current one",
          w == (datetime(2026, 8, 1, tzinfo=ist), datetime(2026, 9, 1, tzinfo=ist)), w)


def test_backfill_sync():
    """WP9: a backfill reads one earlier month and never touches the normal cursor."""
    now = datetime.now(timezone.utc)
    end = tasks._month_start_ist(now)
    start = tasks._month_start_ist(end - timedelta(days=1))
    queries = []

    def handler(method, url, params):
        if url.endswith("/messages"):
            queries.append(params.get("q"))
            return FakeResponse(200, {"messages": [{"id": "m1"}, {"id": "m2"}]})
        msg = url.rsplit("/", 1)[1]
        if msg == "m2":
            return FakeResponse(429, {})
        return FakeResponse(200, alert_message(msg, ALERT))

    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": token_crypto.encrypt("ref")}],
        ("gmail_sync", "select"): [{"last_fetched": "2026-09-20T08:00:00+00:00", "pending_message_ids": ["old"],
                                     "history_from": end.isoformat()}],
        ("transactions", "upsert"): [{"id": "r1"}],
    })
    tasks.supabase_admin = db
    tasks.requests = FakeHTTP(handler)
    tasks.run_pipeline_task = lambda uid: PIPELINE_RUNS.append(uid)
    PIPELINE_RUNS.clear()
    SLEEPS.clear()
    result = tasks.backfill_gmail_for_user_task(U, "job-b1")

    updates = [c["payload"] for c in db.find("gmail_sync", "update")]
    check("WP9 the backfill searches exactly the month before the oldest one read",
          queries == [tasks._gmail_window_query(start, end)], queries)
    check("WP9 the oldest month read moves back",
          any(u.get("history_from") == start.isoformat() for u in updates), updates)
    check("WP9 the backfill never moves the normal read cursor",
          not any("last_fetched" in u for u in updates), updates)
    check("WP9 a message that could not be read is added to the retry list, the old ones kept",
          any(u.get("pending_message_ids") == ["old", "m2"] for u in updates), updates)
    check("WP9 the backfill finishes and runs the pipeline",
          result.get("status") == "ok" and PIPELINE_RUNS == [U], result)

    # At the limit: refused with a fixed message, Gmail untouched.
    queries.clear()
    old = tasks._month_start_ist(now.replace(year=now.year - 2))
    db = FakeDB({
        ("gmail_credentials", "select"): [{"access_token": "acc", "refresh_token_encrypted": None}],
        ("gmail_sync", "select"): [{"history_from": old.isoformat()}],
    })
    tasks.supabase_admin = db
    result = tasks.backfill_gmail_for_user_task(U, "job-b2")
    final = db.find("sync_jobs", "update")[-1]["payload"]
    check("WP9 a backfill past the limit fails with the fixed message and reads nothing",
          queries == [] and final.get("status") == "failed" and final.get("error") == tasks.HISTORY_LIMIT_MESSAGE
          and result.get("status") == "skipped", (final, result))


def test_first_sync_records_where_history_starts():
    """WP9: the first normal sync records the month it started reading from."""
    def handler(method, url, params):
        return FakeResponse(200, {"messages": []}) if url.endswith("/messages") else FakeResponse(200, {})
    cred = [{"access_token": "acc", "refresh_token_encrypted": None}]

    db = FakeDB({("gmail_credentials", "select"): cred, ("gmail_sync", "select"): [{"last_fetched": None, "history_from": None}]})
    _sync(db, handler, "job-h1")
    first = [c["payload"] for c in db.find("gmail_sync", "update") if "last_fetched" in c["payload"]][0]
    month = tasks._month_start_ist(datetime.now(timezone.utc))
    check("WP9 a first sync records the start of the month it read", first.get("history_from") == month.isoformat(), first)

    db = FakeDB({("gmail_credentials", "select"): cred, ("gmail_sync", "select"): [{"last_fetched": "2026-09-20T08:00:00+00:00", "history_from": None}]})
    _sync(db, handler, "job-h2")
    later = [c["payload"] for c in db.find("gmail_sync", "update") if "last_fetched" in c["payload"]][0]
    check("WP9 a later sync leaves an unknown start alone", "history_from" not in later, later)

    db = FakeDB({("gmail_credentials", "select"): cred, ("gmail_sync", "select"): [{"last_fetched": None}]})
    _sync(db, handler, "job-h3")
    old = [c["payload"] for c in db.find("gmail_sync", "update") if "last_fetched" in c["payload"]][0]
    check("WP9 a database without the column still syncs", "history_from" not in old, old)


def test_jobs_and_fan_out():
    db = FakeDB({("sync_jobs", "insert"): [{"id": "job-9"}]})
    tasks.supabase_admin = db
    job_id = tasks.create_sync_job(U, "scheduled", "cron_runner")
    inserted = db.find("sync_jobs", "insert")
    check("3.7 create_sync_job stores kind and runner and returns the id",
          job_id == "job-9" and inserted and inserted[0]["payload"] == {"user_id": U, "kind": "scheduled", "runner": "cron_runner"},
          inserted)

    runs = []
    real_task = tasks.fetch_gmail_for_user_task
    tasks.fetch_gmail_for_user_task = lambda uid, job_id=None, runner="api": runs.append((uid, job_id, runner)) or {"status": "ok"}
    try:
        db = FakeDB({("gmail_sync", "select"): [{"user_id": "u1"}, {"user_id": "u2"}]})
        tasks.supabase_admin = db
        tasks.fetch_gmail_for_all_users_task(runner="cron_runner")
        select = db.find("gmail_sync", "select")[0]["filters"]
        check("3.3 scheduled syncs skip users who must reconnect", ("eq", "needs_reconnect", False) in select, select)
        check("WP6 each scheduled run also prunes old event rows", bool(db.find("app_events", "delete")))
        check("3.7 each scheduled sync records which process ran it",
              sorted(runs) == [("u1", None, "cron_runner"), ("u2", None, "cron_runner")], runs)
    finally:
        tasks.fetch_gmail_for_user_task = real_task


if __name__ == "__main__":
    for test in (test_fingerprints, test_raw_to_bronze, test_bronze_to_silver, test_categorise,
                 test_prediction_order, test_other_rule_and_raced_batch, test_correction_does_not_leak,
                 test_gmail_query, test_list_pagination, test_refresh_rejected,
                 test_sync_success, test_logs_hold_no_email_text, test_sync_reconnect, test_sync_not_connected_and_failure,
                 test_download_failures, test_ist_dates, test_stale_jobs,
                 test_sync_slots, test_sync_counters, test_one_active_sync, test_prune_old_rows,
                 test_history_window, test_backfill_sync, test_first_sync_records_where_history_starts,
                 test_jobs_and_fan_out):
        test()
    print(f"\n{len(FAILED)} check(s) failed" if FAILED else "\nALL PIPELINE CHECKS PASSED")
    sys.exit(1 if FAILED else 0)
