"""
test_api_security.py  ─  offline checks for the API (Phases 2 and 3)
═══════════════════════════════════════════════════════════════════
Covers CORS, the cron secret, OAuth state, token encryption, config, account
deletion, the sync rate limit, reconnect and sync jobs.
Row-level security and the SQL functions are tested by
supabase/tests/run_local.sh; the pipeline by test_pipeline.py.

Needs no network, no Supabase project and no .env: the script sets
placeholder settings itself, and every database and Google call goes to an
in-memory fake. Run in the dev image from the repo root (Git Bash on Windows:
prefix MSYS_NO_PATHCONV=1 and use "$(pwd -W)"):

    docker run --rm --network none -v "$(pwd)/backend:/app:ro" \
        spendstream-backend python test_api_security.py

Exit code is non-zero if any check fails.
"""

import inspect
import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cryptography.fernet import Fernet

# Placeholder settings, set before config.py reads them. They override any
# real .env values, so nothing here can reach a real service.
os.environ.update({
    "SUPABASE_URL":              "https://placeholder.supabase.co",
    "SUPABASE_ANON_KEY":         "placeholder.placeholder.placeholder",
    "SUPABASE_SERVICE_ROLE_KEY": "placeholder.placeholder.placeholder",
    "GOOGLE_CLIENT_ID":          "test-client-id",
    "GOOGLE_CLIENT_SECRET":      "test-client-secret",
    "CRON_SECRET":               "test-cron-secret",
    "TOKEN_ENCRYPTION_KEY":      Fernet.generate_key().decode(),
    "FRONTEND_URL":              "http://localhost:5173",
    "BACKEND_URL":               "http://localhost:8000",
})

import requests                                  # noqa: E402
from fastapi import HTTPException                # noqa: E402
from fastapi.testclient import TestClient        # noqa: E402

import config                                    # noqa: E402
import token_crypto                              # noqa: E402
import main                                      # noqa: E402

FAILED: list[str] = []


def check(name: str, ok, detail="") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   [{detail}]"))
    if not ok:
        FAILED.append(name)


# ─────────────────────────────────────────────────────────────────────────────
# Fakes
# ─────────────────────────────────────────────────────────────────────────────

class FakeResult:
    def __init__(self, data):
        self.data = data


class FakeQuery:
    """Records one chained supabase-py call and returns canned rows."""

    def __init__(self, db, table):
        self.db, self.table = db, table
        self.op, self.payload, self.filters = None, None, []

    def _set(self, op, payload=None):
        self.op, self.payload = op, payload
        return self

    def select(self, *cols): return self._set("select", cols)
    def insert(self, data):  return self._set("insert", data)
    def upsert(self, data):  return self._set("upsert", data)
    def update(self, data):  return self._set("update", data)
    def delete(self):        return self._set("delete")

    def _filter(self, *f):
        self.filters.append(f)
        return self

    def eq(self, col, val):  return self._filter("eq", col, val)
    def lt(self, col, val):  return self._filter("lt", col, val)
    def gte(self, col, val): return self._filter("gte", col, val)
    def limit(self, n):      return self._filter("limit", n)
    def in_(self, col, val): return self._filter("in", col, list(val))
    def or_(self, expr):     return self._filter("or", expr)

    def execute(self):
        self.db.calls.append((self.table, self.op, self.payload, list(self.filters)))
        return FakeResult(self.db.responses.get((self.table, self.op), []))


class FakeAuthAdmin:
    def __init__(self, db):
        self.db = db

    def delete_user(self, user_id):
        self.db.calls.append(("auth.users", "delete_user", user_id, []))


class FakeAuth:
    def __init__(self, db):
        self.admin = FakeAuthAdmin(db)


class FakeDB:
    """Stands in for main.supabase_admin. responses: {(table, op): rows}."""

    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []
        self.auth = FakeAuth(self)

    def table(self, name):
        return FakeQuery(self, name)


class FakeResponse:
    def __init__(self, status, body):
        self.status_code, self._body = status, body

    def json(self):
        return self._body


class FakeHTTP:
    """Stands in for the requests module inside main.py."""
    RequestException = requests.RequestException

    def __init__(self, body=None, status=200):
        self.body, self.status, self.posts = body or {}, status, []

    def post(self, url, data=None, timeout=None):
        self.posts.append((url, data))
        return FakeResponse(self.status, self.body)


# ─────────────────────────────────────────────────────────────────────────────
# Harness
# ─────────────────────────────────────────────────────────────────────────────

USER_ID  = "11111111-1111-1111-1111-111111111111"
AUTH     = {"Authorization": "Bearer good-token"}
FRONTEND = config.FRONTEND_URL
CALLBACK = f"{config.BACKEND_URL}/auth/callback"
SCHEDULED: list[tuple] = []
JOBS_CREATED: list[tuple] = []


class FakeUser:
    id    = USER_ID
    email = "a@example.test"


def fake_get_user(request):
    if request.headers.get("Authorization") != AUTH["Authorization"]:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return FakeUser()


def fake_create_sync_job(user_id, kind, runner="api"):
    JOBS_CREATED.append((user_id, kind, runner))
    return "job-1"


main.get_user_from_token            = fake_get_user
main.create_sync_job                = fake_create_sync_job
main.has_active_sync                = lambda user_id: False
main.backfill_gmail_for_user_task   = lambda user_id, job_id: SCHEDULED.append(("backfill", user_id, job_id))
main.fetch_gmail_for_user_task      = lambda user_id, job_id=None, runner="api": SCHEDULED.append(("user", user_id, job_id))
main.fetch_gmail_for_all_users_task = lambda runner="api": SCHEDULED.append(("all",))

client = TestClient(main.app)


def raises(fn, exc) -> bool:
    try:
        fn()
    except exc:
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Checks
# ─────────────────────────────────────────────────────────────────────────────

def test_config():
    saved = config.CRON_SECRET
    config.CRON_SECRET = ""
    try:
        config.validate_api_config()
        check("2.7 an empty CRON_SECRET stops startup", False, "no error raised")
    except config.ConfigError as e:
        check("2.7 an empty CRON_SECRET stops startup", "CRON_SECRET" in str(e), e)
    finally:
        config.CRON_SECRET = saved

    saved = config.TOKEN_ENCRYPTION_KEY
    config.TOKEN_ENCRYPTION_KEY = "not-a-fernet-key"
    token_crypto._fernet = None
    try:
        config.validate_api_config()
        check("2.7 a malformed TOKEN_ENCRYPTION_KEY stops startup", False, "no error raised")
    except config.ConfigError as e:
        check("2.7 a malformed TOKEN_ENCRYPTION_KEY stops startup", "TOKEN_ENCRYPTION_KEY" in str(e), e)
    finally:
        config.TOKEN_ENCRYPTION_KEY = saved
        token_crypto._fernet = None

    check("2.7 complete settings pass", not raises(config.validate_api_config, Exception))


def test_crypto():
    ct = token_crypto.encrypt("refresh-abc")
    check("2.5 encrypt then decrypt returns the token", token_crypto.decrypt(ct) == "refresh-abc")
    check("2.5 ciphertext does not contain the token", "refresh-abc" not in ct)
    i = len(ct) // 2
    tampered = ct[:i] + ("A" if ct[i] != "A" else "B") + ct[i + 1:]
    check("2.5 tampered ciphertext is rejected",
          raises(lambda: token_crypto.decrypt(tampered), token_crypto.InvalidToken))


def test_cors():
    evil = client.options("/fetch-gmail", headers={
        "Origin": "https://evil.example", "Access-Control-Request-Method": "GET"})
    check("2.2 CORS refuses another origin",
          "access-control-allow-origin" not in evil.headers, dict(evil.headers))

    ok = client.options("/fetch-gmail", headers={
        "Origin": FRONTEND, "Access-Control-Request-Method": "GET",
        "Access-Control-Request-Headers": "authorization"})
    check("2.2 CORS allows FRONTEND_URL",
          ok.headers.get("access-control-allow-origin") == FRONTEND, dict(ok.headers))
    check("2.2 CORS does not allow credentials",
          "access-control-allow-credentials" not in ok.headers, dict(ok.headers))


def test_cron():
    SCHEDULED.clear()
    check("2.3 cron without the secret is 403", client.post("/cron/fetch-all").status_code == 403)
    check("2.3 cron with a wrong secret is 403",
          client.post("/cron/fetch-all", headers={"X-Cron-Secret": "wrong"}).status_code == 403)
    r = client.post("/cron/fetch-all", headers={"X-Cron-Secret": "test-cron-secret"})
    check("2.3 cron with the secret is 202 and schedules the fetch",
          r.status_code == 202 and SCHEDULED == [("all",)], f"{r.status_code} {SCHEDULED}")


def test_removed_routes():
    check("SEC-04 GET /auth/google?token= no longer exists",
          client.get("/auth/google", params={"token": "x"}).status_code == 404)
    check("2.1 POST /correct-category no longer exists",
          client.post("/correct-category", json={}).status_code == 404)
    check("Phase 1 POST /upload-file no longer exists",
          client.post("/upload-file").status_code == 404)
    check("SEC-08 the /protected debug route (it echoed the email) no longer exists",
          client.get("/protected", headers=AUTH).status_code == 404)


def test_health():
    main.supabase_admin = FakeDB()
    main._health_hits = {}
    r = client.get("/health")
    check("Phase 7 /health reports ok when the database answers, without a login",
          r.status_code == 200 and r.json() == {"status": "ok", "database": "ok"}, r.text)

    class Broken(FakeDB):
        def table(self, name):
            raise ConnectionError("db down at 10.0.0.5 password=secret")
    main.supabase_admin = Broken()
    r = client.get("/health")
    check("Phase 7 /health is 503 when the database is unreachable", r.status_code == 503, r.status_code)
    check("Phase 7 /health does not leak the error text", "secret" not in r.text and "10.0.0.5" not in r.text, r.text)


def test_health_rate_limit():
    main.supabase_admin = FakeDB()
    main._health_hits = {}
    for _ in range(main.HEALTH_RATE_LIMIT):
        r = client.get("/health")
    check("SEC-06 /health allows up to the per-IP limit", r.status_code == 200, r.status_code)
    r = client.get("/health")
    check("SEC-06 /health is 429 with Retry-After once the limit is exceeded",
          r.status_code == 429 and r.headers.get("retry-after") == "60", (r.status_code, dict(r.headers)))

    main._health_hits = {}
    real_limit = main.HEALTH_RATE_LIMIT
    main.HEALTH_RATE_LIMIT = 0
    try:
        check("SEC-06 the check fails when the limit is removed (proves it is load-bearing)",
              client.get("/health").status_code == 429)
    finally:
        main.HEALTH_RATE_LIMIT = real_limit
        main._health_hits = {}


def test_model_info_removed():
    check("SEC-06 /model-info no longer exists (nothing called it)",
          client.get("/model-info", headers=AUTH).status_code == 404)


def test_ping_is_async():
    # A plain `def` route waits for a free thread in Starlette's pool, which
    # running syncs can fill; Render would then fail the health check and
    # restart the service, killing every sync in flight.
    check("/ping is async, so a saturated thread pool cannot fail Render's health check",
          inspect.iscoroutinefunction(main.ping))
    r = client.get("/ping")
    check("/ping answers alive", r.status_code == 200 and r.json() == {"status": "alive"}, r.text)


def test_oauth_start_throttle():
    main.supabase_admin = FakeDB({("oauth_states", "select"): [{"state": str(i)} for i in range(main.OAUTH_START_LIMIT)]})
    r = client.post("/auth/google/start", headers=AUTH)
    inserted = [c for c in main.supabase_admin.calls if c[:2] == ("oauth_states", "insert")]
    check("SEC-09 a user at the connect-attempt limit gets 429 with Retry-After",
          r.status_code == 429 and r.headers.get("retry-after") == "600", (r.status_code, dict(r.headers)))
    check("SEC-09 no state row is stored once the limit is reached", not inserted, inserted)
    counted = [f for c in main.supabase_admin.calls if c[:2] == ("oauth_states", "select") for f in c[3]]
    check("SEC-09 only this user's recent attempts are counted",
          ("eq", "user_id", USER_ID) in counted and any(f[0] == "gte" and f[1] == "created_at" for f in counted), counted)
    main.supabase_admin = FakeDB({("oauth_states", "select"): [{"state": "1"}]})
    check("SEC-09 below the limit the start still works",
          client.post("/auth/google/start", headers=AUTH).status_code == 200)


def test_oauth():
    check("2.4 start without a session token is 401",
          client.post("/auth/google/start").status_code == 401)

    db = FakeDB()
    main.supabase_admin = db
    r = client.post("/auth/google/start", headers=AUTH)
    url = r.json().get("url", "") if r.status_code == 200 else ""
    q = parse_qs(urlparse(url).query)
    inserted = [c[2] for c in db.calls if c[:2] == ("oauth_states", "insert")]
    state = inserted[0]["state"] if inserted else None
    cleanup = [c for c in db.calls if c[:2] == ("oauth_states", "delete") and c[3] and c[3][0][:2] == ("lt", "expires_at")]
    check("2.4 start clears expired states first", bool(cleanup), db.calls)
    check("2.4 start stores a one-time state for the user",
          bool(state) and inserted[0]["user_id"] == USER_ID and len(state) >= 40, db.calls)
    check("2.4 the consent URL carries that state", q.get("state") == [state], q)
    check("2.4 the consent URL carries the registered redirect URI", q.get("redirect_uri") == [CALLBACK], q)
    check("2.4 the consent URL does not contain the session token", "good-token" not in url, url)

    future = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    past   = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    main.supabase_admin = FakeDB({("oauth_states", "delete"): [{"user_id": USER_ID, "expires_at": future}]})
    check("2.4 a live state resolves to its user", main._consume_oauth_state("s") == USER_ID)
    main.supabase_admin = FakeDB({("oauth_states", "delete"): [{"user_id": USER_ID, "expires_at": past}]})
    check("2.4 an expired state is refused", main._consume_oauth_state("s") is None)
    main.supabase_admin = FakeDB()
    check("2.4 an unknown state is refused", main._consume_oauth_state("s") is None)

    def callback(params, state_user, http=None):
        rows = {("oauth_states", "delete"): [{"user_id": state_user, "expires_at": future}]} if state_user else {}
        db = FakeDB(rows)
        main.supabase_admin, main.requests = db, (http or FakeHTTP())
        try:
            resp = client.get("/auth/callback", params=params, follow_redirects=False)
        finally:
            main.requests = requests
        return resp, db

    def lands_on(resp, result):
        return resp.status_code in (302, 307) and resp.headers.get("location") == f"{FRONTEND}/?gmail={result}"

    r, _ = callback({"state": "unknown", "code": "c"}, None)
    check("2.4 callback with an unknown state returns gmail=expired", lands_on(r, "expired"), r.headers)
    r, _ = callback({}, None)
    check("2.4 callback without a state returns gmail=expired", lands_on(r, "expired"), r.headers)
    r, _ = callback({"state": "s", "error": "access_denied"}, USER_ID)
    check("2.4 callback after the user declines returns gmail=denied", lands_on(r, "denied"), r.headers)
    r, _ = callback({"state": "s", "code": "c"}, USER_ID, FakeHTTP({"error": "invalid_grant"}))
    check("2.4 a failed token exchange returns gmail=error", lands_on(r, "error"), r.headers)

    http = FakeHTTP({"access_token": "access-123", "refresh_token": "refresh-456"})
    r, db = callback({"state": "s", "code": "c"}, USER_ID, http)
    creds = [c[2] for c in db.calls if c[:2] == ("gmail_credentials", "upsert")]
    sync  = [c[2] for c in db.calls if c[:2] == ("gmail_sync", "upsert")]
    check("2.4 a good callback returns gmail=connected", lands_on(r, "connected"), r.headers)
    check("2.4 the code is exchanged with the registered redirect URI",
          bool(http.posts) and http.posts[0][1]["redirect_uri"] == CALLBACK, http.posts)
    check("2.5 the refresh token is stored encrypted",
          bool(creds) and "refresh_token" not in creds[0]
          and token_crypto.decrypt(creds[0]["refresh_token_encrypted"]) == "refresh-456", creds)
    check("2.5 the stored ciphertext does not contain the token",
          bool(creds) and "refresh-456" not in creds[0]["refresh_token_encrypted"], creds)
    check("2.5 gmail_sync receives no token", bool(sync) and not any("token" in k for k in sync[0]), sync)
    check("3.3 connecting clears needs_reconnect", bool(sync) and sync[0].get("needs_reconnect") is False, sync)


def test_sync_limit():
    check("2.10 sync without a session token is 401", client.get("/fetch-gmail").status_code == 401)

    main.supabase_admin = FakeDB({("gmail_sync", "update"): [{"user_id": USER_ID}]})
    check("2.10 the claim succeeds with no recent sync", main._claim_manual_sync(USER_ID) == "ok")
    update_filters = [f for c in main.supabase_admin.calls if c[:2] == ("gmail_sync", "update") for f in c[3]]
    or_filters = [f[1] for f in update_filters if f[0] == "or"]
    check("2.10 the claim matches only rows never synced or synced over 10 minutes ago",
          bool(or_filters) and "last_sync_requested_at.is.null" in or_filters[0]
          and "last_sync_requested_at.lt." in or_filters[0], or_filters)
    check("3.3 the claim never matches a connection that needs reconnecting",
          ("eq", "needs_reconnect", False) in update_filters, update_filters)

    main.supabase_admin = FakeDB({("gmail_sync", "select"): [{"needs_reconnect": False}]})
    check("2.10 the claim is limited after a recent sync", main._claim_manual_sync(USER_ID) == "limited")
    main.supabase_admin = FakeDB({("gmail_sync", "select"): [{"needs_reconnect": True}]})
    check("3.3 the claim reports a connection that needs reconnecting",
          main._claim_manual_sync(USER_ID) == "reconnect_required")
    main.supabase_admin = FakeDB()
    check("2.10 the claim reports a missing connection", main._claim_manual_sync(USER_ID) == "not_connected")

    real_claim = main._claim_manual_sync
    try:
        for outcome, status in (("limited", 429), ("not_connected", 409), ("reconnect_required", 409), ("ok", 202)):
            SCHEDULED.clear()
            JOBS_CREATED.clear()
            main._claim_manual_sync = lambda user_id, o=outcome: o
            r = client.get("/fetch-gmail", headers=AUTH)
            check(f"2.10 /fetch-gmail returns {status} when the claim is {outcome}", r.status_code == status, r.status_code)
            if outcome == "limited":
                check("2.10 the 429 says when to retry", r.headers.get("retry-after") == "600", dict(r.headers))
            if outcome == "reconnect_required":
                check("3.3 the 409 tells the user to reconnect",
                      "Reconnect Gmail" in r.json().get("detail", ""), r.json())
            if outcome == "ok":
                check("3.7 a manual sync job is created", JOBS_CREATED == [(USER_ID, "manual", "api")], JOBS_CREATED)
                check("3.7 the response carries the job id", r.json().get("job_id") == "job-1", r.json())
                check("3.7 the fetch runs for that job", SCHEDULED == [("user", USER_ID, "job-1")], SCHEDULED)
            else:
                check(f"2.10 nothing is scheduled when the claim is {outcome}",
                      SCHEDULED == [] and JOBS_CREATED == [], (SCHEDULED, JOBS_CREATED))
    finally:
        main._claim_manual_sync = real_claim


def test_sync_already_active():
    """WP4 / B3: a second sync for a user who is already syncing is refused with 409."""
    real = (main.has_active_sync, main._claim_manual_sync, main.create_sync_job)
    claims = []
    try:
        main._claim_manual_sync = lambda user_id: claims.append(user_id) or "ok"
        main.has_active_sync = lambda user_id: True
        SCHEDULED.clear()
        r = client.get("/fetch-gmail", headers=AUTH)
        check("WP4 /fetch-gmail is 409 while a sync is already running",
              r.status_code == 409 and "already running" in r.json().get("detail", ""), (r.status_code, r.text))
        check("WP4 the refusal does not use up the 10-minute window", claims == [], claims)
        check("WP4 nothing is scheduled for a refused sync", SCHEDULED == [], SCHEDULED)

        # The race: no active job when checked, but another request got in first.
        main.has_active_sync = lambda user_id: False

        def busy(*args, **kwargs):
            raise main.SyncAlreadyActive("sync_jobs_one_active")
        main.create_sync_job = busy
        SCHEDULED.clear()
        r = client.get("/fetch-gmail", headers=AUTH)
        check("WP4 losing the race for the active-sync slot is also 409",
              r.status_code == 409 and SCHEDULED == [], (r.status_code, SCHEDULED))
    finally:
        main.has_active_sync, main._claim_manual_sync, main.create_sync_job = real


def test_cron_status():
    """WP5: the scheduled run can be checked after it was started."""
    since = "2026-10-01T02:30:00Z"
    secret = {"X-Cron-Secret": "test-cron-secret"}
    check("WP5 /cron/status without the secret is 403",
          client.get("/cron/status", params={"since": since}).status_code == 403)
    check("WP5 /cron/status with a wrong secret is 403",
          client.get("/cron/status", params={"since": since}, headers={"X-Cron-Secret": "wrong"}).status_code == 403)

    main.supabase_admin = FakeDB({("sync_jobs", "select"): [
        {"status": "succeeded", "error": None},
        {"status": "running",   "error": None},
        {"status": "queued",    "error": None},
        {"status": "failed",    "error": main.FAILED_MESSAGE},
        {"status": "failed",    "error": main.INTERRUPTED_MESSAGE},
        {"status": "failed",    "error": main.RECONNECT_MESSAGE},
    ]})
    r = client.get("/cron/status", params={"since": since}, headers=secret)
    check("WP5 counts running, succeeded, unexpected and expected failures",
          r.status_code == 200 and r.json() == {"running": 2, "succeeded": 1,
                                                "failed_unexpected": 2, "failed_expected": 1}, r.text)
    calls = [c for c in main.supabase_admin.calls if c[:2] == ("sync_jobs", "select")]
    check("WP5 only jobs created since the given time are counted",
          len(calls) == 1 and any(f[0] == "gte" and f[1] == "created_at" for f in calls[0][3]), calls)
    check("WP5 the response carries counts only, no user ids or error text",
          set(r.json()) == {"running", "succeeded", "failed_unexpected", "failed_expected"})
    check("WP5 a missing or malformed since is rejected",
          client.get("/cron/status", headers=secret).status_code == 422
          and client.get("/cron/status", params={"since": "yesterday"}, headers=secret).status_code == 422)


def test_backfill_route():
    """WP9: POST /backfill-gmail reads one earlier month, under the same limits as a manual sync."""
    check("WP9 /backfill-gmail without a session token is 401", client.post("/backfill-gmail").status_code == 401)
    real = (main.has_active_sync, main._claim_manual_sync, main.history_window)
    claims = []
    try:
        main.history_window = lambda user_id: ("start", "end")
        main.has_active_sync = lambda user_id: False
        main._claim_manual_sync = lambda user_id: claims.append(user_id) or "ok"
        SCHEDULED.clear()
        JOBS_CREATED.clear()
        r = client.post("/backfill-gmail", headers=AUTH)
        check("WP9 /backfill-gmail is 202 with a job id", r.status_code == 202 and r.json().get("job_id") == "job-1", r.text)
        check("WP9 the job is a backfill and runs the backfill task",
              JOBS_CREATED == [(USER_ID, "backfill", "api")] and SCHEDULED == [("backfill", USER_ID, "job-1")],
              (JOBS_CREATED, SCHEDULED))

        for outcome, status in (("limited", 429), ("not_connected", 409), ("reconnect_required", 409)):
            SCHEDULED.clear()
            main._claim_manual_sync = lambda user_id, o=outcome: o
            r = client.post("/backfill-gmail", headers=AUTH)
            check(f"WP9 /backfill-gmail shares the manual-sync limits: {outcome} is {status}",
                  r.status_code == status and SCHEDULED == [], (r.status_code, SCHEDULED))

        claims.clear()
        main._claim_manual_sync = lambda user_id: claims.append(user_id) or "ok"
        main.has_active_sync = lambda user_id: True
        r = client.post("/backfill-gmail", headers=AUTH)
        check("WP9 a backfill is refused while a sync is running, without using the window",
              r.status_code == 409 and claims == [], (r.status_code, claims))

        main.has_active_sync = lambda user_id: False
        main.history_window = lambda user_id: None
        SCHEDULED.clear()
        r = client.post("/backfill-gmail", headers=AUTH)
        check("WP9 a backfill past the limit is 409 with the fixed message, without using the window",
              r.status_code == 409 and "12 months" in r.json().get("detail", "") and claims == [] and SCHEDULED == [],
              (r.status_code, r.text, claims))
    finally:
        main.has_active_sync, main._claim_manual_sync, main.history_window = real


def test_interrupted_jobs():
    db = FakeDB()
    main.supabase_admin = db
    main._fail_interrupted_jobs()
    updates = [c for c in db.calls if c[:2] == ("sync_jobs", "update")]
    check("3.7 startup marks interrupted jobs failed",
          bool(updates) and updates[0][2].get("status") == "failed" and updates[0][2].get("error"), updates)
    check("3.7 only this process's queued or running jobs are touched",
          bool(updates) and ("eq", "runner", "api") in updates[0][3]
          and ("in", "status", ["queued", "running"]) in updates[0][3], updates)


def test_delete_account():
    check("2.8 delete without a session token is 401", client.delete("/account").status_code == 401)

    db   = FakeDB({("gmail_credentials", "select"): [
        {"access_token": "access-1", "refresh_token_encrypted": token_crypto.encrypt("refresh-789")}]})
    http = FakeHTTP(status=200)
    main.supabase_admin, main.requests = db, http
    try:
        r = client.delete("/account", headers=AUTH)
    finally:
        main.requests = requests

    deletes = [c for c in db.calls if c[1] == "delete"]
    check("2.8 delete returns 200", r.status_code == 200, r.status_code)
    check("2.8 every user table is cleared, children before parents",
          [c[0] for c in deletes] == list(main.USER_TABLES), [c[0] for c in deletes])
    check("3.6 the monthly totals view is not deleted from; it has no rows of its own",
          "gold_monthly_summary" not in main.USER_TABLES, main.USER_TABLES)
    check("3.7 sync jobs are deleted with the account", "sync_jobs" in main.USER_TABLES, main.USER_TABLES)
    check("WP6 event rows are deleted with the account", "app_events" in main.USER_TABLES, main.USER_TABLES)
    check("SEC correction rules are deleted with the account", "user_merchant_rules" in main.USER_TABLES, main.USER_TABLES)
    check("2.8 every delete is scoped to the user",
          all(("eq", "user_id", USER_ID) in c[3] for c in deletes), deletes)
    check("2.8 the auth user is deleted last",
          db.calls[-1][:3] == ("auth.users", "delete_user", USER_ID), db.calls[-1])
    check("2.8 Google is asked to revoke the decrypted refresh token",
          bool(http.posts) and http.posts[0][0].endswith("/revoke")
          and http.posts[0][1]["token"] == "refresh-789", http.posts)


if __name__ == "__main__":
    for test in (test_config, test_crypto, test_cors, test_cron, test_removed_routes,
                 test_health, test_health_rate_limit, test_model_info_removed, test_ping_is_async, test_oauth,
                 test_oauth_start_throttle, test_sync_limit, test_sync_already_active, test_backfill_route, test_cron_status,
                 test_interrupted_jobs, test_delete_account):
        test()
    print(f"\n{len(FAILED)} check(s) failed" if FAILED else "\nALL API SECURITY CHECKS PASSED")
    sys.exit(1 if FAILED else 0)
