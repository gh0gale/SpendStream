"""
config.py — every environment variable the backend reads, in one place.

The API calls validate_api_config() at import, so a missing or malformed
value stops startup instead of failing on the first request that needs it.
Scripts (training, tests, cron) need only the Supabase values.
New variables go here and in backend/.env.example, in the same change.
"""

import os

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))


def _get(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


SUPABASE_URL              = _get("SUPABASE_URL")
SUPABASE_ANON_KEY         = _get("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_ROLE_KEY = _get("SUPABASE_SERVICE_ROLE_KEY")
GOOGLE_CLIENT_ID          = _get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET      = _get("GOOGLE_CLIENT_SECRET")
CRON_SECRET               = _get("CRON_SECRET")
TOKEN_ENCRYPTION_KEY      = _get("TOKEN_ENCRYPTION_KEY")
FRONTEND_URL              = _get("FRONTEND_URL", "http://localhost:5173").rstrip("/")
BACKEND_URL               = _get("BACKEND_URL", "http://localhost:8000").rstrip("/")
CRON_MAX_WORKERS          = int(_get("CRON_MAX_WORKERS", "5"))
# Optional. weekly_retrain.py calls it after shipping a model so the host
# restarts the backend, which then downloads the new model.
DEPLOY_HOOK_URL           = _get("DEPLOY_HOOK_URL")

SUPABASE_VARS = ("SUPABASE_URL", "SUPABASE_ANON_KEY", "SUPABASE_SERVICE_ROLE_KEY")
API_VARS      = SUPABASE_VARS + (
    "GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET", "CRON_SECRET", "TOKEN_ENCRYPTION_KEY",
)


class ConfigError(RuntimeError):
    """A required setting is missing or malformed."""


def require(*names: str) -> None:
    missing = [n for n in names if not globals().get(n)]
    if missing:
        raise ConfigError(
            "Missing required environment variables: " + ", ".join(missing)
            + ". Copy backend/.env.example to backend/.env and fill them in."
        )


def validate_api_config() -> None:
    """Everything the API server needs. Called once by main.py at import."""
    require(*API_VARS)
    import token_crypto
    token_crypto.validate_key()


def admin_client():
    """Service-role client. Bypasses row-level security: server-side only."""
    require(*SUPABASE_VARS)
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def anon_client():
    """Anon-key client. main.py uses it only to validate user JWTs."""
    require(*SUPABASE_VARS)
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
