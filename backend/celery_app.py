# """
# celery_app.py — Celery application factory.

# Reads REDIS_URL from the environment (set in .env via python-dotenv).
# Import this module wherever you need the `celery` instance.
# """

# import os
# from celery import Celery
# from dotenv import load_dotenv

# load_dotenv()

# REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# celery = Celery(
#     "finance_app",
#     broker=REDIS_URL,
#     backend=REDIS_URL,
# )

# celery.conf.update(
#     # ── Serialisation ────────────────────────────────────────────────────────
#     task_serializer="json",
#     result_serializer="json",
#     accept_content=["json"],

#     # ── Timezone ─────────────────────────────────────────────────────────────
#     timezone="UTC",
#     enable_utc=True,

#     # ── Reliability ──────────────────────────────────────────────────────────
#     task_acks_late=True,           # re-queue task if worker crashes mid-flight
#     task_reject_on_worker_lost=True,

#     # ── Result expiry (24 h) ─────────────────────────────────────────────────
#     result_expires=86_400,

#     # ── Broker transport options (Upstash requires SSL) ───────────────────────
#     broker_transport_options={
#         "visibility_timeout": 3600,   # seconds before unacked task is re-queued
#         "socket_keepalive": True,
#     },

#     # ── Worker concurrency hint (override via CLI --concurrency) ──────────────
#     worker_concurrency=4,

#     # ── Task routing (single default queue is fine for now) ───────────────────
#     task_default_queue="default",
# )

import os
import ssl  # <--- NEW: Required for Upstash
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")

# Auto-correct if you accidentally used 'redis://' instead of 'rediss://'
if REDIS_URL and REDIS_URL.startswith("redis://"):
    REDIS_URL = REDIS_URL.replace("redis://", "rediss://", 1)

celery = Celery(
    "finance_app",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['tasks']
)

celery.conf.update(
    # ── Upstash SSL Fixes ────────────────────────────────────────────────────
    broker_use_ssl={'ssl_cert_reqs': ssl.CERT_NONE},
    redis_backend_use_ssl={'ssl_cert_reqs': ssl.CERT_NONE},

    # ── Serialisation ────────────────────────────────────────────────────────
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # ── Timezone ─────────────────────────────────────────────────────────────
    timezone="UTC",
    enable_utc=True,

    # ── Reliability ──────────────────────────────────────────────────────────
    task_acks_late=True,
    task_reject_on_worker_lost=True,

    # ── Result expiry (24 h) ─────────────────────────────────────────────────
    result_expires=86_400,

    

    worker_concurrency=4,
    task_default_queue="default",

    # ── 1. Stop Wasted Redis Writes ──────────────────────────────────────────
    worker_send_task_events=False,    # Don't send internal monitoring events
    task_send_sent_event=False,       # Don't log to Redis when a task is sent
    
    # Optional: If you aren't using the /task/{task_id} polling endpoint, uncomment this:
    # task_ignore_result=True,        # Stops Celery from saving results back to Redis entirely

    # ── 2. Relax the Redis Reads (Polling) ───────────────────────────────────
    broker_transport_options={
        "visibility_timeout": 3600,
        "socket_keepalive": True,
        "polling_interval": 2.0,      # <--- Wait 2 seconds between checking for tasks
    },
)