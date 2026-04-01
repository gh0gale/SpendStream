#!/usr/bin/env python3
"""
cron_runner.py — Enqueue the Gmail batch-fetch Celery task.

This script no longer runs the ETL/ML work itself. It connects to the
Redis broker and enqueues fetch_gmail_for_all_users_task, which fans out
one Celery task per connected user. Workers do the rest.

Usage options
─────────────

  Option A — Direct Python cron (recommended for VPS / bare-metal):
    Run this script via cron. Workers must already be running separately.

    Crontab entry (3× / day — 8 am, 2 pm, 11 pm):
      0 8,14,23 * * * /path/to/venv/bin/python /path/to/cron_runner.py \
          >> /var/log/gmail_cron.log 2>&1

  Option B — HTTP trigger (FastAPI already running):
      0 8,14,23 * * * curl -s -X POST https://yourapi.com/cron/fetch-all \
          -H "X-Cron-Secret: YOUR_SECRET" >> /var/log/gmail_cron.log 2>&1

  Option C — GitHub Actions / Railway / Render cron:
    Scheduled workflow that hits the HTTP endpoint:

      on:
        schedule:
          - cron: '0 8,14,23 * * *'
      jobs:
        fetch:
          runs-on: ubuntu-latest
          steps:
            - run: |
                curl -s -X POST ${{ secrets.API_URL }}/cron/fetch-all \
                  -H "X-Cron-Secret: ${{ secrets.CRON_SECRET }}"

  Option D — Celery Beat (self-contained scheduled tasks, no external cron):
    Add to celery_app.py:

      from celery.schedules import crontab
      celery.conf.beat_schedule = {
          "fetch-gmail-3x-daily": {
              "task":     "tasks.fetch_gmail_for_all_users_task",
              "schedule": crontab(hour="8,14,23", minute=0),
          },
      }

    Then run Beat alongside the worker:
      celery -A celery_app.celery beat --loglevel=info
"""

import sys
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Add project root to path so imports work regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from tasks import fetch_gmail_for_all_users_task


if __name__ == "__main__":
    log.info("Cron runner started — enqueuing batch Gmail fetch task")

    try:
        task = fetch_gmail_for_all_users_task.delay()
        log.info("Task enqueued successfully.")
        log.info("  Task ID : %s", task.id)
        log.info("  Status  : %s", task.status)
        log.info(
            "Workers will process all connected users. "
            "Check worker logs or poll /task/%s for progress.",
            task.id,
        )

    except Exception as e:
        log.exception("Failed to enqueue batch task: %s", e)
        log.error(
            "Is Redis running and REDIS_URL set correctly in .env? "
            "Are Celery workers running?"
        )
        sys.exit(1)

    log.info("Cron runner complete — exiting (workers are handling the rest)")
    sys.exit(0)