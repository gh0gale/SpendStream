"""
cron_runner.py — Run the Gmail batch-fetch directly (no Celery needed).

Usage
─────
  python cron_runner.py

Crontab entry (3× / day — 8 am, 2 pm, 11 pm):
  0 8,14,23 * * * /path/to/venv/bin/python /path/to/cron_runner.py >> /var/log/gmail_cron.log 2>&1
"""

import sys
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tasks import fetch_gmail_for_all_users_task   # config.py loads backend/.env
import model_store


if __name__ == "__main__":
    log.info("Cron runner started — running batch Gmail fetch")

    try:
        model_store.ensure_local()      # no model file after a fresh clone
        result = fetch_gmail_for_all_users_task(runner="cron_runner")
        log.info("Batch fetch complete: %s", result)

    except Exception as e:
        log.exception("Batch fetch failed: %s", e)
        sys.exit(1)

    log.info("Cron runner complete")
    sys.exit(0)