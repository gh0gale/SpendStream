#!/usr/bin/env python3
"""
cron_runner.py — Standalone script to trigger the Gmail batch fetch.

Usage options:

  Option A — Direct Python cron (no HTTP server needed):
    Run this file directly via cron. It imports and calls
    fetch_gmail_for_all_users() without going through FastAPI.

    Crontab entry (3x/day — 8am, 2pm, 11pm):
      0 8,14,23 * * * /path/to/venv/bin/python /path/to/cron_runner.py >> /var/log/gmail_cron.log 2>&1

  Option B — HTTP trigger (hits the /cron/fetch-all endpoint):
    If FastAPI is already running (e.g. on a server), use curl:
      0 8,14,23 * * * curl -s -X POST https://yourapi.com/cron/fetch-all \
          -H "X-Cron-Secret: YOUR_SECRET" >> /var/log/gmail_cron.log 2>&1

  Option C — GitHub Actions / Railway / Render cron:
    Use a scheduled workflow or service cron that hits the HTTP endpoint.
    GitHub Actions example (.github/workflows/gmail_cron.yml):

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

# Add project root to path so imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from main import fetch_gmail_for_all_users


if __name__ == "__main__":
    log.info("Cron runner started")
    summary = fetch_gmail_for_all_users()

    log.info("Cron runner complete:")
    log.info("  Users processed : %d", summary["users_processed"])
    log.info("  OK              : %d", summary["ok"])
    log.info("  Skipped         : %d", summary["skipped"])
    log.info("  Errors          : %d", summary["errors"])
    log.info("  Elapsed         : %.1fs", summary["elapsed_seconds"])

    # Exit with non-zero code if any errors (useful for cron monitoring)
    if summary["errors"] > 0:
        sys.exit(1)