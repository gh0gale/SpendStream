"""
test_gmail_parser.py  ─  checks gmail_parser.py
══════════════════════════════════════════════════════
Two kinds of cases:

  * SYNTHETIC cases below: made-up strings that pin down what the current
    regexes do. They are NOT real bank formats and prove nothing about how
    any bank writes its alerts.
  * REAL fixtures in backend/tests/fixtures/gmail/*.json: anonymised bank
    alert emails, one per file (format in that folder's README.md). Adding
    them is on docs/USER_TODO.md.

Needs no network, no .env and no database. Run from backend/:

    python test_gmail_parser.py

Exit code is non-zero if any case fails. Banks without a real fixture are
listed but do not fail the run.
"""

import base64
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gmail_parser import decode_body, html_to_text, message_to_transaction, parse_bank_alert   # noqa: E402

FIXTURE_DIR     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests", "fixtures", "gmail")
SUPPORTED_BANKS = ("HDFC", "ICICI", "SBI", "Axis", "Kotak", "Yes Bank", "IDFC FIRST", "IndusInd", "Federal", "Bank of Baroda",
                   "PNB", "Canara", "Union Bank", "RBL", "AU Small Finance", "IDBI")   # the tasks.BANK_QUERY senders
FAILED: list[str] = []


def check(name: str, ok, detail="") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + ("" if ok else f"   [{detail}]"))
    if not ok:
        FAILED.append(name)


# (description, body, expected result of parse_bank_alert)
SYNTHETIC = [
    ("amount before the keyword, receiver after 'to'",
     "Rs.1,250.50 debited from a/c XX1234 to VPA swiggy@icici on 15-09-26",
     {"amount": 1250.50, "receiver": "VPA swiggy@icici"}),
    ("keyword before the amount, rupee sign",
     "You have paid ₹ 60 to RAHUL SHARMA on 15 Sep",
     {"amount": 60.0, "receiver": "RAHUL SHARMA"}),
    ("INR with no 'to ... on' phrase gives receiver UNKNOWN",
     "INR 499.00 spent on your card at AMAZON",
     {"amount": 499.0, "receiver": "UNKNOWN"}),
    ("no debit keyword is not an alert",
     "Your OTP is 123456. Do not share it.",
     None),
    ("a credit is not a debit",
     "Rs 500 credited to your account on 15-09-26",
     None),
    ("a zero amount is ignored",
     "Rs. 0.00 debited from a/c XX1234 to VPA a@okaxis on 15-09-26",
     None),
    ("HDFC 'towards VPA x@y (NAME) on <date>': parentheses dropped",
     "Rs.2090.00 is debited from your account ending 1234 towards VPA somebody-1@oksbi (A PERSON NAME) on 21-09-26. UPI transaction reference no.: 111122223333.",
     {"amount": 2090.0, "receiver": "VPA somebody-1@oksbi A PERSON NAME"}),
    ("'on your card' does not end the receiver early",
     "Rs 99 debited to NETFLIX on your card XX1234 on 05-09-26",
     {"amount": 99.0, "receiver": "NETFLIX on your card XX1234"}),
    # DATA-08 (2026-09-27): money-shaped texts that are not a completed debit.
    ("a credit phrased with 'paid' is not a debit",
     "Rs.5000 paid to your account XX1234 by RAHUL on 01-09-26", None),
    ("a refund is not a debit",
     "Refund of Rs 499 paid to your card from AMAZON on 02-09", None),
    ("a future autopay mandate is not a debit",
     "Rs 649 will be debited to NETFLIX on 05-09-26 for your autopay mandate", None),
    ("an OTP message is not a debit",
     "OTP for txn of Rs 999 at FLIPKART is 123456. Do not share. Amount will be debited on success", None),
    ("a declined payment is not a debit",
     "Your payment of Rs 250 to ZOMATO on 03-09-26 was declined and not debited", None),
    ("a reversal is not a debit",
     "Rs 300 debited on 01-09-26 has been reversed to your account", None),
    ("an HDFC credit alert is not a debit",
     "We're writing to inform you that Rs.1500.00 has been successfully credited to your HDFC Bank account ending in 1234", None),
    ("a balance printed first is not taken as the amount",
     "Avl Bal Rs 12,000.00. Rs 250 debited from a/c XX12 to SWIGGY on 03-09-26",
     {"amount": 250.0, "receiver": "SWIGGY"}),
    ("debited and credited to a beneficiary is still a debit",
     "Rs 400 debited from a/c XX12 and credited to VPA shop@ybl on 04-09-26",
     {"amount": 400.0, "receiver": "VPA shop@ybl"}),
    ("amount and keyword on different lines do not match (no DOTALL)",
     "Rs. 250.00\nwas debited from your account",
     None),
]


def test_synthetic():
    for description, body, expected in SYNTHETIC:
        got = parse_bank_alert(body)
        check(f"synthetic: {description}", got == expected, f"got {got}")


def test_message_to_transaction():
    body = "You have paid ₹ 60 to RAHUL SHARMA on 15 Sep"
    data = base64.urlsafe_b64encode(body.encode()).decode().rstrip("=")   # Gmail may omit padding
    message = {
        "id": "msg-123",
        "payload": {
            "mimeType": "text/plain",
            "body":     {"data": data},
            "headers":  [{"name": "Date", "value": "Tue, 15 Sep 2026 10:05:33 +0530"}],
        },
    }
    row = message_to_transaction("user-1", message)
    check("message: base64url without padding decodes", decode_body(data) == body)
    check("message: row carries the Gmail message id", row and row["message_id"] == "msg-123", row)
    check("message: amount and receiver come from the body",
          row and row["amount"] == 60.0 and row["receiver"] == "RAHUL SHARMA", row)
    check("message: timestamp comes from the Date header",
          row and row["timestamp"] == "2026-09-15T10:05:33+05:30", row)

    nested = {
        "id": "msg-456",
        "payload": {"mimeType": "multipart/alternative", "parts": [
            {"mimeType": "multipart/related", "parts": [
                {"mimeType": "text/plain", "body": {"data": data}},
            ]},
        ], "headers": []},
    }
    check("message: body found inside nested parts",
          (message_to_transaction("user-1", nested) or {}).get("message_id") == "msg-456")

    html = ("<!doctype html><html><head><style>td{color:#333}</style></head><body><table><tr><td>"
            "Rs.179.00 is debited from your account ending 1234 towards VPA shop@ybl (A&amp;B STORE) on 13-09-26."
            "</td></tr></table></body></html>")
    html_msg = {"id": "msg-html", "payload": {"mimeType": "text/html",
                "body": {"data": base64.urlsafe_b64encode(html.encode()).decode()}, "headers": []}}
    row = message_to_transaction("user-1", html_msg)
    check("message: HTML body is read as text (receiver no longer UNKNOWN)",
          row and row["receiver"] == "VPA shop@ybl A&B STORE" and row["amount"] == 179.0, row)
    check("message: raw_text holds visible text, not the HTML head",
          row and row["raw_text"].startswith("Rs.179.00"), row and row["raw_text"][:40])

    not_alert = {"id": "msg-789", "payload": {"body": {"data": base64.urlsafe_b64encode(b"Hello").decode()}}}
    check("message: a non-alert email gives no row", message_to_transaction("user-1", not_alert) is None)


def test_real_fixtures():
    files = sorted(glob.glob(os.path.join(FIXTURE_DIR, "*.json")))
    covered = set()
    for path in files:
        name = os.path.basename(path)
        try:
            with open(path, encoding="utf-8") as f:
                fixture = json.load(f)
            got = parse_bank_alert(html_to_text(fixture["body"]))   # as message_to_transaction does
            expected = fixture.get("expected")
            if expected is not None:
                expected = {"amount": float(expected["amount"]), "receiver": expected["receiver"]}
            check(f"real fixture {name} ({fixture.get('bank', '?')})", got == expected, f"got {got}")
            covered.add(fixture.get("bank", ""))
        except (OSError, ValueError, KeyError) as e:
            check(f"real fixture {name} is readable", False, e)

    if not files:
        print("NOTE  No real bank fixtures yet: see tests/fixtures/gmail/README.md and docs/USER_TODO.md.")
    missing = [b for b in SUPPORTED_BANKS if b not in covered]
    if missing:
        print("NOTE  Banks without a real fixture: " + ", ".join(missing))


if __name__ == "__main__":
    test_synthetic()
    test_message_to_transaction()
    test_real_fixtures()
    print(f"\n{len(FAILED)} check(s) failed" if FAILED else "\nALL GMAIL PARSER CHECKS PASSED")
    sys.exit(1 if FAILED else 0)
