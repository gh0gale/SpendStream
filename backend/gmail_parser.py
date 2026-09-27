"""
gmail_parser.py — turn Gmail API messages into raw transaction rows.

Pure functions: no network, no database, no settings. tasks.py downloads the
messages; everything here only reads them. Tested by test_gmail_parser.py.
"""

import base64
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser

_KEYWORD      = re.compile(r"(?i)debited|spent|paid")
_AMOUNT_FIRST = re.compile(r"(?i)(?:rs\.?|inr|₹)\s?([\d,]+\.?\d{0,2}).*?(?:debited|spent|paid)")
_AMOUNT_AFTER = re.compile(r"(?i)(?:debited|spent|paid).*?(?:rs\.?|inr|₹)\s?([\d,]+\.?\d{0,2})")
# DATA-08: texts that mention money leaving but are not a completed debit.
# Checked before any amount is read.
_NOT_A_DEBIT = re.compile(
    r"(?i)\b(otp|one[\s-]time\s+password|will\s+be\s+debited|to\s+be\s+debited|"
    r"refund(ed)?|revers(al|ed)|declined|transaction\s+(has\s+)?failed|"
    r"unsuccessful|paid\s+to\s+your\s+(a/?c|account|card))\b"
)
_CREDIT  = re.compile(r"(?i)\b(credited|received)\b")
_DEBITED = re.compile(r"(?i)\bdebited\b")
# The amount right before the debit verb, in the same sentence, so a balance
# printed first ("Avl Bal Rs 12,000.00. Rs 250 debited") is not taken.
_AMOUNT_AT_VERB = re.compile(
    r"(?i)(?:rs\.?|inr|₹)\s?([\d,]+(?:\.\d{1,2})?)(?:(?!rs\.?\s?\d|inr|₹)[^.\n])"
    r"{0,40}?\b(?:debited|spent|paid)\b"
)

# HDFC writes "debited ... towards VPA x@y (NAME) on 21-09-26"; older
# formats "to NAME on". The date after "on" keeps "on your card" from ending
# the receiver early.
_RECEIVER     = re.compile(r"(?i)\b(?:towards|to)\s+(.+?)\s+on\s+\d")
_RECEIVER_OLD = re.compile(r"(?i)to\s+(.*?)\s+on")
_SKIP_TAGS    = {"head", "style", "script"}


class _TextExtractor(HTMLParser):
    """Visible text of an HTML email, one space between text runs."""

    def __init__(self):
        super().__init__()
        self.parts: list[str] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        self._skip += tag in _SKIP_TAGS

    def handle_endtag(self, tag):
        self._skip -= tag in _SKIP_TAGS

    def handle_data(self, data):
        if self._skip <= 0 and data.strip():
            self.parts.append(data.strip())


def html_to_text(body: str) -> str:
    """Plain text of a body; HTML is stripped (entities decoded), plain text passes through."""
    if "<" not in body or ">" not in body:
        return body
    parser = _TextExtractor()
    parser.feed(body)
    parser.close()
    return re.sub(r"\s+", " ", " ".join(parser.parts)).strip()


def parse_email_date(value):
    """Parse an email Date header (RFC 2822). None if it cannot be parsed."""
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None


def extract_body(payload: dict) -> str | None:
    """Recursively find the first text/plain or text/html body (base64url) in a message payload."""
    if "parts" in payload:
        for part in payload["parts"]:
            mime = part.get("mimeType", "")
            if mime in ["text/plain", "text/html"]:
                data = part["body"].get("data")
                if data:
                    return data
            if "parts" in part:
                result = extract_body(part)
                if result:
                    return result
    else:
        return payload.get("body", {}).get("data")
    return None


def decode_body(data: str) -> str:
    """Decode Gmail's base64url body; tolerates missing padding."""
    padded = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(padded).decode("utf-8", errors="ignore")


def parse_bank_alert(body: str) -> dict | None:
    """
    Pull the debit amount and receiver out of a bank alert's text.

    Returns {"amount": float, "receiver": str}, or None when the text is not a
    debit alert with a positive amount. The receiver is "UNKNOWN" when the
    text has no "to <name> on" phrase. Lines are matched one at a time
    (no DOTALL), so amount and keyword must share a line.
    """
    if not body or not _KEYWORD.search(body):
        return None
    if _NOT_A_DEBIT.search(body):
        return None
    if _CREDIT.search(body) and not _DEBITED.search(body):
        return None

    match = _AMOUNT_AT_VERB.search(body) or _AMOUNT_FIRST.search(body) or _AMOUNT_AFTER.search(body)
    if not match:
        return None
    try:
        amount = float(match.group(1).replace(",", ""))
    except ValueError:
        return None
    if amount <= 0:
        return None

    receiver_match = _RECEIVER.search(body) or _RECEIVER_OLD.search(body)
    receiver = receiver_match.group(1) if receiver_match else "UNKNOWN"
    # "VPA x@y (NAME)" -> "VPA x@y NAME", the shape merchant_identity expects.
    receiver = re.sub(r"\s+", " ", receiver.replace("(", " ").replace(")", " ")).strip() or "UNKNOWN"
    return {"amount": amount, "receiver": receiver[:100]}


def message_to_transaction(user_id: str, message: dict, fallback_time: datetime | None = None) -> dict | None:
    """
    Turn one Gmail API message (format=full) into a raw `transactions` row,
    or None if it is not a debit alert. The row carries the Gmail message id,
    which the database uses to drop duplicates.
    """
    payload  = message.get("payload", {})
    raw_body = extract_body(payload)
    if not raw_body:
        return None

    body   = html_to_text(decode_body(raw_body))
    parsed = parse_bank_alert(body)
    if not parsed:
        return None

    email_date = None
    for header in payload.get("headers", []):
        if header.get("name") == "Date":
            email_date = parse_email_date(header["value"])
            break

    return {
        "user_id":          user_id,
        "amount":           parsed["amount"],
        "receiver":         parsed["receiver"],
        "transaction_type": "debit",
        "timestamp":        (email_date or fallback_time or datetime.now(timezone.utc)).isoformat(),
        "source":           "gmail",
        "raw_text":         body[:200],
        "message_id":       message["id"],
    }
