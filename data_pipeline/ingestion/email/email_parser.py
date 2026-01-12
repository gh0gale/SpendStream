import base64
from data_pipeline.ingestion.common.schema import raw_transaction

def _get_header(headers, name):
    for h in headers:
        if h["name"].lower() == name.lower():
            return h["value"]
    return "unknown"

def _decode_body(data):
    try:
        return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
    except Exception:
        return ""

def _extract_body(payload):
    """
    Recursively walk through email payload parts
    and extract text/plain or text/html
    """
    # Case 1: direct body
    if payload.get("body", {}).get("data"):
        return _decode_body(payload["body"]["data"])

    # Case 2: multipart email
    for part in payload.get("parts", []):
        mime_type = part.get("mimeType", "")

        if mime_type in ["text/plain", "text/html"]:
            if part.get("body", {}).get("data"):
                return _decode_body(part["body"]["data"])

        # Recursive case (nested parts)
        if "parts" in part:
            result = _extract_body(part)
            if result:
                return result

    return ""

def parse_email(service, msg_id):
    msg = service.users().messages().get(
        userId="me",
        id=msg_id,
        format="full"
    ).execute()

    payload = msg.get("payload", {})
    headers = payload.get("headers", [])

    sender = _get_header(headers, "From")
    timestamp = msg.get("internalDate", "")

    body = _extract_body(payload)

    return raw_transaction(
        source="email",
        sender=sender,
        timestamp=timestamp,
        raw_text=body,
        metadata={"message_id": msg_id}
    )
