import requests

from data_pipeline.ingestion.email.email_client import get_gmail_service
from data_pipeline.ingestion.email.email_reader import fetch_messages
from data_pipeline.ingestion.email.email_parser import parse_email
from data_pipeline.ingestion.email.email_writer import write_email_raw

BACKEND_URL = "http://127.0.0.1:8000"


def main():
    # 1️⃣ Start ingestion run
    response = requests.post(
        f"{BACKEND_URL}/ingestion/start",
        params={"source": "email"}
    )

    data = response.json()
    run_id = data.get("run_id")

    if run_id:
        print(f"[INFO] Ingestion run started: {run_id}")
    else:
        print("[WARN] No run_id returned, continuing without run tracking")

    service = get_gmail_service("data_pipeline/configs/token.json")

    query = "debit OR credit OR spent OR transaction OR INR"
    messages = fetch_messages(service, query=query, max_results=10)

    print(f"[INFO] Found {len(messages)} emails")

    for msg in messages:
        record = parse_email(service, msg["id"])
        message_id = record["metadata"]["message_id"]

        check = requests.post(
            f"{BACKEND_URL}/internal/email/check",
            json={"message_id": message_id}
        )

        if not check.json().get("process", True):
            print(f"[SKIP] Skipping duplicate email {message_id}")
            continue

        if not record["raw_text"]:
            print(f"[WARN] Empty body for message {message_id}")

        path = write_email_raw(record)
        print(f"[SUCCESS] Stored -> {path}")

    # 3 Finish ingestion run (only if run_id exists)
    if run_id:
        requests.post(
            f"{BACKEND_URL}/ingestion/finish",
            params={"run_id": run_id}
        )
        print(f"[COMPLETE] Ingestion run finished: {run_id}")
    else:
        print("[INFO] No run_id - skipping ingestion finish")


if __name__ == "__main__":
    main()
