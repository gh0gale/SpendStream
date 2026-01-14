


import requests

from data_pipeline.ingestion.email.email_client import get_gmail_service
from data_pipeline.ingestion.email.email_reader import fetch_messages
from data_pipeline.ingestion.email.email_parser import parse_email
from data_pipeline.ingestion.email.email_writer import write_email_raw

BACKEND_URL = "http://127.0.0.1:8000/"


def main():
    # 1️⃣ Start ingestion run
    response = requests.post(
        f"{BACKEND_URL}/ingestion/start",
        params={"source": "email"}
    )
    run_id = response.json()["run_id"]
    print(f"🚀 Ingestion run started: {run_id}")

    service = get_gmail_service("data_pipeline/configs/token.json")

    query = "debit OR credit OR spent OR transaction OR INR"
    messages = fetch_messages(service, query=query, max_results=10)

    print(f"📧 Found {len(messages)} emails")

    for msg in messages:
        record = parse_email(service, msg["id"])
        message_id = record["metadata"]["message_id"]

        # 2️⃣ Ask backend if this email was already processed
        check = requests.post(
            f"{BACKEND_URL}/internal/email/check",
            json={"message_id": message_id}
        )

        if not check.json()["process"]:
            print(f"⏭️ Skipping duplicate email {message_id}")
            continue

        if not record["raw_text"]:
            print(f"⚠️ Empty body for message {message_id}")

        path = write_email_raw(record)
        print(f"✅ Stored → {path}")

    # 3️⃣ Finish ingestion run
    requests.post(
        f"{BACKEND_URL}/ingestion/finish",
        params={"run_id": run_id}
    )
    print(f"🏁 Ingestion run finished: {run_id}")

    response = requests.post(
    f"{BACKEND_URL}/ingestion/start",
    params={"source": "email"}
    )

    print("Status code:", response.status_code)
    print("Response text:", response.text)
    print("Response JSON:", response.json())


    run_id = response.json()["run_id"]



if __name__ == "__main__":
    main()
