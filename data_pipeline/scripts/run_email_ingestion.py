from data_pipeline.ingestion.email.email_client import get_gmail_service
from data_pipeline.ingestion.email.email_reader import fetch_messages
from data_pipeline.ingestion.email.email_parser import parse_email
from data_pipeline.ingestion.email.email_writer import write_email_raw


def main():
    service = get_gmail_service("data_pipeline/configs/token.json")

    query = "debit OR credit OR spent OR transaction OR INR"
    messages = fetch_messages(service, query=query, max_results=10)

    print(f"📧 Found {len(messages)} emails")

    for msg in messages:
        record = parse_email(service, msg["id"])

        if not record["raw_text"]:
            print(f"⚠️ Empty body for message {msg['id']}")

        path = write_email_raw(record)
        print(f"✅ Stored → {path}")

if __name__ == "__main__":
    main()
