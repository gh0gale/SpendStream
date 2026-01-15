

from pathlib import Path
import requests

from data_pipeline.ingestion.files.csv_reader import read_csv
from data_pipeline.ingestion.files.pdf_reader import read_pdf
from data_pipeline.ingestion.files.file_parser import (
    parse_csv_row,
    parse_pdf_text
)
from data_pipeline.ingestion.files.file_writer import write_file_raw
from data_pipeline.common.utils.file_hash import compute_file_hash


INPUT_DIR = Path("data_pipeline/input_files")
BACKEND_URL = "http://localhost:8000"


def main():
    # 1️⃣ Start ingestion run
    response = requests.post(
        f"{BACKEND_URL}/ingestion/start",
        params={"source": "file"}
    )
    run_id = response.json()["run_id"]
    print(f"🚀 File ingestion run started: {run_id}")

    files = list(INPUT_DIR.iterdir())
    print(f"📂 Found {len(files)} files for ingestion")

    for file in files:
        if not file.is_file():
            continue

        # 2️⃣ Compute file hash
        file_hash = compute_file_hash(file)

        # 3️⃣ Ask backend if this file was already processed
        check = requests.post(
            f"{BACKEND_URL}/internal/file/check",
            json={
                "file_hash": file_hash,
                "file_name": file.name,
                "source": file.suffix.replace(".", "")
            }
        )

        if not check.json()["process"]:
            print(f"⏭️ Skipping duplicate file → {file.name}")
            continue

        # 🔒 EXISTING LOGIC — UNCHANGED
        if file.suffix.lower() == ".csv":
            rows = read_csv(file)
            for row in rows:
                record = parse_csv_row(row, file.name)
                write_file_raw(record)

            print(f"✅ CSV ingested → {file.name}")

        elif file.suffix.lower() == ".pdf":
            text = read_pdf(file)
            record = parse_pdf_text(text, file.name)
            write_file_raw(record)

            print(f"✅ PDF ingested → {file.name}")

        else:
            print(f"⚠️ Skipping unsupported file → {file.name}")

    # 4️⃣ Finish ingestion run
    requests.post(
        f"{BACKEND_URL}/ingestion/finish",
        params={"run_id": run_id}
    )
    print(f"🏁 File ingestion run finished: {run_id}")


if __name__ == "__main__":
    main()
