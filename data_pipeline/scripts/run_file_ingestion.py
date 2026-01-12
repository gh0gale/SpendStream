from pathlib import Path

from data_pipeline.ingestion.files.csv_reader import read_csv
from data_pipeline.ingestion.files.pdf_reader import read_pdf
from data_pipeline.ingestion.files.file_parser import (
    parse_csv_row,
    parse_pdf_text
)
from data_pipeline.ingestion.files.file_writer import write_file_raw


INPUT_DIR = Path("data_pipeline/input_files")

def main():
    files = list(INPUT_DIR.iterdir())
    print(f"📂 Found {len(files)} files for ingestion")

    for file in files:
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

if __name__ == "__main__":
    main()
