import json
import uuid
from pathlib import Path

BASE_PATH = Path("data_pipeline/storage/raw/email")

def write_email_raw(data):
    BASE_PATH.mkdir(parents=True, exist_ok=True)

    file_path = BASE_PATH / f"{uuid.uuid4()}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return str(file_path)
