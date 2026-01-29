from fastapi import APIRouter, Depends,UploadFile, File
from sqlalchemy.orm import Session
from pathlib import Path
import shutil

from ..database.init import get_db
from ..services.ingestion_service import start_ingestion_run,finish_ingestion_run

router = APIRouter(prefix="/file", tags=["File"])

# UPLOAD_DIR = Path("data_pipeline/input_files")
BASE_DIR = Path(__file__).resolve().parents[2]
UPLOAD_DIR = BASE_DIR / "data_pipeline" / "input_files"

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)



@router.post("/upload")
def upload_file(file: UploadFile = File(...)):
    file_path = UPLOAD_DIR / file.filename

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return {
        "message": "File uploaded successfully",
        "filename": file.filename
    }

#  CHANGE
@router.post("/trigger-ingestion")
def run_file_ingestion(db: Session = Depends(get_db)):
    run = start_ingestion_run(db, source="file")

    import subprocess
    try:
        subprocess.run(
            ["python", "-m", "data_pipeline.scripts.run_file_ingestion"],
            check=True
        )

    except subprocess.CalledProcessError as e:
        finish_ingestion_run(db, run.id, status="failed")
        return {
            "run_id": run.id,
            "status": "error",
            "message": "File ingestion failed",
            "error": str(e)
        }

    finish_ingestion_run(db, run.id)

    return {
        "run_id": run.id,
        "status": "success",
        "message": "File ingestion completed successfully (duplicates skipped if any)"
    }





