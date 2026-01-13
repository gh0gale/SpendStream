from fastapi import APIRouter
from datetime import datetime
import subprocess

from ..database.init import SessionLocal
from ..models.ingestion_run import IngestionRun

router = APIRouter()

@router.post("/email")
def trigger_email_ingestion():
    db = SessionLocal()

    run = IngestionRun(
        source="email",
        status="running"
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    subprocess.run(
        ["python", "-m", "data_pipeline.scripts.run_email_ingestion"],
        shell=True
    )

    run.status = "completed"
    run.finished_at = datetime.utcnow()
    db.commit()

    return {
        "message": "Email ingestion completed",
        "run_id": run.id
    }
