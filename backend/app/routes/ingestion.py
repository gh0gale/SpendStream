from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..database.init import get_db
from ..services.ingestion_service import start_ingestion_run,finish_ingestion_run


router = APIRouter(prefix="/ingestion", tags=["Ingestion"])


@router.post("/start")
def start_ingestion(source: str, db: Session = Depends(get_db)):
    run = start_ingestion_run(db, source)
    return {
        "run_id": run.id,
        "source": run.source,
        "status": run.status
    }


@router.post("/finish")
def finish_ingestion(run_id: int, db: Session = Depends(get_db)):
    finish_ingestion_run(db, run_id)
    return {
        "run_id": run_id,
        "status": "completed"
    }
