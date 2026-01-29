from sqlalchemy.orm import Session
from datetime import datetime

from ..models.ingestion_run import IngestionRun
from ..models.ingestion_item import IngestedItem
from ..repositories.processed_email_repo import is_email_processed,mark_email_processed

def start_ingestion_run(db: Session, source: str) -> IngestionRun:
    run = IngestionRun(
        source=source,
        status="running"
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def finish_ingestion_run(db: Session, run_id: int, status: str = "completed"):
    run = db.query(IngestionRun).filter(IngestionRun.id == run_id).first()
    if run:
        run.status = status
        run.finished_at = datetime.utcnow()
        db.commit()


def is_item_already_ingested(
    db: Session,
    source: str,
    external_id: str
) -> bool:
    return db.query(IngestedItem).filter(
        IngestedItem.source == source,
        IngestedItem.external_id == external_id
    ).first() is not None


def mark_item_ingested(
    db: Session,
    source: str,
    external_id: str
):
    item = IngestedItem(
        source=source,
        external_id=external_id
    )
    db.add(item)
    db.commit()

def process_email_message(db, message_id: str):
    if is_email_processed(db, message_id):
        return False  # already processed

    mark_email_processed(db, message_id)
    return True
