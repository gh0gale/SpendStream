from sqlalchemy.orm import Session
from ..models.processed_email import ProcessedEmail


def is_email_processed(db: Session, message_id: str) -> bool:
    return (
        db.query(ProcessedEmail)
        .filter(ProcessedEmail.message_id == message_id)
        .first()
        is not None
    )


def mark_email_processed(db: Session, message_id: str):
    record = ProcessedEmail(message_id=message_id)
    db.add(record)
    db.commit()
