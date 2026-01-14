from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..database.init import get_db
from ..repositories.processed_email_repo import is_email_processed, mark_email_processed

router = APIRouter(prefix="/internal/email", tags=["Internal"])


@router.post("/check")
def check_email(payload: dict, db: Session = Depends(get_db)):
    """
    Check if an email has already been processed.
    If not processed, mark it as processed and return {"process": True}.
    If already processed, return {"process": False}.
    """
    message_id = payload["message_id"]

    if is_email_processed(db, message_id):
        return {"process": False}

    mark_email_processed(db, message_id)
    return {"process": True}
