from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..database.init import get_db
from ..models.init import ProcessedFile

router = APIRouter(prefix="/internal/file", tags=["Internal"])


@router.post("/check")
def check_file(payload: dict, db: Session = Depends(get_db)):
    file_hash = payload["file_hash"]

    existing = db.query(ProcessedFile).filter_by(file_hash=file_hash).first()

    if existing:
        return {"process": False}

    record = ProcessedFile(
        file_hash=file_hash,
        file_name=payload.get("file_name"),
        source=payload.get("source", "file")
    )
    db.add(record)
    db.commit()

    return {"process": True}
