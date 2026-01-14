from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime
from ..database.init import Base


class ProcessedFile(Base):
    __tablename__ = "processed_files"

    id = Column(Integer, primary_key=True)
    file_hash = Column(String, unique=True, index=True)
    file_name = Column(String)
    source = Column(String)  # csv / pdf
    processed_at = Column(DateTime, default=datetime.utcnow)
