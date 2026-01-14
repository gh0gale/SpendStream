from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func

from ..database.init import Base


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, nullable=False)  # mail / file / sms
    status = Column(String, nullable=False)  # running / completed / failed

    started_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
