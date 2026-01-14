from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime

from ..database.init import Base


class ProcessedEmail(Base):
    __tablename__ = "processed_emails"

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(String, unique=True, index=True, nullable=False)
    source = Column(String, default="email")
    processed_at = Column(DateTime, default=datetime.utcnow)

    
