from sqlalchemy import Column, Integer, String, DateTime, UniqueConstraint
from sqlalchemy.sql import func

from ..database.init import Base


class IngestedItem(Base):
    __tablename__ = "ingested_items"

    id = Column(Integer, primary_key=True, index=True)

    source = Column(String, nullable=False)        # mail / file
    external_id = Column(String, nullable=False)  # message_id / filename

    ingested_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_source_external_id"),
    )
