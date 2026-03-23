from sqlalchemy import Column, String, Numeric, Float, DateTime
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime, timezone
from db.base import Base
import uuid

class TransactionRaw(Base):
    __tablename__ = "transactions_raw"

    transaction_id    = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id           = Column(UUID(as_uuid=True), nullable=False)
    amount            = Column(Numeric(12, 2), nullable=False)
    currency          = Column(String(3), nullable=False)
    transaction_type  = Column(String(20), nullable=False)
    status            = Column(String(20), nullable=False)
    merchant_name     = Column(String(255))
    merchant_category = Column(String(100))
    card_last4        = Column(String(4))
    latitude          = Column(Float)
    longitude         = Column(Float)
    created_at        = Column(DateTime(timezone=True), nullable=False)
    ingested_at       = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
