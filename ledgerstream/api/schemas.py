from pydantic import BaseModel, UUID4, field_validator
from datetime import datetime
from decimal import Decimal
from typing import Optional

class TransactionIn(BaseModel):
    transaction_id:    UUID4
    user_id:           UUID4
    amount:            Decimal
    currency:          str
    transaction_type:  str
    status:            str
    merchant_name:     Optional[str] = None
    merchant_category: Optional[str] = None
    card_last4:        Optional[str] = None
    latitude:          Optional[float] = None
    longitude:         Optional[float] = None
    created_at:        datetime

    @field_validator('currency')
    @classmethod
    def currency_must_be_3_chars(cls, v):
        if len(v) != 3:
            raise ValueError('currency deve ter exatamente 3 caracteres (ex: BRL, USD)')
        return v.upper()

    @field_validator("status")
    @classmethod
    def status_must_be_valid(cls, v):
        allowed = {"approved", "pending", "declined", "refund"}
        if v not in allowed:
            raise ValueError(f"status deve ser um de: {allowed}")
        return v
 
    @field_validator("card_last4")
    @classmethod
    def card_last4_must_be_4_digits(cls, v):
        if v is not None and (len(v) != 4 or not v.isdigit()):
            raise ValueError("card_last4 deve ter exatamente 4 dígitos")
        return v
 
class TransactionOut(BaseModel):
    transaction_id: UUID4
    status:         str
    ingested_at:    datetime

    model_config = {"from_attributes": True}
