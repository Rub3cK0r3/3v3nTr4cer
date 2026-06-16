from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    BigInteger,
    Index,
    CHAR,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB
from typing import Dict, Optional
from pydantic import BaseModel
from .base_model import Base

# In order to be able to do Sanity checks 
REQUIRED_EVENT_FIELDS = ["id", "app_name", "type", "payload"]

class Event(Base):
    __tablename__ = "events"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    severity: Mapped[str] = mapped_column(String(10), nullable=False)
    stack: Mapped[str | None] = mapped_column(Text)
    type: Mapped[str | None] = mapped_column(String(100))
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    received_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    resource: Mapped[str | None] = mapped_column(Text)
    referrer: Mapped[str | None] = mapped_column(Text)
    app_name: Mapped[str] = mapped_column(String(255), nullable=False)
    app_version: Mapped[str | None] = mapped_column(String(50))
    app_stage: Mapped[str | None] = mapped_column(String(50))
    tags: Mapped[dict | None] = mapped_column(JSONB)
    endpoint_id: Mapped[str] = mapped_column(String(64), nullable=False)
    endpoint_language: Mapped[str | None] = mapped_column(String(10))
    endpoint_platform: Mapped[str | None] = mapped_column(String(50))
    endpoint_os: Mapped[str | None] = mapped_column(String(50))
    endpoint_os_version: Mapped[str | None] = mapped_column(String(20))
    endpoint_runtime: Mapped[str | None] = mapped_column(String(50))
    endpoint_runtime_version: Mapped[str | None] = mapped_column(String(20))
    endpoint_country: Mapped[str | None] = mapped_column(CHAR(2))
    endpoint_user_agent: Mapped[str | None] = mapped_column(Text)
    endpoint_device_type: Mapped[str | None] = mapped_column(String(20))

Index("idx_severity", Event.severity)
Index("idx_app_name", Event.app_name)
Index("idx_app_stage", Event.app_stage)
Index("idx_endpoint_country", Event.endpoint_country)
Index("idx_timestamp", Event.timestamp)
Index("idx_received_at", Event.received_at)

class EventResponse(BaseModel):
    id: str
    severity: str
    stack: Optional[str] = None
    type: Optional[str] = None
    timestamp: int
    received_at: int
    resource: Optional[str] = None
    referrer: Optional[str] = None
    app_name: str
    app_version: Optional[str] = None
    app_stage: Optional[str] = None
    tags: Optional[Dict] = None
    endpoint_id: str
    endpoint_language: Optional[str] = None
    endpoint_platform: Optional[str] = None
    endpoint_os: Optional[str] = None
    endpoint_os_version: Optional[str] = None
    endpoint_runtime: Optional[str] = None
    endpoint_runtime_version: Optional[str] = None
    endpoint_country: Optional[str] = None
    endpoint_user_agent: Optional[str] = None
    endpoint_device_type: Optional[str] = None
    model_config = {
        "from_attributes": True 
    }

class EventCreate(BaseModel):
    severity: str
    stack: Optional[str] = None
    type: Optional[str] = None
    timestamp: int
    resource: Optional[str] = None
    referrer: Optional[str] = None
    app_name: str
    app_version: Optional[str] = None
    app_stage: Optional[str] = None
    tags: Optional[Dict] = None
    endpoint_id: str
    endpoint_language: Optional[str] = None
    endpoint_platform: Optional[str] = None
    endpoint_os: Optional[str] = None
    endpoint_os_version: Optional[str] = None
    endpoint_runtime: Optional[str] = None
    endpoint_runtime_version: Optional[str] = None
    endpoint_country: Optional[str] = None
    endpoint_user_agent: Optional[str] = None
    endpoint_device_type: Optional[str] = None

#######################################################################
# I introduce here the User model based on what we could use in the db
#######################################################################
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
