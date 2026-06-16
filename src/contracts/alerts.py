from .events import Base
from sqlalchemy import (
    String,
    Text,
    BigInteger,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB

# In order to be able to do sanity checks for alerts..
REQUIRED_ALERT_FIELDS = ["id", "severity", "resource"]

# Optional sanity checks
ALERT_SEVERITIES = {"error", "fatal"}

# Severity ordering for threshold-based alerting
SEVERITY_LEVELS = {
    "debug": 10,
    "info": 20,
    "warning": 30,
    "error": 40,
    "fatal": 50,
}

class Alert(Base):
    __tablename__ = "alerts"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    severity: Mapped[str] = mapped_column(String(10), nullable=False)
    resource: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
