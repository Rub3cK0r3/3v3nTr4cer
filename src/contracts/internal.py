from typing import Any

from pydantic import BaseModel, Field


class PipelineEventIn(BaseModel):
    id: str | None = None
    app_name: str | None = None
    type: str
    payload: dict[str, Any]
    severity: str | None = None
    timestamp: int | None = None
    resource: str | None = None
    referrer: str | None = None


class DeadLetterEventIn(PipelineEventIn):
    retries: int = 0
    last_error: str | None = None


class PipelineAlertIn(BaseModel):
    id: str | None = None
    severity: str
    resource: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class HealthResponse(BaseModel):
    status: str