from __future__ import annotations

import os
import asyncio
import httpx
from typing import TYPE_CHECKING, Any
from loguru import logger

# Sanity checks in this frontier too...
from contracts.events import REQUIRED_EVENT_FIELDS

if TYPE_CHECKING:
    import asyncpg


class EventProcessor:
    """
    Processor responsible for persisting events in the backend.

    This component follows a pattern similar to a real message queue:

    1. It tries to deliver the event immediately.
    2. If it fails, it retries a limited number of times.
    3. If the retry budget is exhausted, the event is moved to a dead-letter
       queue (DLQ) for durable storage and later inspection.

    The goal is to avoid losing messages that cannot be processed successfully
    while preserving enough context for troubleshooting.
    """

    def __init__(
        self,
        db_pool: "asyncpg.pool.Pool | None" = None,
        backend_base_url: str | None = None,
        max_retries: int = 3,
        retry_backoff_seconds: float | None = None,
    ):
        """
        Initialize the processor.

        Args:
            db_pool: Kept for backward compatibility, although it is not used
                directly because persistence currently goes through the backend.
            backend_base_url: Base URL of the HTTP backend.
            max_retries: Number of retries before moving the event to the DLQ.
        """
        self.db_pool = db_pool
        self.backend_base_url = backend_base_url or os.getenv("BACKEND_BASE_URL", "http://backend:8000")
        self.max_retries = max(1, max_retries)
        self.retry_backoff_seconds = retry_backoff_seconds if retry_backoff_seconds is not None else float(
            os.getenv("RETRY_BACKOFF_SECONDS", "0.5")
        )
        self.internal_token = os.getenv("INTERNAL_TOKEN", "")
        self.dead_letter_endpoint = "/internal/pipeline/dead-letter-events"

    async def handle(self, event: dict[str, Any]):
        """
        Main entry point for the processor.

        The business flow remains simple: if the event is invalid, it is skipped;
        if it is valid, it is attempted with retries and, if it fails, it is
        recorded in the DLQ.
        """
        if not self._validate_event(event):
            logger.bind(event_id=event.get("id"), app_name=event.get("app_name"), pipeline_stage="validation").warning(
                "Invalid event skipped"
            )
            return

        await self._process_with_retries(event)

    async def _process_with_retries(self, event: dict[str, Any]):
        """
        Attempt delivery of the event multiple times before sending it to the DLQ.

        This is a minimal implementation of a retry-and-dead-letter flow. In a
        RabbitMQ or Kafka-style system, the broker would retry delivery and then
        move the message to a special error queue after retries are exhausted.
        Here, that behavior is simulated explicitly inside the processor.
        """
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                await self._insert_event(event)
                return
            except NonRecoverableError as exc:
                logger.bind(event_id=event.get("id"), app_name=event.get("app_name"), retry_count=attempt, pipeline_stage="processing").error(
                    "Non-recoverable event failure: {}", exc
                )
                await self._persist_dead_letter(event, retries=attempt, error=str(exc))
                return
            except RecoverableError as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    logger.bind(event_id=event.get("id"), app_name=event.get("app_name"), retry_count=attempt, pipeline_stage="dlq").error(
                        "Retry budget exhausted; moving event to dead-letter queue: {}", exc
                    )
                    await self._persist_dead_letter(event, retries=attempt, error=str(exc))
                    return

                delay = min(self.retry_backoff_seconds * (2 ** (attempt - 1)), 60.0)
                logger.bind(event_id=event.get("id"), app_name=event.get("app_name"), retry_count=attempt, pipeline_stage="processing").warning(
                    "Recoverable event failure; retrying in {} seconds: {}", delay, exc
                )
                await asyncio.sleep(delay)

        # This point should only be reached if the loop exits unexpectedly.
        # In that case, the event is still preserved in the DLQ with the last
        # known error information.
        if last_error is not None:
            await self._persist_dead_letter(event, retries=self.max_retries, error=str(last_error))

    async def _insert_event(self, event: dict[str, Any]):
        """
        Persist an event through the internal backend endpoint.

        This method does not swallow the error locally so the calling layer can
        decide whether to retry or route the event to the DLQ.
        """
        try:
            async with httpx.AsyncClient(base_url=self.backend_base_url, timeout=5.0) as client:
                payload = {
                    "id": event.get("id"),
                    "app_name": event.get("app_name", "unknown-app"),
                    "type": event.get("type", "unknown"),
                    "payload": event,
                    "severity": event.get("severity", "info"),
                    "timestamp": event.get("timestamp"),
                    "resource": event.get("resource"),
                    "referrer": event.get("referrer"),
                }
                response = await client.post(
                    "/internal/pipeline/events", json=payload, headers=self._internal_headers()
                )
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    if 400 <= response.status_code < 500 and response.status_code not in {408, 429}:
                        raise NonRecoverableError(str(exc)) from exc
                    raise RecoverableError(str(exc)) from exc

        except Exception as exc:
            if isinstance(exc, (NonRecoverableError, RecoverableError)):
                raise
            raise RecoverableError(str(exc)) from exc

    async def _persist_dead_letter(self, event: dict[str, Any], retries: int, error: str):
        """
        Move a failed event to the dead-letter queue persisted in PostgreSQL.

        In this project, the DLQ is modeled as an internal backend endpoint that
        stores the event together with the retry count and the latest error.
        This ensures the message is not lost and remains inspectable later.
        """
        try:
            async with httpx.AsyncClient(base_url=self.backend_base_url, timeout=5.0) as client:
                payload = {
                    "id": event.get("id"),
                    "app_name": event.get("app_name", "unknown-app"),
                    "type": event.get("type", "unknown"),
                    "payload": event,
                    "severity": event.get("severity", "error"),
                    "timestamp": event.get("timestamp"),
                    "resource": event.get("resource"),
                    "referrer": event.get("referrer"),
                    "retries": retries,
                    "last_error": error,
                }
                response = await client.post(
                    self.dead_letter_endpoint, json=payload, headers=self._internal_headers()
                )
                response.raise_for_status()
        except Exception as exc:
            # The DLQ is a resilience measure. If its persistence fails, we log
            # it so the diagnostic information is not lost.
            logger.bind(event_id=event.get("id"), app_name=event.get("app_name"), retry_count=retries, pipeline_stage="dlq").error(
                "Dead-letter persistence failed: {}", exc
            )

    def _internal_headers(self) -> dict[str, str]:
        return {"X-Internal-Token": self.internal_token} if self.internal_token else {}

    def _validate_event(self, event: dict[str, Any]) -> bool:
        """
        Validate that an event contains the minimum required fields.

        Args:
            event: Event data as a dictionary.

        Returns:
            True if the event is valid, False otherwise.
        """
        return all(field in event for field in REQUIRED_EVENT_FIELDS)


class RecoverableError(Exception):
    """A processing failure that may succeed on a later attempt."""


class NonRecoverableError(Exception):
    """A processing failure that should be persisted directly in the DLQ."""
