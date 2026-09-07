from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from time import time
from typing import Any

from loguru import logger
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from contracts.events import EventOutbox
from core.backend.database import engine

PublisherHandler = Callable[[dict[str, Any]], Awaitable[None]]


class OutboxPublisher:
    """Claim and publish durable messages with at-least-once semantics."""

    def __init__(
        self,
        handler: PublisherHandler,
        batch_size: int = 100,
        poll_interval: float = 1.0,
        stale_after_seconds: int = 300,
    ) -> None:
        if batch_size <= 0 or poll_interval <= 0 or stale_after_seconds <= 0:
            raise ValueError("publisher settings must be positive")
        self.handler = handler
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.stale_after_seconds = stale_after_seconds
        self._stop_event = asyncio.Event()

    def claim_pending(self) -> list[str]:
        """Claim pending messages and recover stale processing claims atomically."""
        now_ms = int(time() * 1000)
        stale_before = now_ms - self.stale_after_seconds * 1000
        with Session(engine) as session:
            session.execute(
                update(EventOutbox)
                .where(EventOutbox.status == "processing", EventOutbox.created_at < stale_before)
                .values(status="pending")
            )
            rows = session.execute(
                select(EventOutbox)
                .where(EventOutbox.status == "pending")
                .order_by(EventOutbox.created_at)
                .with_for_update(skip_locked=True)
                .limit(self.batch_size)
            ).scalars().all()
            ids = [row.id for row in rows]
            for row in rows:
                row.status = "processing"
                row.attempts += 1
            session.commit()
            return ids

    async def publish_batch(self) -> int:
        claimed_ids = self.claim_pending()
        published = 0
        for outbox_id in claimed_ids:
            with Session(engine) as session:
                row = session.get(EventOutbox, outbox_id)
                if row is None:
                    continue
                try:
                    await self.handler(row.payload)
                except Exception as exc:
                    row.status = "failed"
                    row.last_error = str(exc)
                    session.commit()
                    logger.bind(event_id=row.event_id, retry_count=row.attempts, pipeline_stage="outbox").error(
                        "Outbox publication failed: {}", exc
                    )
                    continue
                row.status = "published"
                row.published_at = int(time() * 1000)
                row.last_error = None
                session.commit()
                published += 1
        return published

    async def run_forever(self) -> None:
        while not self._stop_event.is_set():
            await self.publish_batch()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                continue

    def stop(self) -> None:
        self._stop_event.set()
