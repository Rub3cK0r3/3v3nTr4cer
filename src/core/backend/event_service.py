from __future__ import annotations

from time import time
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from contracts.alerts import ALERT_SEVERITIES, Alert
from contracts.events import Event, EventCreate, EventOutbox


def list_events(db: Session) -> list[Event]:
    return list(db.execute(select(Event)).scalars().all())


def get_event(db: Session, event_id: str) -> Event | None:
    return db.get(Event, event_id)


def create_event(db: Session, payload: EventCreate) -> Event:
    """Persist an event and its Outbox record in one transaction."""
    existing_event = db.get(Event, payload.id)
    if existing_event is not None:
        return existing_event

    event_data = payload.model_dump()
    event = Event(**event_data, received_at=event_data["timestamp"])
    outbox_payload = {
        "id": event.id,
        "app_name": event.app_name,
        "type": event.type or "event",
        "payload": event_data,
        "severity": event.severity,
        "timestamp": event.timestamp,
        "resource": event.resource,
        "referrer": event.referrer,
    }
    outbox = EventOutbox(
        id=str(uuid4()),
        event_id=event.id,
        event_type="event.created",
        payload=outbox_payload,
        status="pending",
        created_at=int(time() * 1000),
    )
    db.add(event)
    db.add(outbox)

    if event.severity in ALERT_SEVERITIES:
        db.add(
            Alert(
                id=event.id,
                severity=event.severity,
                resource=event.resource,
                payload=event_data,
                created_at=event.received_at,
            )
        )

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing_event = db.get(Event, event.id)
        if existing_event is None:
            raise
        return existing_event

    db.refresh(event)
    return event