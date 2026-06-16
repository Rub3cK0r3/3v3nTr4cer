from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.orm import Session
from contracts.events import Event,EventCreate
from contracts.alerts import Alert,ALERT_SEVERITIES

def list_events(db: Session) -> List[Event]:
    # Retrieve all events from the database.
    stmt = select(Event)
    return db.execute(stmt).scalars().all()


def get_event(db: Session, event_id: str) -> Optional[Event]:
    # Retrieve a single event by its ID.
    stmt = select(Event).where(Event.id == event_id)
    return db.execute(stmt).scalars().first()


def create_event(db: Session, payload: EventCreate) -> Event:
    # Create a new event and, when applicable, persist a corresponding alert.
    event = Event(**payload.model_dump())
    db.add(event)

    if event.severity in ALERT_SEVERITIES:
        alert = Alert(
            id=event.id,
            severity=event.severity,
            resource=event.resource,
            payload=payload.model_dump(),
            created_at=event.received_at or event.timestamp,
        )
        db.add(alert)

    db.commit()
    db.refresh(event)
    return event
