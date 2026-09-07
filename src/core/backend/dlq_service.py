from sqlalchemy.orm import Session

from contracts.events import DeadLetterEvent


def create_dead_letter(db: Session, item: DeadLetterEvent) -> DeadLetterEvent:
    existing_item = db.get(DeadLetterEvent, item.id)
    if existing_item is not None:
        return existing_item
    db.add(item)
    db.commit()
    db.refresh(item)
    return item