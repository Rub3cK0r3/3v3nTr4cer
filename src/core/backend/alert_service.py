from sqlalchemy.orm import Session

from contracts.alerts import Alert


def create_alert(db: Session, alert: Alert) -> Alert:
    existing_alert = db.get(Alert, alert.id)
    if existing_alert is not None:
        return existing_alert
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return alert