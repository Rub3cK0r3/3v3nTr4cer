import os
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from uuid import uuid4
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session
from contracts.base_model import Base
from contracts.events import Event,User,EventCreate,EventResponse
from contracts.alerts import Alert
from database import engine
# services defined in events_service.py
from events_service import (
    list_events as list_events_service,
    get_event as get_event_service,
    create_event as create_event_service,
)
# Database URL, JWT secret key, algorithm, and token expiry
SECRET_KEY = os.getenv("SECRET_KEY", "supersecretkey")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Initialize FastAPI
app = FastAPI()

def get_db():
    # Dependency: returns a database session and closes it automatically.
    with Session(engine) as session:
        yield session

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    # Verify that the provided password matches the hashed password.
    return pwd_context.verify(plain_password, hashed_password)

def get_user_by_username(db: Session, username: str) -> Optional[User]:
    # Fetch a user from the database by username.
    stmt = select(User).where(User.username == username)
    return db.execute(stmt).scalars().first()

def authenticate_user(db: Session, username: str, password: str) -> Optional[User]:
    # Authenticate a user by username and password.
    user = get_user_by_username(db, username)
    if not user:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    # Create a JWT token with an optional expiration.
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    # Dependency that returns the currently authenticated user from a JWT token.
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        user = get_user_by_username(db, username)
        if user is None:
            raise credentials_exception
        return user
    except JWTError:
        raise credentials_exception

@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    # Authenticate a user and return a JWT token.
    # Request: form data with username and password
    # Response: access_token and token_type
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    access_token = create_access_token(
        data={"sub": user.username},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/v1/events", response_model=List[EventResponse])
def list_events(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    events = list_events_service(db)
    return events

@app.get("/v1/events/{event_id}", response_model=EventResponse)
def get_event(event_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    event = get_event_service(db, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return event

@app.post("/v1/events", response_model=EventResponse)
def create_event(payload: EventCreate, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    event = create_event_service(db, payload)
    return event

# Internal pipeline contracts (no auth, used by async workers)
# This is part of the initial implementation and they are 
# structure decision I made throughout the time I started in this project
# and it's for the sake of data integrity , safety and so on and so forth 

class PipelineEventIn(BaseModel):
    id: Optional[str] = None
    app_name: Optional[str] = None
    type: str
    payload: Dict[str, Any]
    severity: Optional[str] = None
    timestamp: Optional[int] = None
    resource: Optional[str] = None
    referrer: Optional[str] = None

class PipelineAlertIn(BaseModel):
    id: Optional[str] = None
    severity: str
    resource: Optional[str] = None
    payload: Dict[str, Any] = {}


@app.post("/internal/pipeline/events")
def ingest_pipeline_event(payload: PipelineEventIn, db: Session = Depends(get_db)):
    # Internal endpoint used by async pipeline components to persist events.
    # It maps a minimal event structure into the richer Event model expected
    # by the main database schema.

    now_ms = int(datetime.utcnow().timestamp() * 1000)

    event_id = payload.id or payload.payload.get("id") or str(uuid4())
    app_name = payload.app_name or payload.payload.get("app_name") or "unknown-app"

    event = Event(
        id=event_id,
        severity=payload.severity or payload.payload.get("severity", "info"),
        stack=payload.payload.get("stack"),
        type=payload.type,
        timestamp=payload.timestamp or payload.payload.get("timestamp", now_ms),
        received_at=now_ms,
        resource=payload.resource or payload.payload.get("resource"),
        referrer=payload.referrer or payload.payload.get("referrer"),
        app_name=app_name,
        app_version=payload.payload.get("app_version"),
        app_stage=payload.payload.get("app_stage"),
        tags=payload.payload.get("tags"),
        endpoint_id=payload.payload.get("endpoint_id", "unknown-endpoint"),
        endpoint_language=payload.payload.get("endpoint_language"),
        endpoint_platform=payload.payload.get("endpoint_platform"),
        endpoint_os=payload.payload.get("endpoint_os"),
        endpoint_os_version=payload.payload.get("endpoint_os_version"),
        endpoint_runtime=payload.payload.get("endpoint_runtime"),
        endpoint_runtime_version=payload.payload.get("endpoint_runtime_version"),
        endpoint_country=payload.payload.get("endpoint_country"),
        endpoint_user_agent=payload.payload.get("endpoint_user_agent"),
        endpoint_device_type=payload.payload.get("endpoint_device_type"),
    )

    db.add(event)
    db.commit()
    db.refresh(event)

    return event


@app.post("/internal/pipeline/alerts")
def ingest_pipeline_alert(payload: PipelineAlertIn, db: Session = Depends(get_db)):
    # Internal endpoint used by async pipeline components to persist alerts.

    now_ms = int(datetime.utcnow().timestamp() * 1000)

    alert_id = payload.id or payload.payload.get("id") or str(uuid4())

    alert = Alert(
        id=alert_id,
        severity=payload.severity,
        resource=payload.resource or payload.payload.get("resource"),
        payload=payload.payload,
        created_at=now_ms,
    )

    db.add(alert)
    db.commit()
    db.refresh(alert)

    return alert

# Create database tables (development only)
Base.metadata.create_all(engine)
