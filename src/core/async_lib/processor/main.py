import os
import asyncpg
import httpx
# Sanity checks in this frontier too...
from contracts.events import REQUIRED_EVENT_FIELDS
class EventProcessor: 
    # Handles business logic for processing events asynchronously.
    #
    # Responsibilities:
    #     - Validate incoming events.
    #     - Persist valid events via the backend API.
    #     - Encapsulate event-level business rules.
    #
    # This class does NOT manage:
    #     - Async workers
    #     - Queues
    #     - PostgreSQL listeners
    #     - Application lifecycle
    # 

    def __init__(self, db_pool: asyncpg.pool.Pool | None = None, backend_base_url: str | None = None): 
        # Initializes the EventProcessor.
        # Args:
        #     db_pool (asyncpg.pool.Pool | None): Kept for backwards compatibility but no longer used.
        #     backend_base_url (str | None): Base URL of the backend API.

        self.db_pool = db_pool
        self.backend_base_url = backend_base_url or os.getenv("BACKEND_BASE_URL", "http://backend:8000")

    async def handle(self, event: dict):
        # Main processing entry point used by AsyncManager workers.
        # Args:
        #     event (dict): Incoming event dictionary.
        if not self._validate_event(event):
            print("Invalid event, skipping:", event)
            return

        await self._insert_event(event)

    async def _insert_event(self, event: dict):
        # Persists an event by calling the internal backend API instead of
        # writing directly to PostgreSQL.
        try:
            async with httpx.AsyncClient(base_url=self.backend_base_url, timeout=5.0) as client:
                payload = {
                    "id": event["id"],
                    "app_name": event["app_name"],
                    "type": event["type"],
                    "payload": event,
                }
                response = await client.post("/internal/pipeline/events", json=payload)
                response.raise_for_status()

            print(f"Processed event via API: {event['id']}")

        except Exception as e:
            print(f"API write failed for event {event.get('id')}: {e}")

    def _validate_event(self, event: dict) -> bool:
        # Validates that an event contains all required fields.
        # Args:
        #     event (dict): Event to validate.
        # Returns:
        #     bool: True if valid, False otherwise.
        return all(field in event for field in REQUIRED_EVENT_FIELDS)
