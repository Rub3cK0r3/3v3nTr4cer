import asyncio
import json
import os
import signal
import asyncpg
import httpx
from contracts.events import REQUIRED_EVENT_FIELDS
# import AsyncManager so we need it as Base Class in our Implementation
# for better scalability
from core.async_lib.async_manager import AsyncManager
class AsyncCollector:
    # The AsyncCollector core class is a class that
    # handles the behaviour of a Data Collector before
    # data enters the exposure via the Backend APIs
    def __init__(self, db_dsn, fastapi_app=None, worker_count=4, max_queue_size=1000, backend_base_url: str | None = None):

        self.db_dsn = db_dsn
        self.db_pool = None
        self.listener_task = None

        self.async_manager = AsyncManager(
            worker_count=worker_count,
            max_queue_size=max_queue_size
        )

        # FastAPI (for WebSockets)
        self.fastapi_app = fastapi_app

        # Backend API base URL (persist events by service)
        self.backend_base_url = backend_base_url or os.getenv("BACKEND_BASE_URL", "http://backend:8000")

    async def start(self):
        #Initialize events DB pool, workers and listener 
        self.db_pool = await asyncpg.create_pool(dsn=self.db_dsn)

        # Iniciar workers (procesan eventos)
        await self.async_manager.start(self._process_event)

        # Lanzar listener PostgreSQL
        self.listener_task = asyncio.create_task(self._listener_loop())

    async def _listener_loop(self):
        # LISTENS channel 'events_channel' in PostgreSQL
        # Implement it so we can also use several db types
        while not self.async_manager.shutdown_event.is_set():
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.add_listener("events_channel", self._notify_callback)

                    while not self.async_manager.shutdown_event.is_set():
                        await asyncio.sleep(1)  # keep alive

            except Exception as e:
                print(f"LISTENER ERROR:{e}")
                await asyncio.sleep(2)

    def _notify_callback(self, connection, pid, channel, payload):
        # Is triggered when recieving NOTIFY from PostgreSQL
        try:
            event = json.loads(payload)

            if not self._validate_event(event):
                print(f"INVALID EVENT, skipping:{event}")
                return

            asyncio.create_task(self.async_manager.enqueue(event))

        except json.JSONDecodeError:
            print(f"INVALID JSON payload:{payload}")

    async def _process_event(self, event):
        # "Processes" an event: persists by API and notifies FastAPI WebSocket
        await self._insert_event_api(event)
        await self._notify_fastapi(event)

    async def _insert_event_api(self, event):
        # Makes an "insetion", calls to the endpoint which makes an insetion in the backend
        try:
            async with httpx.AsyncClient(base_url=self.backend_base_url, timeout=5.0) as client:
                payload = {
                    "type": event["type"],
                    "payload": event,
                }
                response = await client.post("/internal/pipeline/events", json=payload)
                response.raise_for_status()

        except Exception as e:
            print(f"API write failed for event:..", event, "Error:", e)

    async def _notify_fastapi(self, event):
        #Sends event everybody with WebSockets connected in FastAPI
        if not self.fastapi_app:
            return

        # La app debe tener lista de conexiones: active_connections
        for ws in getattr(self.fastapi_app, "active_connections", []):
            try:
                await ws.send_json(event)
            except Exception as e:
                print(f"Error enviando evento a websocket:{e}")

    # Minimal sanity validation done before the data to the backend...
    def _validate_event(self, event):
        return all(field in event for field in REQUIRED_EVENT_FIELDS)

    async def stop(self):
        #Shuts down listener, workers and DB pool

        await self.async_manager.stop()

        # Cancel it if listener is pending 
        if self.listener_task:
            self.listener_task.cancel()
            try:
                await self.listener_task
            except asyncio.CancelledError:
                pass

        # Close DB pool 
        await self.db_pool.close()
        print("Collector stopped gracefully.")


async def main():
    from fastapi import FastAPI

    app = FastAPI()
    app.active_connections = []  # WebSockets list for notifications 

    db_dsn = os.getenv("DATABASE_URL", "postgresql://devuser:devpass@db:5432/eventdb")

    collector = AsyncCollector(
        db_dsn=db_dsn,
        fastapi_app=app,
        worker_count=4,
    )

    await collector.start()

    # SIGINT/SIGTERM handling..
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()
    await collector.stop()


if __name__ == "__main__":
    asyncio.run(main())

