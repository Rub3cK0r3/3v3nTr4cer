import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from core.async_lib.collector.main import AsyncCollector

class TestAsyncCollector(unittest.IsolatedAsyncioTestCase):
    async def test_insert_event_api_posts_event_payload(self):
        with patch("core.async_lib.collector.main.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_response = MagicMock(status_code=200)
            mock_response.raise_for_status = MagicMock()
            mock_client.post.return_value = mock_response
            mock_client_cls.return_value = mock_client

            collector = AsyncCollector(db_dsn="postgresql://user:pass@localhost:5432/testdb")
            event = {"id": "evt-1", "app_name": "svc", "type": "t", "payload": {"x": 1}}

            await collector._insert_event_api(event)

            mock_client.post.assert_awaited_once()
            args, kwargs = mock_client.post.call_args
            self.assertEqual(args[0], "/internal/pipeline/events")
            self.assertEqual(kwargs["json"]["payload"], event)

    async def test_notify_fastapi_sends_to_all_connections(self):
        class DummyWebSocket:
            def __init__(self):
                self.sent = []

            async def send_json(self, payload):
                self.sent.append(payload)

        app = MagicMock()
        app.active_connections = [DummyWebSocket(), DummyWebSocket()]
        collector = AsyncCollector(db_dsn="postgresql://user:pass@localhost:5432/testdb", fastapi_app=app)

        event = {"id": "evt-1", "app_name": "svc", "type": "t", "payload": {"x": 1}}
        await collector._notify_fastapi(event)

        self.assertEqual(len(app.active_connections[0].sent), 1)
        self.assertEqual(app.active_connections[0].sent[0], event)
        self.assertEqual(len(app.active_connections[1].sent), 1)

    async def test_notify_callback_rejects_invalid_json_payload(self):
        collector = AsyncCollector(db_dsn="postgresql://user:pass@localhost:5432/testdb")
        collector._validate_event = lambda event: True

        with patch("core.async_lib.collector.main.asyncio.create_task") as mock_create_task:
            collector._notify_callback(None, None, None, "not-json")
            mock_create_task.assert_not_called()

    async def test_notify_callback_rejects_invalid_event(self):
        collector = AsyncCollector(db_dsn="postgresql://user:pass@localhost:5432/testdb")
        collector._validate_event = lambda event: False

        with patch("core.async_lib.collector.main.asyncio.create_task") as mock_create_task:
            collector._notify_callback(None, None, None, json.dumps({"id": "evt"}))
            mock_create_task.assert_not_called()
