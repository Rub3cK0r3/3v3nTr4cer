import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from core.async_lib.processor.main import EventProcessor

class TestEventProcessor(unittest.IsolatedAsyncioTestCase):
    @patch("core.async_lib.processor.main.httpx.AsyncClient")
    async def test_handle_posts_event_when_valid(self, mock_client_cls):
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_response = MagicMock(status_code=200)
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        processor = EventProcessor()
        event = {"id": "1", "app_name": "App", "type": "T", "payload": {}}

        await processor.handle(event)

        mock_client.post.assert_awaited_once()
        args, kwargs = mock_client.post.call_args
        self.assertEqual(args[0], "/internal/pipeline/events")
        self.assertEqual(kwargs["json"]["id"], "1")

    @patch("core.async_lib.processor.main.httpx.AsyncClient")
    async def test_handle_skips_invalid_event(self, mock_client_cls):
        mock_client_cls.return_value = AsyncMock()

        processor = EventProcessor()
        event = {"id": "2", "app_name": "App"}

        await processor.handle(event)

        mock_client_cls.return_value.post.assert_not_awaited()
