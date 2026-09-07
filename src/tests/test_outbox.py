import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from contracts.events import Event, EventCreate, EventOutbox
from core.async_lib.outbox.publisher import OutboxPublisher
from core.backend import event_service


class TestEventOutboxTransaction(unittest.TestCase):
    def test_create_event_adds_event_and_outbox_together(self):
        db = MagicMock()
        db.get.return_value = None
        payload = EventCreate(
            id="event-atomic-1",
            severity="info",
            type="client.error",
            timestamp=1700000000000,
            app_name="demo",
            endpoint_id="endpoint-1",
        )

        event = event_service.create_event(db, payload)

        self.assertIsInstance(event, Event)
        added_objects = [call.args[0] for call in db.add.call_args_list]
        self.assertTrue(any(isinstance(item, Event) for item in added_objects))
        outbox_items = [item for item in added_objects if isinstance(item, EventOutbox)]
        self.assertEqual(len(outbox_items), 1)
        self.assertEqual(outbox_items[0].event_id, "event-atomic-1")
        self.assertEqual(outbox_items[0].status, "pending")
        self.assertEqual(outbox_items[0].payload["payload"]["id"], "event-atomic-1")
        db.commit.assert_called_once()


class TestOutboxPublisher(unittest.IsolatedAsyncioTestCase):
    async def test_publish_marks_message_published(self):
        row = SimpleNamespace(
            id="outbox-1", event_id="event-1", payload={"id": "event-1"}, attempts=1,
            status="processing", published_at=None, last_error="old",
        )
        session = MagicMock()
        session.get.return_value = row
        session_context = MagicMock()
        session_context.__enter__.return_value = session
        session_context.__exit__.return_value = None

        publisher = OutboxPublisher(AsyncMock(), poll_interval=0.01)
        publisher.claim_pending = MagicMock(return_value=["outbox-1"])
        with patch("core.async_lib.outbox.publisher.Session", return_value=session_context):
            self.assertEqual(await publisher.publish_batch(), 1)

        publisher.handler.assert_awaited_once_with({"id": "event-1"})
        self.assertEqual(row.status, "published")
        self.assertIsNotNone(row.published_at)
        session.commit.assert_called_once()

    async def test_publish_marks_message_failed(self):
        row = SimpleNamespace(
            id="outbox-2", event_id="event-2", payload={"id": "event-2"}, attempts=2,
            status="processing", published_at=None, last_error=None,
        )
        session = MagicMock()
        session.get.return_value = row
        session_context = MagicMock()
        session_context.__enter__.return_value = session
        session_context.__exit__.return_value = None

        handler = AsyncMock(side_effect=RuntimeError("consumer unavailable"))
        publisher = OutboxPublisher(handler, poll_interval=0.01)
        publisher.claim_pending = MagicMock(return_value=["outbox-2"])
        with patch("core.async_lib.outbox.publisher.Session", return_value=session_context):
            self.assertEqual(await publisher.publish_batch(), 0)

        self.assertEqual(row.status, "failed")
        self.assertEqual(row.last_error, "consumer unavailable")
        session.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
